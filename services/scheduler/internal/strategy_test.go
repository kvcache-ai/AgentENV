package scheduler

import (
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"google.golang.org/protobuf/proto"
)

func TestRoundRobin(t *testing.T) {
	s := &RoundRobinStrategy{}
	nodes := []RichNode{{Node: Node{ID: "a"}}, {Node: Node{ID: "b"}}, {Node: Node{ID: "c"}}}

	got1, _ := s.Select(nodes, nil)
	got2, _ := s.Select(nodes, nil)
	got3, _ := s.Select(nodes, nil)
	got4, _ := s.Select(nodes, nil)

	if got1.ID != "a" || got2.ID != "b" || got3.ID != "c" || got4.ID != "a" {
		t.Fatalf("unexpected order: %s %s %s %s", got1.ID, got2.ID, got3.ID, got4.ID)
	}
}

func TestRandomNoNodes(t *testing.T) {
	s := NewRandomStrategy()
	_, err := s.Select(nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestLeastLoadedNoNodes(t *testing.T) {
	s := &LeastLoadedStrategy{}
	_, err := s.Select(nil, nil)
	if !errors.Is(err, ErrNoNodes) {
		t.Fatalf("expected ErrNoNodes, got %v", err)
	}
}

func TestLeastLoadedUsesProjectedAllocation(t *testing.T) {
	s := &LeastLoadedStrategy{}
	nodes := []RichNode{
		richNode("small", 10, 1, 1_000, 100),
		richNode("large", 100, 20, 10_000, 2_000),
	}

	withoutHint, err := s.Select(nodes, nil)
	if err != nil {
		t.Fatal(err)
	}
	if withoutHint.ID != "small" {
		t.Fatalf("expected small node before projected request, got %s", withoutHint.ID)
	}

	hint := &schedulerv1.ScheduleRequestHint{
		Kind: &schedulerv1.ScheduleRequestHint_NewColdSandbox{
			NewColdSandbox: &schedulerv1.NewColdSandboxHint{
				CpuCount: 10,
				MemoryMb: 1,
			},
		},
	}
	withHint, err := s.Select(nodes, hint)
	if err != nil {
		t.Fatal(err)
	}
	if withHint.ID != "large" {
		t.Fatalf("expected large node after projected request, got %s", withHint.ID)
	}
}

func TestLeastLoadedUsesStartingAndSandboxCountsAsTieBreakers(t *testing.T) {
	s := &LeastLoadedStrategy{}
	a := richNode("a", 8, 4, 1_000, 500)
	b := richNode("b", 8, 4, 1_000, 500)
	a.Snapshot.SandboxStartingCount = 1
	b.Snapshot.SandboxCount = 1

	got, err := s.Select([]RichNode{a, b}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "b" {
		t.Fatalf("expected fewer-starting node b, got %s", got.ID)
	}
}

func TestLeastLoadedPrefersObservedCapacity(t *testing.T) {
	s := &LeastLoadedStrategy{}
	nodes := []RichNode{
		{Node: Node{ID: "unknown"}},
		richNode("observed", 8, 7, 1_000, 900),
	}

	got, err := s.Select(nodes, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "observed" {
		t.Fatalf("expected observed node, got %s", got.ID)
	}
}

func TestLeastLoadedRequiresCapacityForRequestedResources(t *testing.T) {
	s := &LeastLoadedStrategy{}
	fullyObserved := richNode("fully-observed", 8, 7, 1_000, 900)

	missingMemory := richNode("missing-memory", 8, 1, 0, 0)
	memoryHint := &schedulerv1.ScheduleRequestHint{
		Kind: &schedulerv1.ScheduleRequestHint_NewColdSandbox{
			NewColdSandbox: &schedulerv1.NewColdSandboxHint{MemoryMb: 1},
		},
	}
	got, err := s.Select([]RichNode{missingMemory, fullyObserved}, memoryHint)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != fullyObserved.ID {
		t.Fatalf("expected node with observed memory capacity, got %s", got.ID)
	}

	missingCPU := richNode("missing-cpu", 0, 0, 1_000, 100)
	cpuHint := &schedulerv1.ScheduleRequestHint{
		Kind: &schedulerv1.ScheduleRequestHint_NewColdSandbox{
			NewColdSandbox: &schedulerv1.NewColdSandboxHint{CpuCount: 1},
		},
	}
	got, err = s.Select([]RichNode{missingCPU, fullyObserved}, cpuHint)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != fullyObserved.ID {
		t.Fatalf("expected node with observed CPU capacity, got %s", got.ID)
	}
}

func TestLeastLoadedRoundRobinsEqualCandidates(t *testing.T) {
	s := &LeastLoadedStrategy{}
	nodes := []RichNode{
		richNode("a", 8, 4, 1_000, 500),
		richNode("b", 8, 4, 1_000, 500),
	}

	got1, _ := s.Select(nodes, nil)
	got2, _ := s.Select(nodes, nil)
	got3, _ := s.Select(nodes, nil)
	if got1.ID != "a" || got2.ID != "b" || got3.ID != "a" {
		t.Fatalf("unexpected tie order: %s %s %s", got1.ID, got2.ID, got3.ID)
	}
}

func TestLeastLoadedReservesConcurrentSelections(t *testing.T) {
	s := &LeastLoadedStrategy{}
	nodes := []RichNode{
		richNode("a", 8, 0, 8_000, 0),
		richNode("b", 8, 0, 8_000, 0),
	}
	hint := &schedulerv1.ScheduleRequestHint{
		Kind: &schedulerv1.ScheduleRequestHint_NewColdSandbox{
			NewColdSandbox: &schedulerv1.NewColdSandboxHint{CpuCount: 1},
		},
	}

	const requests = 64
	start := make(chan struct{})
	counts := map[string]int{}
	var countsMu sync.Mutex
	var wg sync.WaitGroup
	for range requests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			selected, err := s.Select(nodes, hint)
			if err != nil {
				t.Errorf("select node: %v", err)
				return
			}
			countsMu.Lock()
			counts[selected.ID]++
			countsMu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	if counts["a"] != requests/2 || counts["b"] != requests/2 {
		t.Fatalf("expected balanced reservations, got a=%d b=%d", counts["a"], counts["b"])
	}
}

func TestLeastLoadedExpiresFailedReservations(t *testing.T) {
	now := time.Unix(100, 0)
	s := &LeastLoadedStrategy{
		now:            func() time.Time { return now },
		reservationTTL: time.Second,
	}
	nodes := []RichNode{
		richNode("a", 8, 0, 8_000, 0),
		richNode("b", 8, 4, 8_000, 4_000),
	}
	hint := &schedulerv1.ScheduleRequestHint{
		Kind: &schedulerv1.ScheduleRequestHint_NewColdSandbox{
			NewColdSandbox: &schedulerv1.NewColdSandboxHint{CpuCount: 4},
		},
	}

	first, _ := s.Select(nodes, hint)
	second, _ := s.Select(nodes, hint)
	if first.ID != "a" || second.ID != "b" {
		t.Fatalf("expected pending reservation to shift selection, got %s then %s", first.ID, second.ID)
	}

	now = now.Add(2 * time.Second)
	afterExpiry, _ := s.Select(nodes, hint)
	if afterExpiry.ID != "a" {
		t.Fatalf("expected expired reservations to be ignored, got %s", afterExpiry.ID)
	}
}

func TestLeastLoadedReconcilesReservationsFromHeartbeat(t *testing.T) {
	now := time.Unix(100, 0)
	s := &LeastLoadedStrategy{now: func() time.Time { return now }}
	node := richNode("a", 8, 0, 8_000, 0)
	node.Snapshot.ReportedAtUnixMs = 1
	hint := &schedulerv1.ScheduleRequestHint{
		Kind: &schedulerv1.ScheduleRequestHint_NewColdSandbox{
			NewColdSandbox: &schedulerv1.NewColdSandboxHint{CpuCount: 1},
		},
	}

	if _, err := s.Select([]RichNode{node}, hint); err != nil {
		t.Fatal(err)
	}
	if got := len(s.pendingByNode["a"].items); got != 1 {
		t.Fatalf("expected one pending reservation, got %d", got)
	}

	node.Snapshot.ReportedAtUnixMs = 2
	node.Snapshot.SandboxStartingCount = 1
	node.Snapshot.AllocatedCpu = 1
	if pending := s.pendingResources(node); pending.count != 0 {
		t.Fatalf("expected heartbeat to consume reservation, got %d", pending.count)
	}
	if _, exists := s.pendingByNode["a"]; exists {
		t.Fatal("expected reconciled reservation state to be removed")
	}
}

func TestLeastLoadedKeepsResourcesReservedWhileSandboxIsStarting(t *testing.T) {
	now := time.Unix(100, 0)
	s := &LeastLoadedStrategy{now: func() time.Time { return now }}
	node := richNode("a", 8, 0, 8_000, 0)
	node.Snapshot.ReportedAtUnixMs = 1
	hint := &schedulerv1.ScheduleRequestHint{
		Kind: &schedulerv1.ScheduleRequestHint_NewColdSandbox{
			NewColdSandbox: &schedulerv1.NewColdSandboxHint{
				CpuCount: 2,
				MemoryMb: 1,
			},
		},
	}

	if _, err := s.Select([]RichNode{node}, hint); err != nil {
		t.Fatal(err)
	}

	node.Snapshot.ReportedAtUnixMs = 2
	node.Snapshot.SandboxStartingCount = 1
	pending := s.pendingResources(node)
	if pending.count != 0 {
		t.Fatalf("expected starting sandbox count to acknowledge pending count, got %d", pending.count)
	}
	if pending.cpu != 2 || pending.memoryBytes != 1024*1024 {
		t.Fatalf(
			"expected resources to remain reserved while starting, got cpu=%v memory=%v",
			pending.cpu,
			pending.memoryBytes,
		)
	}

	node.Snapshot.ReportedAtUnixMs = 3
	node.Snapshot.SandboxStartingCount = 0
	node.Snapshot.SandboxCount = 1
	node.Snapshot.AllocatedCpu = 2
	node.Snapshot.AllocatedMemoryBytes = 1024 * 1024
	if pending = s.pendingResources(node); pending != (pendingResources{}) {
		t.Fatalf("expected running allocation to reconcile resources, got %+v", pending)
	}
	if _, exists := s.pendingByNode["a"]; exists {
		t.Fatal("expected fully reconciled reservation state to be removed")
	}
}

func TestLeastLoadedMatchesResourceDeltasInsteadOfConsumingFIFO(t *testing.T) {
	now := time.Unix(100, 0)
	s := &LeastLoadedStrategy{now: func() time.Time { return now }}
	node := richNode("a", 8, 0, 8_000, 0)
	node.Snapshot.ReportedAtUnixMs = 1
	largeHint := &schedulerv1.ScheduleRequestHint{
		Kind: &schedulerv1.ScheduleRequestHint_NewColdSandbox{
			NewColdSandbox: &schedulerv1.NewColdSandboxHint{CpuCount: 4},
		},
	}
	smallHint := &schedulerv1.ScheduleRequestHint{
		Kind: &schedulerv1.ScheduleRequestHint_NewColdSandbox{
			NewColdSandbox: &schedulerv1.NewColdSandboxHint{CpuCount: 1},
		},
	}

	if _, err := s.Select([]RichNode{node}, largeHint); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Select([]RichNode{node}, smallHint); err != nil {
		t.Fatal(err)
	}

	node.Snapshot.ReportedAtUnixMs = 2
	node.Snapshot.SandboxCount = 1
	pending := s.pendingResources(node)
	if pending.cpu != 5 || pending.count != 1 {
		t.Fatalf("expected starting allocation to keep both resource reservations, got %+v", pending)
	}

	node.Snapshot.ReportedAtUnixMs = 3
	node.Snapshot.AllocatedCpu = 1
	pending = s.pendingResources(node)
	if pending.cpu != 4 {
		t.Fatalf("expected 1-CPU reservation to be reconciled, got pending CPU %v", pending.cpu)
	}
	if pending.count != 1 {
		t.Fatalf("expected one sandbox count reservation to remain, got %d", pending.count)
	}
}

func TestNewStrategyLeastLoaded(t *testing.T) {
	if got := NewStrategy(" LEAST_LOADED ").Name(); got != "least_loaded" {
		t.Fatalf("expected least_loaded, got %s", got)
	}
}

func TestLeastLoadedReducesPeakPressureOnHeterogeneousNodes(t *testing.T) {
	nodes := []RichNode{
		richNode("small", 16, 0, 32*1024*1024*1024, 0),
		richNode("medium", 64, 0, 128*1024*1024*1024, 0),
		richNode("large", 192, 0, 384*1024*1024*1024, 0),
	}
	hint := &schedulerv1.ScheduleRequestHint{
		Kind: &schedulerv1.ScheduleRequestHint_NewColdSandbox{
			NewColdSandbox: &schedulerv1.NewColdSandboxHint{
				CpuCount: 2,
				MemoryMb: 4096,
			},
		},
	}

	roundRobinNodes := replayAssignments(t, &RoundRobinStrategy{}, nodes, hint, 300)
	leastLoadedNodes := replayAssignments(t, &LeastLoadedStrategy{}, nodes, hint, 300)
	roundRobinPeak := peakPressure(roundRobinNodes)
	leastLoadedPeak := peakPressure(leastLoadedNodes)

	t.Logf("peak allocation pressure: round_robin=%.4f least_loaded=%.4f", roundRobinPeak, leastLoadedPeak)
	if leastLoadedPeak >= roundRobinPeak/2 {
		t.Fatalf(
			"expected least_loaded peak pressure below half of round_robin: round_robin=%.4f least_loaded=%.4f",
			roundRobinPeak,
			leastLoadedPeak,
		)
	}
}

func BenchmarkRoundRobinSelect1000Nodes(b *testing.B) {
	nodes := benchmarkNodes()
	s := &RoundRobinStrategy{}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := s.Select(nodes, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLeastLoadedSelect1000Nodes(b *testing.B) {
	nodes := benchmarkNodes()
	s := &LeastLoadedStrategy{}
	hint := benchmarkHint()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := s.Select(nodes, hint); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkNodes() []RichNode {
	nodes := make([]RichNode, 1000)
	for i := range nodes {
		nodes[i] = richNode(
			"node-"+strconv.Itoa(i),
			192,
			uint32(i%160),
			2*1024*1024*1024*1024,
			uint64(i%1800)*1024*1024*1024,
		)
	}
	return nodes
}

func benchmarkHint() *schedulerv1.ScheduleRequestHint {
	return &schedulerv1.ScheduleRequestHint{
		Kind: &schedulerv1.ScheduleRequestHint_NewColdSandbox{
			NewColdSandbox: &schedulerv1.NewColdSandboxHint{
				CpuCount: 2,
				MemoryMb: 2048,
			},
		},
	}
}

func replayAssignments(
	t *testing.T,
	strategy Strategy,
	initial []RichNode,
	hint *schedulerv1.ScheduleRequestHint,
	count int,
) []RichNode {
	t.Helper()
	nodes := cloneRichNodes(initial)
	cold := hint.GetNewColdSandbox()
	for range count {
		selected, err := strategy.Select(nodes, hint)
		if err != nil {
			t.Fatal(err)
		}
		for i := range nodes {
			if nodes[i].ID != selected.ID {
				continue
			}
			nodes[i].Snapshot.AllocatedCpu += cold.GetCpuCount()
			nodes[i].Snapshot.AllocatedMemoryBytes += cold.GetMemoryMb() * 1024 * 1024
			nodes[i].Snapshot.SandboxCount++
			break
		}
	}
	return nodes
}

func cloneRichNodes(nodes []RichNode) []RichNode {
	cloned := make([]RichNode, len(nodes))
	for i, node := range nodes {
		cloned[i] = node
		if node.Snapshot != nil {
			cloned[i].Snapshot = proto.Clone(node.Snapshot).(*schedulerv1.NodeSnapshot)
		}
	}
	return cloned
}

func peakPressure(nodes []RichNode) float64 {
	peak := 0.0
	for _, node := range nodes {
		load := projectedNodeLoad(node.Snapshot, 0, 0)
		if load.pressure > peak {
			peak = load.pressure
		}
	}
	return peak
}

func richNode(id string, cpuCount, allocatedCPU uint32, memoryTotal, allocatedMemory uint64) RichNode {
	return RichNode{
		Node: Node{ID: id},
		Snapshot: &schedulerv1.NodeSnapshot{
			CpuCount:             cpuCount,
			AllocatedCpu:         allocatedCPU,
			MemoryTotalBytes:     memoryTotal,
			AllocatedMemoryBytes: allocatedMemory,
		},
	}
}
