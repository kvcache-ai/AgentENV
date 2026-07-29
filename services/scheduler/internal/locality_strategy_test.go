package scheduler

import (
	"fmt"
	"sync"
	"testing"

	schedulerv1 "agentenv/services/api/proto"
)

func TestGroupedLocalityInterleavesImageGroups(t *testing.T) {
	strategy := NewGroupedLocalityStrategy(LocalityGroupLimits{MaxSandboxCount: 2})
	nodes := readyNodes("a", "b", "c")

	got := []string{
		selectNodeID(t, strategy, nodes, coldHint("ubuntu", 0, 0)),
		selectNodeID(t, strategy, nodes, coldHint("python", 0, 0)),
		selectNodeID(t, strategy, nodes, coldHint("ubuntu", 0, 0)),
		selectNodeID(t, strategy, nodes, coldHint("python", 0, 0)),
		selectNodeID(t, strategy, nodes, coldHint("ubuntu", 0, 0)),
		selectNodeID(t, strategy, nodes, coldHint("python", 0, 0)),
	}
	want := []string{"a", "b", "a", "b", "c", "a"}
	if !equalNodeIDs(got, want) {
		t.Fatalf("placements = %v, want %v", got, want)
	}
}

func TestGroupedLocalityClosesGroupAtCPULimit(t *testing.T) {
	strategy := NewGroupedLocalityStrategy(LocalityGroupLimits{
		MaxSandboxCount: 10,
		MaxCPUCount:     4,
	})
	nodes := readyNodes("a", "b")

	got := []string{
		selectNodeID(t, strategy, nodes, coldHint("ubuntu", 3, 0)),
		selectNodeID(t, strategy, nodes, coldHint("ubuntu", 2, 0)),
		selectNodeID(t, strategy, nodes, coldHint("ubuntu", 2, 0)),
		selectNodeID(t, strategy, nodes, coldHint("ubuntu", 1, 0)),
	}
	want := []string{"a", "b", "b", "a"}
	if !equalNodeIDs(got, want) {
		t.Fatalf("placements = %v, want %v", got, want)
	}
}

func TestGroupedLocalityClosesGroupAtMemoryLimit(t *testing.T) {
	strategy := NewGroupedLocalityStrategy(LocalityGroupLimits{
		MaxSandboxCount: 10,
		MaxMemoryMB:     1024,
	})
	nodes := readyNodes("a", "b")

	got := []string{
		selectNodeID(t, strategy, nodes, coldHint("ubuntu", 0, 768)),
		selectNodeID(t, strategy, nodes, coldHint("ubuntu", 0, 512)),
		selectNodeID(t, strategy, nodes, coldHint("ubuntu", 0, 512)),
	}
	want := []string{"a", "b", "b"}
	if !equalNodeIDs(got, want) {
		t.Fatalf("placements = %v, want %v", got, want)
	}
}

func TestGroupedLocalityOversizedRequestDoesNotLeaveOpenGroup(t *testing.T) {
	strategy := NewGroupedLocalityStrategy(LocalityGroupLimits{
		MaxSandboxCount: 10,
		MaxCPUCount:     2,
	})
	nodes := readyNodes("a", "b")

	first := selectNodeID(t, strategy, nodes, coldHint("ubuntu", 4, 0))
	second := selectNodeID(t, strategy, nodes, coldHint("ubuntu", 4, 0))
	if first != "a" || second != "b" {
		t.Fatalf("oversized placements = %s %s, want a b", first, second)
	}
	if len(strategy.groups) != 0 {
		t.Fatalf("oversized requests left %d open groups, want 0", len(strategy.groups))
	}
}

func TestGroupedLocalityClosesGroupWhenNodeBecomesIneligible(t *testing.T) {
	strategy := NewGroupedLocalityStrategy(LocalityGroupLimits{MaxSandboxCount: 3})
	nodes := readyNodes("a", "b")

	if got := selectNodeID(t, strategy, nodes, coldHint("ubuntu", 0, 0)); got != "a" {
		t.Fatalf("first placement = %s, want a", got)
	}

	// The service removes resource-constrained nodes before calling Select.
	if got := selectNodeID(t, strategy, readyNodes("b", "c"), coldHint("ubuntu", 0, 0)); got != "b" {
		t.Fatalf("replacement placement = %s, want b", got)
	}
}

func TestGroupedLocalitySkipsNodesWithoutReadyHeartbeat(t *testing.T) {
	strategy := NewGroupedLocalityStrategy(LocalityGroupLimits{MaxSandboxCount: 2})
	nodes := []RichNode{
		{Node: Node{ID: "missing"}},
		{
			Node: Node{ID: "unhealthy"},
			Snapshot: &schedulerv1.NodeSnapshot{
				Status: schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY,
			},
		},
		readyNodes("ready")[0],
	}

	if got := selectNodeID(t, strategy, nodes, coldHint("ubuntu", 0, 0)); got != "ready" {
		t.Fatalf("placement = %s, want ready", got)
	}
}

func TestGroupedLocalityGroupsTemplatesByExactReference(t *testing.T) {
	strategy := NewGroupedLocalityStrategy(LocalityGroupLimits{MaxSandboxCount: 2})
	nodes := readyNodes("a", "b")

	got := []string{
		selectNodeID(t, strategy, nodes, templateHint("base")),
		selectNodeID(t, strategy, nodes, templateHint("alias")),
		selectNodeID(t, strategy, nodes, templateHint("base")),
		selectNodeID(t, strategy, nodes, templateHint("alias")),
		selectNodeID(t, strategy, nodes, templateHint("base")),
	}
	want := []string{"a", "b", "a", "b", "a"}
	if !equalNodeIDs(got, want) {
		t.Fatalf("placements = %v, want %v", got, want)
	}
}

func TestGroupedLocalityFallsBackToGlobalRoundRobin(t *testing.T) {
	strategy := NewGroupedLocalityStrategy(LocalityGroupLimits{MaxSandboxCount: 2})
	nodes := readyNodes("a", "b", "c")

	tooLong := coldHint("x", 0, 0)
	tooLong.GetNewColdSandbox().Images[0] = string(make([]byte, maxLocalityGroupKeyBytes+1))
	got := []string{
		selectNodeID(t, strategy, nodes, nil),
		selectNodeID(t, strategy, nodes, coldHint("", 0, 0)),
		selectNodeID(t, strategy, nodes, tooLong),
	}
	want := []string{"a", "b", "c"}
	if !equalNodeIDs(got, want) {
		t.Fatalf("fallback placements = %v, want %v", got, want)
	}
}

func TestGroupedLocalityFallbackDoesNotAdvanceGroupCursor(t *testing.T) {
	strategy := NewGroupedLocalityStrategy(LocalityGroupLimits{MaxSandboxCount: 2})
	nodes := readyNodes("a", "b")

	if got := selectNodeID(t, strategy, nodes, nil); got != "a" {
		t.Fatalf("fallback placement = %s, want a", got)
	}
	if got := selectNodeID(t, strategy, nodes, coldHint("ubuntu", 0, 0)); got != "a" {
		t.Fatalf("first group placement = %s, want a", got)
	}
}

func TestGroupedLocalityBoundsOpenGroupState(t *testing.T) {
	strategy := NewGroupedLocalityStrategy(LocalityGroupLimits{MaxSandboxCount: 2})
	nodes := readyNodes("a")

	for i := 0; i <= maxOpenLocalityGroups; i++ {
		selectNodeID(t, strategy, nodes, coldHint(fmt.Sprintf("image-%d", i), 0, 0))
	}
	if got := len(strategy.groups); got != maxOpenLocalityGroups {
		t.Fatalf("open group count = %d, want %d", got, maxOpenLocalityGroups)
	}
	if _, ok := strategy.groups["image:image-0"]; ok {
		t.Fatal("oldest open group was not evicted")
	}
}

func TestGroupedLocalityCountsConcurrentPlacementsAtomically(t *testing.T) {
	strategy := NewGroupedLocalityStrategy(LocalityGroupLimits{MaxSandboxCount: 10})
	nodes := readyNodes("a", "b")

	const requests = 200
	type result struct {
		nodeID string
		err    error
	}
	results := make(chan result, requests)
	var wg sync.WaitGroup
	wg.Add(requests)
	for i := 0; i < requests; i++ {
		go func() {
			defer wg.Done()
			node, err := strategy.Select(nodes, coldHint("ubuntu", 0, 0))
			results <- result{nodeID: node.ID, err: err}
		}()
	}
	wg.Wait()
	close(results)

	counts := map[string]int{}
	for result := range results {
		if result.err != nil {
			t.Fatalf("Select returned error: %v", result.err)
		}
		counts[result.nodeID]++
	}
	if counts["a"] != requests/2 || counts["b"] != requests/2 {
		t.Fatalf("concurrent placement counts = %v, want equal distribution", counts)
	}
}

func TestNewStrategySelectsLocalityCaseInsensitively(t *testing.T) {
	strategy := NewStrategy(
		" LOCALITY ",
		WithLocalityGroupLimits(LocalityGroupLimits{MaxSandboxCount: 2}),
	)
	if strategy.Name() != "locality" {
		t.Fatalf("strategy name = %q, want locality", strategy.Name())
	}
}

func readyNodes(ids ...string) []RichNode {
	nodes := make([]RichNode, 0, len(ids))
	for _, id := range ids {
		nodes = append(nodes, RichNode{
			Node: Node{ID: id, Endpoint: "http://" + id},
			Snapshot: &schedulerv1.NodeSnapshot{
				Status: schedulerv1.NodeStatus_NODE_STATUS_READY,
			},
		})
	}
	return nodes
}

func coldHint(image string, cpuCount uint32, memoryMB uint64) *schedulerv1.ScheduleRequestHint {
	images := []string(nil)
	if image != "" {
		images = []string{image}
	}
	return &schedulerv1.ScheduleRequestHint{
		Kind: &schedulerv1.ScheduleRequestHint_NewColdSandbox{
			NewColdSandbox: &schedulerv1.NewColdSandboxHint{
				Images:   images,
				CpuCount: cpuCount,
				MemoryMb: memoryMB,
			},
		},
	}
}

func templateHint(templateID string) *schedulerv1.ScheduleRequestHint {
	return &schedulerv1.ScheduleRequestHint{
		Kind: &schedulerv1.ScheduleRequestHint_NewSandbox{
			NewSandbox: &schedulerv1.NewSandboxHint{TemplateId: templateID},
		},
	}
}

func selectNodeID(
	t *testing.T,
	strategy Strategy,
	nodes []RichNode,
	hint *schedulerv1.ScheduleRequestHint,
) string {
	t.Helper()
	node, err := strategy.Select(nodes, hint)
	if err != nil {
		t.Fatalf("Select returned error: %v", err)
	}
	return node.ID
}

func equalNodeIDs(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
