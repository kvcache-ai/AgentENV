package scheduler

import (
	"errors"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

var ErrNoNodes = errors.New("no nodes available")

type Strategy interface {
	Select(nodes []RichNode, hint *schedulerv1.ScheduleRequestHint) (RichNode, error)
	Name() string
}

type RoundRobinStrategy struct {
	next uint64
}

func (s *RoundRobinStrategy) Select(nodes []RichNode, _ *schedulerv1.ScheduleRequestHint) (RichNode, error) {
	if len(nodes) == 0 {
		return RichNode{}, ErrNoNodes
	}
	idx := atomic.AddUint64(&s.next, 1)
	return nodes[(idx-1)%uint64(len(nodes))], nil
}

func (s *RoundRobinStrategy) Name() string {
	return "round_robin"
}

type RandomStrategy struct{}

func NewRandomStrategy() *RandomStrategy {
	return &RandomStrategy{}
}

func (s *RandomStrategy) Select(nodes []RichNode, _ *schedulerv1.ScheduleRequestHint) (RichNode, error) {
	if len(nodes) == 0 {
		return RichNode{}, ErrNoNodes
	}
	return nodes[rand.Intn(len(nodes))], nil
}

func (s *RandomStrategy) Name() string {
	return "random"
}

// LeastLoadedStrategy selects the node with the lowest projected allocation
// pressure. Nodes with observed capacity are preferred over nodes without a
// usable heartbeat snapshot. Short-lived reservations make selection and
// projected accounting atomic, preventing concurrent requests from all seeing
// the same stale heartbeat as least loaded.
type LeastLoadedStrategy struct {
	mu             sync.Mutex
	next           uint64
	pendingByNode  map[string]*pendingNodeReservations
	now            func() time.Time
	reservationTTL time.Duration
}

const defaultScheduleReservationTTL = 30 * time.Second

type pendingReservation struct {
	cpu         float64
	memoryBytes float64
	expiresAt   time.Time
}

type pendingNodeReservations struct {
	items             []pendingReservation
	cpu               float64
	memoryBytes       float64
	reportedAtUnixMs  int64
	observedSandboxes uint32
}

type pendingResources struct {
	cpu         float64
	memoryBytes float64
	count       uint32
}

type nodeLoad struct {
	known    bool
	pressure float64
	starting uint32
	running  uint32
	paused   uint32
}

func (s *LeastLoadedStrategy) Select(nodes []RichNode, hint *schedulerv1.ScheduleRequestHint) (RichNode, error) {
	if len(nodes) == 0 {
		return RichNode{}, ErrNoNodes
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	requestedCPU, requestedMemoryBytes := requestedResources(hint)
	now := time.Now()
	if s.now != nil {
		now = s.now()
	}
	for nodeID, state := range s.pendingByNode {
		state.pruneExpired(now)
		if len(state.items) == 0 {
			delete(s.pendingByNode, nodeID)
		}
	}

	best := nodeLoad{}
	bestNode := nodes[0]
	bestFound := false
	bestCount := 0
	for _, node := range nodes {
		reserved := s.pendingResources(node)
		load := projectedNodeLoad(
			node.Snapshot,
			requestedCPU+reserved.cpu,
			requestedMemoryBytes+reserved.memoryBytes,
		)
		load.starting += reserved.count
		if !bestFound || lessLoaded(load, best) {
			best = load
			bestNode = node
			bestFound = true
			bestCount = 1
			continue
		}
		if equalLoad(load, best) {
			bestCount++
		}
	}

	target := s.next % uint64(bestCount)
	s.next++
	for _, node := range nodes {
		reserved := s.pendingResources(node)
		load := projectedNodeLoad(
			node.Snapshot,
			requestedCPU+reserved.cpu,
			requestedMemoryBytes+reserved.memoryBytes,
		)
		load.starting += reserved.count
		if !equalLoad(load, best) {
			continue
		}
		if target == 0 {
			s.reserve(node, requestedCPU, requestedMemoryBytes, now)
			return node, nil
		}
		target--
	}

	s.reserve(bestNode, requestedCPU, requestedMemoryBytes, now)
	return bestNode, nil
}

func (s *LeastLoadedStrategy) pendingResources(node RichNode) pendingResources {
	state := s.pendingByNode[node.ID]
	if state == nil {
		return pendingResources{}
	}

	if snapshot := node.Snapshot; snapshot != nil {
		reportedAt := snapshot.GetReportedAtUnixMs()
		observedSandboxes := snapshot.GetSandboxStartingCount() +
			snapshot.GetSandboxCount() +
			snapshot.GetPausedSandboxCount()
		if reportedAt > state.reportedAtUnixMs {
			if observedSandboxes > state.observedSandboxes {
				state.consume(observedSandboxes - state.observedSandboxes)
			}
			state.reportedAtUnixMs = reportedAt
			state.observedSandboxes = observedSandboxes
		}
	}

	if len(state.items) == 0 {
		delete(s.pendingByNode, node.ID)
		return pendingResources{}
	}
	return pendingResources{
		cpu:         state.cpu,
		memoryBytes: state.memoryBytes,
		count:       uint32(len(state.items)),
	}
}

func (s *LeastLoadedStrategy) reserve(
	node RichNode,
	cpu float64,
	memoryBytes float64,
	now time.Time,
) {
	if s.pendingByNode == nil {
		s.pendingByNode = make(map[string]*pendingNodeReservations)
	}
	state := s.pendingByNode[node.ID]
	if state == nil {
		state = &pendingNodeReservations{}
		if snapshot := node.Snapshot; snapshot != nil {
			state.reportedAtUnixMs = snapshot.GetReportedAtUnixMs()
			state.observedSandboxes = snapshot.GetSandboxStartingCount() +
				snapshot.GetSandboxCount() +
				snapshot.GetPausedSandboxCount()
		}
		s.pendingByNode[node.ID] = state
	}
	ttl := s.reservationTTL
	if ttl <= 0 {
		ttl = defaultScheduleReservationTTL
	}
	state.items = append(state.items, pendingReservation{
		cpu:         cpu,
		memoryBytes: memoryBytes,
		expiresAt:   now.Add(ttl),
	})
	state.cpu += cpu
	state.memoryBytes += memoryBytes
}

func (s *pendingNodeReservations) pruneExpired(now time.Time) {
	expired := 0
	for expired < len(s.items) && !s.items[expired].expiresAt.After(now) {
		s.cpu -= s.items[expired].cpu
		s.memoryBytes -= s.items[expired].memoryBytes
		expired++
	}
	s.items = s.items[expired:]
}

func (s *pendingNodeReservations) consume(count uint32) {
	consume := int(count)
	if consume > len(s.items) {
		consume = len(s.items)
	}
	for _, reservation := range s.items[:consume] {
		s.cpu -= reservation.cpu
		s.memoryBytes -= reservation.memoryBytes
	}
	s.items = s.items[consume:]
}

func (s *LeastLoadedStrategy) Name() string {
	return "least_loaded"
}

func projectedNodeLoad(snapshot *schedulerv1.NodeSnapshot, requestedCPU, requestedMemoryBytes float64) nodeLoad {
	if snapshot == nil {
		return nodeLoad{}
	}
	if (requestedCPU > 0 && snapshot.GetCpuCount() == 0) ||
		(requestedMemoryBytes > 0 && snapshot.GetMemoryTotalBytes() == 0) {
		return nodeLoad{}
	}

	pressure := 0.0
	known := false
	if snapshot.GetCpuCount() > 0 {
		known = true
		pressure = math.Max(
			pressure,
			(float64(snapshot.GetAllocatedCpu())+requestedCPU)/float64(snapshot.GetCpuCount()),
		)
	}
	if snapshot.GetMemoryTotalBytes() > 0 {
		known = true
		pressure = math.Max(
			pressure,
			(float64(snapshot.GetAllocatedMemoryBytes())+requestedMemoryBytes)/
				float64(snapshot.GetMemoryTotalBytes()),
		)
	}

	return nodeLoad{
		known:    known,
		pressure: pressure,
		starting: snapshot.GetSandboxStartingCount(),
		running:  snapshot.GetSandboxCount(),
		paused:   snapshot.GetPausedSandboxCount(),
	}
}

func requestedResources(hint *schedulerv1.ScheduleRequestHint) (cpu, memoryBytes float64) {
	cold := hint.GetNewColdSandbox()
	if cold == nil {
		return 0, 0
	}
	return float64(cold.GetCpuCount()), float64(cold.GetMemoryMb()) * 1024 * 1024
}

func lessLoaded(a, b nodeLoad) bool {
	if a.known != b.known {
		return a.known
	}
	if a.pressure != b.pressure {
		return a.pressure < b.pressure
	}
	if a.starting != b.starting {
		return a.starting < b.starting
	}
	if a.running != b.running {
		return a.running < b.running
	}
	return a.paused < b.paused
}

func equalLoad(a, b nodeLoad) bool {
	return a.known == b.known &&
		a.pressure == b.pressure &&
		a.starting == b.starting &&
		a.running == b.running &&
		a.paused == b.paused
}

func NewStrategy(name string) Strategy {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "random":
		return NewRandomStrategy()
	case "least_loaded":
		return &LeastLoadedStrategy{}
	case "round_robin":
		fallthrough
	default:
		return &RoundRobinStrategy{}
	}
}
