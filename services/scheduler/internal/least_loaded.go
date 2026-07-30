package scheduler

import (
	"math"
	"sync"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

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

const defaultScheduleReservationTTL = 10 * time.Minute

type pendingReservation struct {
	cpu                   float64
	memoryBytes           float64
	countAcknowledged     bool
	resourcesAcknowledged bool
	expiresAt             time.Time
}

type pendingNodeReservations struct {
	items                   []pendingReservation
	cpu                     float64
	memoryBytes             float64
	pendingCount            uint32
	observedGeneration      uint64
	observedCreateSuccesses uint64
	observedCreateFails     uint64
	observedAllocatedCPU    uint32
	observedAllocatedMemory uint64
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
			if isSandboxCreation(hint) {
				s.reserve(node, requestedCPU, requestedMemoryBytes, now)
			}
			return node, nil
		}
		target--
	}

	if isSandboxCreation(hint) {
		s.reserve(bestNode, requestedCPU, requestedMemoryBytes, now)
	}
	return bestNode, nil
}

func (s *LeastLoadedStrategy) pendingResources(node RichNode) pendingResources {
	state := s.pendingByNode[node.ID]
	if state == nil {
		return pendingResources{}
	}

	if snapshot := node.Snapshot; snapshot != nil {
		if node.SnapshotGeneration > state.observedGeneration {
			if snapshot.GetCreateFails() > state.observedCreateFails {
				state.acknowledgeFailures(
					uint32(min(
						snapshot.GetCreateFails()-state.observedCreateFails,
						uint64(^uint32(0)),
					)),
				)
			}
			if snapshot.GetCreateSuccesses() > state.observedCreateSuccesses {
				state.acknowledgeCounts(
					uint32(min(
						snapshot.GetCreateSuccesses()-state.observedCreateSuccesses,
						uint64(^uint32(0)),
					)),
				)
			}
			cpuDelta := positiveUint32Delta(
				snapshot.GetAllocatedCpu(),
				state.observedAllocatedCPU,
			)
			memoryDelta := positiveUint64Delta(
				snapshot.GetAllocatedMemoryBytes(),
				state.observedAllocatedMemory,
			)
			state.acknowledgeResources(float64(cpuDelta), float64(memoryDelta))
			state.compactAcknowledged()
			state.observedGeneration = node.SnapshotGeneration
			state.observedCreateSuccesses = snapshot.GetCreateSuccesses()
			state.observedCreateFails = snapshot.GetCreateFails()
			state.observedAllocatedCPU = snapshot.GetAllocatedCpu()
			state.observedAllocatedMemory = snapshot.GetAllocatedMemoryBytes()
		}
	}

	if len(state.items) == 0 && state.pendingCount == 0 {
		delete(s.pendingByNode, node.ID)
		return pendingResources{}
	}
	return pendingResources{
		cpu:         state.cpu,
		memoryBytes: state.memoryBytes,
		count:       state.pendingCount,
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
			state.observedGeneration = node.SnapshotGeneration
			state.observedCreateSuccesses = snapshot.GetCreateSuccesses()
			state.observedCreateFails = snapshot.GetCreateFails()
			state.observedAllocatedCPU = snapshot.GetAllocatedCpu()
			state.observedAllocatedMemory = snapshot.GetAllocatedMemoryBytes()
		}
		s.pendingByNode[node.ID] = state
	}
	ttl := s.reservationTTL
	if ttl <= 0 {
		ttl = defaultScheduleReservationTTL
	}
	state.items = append(state.items, pendingReservation{
		cpu:                   cpu,
		memoryBytes:           memoryBytes,
		resourcesAcknowledged: cpu == 0 && memoryBytes == 0,
		expiresAt:             now.Add(ttl),
	})
	state.cpu += cpu
	state.memoryBytes += memoryBytes
	state.pendingCount++
}

func (s *pendingNodeReservations) pruneExpired(now time.Time) {
	expired := 0
	for expired < len(s.items) && !s.items[expired].expiresAt.After(now) {
		reservation := s.items[expired]
		if !reservation.resourcesAcknowledged {
			s.cpu -= reservation.cpu
			s.memoryBytes -= reservation.memoryBytes
		}
		if !reservation.countAcknowledged {
			s.pendingCount--
		}
		expired++
	}
	s.items = s.items[expired:]
}

func (s *pendingNodeReservations) acknowledgeCounts(count uint32) {
	for i := range s.items {
		if count == 0 {
			return
		}
		if s.items[i].countAcknowledged {
			continue
		}
		s.items[i].countAcknowledged = true
		s.pendingCount--
		count--
	}
}

func (s *pendingNodeReservations) acknowledgeFailures(count uint32) {
	if count == 0 {
		return
	}
	kept := s.items[:0]
	for _, item := range s.items {
		if count > 0 && !item.countAcknowledged {
			if !item.resourcesAcknowledged {
				s.cpu -= item.cpu
				s.memoryBytes -= item.memoryBytes
			}
			s.pendingCount--
			count--
			continue
		}
		kept = append(kept, item)
	}
	s.items = kept
}

func positiveUint32Delta(current, previous uint32) uint32 {
	if current <= previous {
		return 0
	}
	return current - previous
}

func positiveUint64Delta(current, previous uint64) uint64 {
	if current <= previous {
		return 0
	}
	return current - previous
}

func (s *pendingNodeReservations) acknowledgeResources(cpuCredit, memoryCredit float64) {
	for i := range s.items {
		item := &s.items[i]
		if !item.countAcknowledged ||
			item.resourcesAcknowledged ||
			item.cpu > cpuCredit ||
			item.memoryBytes > memoryCredit {
			continue
		}
		item.resourcesAcknowledged = true
		s.cpu -= item.cpu
		s.memoryBytes -= item.memoryBytes
		cpuCredit -= item.cpu
		memoryCredit -= item.memoryBytes
	}
}

func (s *pendingNodeReservations) compactAcknowledged() {
	kept := s.items[:0]
	for _, item := range s.items {
		if !item.countAcknowledged || !item.resourcesAcknowledged {
			kept = append(kept, item)
		}
	}
	s.items = kept
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

func isSandboxCreation(hint *schedulerv1.ScheduleRequestHint) bool {
	switch hint.GetKind().(type) {
	case *schedulerv1.ScheduleRequestHint_NewColdSandbox,
		*schedulerv1.ScheduleRequestHint_NewSandbox:
		return true
	default:
		return false
	}
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
