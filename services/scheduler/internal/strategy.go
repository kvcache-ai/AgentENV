package scheduler

import (
	"errors"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"

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
// usable heartbeat snapshot. Equal candidates are distributed round-robin to
// avoid concentrating requests between heartbeat updates.
type LeastLoadedStrategy struct {
	next uint64
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

	requestedCPU, requestedMemoryBytes := requestedResources(hint)
	best := nodeLoad{}
	bestNode := nodes[0]
	bestFound := false
	bestCount := 0
	for _, node := range nodes {
		load := projectedNodeLoad(node.Snapshot, requestedCPU, requestedMemoryBytes)
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

	target := (atomic.AddUint64(&s.next, 1) - 1) % uint64(bestCount)
	for _, node := range nodes {
		load := projectedNodeLoad(node.Snapshot, requestedCPU, requestedMemoryBytes)
		if !equalLoad(load, best) {
			continue
		}
		if target == 0 {
			return node, nil
		}
		target--
	}

	return bestNode, nil
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
