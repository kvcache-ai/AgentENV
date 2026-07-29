package scheduler

import (
	"container/list"
	"errors"
	"math/rand"
	"sort"
	"strings"
	"sync"
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

// LocalityGroupLimits bounds a same-workload placement group. Zero CPU and
// memory limits disable those checks; MaxSandboxCount is always enforced.
type LocalityGroupLimits struct {
	MaxSandboxCount uint32
	MaxCPUCount     uint32
	MaxMemoryMB     uint64
}

type localityRequest struct {
	key      string
	cpuCount uint32
	memoryMB uint64
}

type localityGroup struct {
	nodeID       string
	sandboxCount uint32
	cpuCount     uint64
	memoryMB     uint64
}

type localityGroupEntry struct {
	key   string
	group localityGroup
}

const (
	maxLocalityGroupKeyBytes = 1024
	maxOpenLocalityGroups    = 10_000
)

// GroupedLocalityStrategy keeps same-workload requests on one node until the
// current group reaches a configured budget. New groups share a global
// round-robin cursor so popular workloads spread progressively across nodes.
type GroupedLocalityStrategy struct {
	mu                 sync.Mutex
	lastGroupNodeID    string
	lastFallbackNodeID string
	limits             LocalityGroupLimits
	groups             map[string]*list.Element
	lru                list.List
}

func NewGroupedLocalityStrategy(limits LocalityGroupLimits) *GroupedLocalityStrategy {
	if limits.MaxSandboxCount == 0 {
		// Production config rejects this, but keep direct construction bounded.
		limits.MaxSandboxCount = 1
	}
	return &GroupedLocalityStrategy{
		limits: limits,
		groups: make(map[string]*list.Element),
	}
}

func (s *GroupedLocalityStrategy) Select(nodes []RichNode, hint *schedulerv1.ScheduleRequestHint) (RichNode, error) {
	ready := readyLocalityNodes(nodes)
	if len(ready) == 0 {
		return RichNode{}, ErrNoNodes
	}

	request, grouped := localityRequestFromHint(hint)

	s.mu.Lock()
	defer s.mu.Unlock()

	if !grouped {
		return selectNext(ready, &s.lastFallbackNodeID), nil
	}

	if element, ok := s.groups[request.key]; ok {
		entry := element.Value.(*localityGroupEntry)
		if node, eligible := findNode(ready, entry.group.nodeID); eligible &&
			groupCanFit(entry.group, request, s.limits) {
			addToGroup(&entry.group, request)
			if groupIsFull(entry.group, s.limits) {
				s.removeGroup(element)
			} else {
				s.lru.MoveToFront(element)
			}
			return node, nil
		}
		s.removeGroup(element)
	}

	node := selectNext(ready, &s.lastGroupNodeID)
	group := localityGroup{nodeID: node.ID}
	addToGroup(&group, request)
	if groupCanRemainOpen(group, s.limits) {
		s.putGroup(request.key, group)
	}
	return node, nil
}

func (s *GroupedLocalityStrategy) Name() string {
	return "locality"
}

func selectNext(nodes []RichNode, lastNodeID *string) RichNode {
	index := 0
	if *lastNodeID != "" {
		index = sort.Search(len(nodes), func(i int) bool {
			return nodes[i].ID > *lastNodeID
		})
		if index == len(nodes) {
			index = 0
		}
	}
	node := nodes[index]
	*lastNodeID = node.ID
	return node
}

func (s *GroupedLocalityStrategy) putGroup(key string, group localityGroup) {
	element := s.lru.PushFront(&localityGroupEntry{key: key, group: group})
	s.groups[key] = element
	if len(s.groups) <= maxOpenLocalityGroups {
		return
	}
	s.removeGroup(s.lru.Back())
}

func (s *GroupedLocalityStrategy) removeGroup(element *list.Element) {
	if element == nil {
		return
	}
	entry := element.Value.(*localityGroupEntry)
	delete(s.groups, entry.key)
	s.lru.Remove(element)
}

func readyLocalityNodes(nodes []RichNode) []RichNode {
	ready := make([]RichNode, 0, len(nodes))
	for _, node := range nodes {
		if node.Snapshot == nil ||
			node.Snapshot.GetStatus() != schedulerv1.NodeStatus_NODE_STATUS_READY {
			continue
		}
		ready = append(ready, node)
	}
	sort.Slice(ready, func(i, j int) bool {
		return ready[i].ID < ready[j].ID
	})
	return ready
}

func localityRequestFromHint(hint *schedulerv1.ScheduleRequestHint) (localityRequest, bool) {
	var request localityRequest
	switch kind := hint.GetKind().(type) {
	case *schedulerv1.ScheduleRequestHint_NewColdSandbox:
		images := kind.NewColdSandbox.GetImages()
		if len(images) == 0 {
			return localityRequest{}, false
		}
		request = localityRequest{
			key:      "image:" + strings.TrimSpace(images[0]),
			cpuCount: kind.NewColdSandbox.GetCpuCount(),
			memoryMB: kind.NewColdSandbox.GetMemoryMb(),
		}
	case *schedulerv1.ScheduleRequestHint_NewSandbox:
		request.key = "template:" + strings.TrimSpace(kind.NewSandbox.GetTemplateId())
	default:
		return localityRequest{}, false
	}
	if strings.HasSuffix(request.key, ":") || len(request.key) > maxLocalityGroupKeyBytes {
		return localityRequest{}, false
	}
	return request, true
}

func findNode(nodes []RichNode, nodeID string) (RichNode, bool) {
	for _, node := range nodes {
		if node.ID == nodeID {
			return node, true
		}
	}
	return RichNode{}, false
}

func groupCanFit(group localityGroup, request localityRequest, limits LocalityGroupLimits) bool {
	if group.sandboxCount >= limits.MaxSandboxCount {
		return false
	}
	if exceedsLimit(group.cpuCount, uint64(request.cpuCount), uint64(limits.MaxCPUCount)) {
		return false
	}
	return !exceedsLimit(group.memoryMB, request.memoryMB, limits.MaxMemoryMB)
}

func exceedsLimit(current, added, limit uint64) bool {
	return limit > 0 && (added > limit || current > limit-added)
}

func addToGroup(group *localityGroup, request localityRequest) {
	group.sandboxCount++
	group.cpuCount += uint64(request.cpuCount)
	group.memoryMB += request.memoryMB
}

func groupIsFull(group localityGroup, limits LocalityGroupLimits) bool {
	return group.sandboxCount >= limits.MaxSandboxCount ||
		(limits.MaxCPUCount > 0 && group.cpuCount >= uint64(limits.MaxCPUCount)) ||
		(limits.MaxMemoryMB > 0 && group.memoryMB >= limits.MaxMemoryMB)
}

func groupCanRemainOpen(group localityGroup, limits LocalityGroupLimits) bool {
	return !groupIsFull(group, limits)
}

type strategyOptions struct {
	localityGroupLimits LocalityGroupLimits
}

type StrategyOption func(*strategyOptions)

func WithLocalityGroupLimits(limits LocalityGroupLimits) StrategyOption {
	return func(options *strategyOptions) {
		options.localityGroupLimits = limits
	}
}

func NewStrategy(name string, opts ...StrategyOption) Strategy {
	options := strategyOptions{
		localityGroupLimits: LocalityGroupLimits{MaxSandboxCount: 1},
	}
	for _, opt := range opts {
		opt(&options)
	}

	switch strings.ToLower(strings.TrimSpace(name)) {
	case "random":
		return NewRandomStrategy()
	case "locality":
		return NewGroupedLocalityStrategy(options.localityGroupLimits)
	case "round_robin":
		fallthrough
	default:
		return &RoundRobinStrategy{}
	}
}
