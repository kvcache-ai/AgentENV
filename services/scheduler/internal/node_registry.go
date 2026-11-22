package scheduler

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/protobuf/proto"
)

type NodeRegistry interface {
	Snapshot(allowLingering bool) []Node
	Contains(node Node) bool
	Resolve(nodeID string) (Node, bool)
	// Ingest applies a heartbeat or pulled report only when it was captured
	// no earlier than the node's latest report: check and update are one
	// locked step, so a heartbeat landing mid-ingest always wins. applied is
	// false when the report was stale and nothing was ingested (#341 review,
	// guozy18). The wire timestamp stays millisecond-truncated; freshness is
	// arbitrated on a full-precision ingest time kept alongside the record.
	// deliverIntersection must be false for pulls: their response is
	// discarded, so marking the one-shot CPU intersection as delivered
	// would starve the node's next real heartbeat (#341 review,
	// yingdi-shan).
	Ingest(req *schedulerv1.HeartbeatRequest, fetchedAt time.Time, now time.Time, deliverIntersection bool) (node Node, cpuConfigJSON string, applied bool, err error)
	ListObserved(clusterID string, now time.Time) []*schedulerv1.ObservedNode
	ListP2pPeers(clusterID string, backend string, excludeNodeID string, now time.Time) []*schedulerv1.P2PPeer
	FilterP2pPeers(clusterID string, backend string, nodeIDs []string, excludeNodeID string, now time.Time) []*schedulerv1.P2PPeer
	GetObserved(nodeID string, clusterID string, now time.Time) (*schedulerv1.ObservedNode, bool)
	// SchedulingSnapshot returns a copy of the resource counters from the latest
	// heartbeat-reported NodeSnapshot for a node.
	// Returns nil if the node has never sent a heartbeat.
	SchedulingSnapshot(nodeID string) *SchedulingSnapshot
	UnregisterObserved(nodeID string, serviceInstanceID string) error
}

var (
	ErrServiceInstanceMismatch = errors.New("service instance mismatch")
	ErrNodeNotInRegistry       = errors.New("node is not in scheduler node list")
	defaultObservedReportTTL   = 30 * time.Second
)

type observedNodeRecord struct {
	node        *schedulerv1.ObservedNode
	p2pEndpoint *schedulerv1.P2PEndpoint
	reportTTL   time.Duration
	// lastReportAt is the full-precision time this record was ingested.
	// LastSeenUnixMs on the proto stays millisecond-truncated for wire
	// compatibility, so pull-vs-heartbeat freshness is arbitrated here
	// instead (#341 review, guozy18).
	lastReportAt time.Time
}

type AtomicNodeRegistry struct {
	mu               sync.RWMutex
	nodesByID        map[string]Node
	lingeringIDs     map[string]bool
	observedTTL      time.Duration
	observed         map[string]observedNodeRecord
	cpuIntersection  map[string]string
	intersectionSent map[string]bool
	// sortedNodes holds the nodes of nodesByID ordered by ID. Set sorts them
	// once per discovery update, so Snapshot does not sort on every Schedule.
	sortedNodes []Node
	// cpuConfigNodes counts, for each cluster, the observed nodes that report
	// each distinct CPU configuration. Nodes on the same hardware report the
	// same text, so the intersection reads the keys of this map and its cost
	// follows the number of distinct configurations, not the node count.
	cpuConfigNodes map[string]map[string]int
}

func NewAtomicNodeRegistry(nodes []Node, observedTTL time.Duration) *AtomicNodeRegistry {
	ttl := defaultObservedReportTTL
	if observedTTL > 0 {
		ttl = observedTTL
	}

	registry := &AtomicNodeRegistry{
		nodesByID:        make(map[string]Node),
		lingeringIDs:     make(map[string]bool),
		observedTTL:      ttl,
		observed:         make(map[string]observedNodeRecord),
		cpuIntersection:  make(map[string]string),
		intersectionSent: make(map[string]bool),
		cpuConfigNodes:   make(map[string]map[string]int),
	}
	registry.Set(nodes, nil)
	return registry
}

// Snapshot returns the discovered nodes in a new slice sorted by ID, without
// lingering nodes unless allowLingering is true. Round robin depends on this
// stable order.
func (r *AtomicNodeRegistry) Snapshot(allowLingering bool) []Node {
	r.mu.RLock()
	result := make([]Node, 0, len(r.sortedNodes))
	for _, node := range r.sortedNodes {
		if r.lingeringIDs[node.ID] && !allowLingering {
			continue
		}
		result = append(result, node)
	}
	r.mu.RUnlock()
	return result
}

func (r *AtomicNodeRegistry) Contains(node Node) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	known, ok := r.nodesByID[node.ID]
	return ok && known.Endpoint == node.Endpoint
}

func (r *AtomicNodeRegistry) Resolve(nodeID string) (Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	node, ok := r.nodesByID[nodeID]
	return node, ok
}

// Set replaces the discovered node list. active nodes are serving and not
// terminating; lingering nodes are serving but terminating (graceful shutdown).
func (r *AtomicNodeRegistry) Set(active []Node, lingering []Node) {
	byID := make(map[string]Node, len(active)+len(lingering))
	for _, node := range active {
		byID[node.ID] = node
	}
	for _, node := range lingering {
		byID[node.ID] = node
	}

	sorted := make([]Node, 0, len(byID))
	for _, node := range byID {
		sorted = append(sorted, node)
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ID < sorted[j].ID
	})

	lIDs := make(map[string]bool, len(lingering))
	for _, node := range lingering {
		lIDs[node.ID] = true
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodesByID = byID
	r.sortedNodes = sorted
	r.lingeringIDs = lIDs
	affectedClusters := make(map[string]struct{})
	for nodeID, record := range r.observed {
		if _, ok := byID[nodeID]; ok {
			continue
		}
		if clusterID := record.node.GetClusterId(); clusterID != "" {
			affectedClusters[clusterID] = struct{}{}
		}
		r.removeCpuConfigNodeLocked(record)
		delete(r.observed, nodeID)
		delete(r.intersectionSent, nodeID)
	}
	for clusterID := range affectedClusters {
		r.invalidateIntersectionLocked(clusterID)
	}
}

func (r *AtomicNodeRegistry) Heartbeat(req *schedulerv1.HeartbeatRequest, now time.Time) (Node, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.heartbeatLocked(req, now, true)
}

func (r *AtomicNodeRegistry) Ingest(req *schedulerv1.HeartbeatRequest, fetchedAt time.Time, now time.Time, deliverIntersection bool) (Node, string, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rec, ok := r.observed[req.GetNodeId()]; ok && rec.lastReportAt.After(fetchedAt) {
		// Stale pull: the node already reported something captured after
		// this data. Leave the newer observation untouched; the caller must
		// skip the roster reconcile too.
		return Node{}, "", false, nil
	}
	node, cpuConfigJSON, err := r.heartbeatLocked(req, now, deliverIntersection)
	if err != nil {
		return Node{}, "", false, err
	}
	return node, cpuConfigJSON, true, nil
}

func (r *AtomicNodeRegistry) heartbeatLocked(req *schedulerv1.HeartbeatRequest, now time.Time, deliverIntersection bool) (Node, string, error) {
	nowMs := now.UTC().UnixMilli()

	machineInfo := cloneMachineInfo(req.GetMachineInfo())

	node, ok := r.nodesByID[req.GetNodeId()]
	if !ok {
		return Node{}, "", ErrNodeNotInRegistry
	}

	prevCPU, existed := "", false
	prev, ok := r.observed[req.GetNodeId()]
	if ok {
		existed = true
		prevCPU = prev.node.GetMachineInfo().GetCpuConfigJson()
		if machineInfo != nil && machineInfo.CpuConfigJson == "" {
			machineInfo.CpuConfigJson = prevCPU
		}
	}

	record := observedNodeRecord{
		node: &schedulerv1.ObservedNode{
			NodeId:            req.GetNodeId(),
			Endpoint:          node.Endpoint,
			ClusterId:         req.GetClusterId(),
			ServiceInstanceId: req.GetServiceInstanceId(),
			Version:           req.GetVersion(),
			Commit:            req.GetCommit(),
			MachineInfo:       machineInfo,
			LastSeenUnixMs:    nowMs,
			Snapshot:          cloneSnapshot(req.GetSnapshot()),
		},
		p2pEndpoint:  cloneP2PEndpoint(req.GetP2PEndpoint()),
		reportTTL:    r.observedTTL,
		lastReportAt: now,
	}
	if record.node.Snapshot.GetReportedAtUnixMs() == 0 {
		record.node.Snapshot.ReportedAtUnixMs = nowMs
	}
	if record.node.Snapshot.GetStatus() == schedulerv1.NodeStatus_NODE_STATUS_UNSPECIFIED {
		record.node.Snapshot.Status = schedulerv1.NodeStatus_NODE_STATUS_CONNECTING
	}

	clusterID := req.GetClusterId()
	cpuConfigChanged := machineInfo.GetCpuConfigJson() != prevCPU
	if !existed || cpuConfigChanged || prev.node.GetClusterId() != clusterID {
		if existed {
			r.removeCpuConfigNodeLocked(prev)
		}
		r.addCpuConfigNodeLocked(record)
	}
	r.observed[req.GetNodeId()] = record

	if !existed || (machineInfo != nil && cpuConfigChanged) {
		r.invalidateIntersectionLocked(clusterID)
	}
	if _, computed := r.cpuIntersection[clusterID]; !computed {
		if r.allConfigsReadyLocked(clusterID) {
			if result := r.computeIntersectionLocked(clusterID); result != "" {
				r.cpuIntersection[clusterID] = result
			}
		}
	}

	if intersection, ok := r.cpuIntersection[clusterID]; ok && !r.intersectionSent[req.GetNodeId()] {
		// A discarded response must not count as delivered: pulls leave the
		// one-shot intersection pending for the node's next real heartbeat
		// (#341 review, yingdi-shan).
		if deliverIntersection {
			r.intersectionSent[req.GetNodeId()] = true
			return node, intersection, nil
		}
	}
	return node, "", nil
}

func (r *AtomicNodeRegistry) invalidateIntersectionLocked(clusterID string) {
	delete(r.cpuIntersection, clusterID)
	for nodeID, rec := range r.observed {
		if rec.node.GetClusterId() == clusterID {
			delete(r.intersectionSent, nodeID)
		}
	}
}

func (r *AtomicNodeRegistry) allConfigsReadyLocked(clusterID string) bool {
	total, withConfig := 0, 0
	for _, rec := range r.observed {
		if rec.node.GetClusterId() != clusterID {
			continue
		}
		total++
		if rec.node.GetMachineInfo().GetCpuConfigJson() != "" {
			withConfig++
		}
	}
	return total > 0 && withConfig == total
}

// addCpuConfigNodeLocked counts the node of record under its CPU
// configuration. A node without a configuration is not counted.
func (r *AtomicNodeRegistry) addCpuConfigNodeLocked(record observedNodeRecord) {
	cpuConfigJSON := record.node.GetMachineInfo().GetCpuConfigJson()
	if cpuConfigJSON == "" {
		return
	}
	clusterID := record.node.GetClusterId()
	if r.cpuConfigNodes[clusterID] == nil {
		r.cpuConfigNodes[clusterID] = make(map[string]int)
	}
	r.cpuConfigNodes[clusterID][cpuConfigJSON]++
}

// removeCpuConfigNodeLocked reverses addCpuConfigNodeLocked for a record that
// is about to be replaced or removed.
func (r *AtomicNodeRegistry) removeCpuConfigNodeLocked(record observedNodeRecord) {
	cpuConfigJSON := record.node.GetMachineInfo().GetCpuConfigJson()
	if cpuConfigJSON == "" {
		return
	}
	clusterID := record.node.GetClusterId()
	nodes := r.cpuConfigNodes[clusterID]
	if nodes[cpuConfigJSON] > 1 {
		nodes[cpuConfigJSON]--
		return
	}
	delete(nodes, cpuConfigJSON)
	if len(nodes) == 0 {
		delete(r.cpuConfigNodes, clusterID)
	}
}

func (r *AtomicNodeRegistry) computeIntersectionLocked(clusterID string) string {
	// Identical configurations intersect to the same result as one of them,
	// so each distinct configuration is parsed once.
	jsons := make([]string, 0, len(r.cpuConfigNodes[clusterID]))
	for cpuConfigJSON := range r.cpuConfigNodes[clusterID] {
		jsons = append(jsons, cpuConfigJSON)
	}
	result, err := IntersectCpuConfigs(jsons)
	if err != nil {
		return ""
	}
	return result
}

func (r *AtomicNodeRegistry) ListObserved(clusterID string, now time.Time) []*schedulerv1.ObservedNode {
	nowMs := now.UTC().UnixMilli()
	trimmedCluster := strings.TrimSpace(clusterID)

	r.mu.RLock()
	defer r.mu.RUnlock()
	nodes := make([]*schedulerv1.ObservedNode, 0, len(r.observed))
	for _, record := range r.observed {
		if trimmedCluster != "" && record.node.GetClusterId() != trimmedCluster {
			continue
		}
		nodes = append(nodes, r.deriveObservedNodeViewLocked(record, nowMs))
	}

	return nodes
}

func (r *AtomicNodeRegistry) ListP2pPeers(clusterID string, backend string, excludeNodeID string, now time.Time) []*schedulerv1.P2PPeer {
	return r.filterP2pPeers(clusterID, backend, nil, excludeNodeID, now)
}

func (r *AtomicNodeRegistry) FilterP2pPeers(clusterID string, backend string, nodeIDs []string, excludeNodeID string, now time.Time) []*schedulerv1.P2PPeer {
	allowed := make(map[string]struct{}, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		allowed[nodeID] = struct{}{}
	}
	if len(allowed) == 0 {
		return nil
	}
	return r.filterP2pPeers(clusterID, backend, allowed, excludeNodeID, now)
}

func (r *AtomicNodeRegistry) filterP2pPeers(clusterID string, backend string, allowed map[string]struct{}, excludeNodeID string, now time.Time) []*schedulerv1.P2PPeer {
	nowMs := now.UTC().UnixMilli()
	trimmedCluster := strings.TrimSpace(clusterID)
	trimmedBackend := strings.TrimSpace(backend)
	trimmedExcludeNodeID := strings.TrimSpace(excludeNodeID)

	r.mu.RLock()
	defer r.mu.RUnlock()
	peers := make([]*schedulerv1.P2PPeer, 0, len(r.observed))
	for _, record := range r.observed {
		if trimmedCluster != "" && record.node.GetClusterId() != trimmedCluster {
			continue
		}
		node := r.deriveObservedNodeViewLocked(record, nowMs)
		if len(allowed) > 0 {
			if _, ok := allowed[node.GetNodeId()]; !ok {
				continue
			}
		}
		if node.GetNodeId() == trimmedExcludeNodeID {
			continue
		}
		if node.GetSnapshot().GetStatus() != schedulerv1.NodeStatus_NODE_STATUS_READY {
			continue
		}
		endpoint := record.p2pEndpoint
		if endpoint.GetBackend() == "" || endpoint.GetAddress() == "" {
			continue
		}
		if trimmedBackend != "" && endpoint.GetBackend() != trimmedBackend {
			continue
		}
		peers = append(peers, &schedulerv1.P2PPeer{
			NodeId:   node.GetNodeId(),
			Endpoint: cloneP2PEndpoint(endpoint),
		})
	}

	return peers
}

func (r *AtomicNodeRegistry) GetObserved(nodeID string, clusterID string, now time.Time) (*schedulerv1.ObservedNode, bool) {
	nowMs := now.UTC().UnixMilli()
	trimmedCluster := strings.TrimSpace(clusterID)

	r.mu.RLock()
	defer r.mu.RUnlock()
	record, ok := r.observed[nodeID]
	if !ok {
		return nil, false
	}
	if trimmedCluster != "" && record.node.GetClusterId() != trimmedCluster {
		return nil, false
	}

	return r.deriveObservedNodeViewLocked(record, nowMs), true
}

func (r *AtomicNodeRegistry) SchedulingSnapshot(nodeID string) *SchedulingSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	record, ok := r.observed[nodeID]
	if !ok || record.node == nil {
		return nil
	}
	snapshot := record.node.GetSnapshot()
	if snapshot == nil {
		return nil
	}
	return &SchedulingSnapshot{
		AllocatedCpu:               snapshot.GetAllocatedCpu(),
		AllocatedMemoryBytes:       snapshot.GetAllocatedMemoryBytes(),
		CpuPercent:                 snapshot.GetCpuPercent(),
		CpuCount:                   snapshot.GetCpuCount(),
		MemoryUsedBytes:            snapshot.GetMemoryUsedBytes(),
		MemoryTotalBytes:           snapshot.GetMemoryTotalBytes(),
		SandboxCount:               snapshot.GetSandboxCount(),
		SandboxStartingCount:       snapshot.GetSandboxStartingCount(),
		PausedSandboxCount:         snapshot.GetPausedSandboxCount(),
		PausedAllocatedCpu:         snapshot.GetPausedAllocatedCpu(),
		PausedAllocatedMemoryBytes: snapshot.GetPausedAllocatedMemoryBytes(),
	}
}

func (r *AtomicNodeRegistry) UnregisterObserved(nodeID string, serviceInstanceID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	record, ok := r.observed[nodeID]
	if !ok {
		return nil
	}
	if record.node.GetServiceInstanceId() != serviceInstanceID {
		return ErrServiceInstanceMismatch
	}

	clusterID := record.node.GetClusterId()
	r.removeCpuConfigNodeLocked(record)
	delete(r.observed, nodeID)
	r.invalidateIntersectionLocked(clusterID)
	return nil
}

// deriveObservedNodeViewLocked builds the external ObservedNode view for a
// heartbeat record, overriding the endpoint and status based on the current
// discovery state. See NodeStatus in scheduler.proto for the full derivation
// table. r.mu must be held by the caller.
func (r *AtomicNodeRegistry) deriveObservedNodeViewLocked(record observedNodeRecord, nowMs int64) *schedulerv1.ObservedNode {
	out := cloneObservedNode(record.node)
	if out.Snapshot == nil {
		out.Snapshot = &schedulerv1.NodeSnapshot{}
	}

	nodeID := out.GetNodeId()

	knownNode, inDiscovery := r.nodesByID[nodeID]
	isLingering := r.lingeringIDs[nodeID]

	if inDiscovery && strings.TrimSpace(knownNode.Endpoint) != "" {
		out.Endpoint = knownNode.Endpoint
	}

	ttl := record.reportTTL
	if ttl <= 0 {
		ttl = defaultObservedReportTTL
	}

	if out.GetLastSeenUnixMs() > 0 && nowMs-out.GetLastSeenUnixMs() > ttl.Milliseconds() {
		out.Snapshot.Status = schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY
	} else if !inDiscovery {
		out.Snapshot.Status = schedulerv1.NodeStatus_NODE_STATUS_CONNECTING
	} else if isLingering {
		out.Snapshot.Status = schedulerv1.NodeStatus_NODE_STATUS_LINGERING
	} else {
		// Active — keep the status reported by the node.
		if out.Snapshot.GetStatus() == schedulerv1.NodeStatus_NODE_STATUS_UNSPECIFIED {
			out.Snapshot.Status = schedulerv1.NodeStatus_NODE_STATUS_CONNECTING
		}
	}

	return out
}

func cloneObservedNode(node *schedulerv1.ObservedNode) *schedulerv1.ObservedNode {
	if node == nil {
		return &schedulerv1.ObservedNode{}
	}
	cloned, ok := proto.Clone(node).(*schedulerv1.ObservedNode)
	if ok {
		return cloned
	}
	return &schedulerv1.ObservedNode{}
}

func cloneSnapshot(snapshot *schedulerv1.NodeSnapshot) *schedulerv1.NodeSnapshot {
	if snapshot == nil {
		return &schedulerv1.NodeSnapshot{}
	}
	cloned, ok := proto.Clone(snapshot).(*schedulerv1.NodeSnapshot)
	if ok {
		return cloned
	}
	return &schedulerv1.NodeSnapshot{}
}

func cloneMachineInfo(machine *schedulerv1.MachineInfo) *schedulerv1.MachineInfo {
	if machine == nil {
		return nil
	}
	cloned, ok := proto.Clone(machine).(*schedulerv1.MachineInfo)
	if ok {
		return cloned
	}
	return nil
}

func cloneP2PEndpoint(endpoint *schedulerv1.P2PEndpoint) *schedulerv1.P2PEndpoint {
	if endpoint == nil {
		return nil
	}
	cloned, ok := proto.Clone(endpoint).(*schedulerv1.P2PEndpoint)
	if ok {
		return cloned
	}
	return nil
}
