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
	Heartbeat(req *schedulerv1.HeartbeatRequest, now time.Time) (Node, string, error)
	ListObserved(clusterID string, now time.Time) []*schedulerv1.ObservedNode
	ListP2pPeers(clusterID string, backend string, excludeNodeID string, now time.Time) []*schedulerv1.P2PPeer
	FilterP2pPeers(clusterID string, backend string, nodeIDs []string, excludeNodeID string, now time.Time) []*schedulerv1.P2PPeer
	GetObserved(nodeID string, clusterID string, now time.Time) (*schedulerv1.ObservedNode, bool)
	// SchedulingSnapshot returns the latest NodeSnapshot with status derived
	// from current discovery state and heartbeat TTL.
	SchedulingSnapshot(nodeID string, now time.Time) *schedulerv1.NodeSnapshot
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
}

type AtomicNodeRegistry struct {
	mu               sync.RWMutex
	nodesByID        map[string]Node
	lingeringIDs     map[string]bool
	observedTTL      time.Duration
	observed         map[string]observedNodeRecord
	cpuIntersection  map[string]string
	intersectionSent map[string]bool
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
	}
	registry.Set(nodes, nil)
	return registry
}

// Snapshot returns discovered nodes filtered by their derived status.
// See NodeStatus in scheduler.proto for the full status derivation table.
func (r *AtomicNodeRegistry) Snapshot(allowLingering bool) []Node {
	r.mu.RLock()
	result := make([]Node, 0, len(r.nodesByID))
	for _, node := range r.nodesByID {
		if r.lingeringIDs[node.ID] && !allowLingering {
			continue
		}
		result = append(result, node)
	}
	r.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
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

	lIDs := make(map[string]bool, len(lingering))
	for _, node := range lingering {
		lIDs[node.ID] = true
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodesByID = byID
	r.lingeringIDs = lIDs
	affectedClusters := make(map[string]struct{})
	for nodeID, record := range r.observed {
		if _, ok := byID[nodeID]; ok {
			continue
		}
		if clusterID := record.node.GetClusterId(); clusterID != "" {
			affectedClusters[clusterID] = struct{}{}
		}
		delete(r.observed, nodeID)
		delete(r.intersectionSent, nodeID)
	}
	for clusterID := range affectedClusters {
		r.invalidateIntersectionLocked(clusterID)
	}
}

func (r *AtomicNodeRegistry) Heartbeat(req *schedulerv1.HeartbeatRequest, now time.Time) (Node, string, error) {
	nowMs := now.UTC().UnixMilli()

	machineInfo := cloneMachineInfo(req.GetMachineInfo())

	r.mu.Lock()
	defer r.mu.Unlock()
	node, ok := r.nodesByID[req.GetNodeId()]
	if !ok {
		return Node{}, "", ErrNodeNotInRegistry
	}

	prevCPU, existed := "", false
	if prev, ok := r.observed[req.GetNodeId()]; ok {
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
		p2pEndpoint: cloneP2PEndpoint(req.GetP2PEndpoint()),
		reportTTL:   r.observedTTL,
	}
	if record.node.Snapshot.GetReportedAtUnixMs() == 0 {
		record.node.Snapshot.ReportedAtUnixMs = nowMs
	}
	if record.node.Snapshot.GetStatus() == schedulerv1.NodeStatus_NODE_STATUS_UNSPECIFIED {
		record.node.Snapshot.Status = schedulerv1.NodeStatus_NODE_STATUS_CONNECTING
	}

	r.observed[req.GetNodeId()] = record

	clusterID := req.GetClusterId()
	if !existed || (machineInfo != nil && machineInfo.GetCpuConfigJson() != prevCPU) {
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
		r.intersectionSent[req.GetNodeId()] = true
		return node, intersection, nil
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

func (r *AtomicNodeRegistry) computeIntersectionLocked(clusterID string) string {
	var jsons []string
	for _, rec := range r.observed {
		if rec.node.GetClusterId() != clusterID {
			continue
		}
		if j := rec.node.GetMachineInfo().GetCpuConfigJson(); j != "" {
			jsons = append(jsons, j)
		}
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

func (r *AtomicNodeRegistry) SchedulingSnapshot(nodeID string, now time.Time) *schedulerv1.NodeSnapshot {
	nowMs := now.UTC().UnixMilli()

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
	out := cloneSnapshot(snapshot)
	out.Status = r.derivedStatusLocked(record, nowMs)
	return out
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

	if inDiscovery && strings.TrimSpace(knownNode.Endpoint) != "" {
		out.Endpoint = knownNode.Endpoint
	}

	out.Snapshot.Status = r.derivedStatusLocked(record, nowMs)
	return out
}

// derivedStatusLocked computes the effective scheduling status for a heartbeat
// record. r.mu must be held by the caller.
func (r *AtomicNodeRegistry) derivedStatusLocked(record observedNodeRecord, nowMs int64) schedulerv1.NodeStatus {
	ttl := record.reportTTL
	if ttl <= 0 {
		ttl = defaultObservedReportTTL
	}

	if lastSeen := record.node.GetLastSeenUnixMs(); lastSeen > 0 && nowMs-lastSeen > ttl.Milliseconds() {
		return schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY
	}
	nodeID := record.node.GetNodeId()
	if _, inDiscovery := r.nodesByID[nodeID]; !inDiscovery {
		return schedulerv1.NodeStatus_NODE_STATUS_CONNECTING
	}
	if r.lingeringIDs[nodeID] {
		return schedulerv1.NodeStatus_NODE_STATUS_LINGERING
	}
	if status := record.node.GetSnapshot().GetStatus(); status != schedulerv1.NodeStatus_NODE_STATUS_UNSPECIFIED {
		return status
	}
	return schedulerv1.NodeStatus_NODE_STATUS_CONNECTING
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
