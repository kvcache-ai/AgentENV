package scheduler

import (
	"fmt"
	"sort"
	"sync"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

type FleetPolicy struct {
	MinNodes             uint32
	MaxNodes             uint32
	WarmNodes            uint32
	MaxSandboxesPerNode  uint32
	MaxMemoryUsedPercent uint32
	EmptyGrace           time.Duration
	DrainGrace           time.Duration
	DemandTTL            time.Duration
}

type FleetNodeReference struct {
	NodeID            string
	ServiceInstanceID string
}

type FleetPlan struct {
	DesiredNodes       uint32
	ReadyNodes         uint32
	ProvisioningNodes  uint32
	CordonCandidates   []FleetNodeReference
	DeleteCandidates   []FleetNodeReference
	UncordonCandidates []FleetNodeReference
	Reason             string
}

type FleetPlanner struct {
	mu             sync.Mutex
	nodes          NodeRegistry
	policy         FleetPolicy
	emptySince     map[string]time.Time
	cordonedAt     map[string]time.Time
	lastDemand     time.Time
	demandTarget   uint32
	pressureTarget uint32
}

func NewFleetPlanner(nodes NodeRegistry, policy FleetPolicy) (*FleetPlanner, error) {
	if nodes == nil {
		return nil, fmt.Errorf("fleet planner requires a node registry")
	}
	if policy.MinNodes == 0 || policy.MaxNodes == 0 || policy.MinNodes > policy.MaxNodes {
		return nil, fmt.Errorf("fleet planner requires positive min nodes <= max nodes")
	}
	if policy.WarmNodes > policy.MaxNodes {
		return nil, fmt.Errorf("fleet planner warm nodes exceeds max nodes")
	}
	if policy.MaxSandboxesPerNode == 0 {
		return nil, fmt.Errorf("fleet planner max sandboxes per node must be positive")
	}
	if policy.MaxMemoryUsedPercent == 0 || policy.MaxMemoryUsedPercent > 100 {
		return nil, fmt.Errorf("fleet planner memory threshold must be between 1 and 100")
	}
	if policy.EmptyGrace <= 0 || policy.DrainGrace <= 0 || policy.DemandTTL <= 0 {
		return nil, fmt.Errorf("fleet planner grace and demand durations must be positive")
	}
	return &FleetPlanner{
		nodes: nodes, policy: policy,
		emptySince: make(map[string]time.Time), cordonedAt: make(map[string]time.Time),
	}, nil
}

func (p *FleetPlanner) RecordDemand(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lastDemand.IsZero() || now.Sub(p.lastDemand) > p.policy.DemandTTL {
		p.demandTarget = 0
	}
	var ready uint32
	for _, node := range p.nodes.ListObserved("", now) {
		if node.GetSnapshot().GetStatus() == schedulerv1.NodeStatus_NODE_STATUS_READY {
			ready = addUint32Saturating(ready, 1)
		}
	}
	p.demandTarget = maxUint32(p.demandTarget, addUint32Saturating(ready, 1))
	if now.After(p.lastDemand) {
		p.lastDemand = now
	}
}

func (p *FleetPlanner) MarkCordoned(nodeID string, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cordonedAt[nodeID] = now
}

func (p *FleetPlanner) MarkUncordoned(nodeID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.cordonedAt, nodeID)
}

func (p *FleetPlanner) Plan(fleetNodeIDs []string, now time.Time) FleetPlan {
	p.mu.Lock()
	defer p.mu.Unlock()

	observed := p.nodes.ListObserved("", now)
	observedIDs := make(map[string]struct{}, len(observed))
	active := make([]*schedulerv1.ObservedNode, 0, len(observed))
	lingering := make([]*schedulerv1.ObservedNode, 0, len(observed))
	var sandboxUnits uint64
	var memoryReserved uint64
	var memoryTotal uint64
	var memoryNodes uint64
	memoryPressure := false
	for _, node := range observed {
		if node.GetNodeId() == "" {
			continue
		}
		observedIDs[node.GetNodeId()] = struct{}{}
		status := node.GetSnapshot().GetStatus()
		if status != schedulerv1.NodeStatus_NODE_STATUS_READY && status != schedulerv1.NodeStatus_NODE_STATUS_LINGERING {
			continue
		}
		if status == schedulerv1.NodeStatus_NODE_STATUS_LINGERING {
			lingering = append(lingering, node)
		} else {
			active = append(active, node)
		}
		snapshot := node.GetSnapshot()
		sandboxUnits += uint64(snapshot.GetSandboxCount()) + uint64(snapshot.GetSandboxStartingCount()) + uint64(snapshot.GetPausedSandboxCount())
		memoryReserved += snapshot.GetAllocatedMemoryBytes() + snapshot.GetPausedAllocatedMemoryBytes()
		if snapshot.GetMemoryTotalBytes() > 0 {
			memoryTotal += snapshot.GetMemoryTotalBytes()
			memoryNodes++
			if snapshot.GetMemoryUsedBytes() >= percentOf(snapshot.GetMemoryTotalBytes(), p.policy.MaxMemoryUsedPercent) {
				memoryPressure = true
			}
		}
		if nodeEmpty(snapshot) {
			if _, exists := p.emptySince[node.GetNodeId()]; !exists {
				p.emptySince[node.GetNodeId()] = now
			}
		} else {
			delete(p.emptySince, node.GetNodeId())
		}
	}

	for nodeID := range p.emptySince {
		if _, exists := observedIDs[nodeID]; !exists {
			delete(p.emptySince, nodeID)
			delete(p.cordonedAt, nodeID)
		}
	}

	uniqueFleet := make(map[string]struct{}, len(fleetNodeIDs))
	var provisioning uint32
	for _, nodeID := range fleetNodeIDs {
		if nodeID == "" {
			continue
		}
		if _, exists := uniqueFleet[nodeID]; exists {
			continue
		}
		uniqueFleet[nodeID] = struct{}{}
		if _, registered := observedIDs[nodeID]; !registered {
			provisioning++
		}
	}

	desired := maxUint32(p.policy.MinNodes, p.policy.WarmNodes)
	if sandboxUnits > 0 {
		workload := ceilDivUint64(sandboxUnits, uint64(p.policy.MaxSandboxesPerNode))
		desired = maxUint32(desired, addUint32Saturating(clampUint64ToUint32(workload), p.policy.WarmNodes))
	}
	if memoryReserved > 0 && memoryNodes > 0 {
		averageTotal := memoryTotal / memoryNodes
		memoryCapacity := averageTotal * uint64(p.policy.MaxMemoryUsedPercent) / 100
		if memoryCapacity > 0 {
			workload := ceilDivUint64(memoryReserved, memoryCapacity)
			desired = maxUint32(desired, addUint32Saturating(clampUint64ToUint32(workload), p.policy.WarmNodes))
		}
	}

	readyAndLingering := clampUint64ToUint32(uint64(len(active) + len(lingering)))
	current := addUint32Saturating(readyAndLingering, provisioning)
	reason := "workload_headroom"
	if memoryPressure {
		if p.pressureTarget == 0 {
			p.pressureTarget = addUint32Saturating(readyAndLingering, 1)
		}
		desired = maxUint32(desired, p.pressureTarget)
		reason = "memory_pressure"
	} else {
		p.pressureTarget = 0
	}
	if !p.lastDemand.IsZero() && now.Sub(p.lastDemand) <= p.policy.DemandTTL {
		desired = maxUint32(desired, p.demandTarget)
		reason = "recent_schedule_pressure"
	} else {
		p.demandTarget = 0
	}
	if desired < p.policy.MinNodes {
		desired = p.policy.MinNodes
	}
	if desired > p.policy.MaxNodes {
		desired = p.policy.MaxNodes
		reason = "maximum_reached"
	}

	plan := FleetPlan{
		DesiredNodes: desired, ReadyNodes: uint32(len(active)), ProvisioningNodes: provisioning,
		Reason: reason,
	}

	if desired > uint32(len(active))+provisioning && len(lingering) > 0 {
		sortObservedNodesDescending(lingering)
		need := desired - uint32(len(active)) - provisioning
		for _, node := range lingering {
			if uint32(len(plan.UncordonCandidates)) >= need {
				break
			}
			plan.UncordonCandidates = append(plan.UncordonCandidates, fleetNodeReference(node))
		}
		return plan
	}

	if provisioning > 0 || current <= desired {
		return plan
	}
	excess := current - desired

	sortObservedNodesDescending(lingering)
	for _, node := range lingering {
		if uint32(len(plan.DeleteCandidates)) >= excess {
			break
		}
		if !nodeEmpty(node.GetSnapshot()) {
			continue
		}
		cordonedAt, known := p.cordonedAt[node.GetNodeId()]
		if !known || now.Sub(cordonedAt) < p.policy.DrainGrace {
			continue
		}
		plan.DeleteCandidates = append(plan.DeleteCandidates, fleetNodeReference(node))
		break // one exact deletion per plan keeps scale-in deliberately slow
	}
	if len(plan.DeleteCandidates) > 0 {
		return plan
	}

	sortObservedNodesDescending(active)
	for _, node := range active {
		if !nodeEmpty(node.GetSnapshot()) {
			continue
		}
		emptySince, known := p.emptySince[node.GetNodeId()]
		if !known || now.Sub(emptySince) < p.policy.EmptyGrace {
			continue
		}
		plan.CordonCandidates = append(plan.CordonCandidates, fleetNodeReference(node))
		break // cordon one candidate at a time
	}
	return plan
}

func nodeEmpty(snapshot *schedulerv1.NodeSnapshot) bool {
	return snapshot != nil && snapshot.GetSandboxCount() == 0 && snapshot.GetSandboxStartingCount() == 0 && snapshot.GetPausedSandboxCount() == 0
}

func fleetNodeReference(node *schedulerv1.ObservedNode) FleetNodeReference {
	return FleetNodeReference{NodeID: node.GetNodeId(), ServiceInstanceID: node.GetServiceInstanceId()}
}

func sortObservedNodesDescending(nodes []*schedulerv1.ObservedNode) {
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].GetNodeId() > nodes[j].GetNodeId() })
}

func ceilDivUint64(numerator, denominator uint64) uint64 {
	if numerator == 0 {
		return 0
	}
	return 1 + (numerator-1)/denominator
}

func clampUint64ToUint32(value uint64) uint32 {
	if value > uint64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(value)
}

func addUint32Saturating(a, b uint32) uint32 {
	if ^uint32(0)-a < b {
		return ^uint32(0)
	}
	return a + b
}

func percentOf(value uint64, percent uint32) uint64 {
	quotient, remainder := value/100, value%100
	return quotient*uint64(percent) + remainder*uint64(percent)/100
}

func maxUint32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}
