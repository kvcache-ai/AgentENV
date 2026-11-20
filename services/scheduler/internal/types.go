package scheduler

import schedulerv1 "agentenv/services/api/proto"

type Node struct {
	ID       string `json:"node_id"`
	Endpoint string `json:"endpoint"`
}

// RichNode combines discovery identity with observed runtime state.
// Strategy implementations can use Snapshot for load-aware scheduling.
type RichNode struct {
	Node
	Snapshot *SchedulingSnapshot // nil if no heartbeat received yet
}

// SchedulingSnapshot holds the resource counters from a node's latest
// NodeSnapshot. Each field has the meaning of the NodeSnapshot field with the
// same name. Schedule copies one for every node on each call, so it leaves out
// the report fields that placement does not read, such as disk metrics.
type SchedulingSnapshot struct {
	AllocatedCpu               uint32
	AllocatedMemoryBytes       uint64
	CpuPercent                 uint32
	CpuCount                   uint32
	MemoryUsedBytes            uint64
	MemoryTotalBytes           uint64
	SandboxCount               uint32
	SandboxStartingCount       uint32
	PausedSandboxCount         uint32
	PausedAllocatedCpu         uint32
	PausedAllocatedMemoryBytes uint64
}

func (n Node) ToProto() *schedulerv1.Node {
	return &schedulerv1.Node{
		NodeId:   n.ID,
		Endpoint: n.Endpoint,
	}
}

func NodeFromProto(node *schedulerv1.Node) Node {
	if node == nil {
		return Node{}
	}
	return Node{
		ID:       node.GetNodeId(),
		Endpoint: node.GetEndpoint(),
	}
}
