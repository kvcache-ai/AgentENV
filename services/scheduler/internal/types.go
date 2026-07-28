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
	Snapshot   *schedulerv1.NodeSnapshot // nil if no heartbeat received yet
	ClusterID  string
	P2pBackend string
}

// NodeSchedulingContext contains heartbeat state used only for placement.
// P2P endpoint addresses remain private to peer discovery.
type NodeSchedulingContext struct {
	Snapshot   *schedulerv1.NodeSnapshot
	ClusterID  string
	P2pBackend string
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
