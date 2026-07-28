package scheduler

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

func TestListObservedFiltersByCluster(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}, {ID: "node-b", Endpoint: "http://node-b"}}, 30*time.Second)
	now := time.Unix(100, 0)

	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}, now)
	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-b",
		ClusterId:         "cluster-b",
		ServiceInstanceId: "svc-b",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}, now)

	nodes := registry.ListObserved("cluster-a", now)
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if got := nodes[0].GetNodeId(); got != "node-a" {
		t.Fatalf("expected node-a, got %s", got)
	}
}

func TestNodeRegistryHeartbeatRejectsUnknownNode(t *testing.T) {
	registry := NewAtomicNodeRegistry(nil, 30*time.Second)

	_, _, err := registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
	}, time.Unix(100, 0))
	if !errors.Is(err, ErrNodeNotInRegistry) {
		t.Fatalf("expected ErrNodeNotInRegistry, got %v", err)
	}
}

func TestObservedNodeBecomesUnhealthyAfterTTL(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, time.Second)
	start := time.Unix(100, 0)

	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}, start)

	node, ok := registry.GetObserved("node-a", "cluster-a", start.Add(2*time.Second))
	if !ok {
		t.Fatal("expected observed node")
	}
	if got := node.GetSnapshot().GetStatus(); got != schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY {
		t.Fatalf("expected status %v, got %v", schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY, got)
	}
}

func TestObservedNodeUsesLatestKnownEndpoint(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)
	now := time.Unix(100, 0)

	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}, now)

	registry.Set([]Node{{ID: "node-a", Endpoint: "http://node-a-new"}}, nil)

	node, ok := registry.GetObserved("node-a", "cluster-a", now)
	if !ok {
		t.Fatal("expected observed node")
	}
	if got := node.GetEndpoint(); got != "http://node-a-new" {
		t.Fatalf("expected updated endpoint %q, got %q", "http://node-a-new", got)
	}
}

func TestHeartbeatStoresP2PEndpointForPeerListing(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)
	now := time.Unix(100, 0)
	endpoint := &schedulerv1.P2PEndpoint{
		Backend: "iroh",
		Address: `{"id":"node-id"}`,
	}

	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
		P2PEndpoint:       endpoint,
	}, now)

	peers := registry.ListP2pPeers("cluster-a", "iroh", "", now)
	if len(peers) != 1 {
		t.Fatalf("expected one P2P peer, got %d", len(peers))
	}
	got := peers[0].GetEndpoint()
	if got.GetBackend() != endpoint.GetBackend() || got.GetAddress() != endpoint.GetAddress() {
		t.Fatalf("unexpected P2P endpoint: %+v", got)
	}

	node, ok := registry.GetObserved("node-a", "cluster-a", now)
	if !ok {
		t.Fatal("expected observed node")
	}
	if node.ProtoReflect().Descriptor().Fields().ByName("p2p_endpoint") != nil {
		t.Fatal("ObservedNode must not expose p2p_endpoint")
	}
}

func TestPeekSchedulingContext(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)
	now := time.Unix(100, 0)
	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{AllocatedCpu: 2},
		P2PEndpoint:       &schedulerv1.P2PEndpoint{Backend: "iroh", Address: "node-a-iroh-endpoint"},
	}, now)

	context := registry.PeekSchedulingContext("node-a")
	if context.ClusterID != "cluster-a" || context.P2pBackend != "iroh" {
		t.Fatalf("unexpected locality context: %+v", context)
	}
	if context.Snapshot.GetAllocatedCpu() != 2 {
		t.Fatalf("unexpected snapshot: %+v", context.Snapshot)
	}

	// Scheduling callers must not be able to mutate registry-owned state.
	context.Snapshot.AllocatedCpu = 99
	if got := registry.PeekSchedulingContext("node-a").Snapshot.GetAllocatedCpu(); got != 2 {
		t.Fatalf("registry snapshot was mutated through scheduling context: %d", got)
	}
}

func TestPeekSchedulingContextWithoutP2PEndpoint(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)
	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{AllocatedCpu: 2},
	}, time.Unix(100, 0))

	context := registry.PeekSchedulingContext("node-a")
	if context.ClusterID != "cluster-a" || context.P2pBackend != "" {
		t.Fatalf("unexpected scheduling context: %+v", context)
	}
	if missing := registry.PeekSchedulingContext("missing"); missing != (NodeSchedulingContext{}) {
		t.Fatalf("missing node context = %+v, want zero value", missing)
	}
}

func TestUnregisterRemovesP2PEndpointPeer(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)
	now := time.Unix(100, 0)

	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
		P2PEndpoint:       &schedulerv1.P2PEndpoint{Backend: "iroh", Address: `{"id":"node-id"}`},
	}, now)
	if err := registry.UnregisterObserved("node-a", "svc-a"); err != nil {
		t.Fatalf("unregister: %v", err)
	}

	if nodes := registry.ListP2pPeers("cluster-a", "iroh", "", now); len(nodes) != 0 {
		t.Fatalf("expected no observed P2P peers after unregister, got %d", len(nodes))
	}
}

func TestListP2pPeersReturnsOnlyReadyMatchingPeers(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
		{ID: "node-c", Endpoint: "http://node-c"},
	}, defaultObservedReportTTL)
	now := time.Unix(100, 0)

	for _, nodeID := range []string{"node-a", "node-b"} {
		registry.Heartbeat(&schedulerv1.HeartbeatRequest{
			NodeId:            nodeID,
			ClusterId:         "cluster-1",
			ServiceInstanceId: "svc-" + nodeID,
			Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
			P2PEndpoint: &schedulerv1.P2PEndpoint{
				Backend: "iroh",
				Address: nodeID + "-iroh-endpoint",
			},
		}, now)
	}
	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-c",
		ClusterId:         "cluster-1",
		ServiceInstanceId: "svc-node-c",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
		P2PEndpoint:       &schedulerv1.P2PEndpoint{Backend: "other", Address: "node-c-other-endpoint"},
	}, now)

	peers := registry.ListP2pPeers("cluster-1", "iroh", "node-a", now)
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(peers))
	}
	if got := peers[0].GetNodeId(); got != "node-b" {
		t.Fatalf("expected node-b, got %s", got)
	}
	if got := peers[0].GetEndpoint().GetAddress(); got != "node-b-iroh-endpoint" {
		t.Fatalf("unexpected endpoint: %s", got)
	}
}

func TestListP2pPeersDropsExpiredAndUnregisteredNodes(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}, time.Second)
	start := time.Unix(100, 0)
	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-1",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
		P2PEndpoint:       &schedulerv1.P2PEndpoint{Backend: "iroh", Address: "node-a-iroh-endpoint"},
	}, start)
	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-b",
		ClusterId:         "cluster-1",
		ServiceInstanceId: "svc-b",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
		P2PEndpoint:       &schedulerv1.P2PEndpoint{Backend: "iroh", Address: "node-b-iroh-endpoint"},
	}, start)
	if err := registry.UnregisterObserved("node-b", "svc-b"); err != nil {
		t.Fatalf("unregister node-b: %v", err)
	}

	peers := registry.ListP2pPeers("cluster-1", "iroh", "", start.Add(2*time.Second))
	if len(peers) != 0 {
		t.Fatalf("expected no peers after ttl/unregister filtering, got %d", len(peers))
	}
}

func TestListObservedReturnsEmptySliceWhenNoNodes(t *testing.T) {
	registry := NewAtomicNodeRegistry(nil, 30*time.Second)
	nodes := registry.ListObserved("", time.Now())
	if len(nodes) != 0 {
		t.Fatalf("expected empty slice, got %d nodes", len(nodes))
	}
}

func TestListObservedReturnsEmptySliceWhenClusterFilterMatchesNothing(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)
	now := time.Unix(100, 0)

	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}, now)

	nodes := registry.ListObserved("cluster-z", now)
	if len(nodes) != 0 {
		t.Fatalf("expected 0 nodes for unknown cluster, got %d", len(nodes))
	}
}

func TestListObservedBecomesUnhealthyAfterTTL(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, time.Second)
	start := time.Unix(100, 0)

	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}, start)

	nodes := registry.ListObserved("", start.Add(2*time.Second))
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if got := nodes[0].GetSnapshot().GetStatus(); got != schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY {
		t.Fatalf("expected UNHEALTHY via ListObserved, got %v", got)
	}
}

func TestLingeringNodeBecomesUnhealthyAfterTTL(t *testing.T) {
	registry := NewAtomicNodeRegistry(nil, time.Second)
	start := time.Unix(100, 0)

	registry.Set(nil, []Node{{ID: "node-a", Endpoint: "http://node-a"}})
	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}, start)

	// Within TTL: no_schedule
	node, ok := registry.GetObserved("node-a", "", start)
	if !ok {
		t.Fatal("expected observed node")
	}
	if got := node.GetSnapshot().GetStatus(); got != schedulerv1.NodeStatus_NODE_STATUS_LINGERING {
		t.Fatalf("expected NO_SCHEDULE within TTL, got %v", got)
	}

	// After TTL: unhealthy overrides no_schedule
	node, ok = registry.GetObserved("node-a", "", start.Add(2*time.Second))
	if !ok {
		t.Fatal("expected observed node")
	}
	if got := node.GetSnapshot().GetStatus(); got != schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY {
		t.Fatalf("expected UNHEALTHY after TTL, got %v", got)
	}
}

func heartbeatWithConfig(t *testing.T, registry *AtomicNodeRegistry, nodeID, clusterID, svcID, cpuJSON string) string {
	t.Helper()
	var mi *schedulerv1.MachineInfo
	if cpuJSON != "" {
		mi = &schedulerv1.MachineInfo{CpuConfigJson: cpuJSON}
	}
	_, cpuConfigJSON, err := registry.Heartbeat(
		&schedulerv1.HeartbeatRequest{
			NodeId:            nodeID,
			ClusterId:         clusterID,
			ServiceInstanceId: svcID,
			MachineInfo:       mi,
		},
		time.Unix(100, 0),
	)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	return cpuConfigJSON
}

func TestHeartbeatReturnsCpuIntersectionForSingleNode(t *testing.T) {
	cfg := buildConfig(nil, []cpuidModifier{{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0xFF)}},
	}}, nil)

	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)

	result := heartbeatWithConfig(t, registry, "node-a", "cluster-1", "svc-a", cfg)
	if result == "" {
		t.Fatal("single-node cluster: expected intersection in response, got empty")
	}
	// Intersection of one config is the config itself.
	var got cpuConfig
	if err := json.Unmarshal([]byte(result), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.CpuidModifiers[0].Modifiers[0].Bitmap != bm32(0xFF) {
		t.Errorf("want %s, got %s", bm32(0xFF), got.CpuidModifiers[0].Modifiers[0].Bitmap)
	}
}

func TestHeartbeatWithholdsCpuIntersectionUntilAllNodesReady(t *testing.T) {
	cfg := buildConfig(nil, []cpuidModifier{{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0xFF)}},
	}}, nil)

	registry := NewAtomicNodeRegistry([]Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}, 30*time.Second)

	// Both nodes observed without CPU config so the cluster size is known.
	heartbeatWithConfig(t, registry, "node-a", "cluster-1", "svc-a", "")
	heartbeatWithConfig(t, registry, "node-b", "cluster-1", "svc-b", "")

	// node-a reports config but node-b has none yet: intersection not computable.
	result := heartbeatWithConfig(t, registry, "node-a", "cluster-1", "svc-a", cfg)
	if result != "" {
		t.Errorf("heartbeat before all observed nodes have configs: expected empty, got non-empty")
	}
}

func TestHeartbeatDeliversIntersectionExactlyOncePerNode(t *testing.T) {
	cfgA := buildConfig(nil, []cpuidModifier{{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0xFF)}},
	}}, nil)
	cfgB := buildConfig(nil, []cpuidModifier{{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0x0F)}},
	}}, nil)

	registry := NewAtomicNodeRegistry([]Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}, 30*time.Second)

	// Seed both nodes without CPU config so the cluster size is known.
	heartbeatWithConfig(t, registry, "node-a", "cluster-1", "svc-a", "")
	heartbeatWithConfig(t, registry, "node-b", "cluster-1", "svc-b", "")

	// node-a reports config: node-b still has none, intersection not computable.
	if r := heartbeatWithConfig(t, registry, "node-a", "cluster-1", "svc-a", cfgA); r != "" {
		t.Errorf("node-a 1st heartbeat: expected empty, got non-empty")
	}

	// node-b reports: triggers intersection, node-b gets it immediately.
	resultB := heartbeatWithConfig(t, registry, "node-b", "cluster-1", "svc-b", cfgB)
	if resultB == "" {
		t.Fatal("node-b heartbeat: expected intersection, got empty")
	}

	// node-a second heartbeat (no config): should now receive the intersection.
	resultA2 := heartbeatWithConfig(t, registry, "node-a", "cluster-1", "svc-a", "")
	if resultA2 == "" {
		t.Fatal("node-a 2nd heartbeat: expected intersection, got empty")
	}

	// node-a third heartbeat: already delivered, must be empty.
	if r := heartbeatWithConfig(t, registry, "node-a", "cluster-1", "svc-a", ""); r != "" {
		t.Errorf("node-a 3rd heartbeat: intersection already sent, expected empty, got non-empty")
	}
	// node-b second heartbeat: already delivered, must be empty.
	if r := heartbeatWithConfig(t, registry, "node-b", "cluster-1", "svc-b", ""); r != "" {
		t.Errorf("node-b 2nd heartbeat: intersection already sent, expected empty, got non-empty")
	}

	// Verify the intersection value is the bitwise AND of both configs.
	var result cpuConfig
	if err := json.Unmarshal([]byte(resultA2), &result); err != nil {
		t.Fatalf("unmarshal intersection: %v", err)
	}
	want := bm32(0xFF & 0x0F)
	got := result.CpuidModifiers[0].Modifiers[0].Bitmap
	if got != want {
		t.Errorf("intersection eax: want %s, got %s", want, got)
	}
}

func TestMultiClusterCpuIntersectionsAreIndependent(t *testing.T) {
	// cluster-x has nodes with bitmaps 0xFF and 0x0F → intersection 0x0F.
	// cluster-y has a single node with bitmap 0xF0 → intersection 0xF0.
	// Neither cluster's intersection should bleed into the other.
	cfgX1 := buildConfig(nil, []cpuidModifier{{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0xFF)}},
	}}, nil)
	cfgX2 := buildConfig(nil, []cpuidModifier{{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0x0F)}},
	}}, nil)
	cfgY := buildConfig(nil, []cpuidModifier{{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0xF0)}},
	}}, nil)

	registry := NewAtomicNodeRegistry([]Node{
		{ID: "x1", Endpoint: "http://x1"},
		{ID: "x2", Endpoint: "http://x2"},
		{ID: "y1", Endpoint: "http://y1"},
	}, 30*time.Second)

	// Seed without configs so cluster sizes are known before any config arrives.
	heartbeatWithConfig(t, registry, "x1", "cluster-x", "svc-x1", "")
	heartbeatWithConfig(t, registry, "x2", "cluster-x", "svc-x2", "")
	heartbeatWithConfig(t, registry, "y1", "cluster-y", "svc-y1", "")

	// x1 and x2 report configs; x2's heartbeat triggers the cluster-x intersection.
	heartbeatWithConfig(t, registry, "x1", "cluster-x", "svc-x1", cfgX1)
	heartbeatWithConfig(t, registry, "x2", "cluster-x", "svc-x2", cfgX2)

	// y1 reports its config; single-node cluster, gets its own config back.
	resultY := heartbeatWithConfig(t, registry, "y1", "cluster-y", "svc-y1", cfgY)
	if resultY == "" {
		t.Fatal("cluster-y single node: expected intersection, got empty")
	}

	// Collect the cluster-x intersection (delivered on the next heartbeat for x1).
	resultX := heartbeatWithConfig(t, registry, "x1", "cluster-x", "svc-x1", "")
	if resultX == "" {
		t.Fatal("cluster-x x1: expected intersection, got empty")
	}

	// cluster-x intersection must be 0xFF & 0x0F = 0x0F.
	var ix cpuConfig
	if err := json.Unmarshal([]byte(resultX), &ix); err != nil {
		t.Fatalf("unmarshal cluster-x intersection: %v", err)
	}
	if got, want := ix.CpuidModifiers[0].Modifiers[0].Bitmap, bm32(0xFF&0x0F); got != want {
		t.Errorf("cluster-x eax: want %s, got %s", want, got)
	}

	// cluster-y intersection must be 0xF0 (unaffected by cluster-x nodes).
	var iy cpuConfig
	if err := json.Unmarshal([]byte(resultY), &iy); err != nil {
		t.Fatalf("unmarshal cluster-y intersection: %v", err)
	}
	if got, want := iy.CpuidModifiers[0].Modifiers[0].Bitmap, bm32(0xF0); got != want {
		t.Errorf("cluster-y eax: want %s, got %s", want, got)
	}

	// cluster-x nodes must not receive cluster-y's intersection and vice versa:
	// subsequent heartbeats with no pending intersection must return empty.
	if r := heartbeatWithConfig(t, registry, "x2", "cluster-x", "svc-x2", ""); r != "" {
		var c cpuConfig
		_ = json.Unmarshal([]byte(r), &c)
		t.Errorf("cluster-x x2: got unexpected intersection %v", c)
	}
	if r := heartbeatWithConfig(t, registry, "y1", "cluster-y", "svc-y1", ""); r != "" {
		var c cpuConfig
		_ = json.Unmarshal([]byte(r), &c)
		t.Errorf("cluster-y y1: got unexpected second delivery %v", c)
	}
}
