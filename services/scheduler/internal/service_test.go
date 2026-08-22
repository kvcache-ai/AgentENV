package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type failingBindingStore struct{}

func (failingBindingStore) Get(string, time.Time) (Node, bool, error) {
	return Node{}, false, errors.New("binding store failed")
}

func (failingBindingStore) Record(string, Node, time.Time) error {
	return errors.New("binding store failed")
}

func (failingBindingStore) ReconcileNode(Node, []string, time.Time) error {
	return errors.New("binding store failed")
}

func registerObservedNodeForTest(t *testing.T, service *Service, nodeID string, serviceInstanceID string) {
	t.Helper()
	_, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            nodeID,
		ClusterId:         "cluster-1",
		ServiceInstanceId: serviceInstanceID,
		Version:           "0.1.0",
		Commit:            "abc123",
		Snapshot: &schedulerv1.NodeSnapshot{
			Status:               schedulerv1.NodeStatus_NODE_STATUS_READY,
			SandboxCount:         2,
			CreateSuccesses:      5,
			ReportedAtUnixMs:     123,
			AllocatedCpu:         2,
			MemoryUsedBytes:      128,
			MemoryTotalBytes:     256,
			AllocatedMemoryBytes: 512,
		},
	})
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
}

func TestLookupNodeReturnsRecordedAssignment(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL)
	service := NewService(
		zap.NewNop(),
		registry,
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)

	_, err := service.RecordAssignment(context.Background(), &schedulerv1.RecordAssignmentRequest{
		SandboxId: "sbx-1",
		Node:      (&Node{ID: "node-a", Endpoint: "http://node-a"}).ToProto(),
	})
	if err != nil {
		t.Fatalf("record assignment failed: %v", err)
	}

	resp, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"})
	if err != nil {
		t.Fatalf("lookup failed: %v", err)
	}
	if got := resp.GetNode().GetNodeId(); got != "node-a" {
		t.Fatalf("expected node-a, got %s", got)
	}
}

func TestListNodesReturnsActiveAndLingeringNodes(t *testing.T) {
	registry := NewAtomicNodeRegistry(nil, defaultObservedReportTTL)
	// node-a: active (included)
	// node-b: lingering (included)
	registry.Set(
		[]Node{{ID: "node-a", Endpoint: "http://node-a"}},
		[]Node{{ID: "node-b", Endpoint: "http://node-b"}},
	)

	service := NewService(
		zap.NewNop(),
		registry,
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)

	resp, err := service.ListNodes(context.Background(), &schedulerv1.ListNodesRequest{})
	if err != nil {
		t.Fatalf("list nodes failed: %v", err)
	}

	if len(resp.GetNodes()) != 2 {
		t.Fatalf("expected 2 nodes (active+lingering), got %d", len(resp.GetNodes()))
	}
	ids := map[string]bool{}
	for _, n := range resp.GetNodes() {
		ids[n.GetNodeId()] = true
	}
	if !ids["node-a"] || !ids["node-b"] {
		t.Fatalf("expected node-a and node-b, got %v", ids)
	}
}

func TestQueryOnlyServiceOnlySupportsLookupNode(t *testing.T) {
	store := NewInMemoryBindingStore(defaultObservedReportTTL)
	store.Record("sbx-1", Node{ID: "node-a", Endpoint: "http://node-a"}, time.Now())
	service := NewQueryOnlyService(zap.NewNop(), store)

	resp, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"})
	if err != nil {
		t.Fatalf("query-only lookup failed: %v", err)
	}
	if got := resp.GetNode().GetNodeId(); got != "node-a" {
		t.Fatalf("expected node-a, got %q", got)
	}

	_, err = service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("expected unsupported Schedule to return unimplemented, got %v", err)
	}
}

func TestLookupNodeReturnsUnavailableWhenBindingStoreFails(t *testing.T) {
	service := NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		failingBindingStore{},
	)

	_, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable, got %v", err)
	}
}

func TestLookupNodeReturnsNotFoundWhenAssignmentMissing(t *testing.T) {
	service := NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)

	_, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestRecordAssignmentReturnsUnavailableWhenBindingStoreFails(t *testing.T) {
	service := NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		failingBindingStore{},
	)

	_, err := service.RecordAssignment(context.Background(), &schedulerv1.RecordAssignmentRequest{
		SandboxId: "sbx-1",
		Node:      (&Node{ID: "node-a", Endpoint: "http://node-a"}).ToProto(),
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable, got %v", err)
	}
}

func TestRecordAssignmentRejectsUnknownNode(t *testing.T) {
	service := NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)

	_, err := service.RecordAssignment(context.Background(), &schedulerv1.RecordAssignmentRequest{
		SandboxId: "sbx-1",
		Node:      (&Node{ID: "node-x", Endpoint: "http://node-x"}).ToProto(),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func TestListNodesReflectsRegistryUpdates(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL)
	service := NewService(
		zap.NewNop(),
		registry,
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)

	registerObservedNodeForTest(t, service, "node-a", "svc-a")

	// Replace discovery with node-b; node-a is removed from discovery.
	registry.Set([]Node{{ID: "node-b", Endpoint: "http://node-b"}}, nil)
	registerObservedNodeForTest(t, service, "node-b", "svc-b")

	resp, err := service.ListNodes(context.Background(), &schedulerv1.ListNodesRequest{})
	if err != nil {
		t.Fatalf("list nodes failed: %v", err)
	}
	if len(resp.GetNodes()) != 1 {
		t.Fatalf("expected 1 node, got %d", len(resp.GetNodes()))
	}
	if got := resp.GetNodes()[0].GetNodeId(); got != "node-b" {
		t.Fatalf("expected node-b, got %q", got)
	}
}

func TestScheduleReturnsUnavailableWhenRegistryIsEmpty(t *testing.T) {
	service := NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry(nil, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)

	_, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: &schedulerv1.ScheduleRequestHint{Kind: &schedulerv1.ScheduleRequestHint_NewSandbox{NewSandbox: &schedulerv1.NewSandboxHint{}}}})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable, got %v", err)
	}
}

func TestGroupedRoundRobinScheduleSkipsStaleHeartbeat(t *testing.T) {
	registry := NewAtomicNodeRegistry(
		[]Node{
			{ID: "node-a", Endpoint: "http://node-a"},
			{ID: "node-b", Endpoint: "http://node-b"},
		},
		time.Second,
	)
	now := time.Now()
	for _, heartbeat := range []struct {
		nodeID string
		at     time.Time
	}{
		{nodeID: "node-a", at: now.Add(-time.Minute)},
		{nodeID: "node-b", at: now},
	} {
		_, _, err := registry.Heartbeat(&schedulerv1.HeartbeatRequest{
			NodeId:            heartbeat.nodeID,
			ClusterId:         "cluster-1",
			ServiceInstanceId: "service-" + heartbeat.nodeID,
			Snapshot: &schedulerv1.NodeSnapshot{
				Status: schedulerv1.NodeStatus_NODE_STATUS_READY,
			},
		}, heartbeat.at)
		if err != nil {
			t.Fatalf("heartbeat %s failed: %v", heartbeat.nodeID, err)
		}
	}

	service := NewService(
		zap.NewNop(),
		registry,
		NewGroupedRoundRobinStrategy(GroupedRoundRobinLimits{MaxSandboxCount: 2}),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)
	response, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{
		Hint: coldHint("ubuntu", 0, 0),
	})
	if err != nil {
		t.Fatalf("Schedule returned error: %v", err)
	}
	if got := response.GetNode().GetNodeId(); got != "node-b" {
		t.Fatalf("scheduled node = %q, want node-b", got)
	}
}

func TestScheduleOnlyConsidersReadyNodes(t *testing.T) {
	registry := NewAtomicNodeRegistry(nil, defaultObservedReportTTL)
	// node-a: active, node-b: lingering
	registry.Set(
		[]Node{{ID: "node-a", Endpoint: "http://node-a"}},
		[]Node{{ID: "node-b", Endpoint: "http://node-b"}},
	)
	service := NewService(
		zap.NewNop(),
		registry,
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)

	// Heartbeat both
	registerObservedNodeForTest(t, service, "node-a", "svc-a")
	registerObservedNodeForTest(t, service, "node-b", "svc-b")

	// Schedule should only pick node-a (ready), never node-b (no_schedule)
	for i := 0; i < 5; i++ {
		resp, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{})
		if err != nil {
			t.Fatalf("schedule failed: %v", err)
		}
		if got := resp.GetNode().GetNodeId(); got != "node-a" {
			t.Fatalf("iteration %d: expected node-a, got %s", i, got)
		}
	}
}

func TestGetObservedNodeAfterHeartbeat(t *testing.T) {
	service := NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)

	registerObservedNodeForTest(t, service, "node-a", "svc-a")

	resolved, err := service.GetNode(context.Background(), &schedulerv1.GetNodeRequest{
		NodeId:    "node-a",
		ClusterId: "cluster-1",
	})
	if err != nil {
		t.Fatalf("resolve observed node failed: %v", err)
	}
	if resolved.GetNode().GetNodeId() != "node-a" {
		t.Fatalf("unexpected resolved node id: %s", resolved.GetNode().GetNodeId())
	}
	if resolved.GetNode().GetSnapshot().GetStatus() != schedulerv1.NodeStatus_NODE_STATUS_READY {
		t.Fatalf("unexpected resolved status: %v", resolved.GetNode().GetSnapshot().GetStatus())
	}
	if resolved.GetNode().GetSnapshot().GetSandboxCount() != 2 {
		t.Fatalf("unexpected sandbox count: %d", resolved.GetNode().GetSnapshot().GetSandboxCount())
	}
}

func TestHeartbeatRebuildsSandboxBindings(t *testing.T) {
	service := NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)

	_, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-1",
		ServiceInstanceId: "svc-a",
		SandboxIds:        []string{"sbx-1", "sbx-2"},
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	})
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	for _, sandboxID := range []string{"sbx-1", "sbx-2"} {
		resp, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: sandboxID})
		if err != nil {
			t.Fatalf("lookup %s failed: %v", sandboxID, err)
		}
		if got := resp.GetNode().GetNodeId(); got != "node-a" {
			t.Fatalf("lookup %s returned node %q, want %q", sandboxID, got, "node-a")
		}
	}
}

func TestHeartbeatRemovesBindingsMissingFromRoster(t *testing.T) {
	service := NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)

	_, err := service.RecordAssignment(context.Background(), &schedulerv1.RecordAssignmentRequest{
		SandboxId: "sbx-1",
		Node:      (&Node{ID: "node-a", Endpoint: "http://node-a"}).ToProto(),
	})
	if err != nil {
		t.Fatalf("record assignment failed: %v", err)
	}
	_, err = service.RecordAssignment(context.Background(), &schedulerv1.RecordAssignmentRequest{
		SandboxId: "sbx-2",
		Node:      (&Node{ID: "node-a", Endpoint: "http://node-a"}).ToProto(),
	})
	if err != nil {
		t.Fatalf("record assignment failed: %v", err)
	}

	_, err = service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-1",
		ServiceInstanceId: "svc-a",
		SandboxIds:        []string{"sbx-1"},
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	})
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	if _, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"}); err != nil {
		t.Fatalf("expected sbx-1 to still be bound, got %v", err)
	}
	if _, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: "sbx-2"}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected missing binding to be removed, got %v", err)
	}
}

func TestUnregisterObservedNodeIsIdempotent(t *testing.T) {
	service := NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)

	registerObservedNodeForTest(t, service, "node-a", "svc-a")

	_, err := service.UnregisterNode(context.Background(), &schedulerv1.UnregisterNodeRequest{
		NodeId:            "node-a",
		ServiceInstanceId: "svc-a",
	})
	if err != nil {
		t.Fatalf("unregister failed: %v", err)
	}

	_, err = service.UnregisterNode(context.Background(), &schedulerv1.UnregisterNodeRequest{
		NodeId:            "node-a",
		ServiceInstanceId: "svc-a",
	})
	if err != nil {
		t.Fatalf("second unregister should be idempotent, got: %v", err)
	}
}

func TestUnregisterNodeRemovesBindingsForNode(t *testing.T) {
	service := NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)

	registerObservedNodeForTest(t, service, "node-a", "svc-a")

	_, err := service.RecordAssignment(context.Background(), &schedulerv1.RecordAssignmentRequest{
		SandboxId: "sbx-1",
		Node:      (&Node{ID: "node-a", Endpoint: "http://node-a"}).ToProto(),
	})
	if err != nil {
		t.Fatalf("record assignment failed: %v", err)
	}

	_, err = service.UnregisterNode(context.Background(), &schedulerv1.UnregisterNodeRequest{
		NodeId:            "node-a",
		ServiceInstanceId: "svc-a",
	})
	if err != nil {
		t.Fatalf("unregister failed: %v", err)
	}

	if _, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"}); status.Code(err) != codes.NotFound {
		t.Fatalf("expected binding to be removed on unregister, got %v", err)
	}
}

func TestP2pArtifactLookupReturnsReadyIndexedProviders(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
		{ID: "node-c", Endpoint: "http://node-c"},
	}, defaultObservedReportTTL)
	service := NewService(
		zap.NewNop(),
		registry,
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)

	for _, nodeID := range []string{"node-a", "node-b", "node-c"} {
		_, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
			NodeId:            nodeID,
			ClusterId:         "cluster-1",
			ServiceInstanceId: "svc-" + nodeID,
			Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
			P2PEndpoint: &schedulerv1.P2PEndpoint{
				Backend: "backend",
				Address: "addr-" + nodeID,
			},
		})
		if err != nil {
			t.Fatalf("heartbeat %s failed: %v", nodeID, err)
		}
	}

	for _, nodeID := range []string{"node-a", "node-b"} {
		_, err := service.RecordP2PArtifact(context.Background(), &schedulerv1.RecordP2PArtifactRequest{
			ClusterId: "cluster-1",
			Backend:   "backend",
			Key:       "artifact-key",
			NodeId:    nodeID,
		})
		if err != nil {
			t.Fatalf("record artifact %s failed: %v", nodeID, err)
		}
	}
	_, err := service.RecordP2PArtifact(context.Background(), &schedulerv1.RecordP2PArtifactRequest{
		ClusterId: "cluster-1",
		Backend:   "other",
		Key:       "artifact-key",
		NodeId:    "node-c",
	})
	if err != nil {
		t.Fatalf("record other backend artifact failed: %v", err)
	}

	resp, err := service.LookupP2PArtifact(context.Background(), &schedulerv1.LookupP2PArtifactRequest{
		ClusterId:     "cluster-1",
		Backend:       "backend",
		Key:           "artifact-key",
		ExcludeNodeId: "node-a",
	})
	if err != nil {
		t.Fatalf("lookup artifact failed: %v", err)
	}
	if len(resp.GetPeers()) != 1 || resp.GetPeers()[0].GetNodeId() != "node-b" {
		t.Fatalf("expected only node-b, got %v", resp.GetPeers())
	}
	if got := resp.GetPeers()[0].GetEndpoint().GetAddress(); got != "addr-node-b" {
		t.Fatalf("expected node-b p2p address, got %q", got)
	}
}

func TestP2pArtifactForgetAndUnregisterRemoveProviders(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}, defaultObservedReportTTL)
	service := NewService(
		zap.NewNop(),
		registry,
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)

	for _, nodeID := range []string{"node-a", "node-b"} {
		_, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
			NodeId:            nodeID,
			ClusterId:         "cluster-1",
			ServiceInstanceId: "svc-" + nodeID,
			Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
			P2PEndpoint:       &schedulerv1.P2PEndpoint{Backend: "backend", Address: "addr-" + nodeID},
		})
		if err != nil {
			t.Fatalf("heartbeat %s failed: %v", nodeID, err)
		}
		_, err = service.RecordP2PArtifact(context.Background(), &schedulerv1.RecordP2PArtifactRequest{
			ClusterId: "cluster-1",
			Backend:   "backend",
			Key:       "artifact-key",
			NodeId:    nodeID,
		})
		if err != nil {
			t.Fatalf("record artifact %s failed: %v", nodeID, err)
		}
	}

	_, err := service.ForgetP2PArtifact(context.Background(), &schedulerv1.ForgetP2PArtifactRequest{
		ClusterId: "cluster-1",
		Backend:   "backend",
		Key:       "artifact-key",
		NodeId:    "node-a",
	})
	if err != nil {
		t.Fatalf("forget artifact failed: %v", err)
	}
	resp, err := service.LookupP2PArtifact(context.Background(), &schedulerv1.LookupP2PArtifactRequest{
		ClusterId: "cluster-1",
		Backend:   "backend",
		Key:       "artifact-key",
	})
	if err != nil {
		t.Fatalf("lookup after forget failed: %v", err)
	}
	if len(resp.GetPeers()) != 1 || resp.GetPeers()[0].GetNodeId() != "node-b" {
		t.Fatalf("expected only node-b after forget, got %v", resp.GetPeers())
	}

	_, err = service.UnregisterNode(context.Background(), &schedulerv1.UnregisterNodeRequest{
		NodeId:            "node-b",
		ServiceInstanceId: "svc-node-b",
	})
	if err != nil {
		t.Fatalf("unregister failed: %v", err)
	}
	resp, err = service.LookupP2PArtifact(context.Background(), &schedulerv1.LookupP2PArtifactRequest{
		ClusterId: "cluster-1",
		Backend:   "backend",
		Key:       "artifact-key",
	})
	if err != nil {
		t.Fatalf("lookup after unregister failed: %v", err)
	}
	if len(resp.GetPeers()) != 0 {
		t.Fatalf("expected unregister to remove remaining artifact provider, got %v", resp.GetPeers())
	}
}

func TestP2pArtifactLookupExcludesExpiredNodes(t *testing.T) {
	shortTTL := 50 * time.Millisecond
	registry := NewAtomicNodeRegistry([]Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}, shortTTL)
	service := NewService(
		zap.NewNop(),
		registry,
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)

	for _, nodeID := range []string{"node-a", "node-b"} {
		_, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
			NodeId:            nodeID,
			ClusterId:         "cluster-1",
			ServiceInstanceId: "svc-" + nodeID,
			Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
			P2PEndpoint:       &schedulerv1.P2PEndpoint{Backend: "backend", Address: "addr-" + nodeID},
		})
		if err != nil {
			t.Fatalf("heartbeat %s failed: %v", nodeID, err)
		}
		_, err = service.RecordP2PArtifact(context.Background(), &schedulerv1.RecordP2PArtifactRequest{
			ClusterId: "cluster-1",
			Backend:   "backend",
			Key:       "artifact-key",
			NodeId:    nodeID,
		})
		if err != nil {
			t.Fatalf("record artifact %s failed: %v", nodeID, err)
		}
	}

	// Both nodes should be visible before TTL expires.
	resp, err := service.LookupP2PArtifact(context.Background(), &schedulerv1.LookupP2PArtifactRequest{
		ClusterId: "cluster-1",
		Backend:   "backend",
		Key:       "artifact-key",
	})
	if err != nil {
		t.Fatalf("lookup before expiry failed: %v", err)
	}
	if len(resp.GetPeers()) != 2 {
		t.Fatalf("expected 2 peers before TTL expiry, got %v", resp.GetPeers())
	}

	// Let node-a's heartbeat expire; refresh node-b.
	time.Sleep(shortTTL + 10*time.Millisecond)
	_, err = service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            "node-b",
		ClusterId:         "cluster-1",
		ServiceInstanceId: "svc-node-b",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
		P2PEndpoint:       &schedulerv1.P2PEndpoint{Backend: "backend", Address: "addr-node-b"},
	})
	if err != nil {
		t.Fatalf("refresh heartbeat node-b failed: %v", err)
	}

	// node-a is now UNHEALTHY (TTL expired); only node-b should be returned.
	resp, err = service.LookupP2PArtifact(context.Background(), &schedulerv1.LookupP2PArtifactRequest{
		ClusterId: "cluster-1",
		Backend:   "backend",
		Key:       "artifact-key",
	})
	if err != nil {
		t.Fatalf("lookup after expiry failed: %v", err)
	}
	if len(resp.GetPeers()) != 1 || resp.GetPeers()[0].GetNodeId() != "node-b" {
		t.Fatalf("expected only node-b after node-a TTL expiry, got %v", resp.GetPeers())
	}
}

func TestUnregisterObservedNodeRejectsServiceInstanceMismatch(t *testing.T) {
	service := NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)

	registerObservedNodeForTest(t, service, "node-a", "svc-a")

	_, err := service.UnregisterNode(context.Background(), &schedulerv1.UnregisterNodeRequest{
		NodeId:            "node-a",
		ServiceInstanceId: "svc-b",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected failed precondition, got %v", err)
	}
}

func TestHeartbeatRejectsUnknownNode(t *testing.T) {
	service := NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)

	_, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            "node-unknown",
		ServiceInstanceId: "svc-x",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func TestHeartbeatRejectsEmptyNodeID(t *testing.T) {
	service := NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)

	_, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            "",
		ServiceInstanceId: "svc-a",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}

func TestHeartbeatRejectsEmptyServiceInstanceID(t *testing.T) {
	service := NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)

	_, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ServiceInstanceId: "",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected invalid argument, got %v", err)
	}
}
