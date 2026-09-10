package scheduler

import (
	"context"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHeartbeatDiscoveryRegistersAuthenticatedNode(t *testing.T) {
	registry := NewHeartbeatNodeRegistry(30 * time.Second)
	service := NewService(
		zap.NewNop(), registry, NewStrategy("round_robin"),
		NewInMemoryBindingStore(time.Minute),
		WithHeartbeatRegistrationToken("registration-secret"),
	)

	_, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		Endpoint:          "http://10.0.0.10:8000",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "service-a",
		RegistrationToken: "registration-secret",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	})
	if err != nil {
		t.Fatalf("heartbeat registration failed: %v", err)
	}

	nodes := registry.Snapshot(false)
	if len(nodes) != 1 || nodes[0].ID != "node-a" || nodes[0].Endpoint != "http://10.0.0.10:8000" {
		t.Fatalf("unexpected schedulable nodes: %#v", nodes)
	}
}

func TestHeartbeatDiscoveryRejectsBadTokenAndEndpoint(t *testing.T) {
	for name, tc := range map[string]struct {
		request  *schedulerv1.HeartbeatRequest
		wantCode codes.Code
	}{
		"bad token": {
			request: &schedulerv1.HeartbeatRequest{
				NodeId: "node-a", Endpoint: "http://10.0.0.10:8000", ServiceInstanceId: "service-a",
				RegistrationToken: "wrong",
			},
			wantCode: codes.Unauthenticated,
		},
		"missing endpoint": {
			request: &schedulerv1.HeartbeatRequest{
				NodeId: "node-a", ServiceInstanceId: "service-a", RegistrationToken: "registration-secret",
			},
			wantCode: codes.InvalidArgument,
		},
	} {
		t.Run(name, func(t *testing.T) {
			registry := NewHeartbeatNodeRegistry(30 * time.Second)
			service := NewService(
				zap.NewNop(), registry, NewStrategy("round_robin"),
				NewInMemoryBindingStore(time.Minute),
				WithHeartbeatRegistrationToken("registration-secret"),
			)
			_, err := service.Heartbeat(context.Background(), tc.request)
			if status.Code(err) != tc.wantCode {
				t.Fatalf("heartbeat code = %v, want %v (err=%v)", status.Code(err), tc.wantCode, err)
			}
			if got := registry.Snapshot(true); len(got) != 0 {
				t.Fatalf("rejected heartbeat registered nodes: %#v", got)
			}
		})
	}
}

func TestHeartbeatDiscoveryExpiresNodeFromScheduling(t *testing.T) {
	registry := NewHeartbeatNodeRegistry(time.Second)
	start := time.Unix(100, 0)
	_, _, err := registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId: "node-a", Endpoint: "http://10.0.0.10:8000", ClusterId: "cluster-a",
		ServiceInstanceId: "service-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}, start)
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
	registry.now = func() time.Time { return start.Add(2 * time.Second) }

	if got := registry.Snapshot(false); len(got) != 0 {
		t.Fatalf("expired node remains schedulable: %#v", got)
	}
	observed, ok := registry.GetObserved("node-a", "cluster-a", start.Add(2*time.Second))
	if !ok || observed.GetSnapshot().GetStatus() != schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY {
		t.Fatalf("expired node status = %#v, want unhealthy", observed)
	}
}

func TestHeartbeatDiscoveryCordonKeepsNodeRoutableButUnschedulable(t *testing.T) {
	registry := NewHeartbeatNodeRegistry(30 * time.Second)
	now := time.Unix(100, 0)
	registry.now = func() time.Time { return now }
	_, _, err := registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId: "node-a", Endpoint: "http://10.0.0.10:8000", ClusterId: "cluster-a",
		ServiceInstanceId: "service-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}, now)
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
	if err := registry.SetCordoned("node-a", "service-a", true); err != nil {
		t.Fatalf("cordon failed: %v", err)
	}

	if got := registry.Snapshot(false); len(got) != 0 {
		t.Fatalf("cordoned node remains schedulable: %#v", got)
	}
	if got := registry.Snapshot(true); len(got) != 1 || got[0].ID != "node-a" {
		t.Fatalf("cordoned node is not routable: %#v", got)
	}
	observed, ok := registry.GetObserved("node-a", "cluster-a", now)
	if !ok || observed.GetSnapshot().GetStatus() != schedulerv1.NodeStatus_NODE_STATUS_LINGERING {
		t.Fatalf("cordoned node status = %#v, want lingering", observed)
	}
}
