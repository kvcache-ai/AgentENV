package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFleetPlanEndpointForwardsInfrastructureMembership(t *testing.T) {
	client := stubSchedulerClient{
		getFleetPlanFunc: func(_ context.Context, req *schedulerv1.GetFleetPlanRequest, _ ...grpc.CallOption) (*schedulerv1.GetFleetPlanResponse, error) {
			if got := req.GetFleetNodeIds(); len(got) != 2 || got[0] != "node-a" || got[1] != "node-booting" {
				t.Fatalf("fleet node ids = %#v", got)
			}
			return &schedulerv1.GetFleetPlanResponse{
				DesiredNodes: 2, ReadyNodes: 1, ProvisioningNodes: 1,
				DeleteCandidates: []*schedulerv1.FleetNodeReference{{NodeId: "node-b", ServiceInstanceId: "service-b"}},
				Reason:           "workload_headroom",
			}, nil
		},
	}
	server := newTestServer(t, client, time.Second, 1<<20)
	req := httptest.NewRequest(http.MethodPost, "/fleet/plan", strings.NewReader(`{"fleetNodeIds":["node-a","node-booting"]}`))
	rec := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !strings.Contains(body, `"desiredNodes":2`) || !strings.Contains(body, `"nodeId":"node-b"`) {
		t.Fatalf("unexpected fleet plan body: %s", body)
	}
}

func TestScheduleWaitsForCapacityInsteadOfFailingFirstRequest(t *testing.T) {
	calls := 0
	client := stubSchedulerClient{
		scheduleFunc: func(_ context.Context, _ *schedulerv1.ScheduleRequest, _ ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			calls++
			if calls == 1 {
				return nil, status.Error(codes.Unavailable, "no nodes available")
			}
			return &schedulerv1.ScheduleResponse{Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: "http://node-a"}}, nil
		},
	}
	server := newTestServer(t, client, time.Second, 1<<20)
	server.scheduleRetryInterval = time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := server.scheduleWithCapacityWait(ctx, &schedulerv1.ScheduleRequest{})
	if err != nil {
		t.Fatalf("schedule failed: %v", err)
	}
	if calls != 2 || response.GetNode().GetNodeId() != "node-a" {
		t.Fatalf("schedule result calls=%d response=%#v", calls, response)
	}
}

func TestFleetCordonEndpointCarriesServiceGeneration(t *testing.T) {
	client := stubSchedulerClient{
		cordonNodeFunc: func(_ context.Context, req *schedulerv1.CordonNodeRequest, _ ...grpc.CallOption) (*schedulerv1.CordonNodeResponse, error) {
			if req.GetNodeId() != "node-a" || req.GetServiceInstanceId() != "service-a" {
				t.Fatalf("cordon request = %#v", req)
			}
			return &schedulerv1.CordonNodeResponse{}, nil
		},
	}
	server := newTestServer(t, client, time.Second, 1<<20)
	req := httptest.NewRequest(http.MethodPost, "/fleet/nodes/node-a/cordon", strings.NewReader(`{"serviceInstanceId":"service-a"}`))
	rec := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
}
