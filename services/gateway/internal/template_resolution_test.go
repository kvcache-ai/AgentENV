package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	schedulerv1 "agentenv/services/api/proto"
	"google.golang.org/grpc"
)

func TestResolveSnapshotLocalityHintResolvesTemplateAlias(t *testing.T) {
	const canonicalID = "550e8400-e29b-41d4-a716-446655440000"

	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/templates/aliases/warm-template" {
			t.Fatalf("unexpected alias lookup: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"templateID": canonicalID,
			"public":     false,
		})
	}))
	defer node.Close()

	server := newTestServer(t, stubSchedulerClient{
		listNodesFunc: func(_ context.Context, _ *schedulerv1.ListNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
			return &schedulerv1.ListNodesResponse{Nodes: []*schedulerv1.Node{{
				NodeId:   "node-a",
				Endpoint: node.URL,
			}}}, nil
		},
	}, 0, 1024)

	request := newHintRequest(t, http.MethodPost, "/sandboxes", `{"templateID":"warm-template"}`)
	hint, err := buildScheduleHint(request)
	if err != nil {
		t.Fatalf("buildScheduleHint returned error: %v", err)
	}
	hint = server.resolveSnapshotLocalityHint(context.Background(), request, hint)

	want := []string{
		"snapshot/v1/artifacts/" + canonicalID + "/vm_state.bin",
		"snapshot/v1/artifacts/" + canonicalID + "/firecracker-manifest.json",
	}
	if got := localityRequirementKeys(hint.GetLocalityRequirements()); !equalStrings(got, want) {
		t.Fatalf("locality requirements = %v, want %v", got, want)
	}
}

func TestResolveSnapshotLocalityHintNormalizesCanonicalID(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{}, 0, 1024)
	request := newHintRequest(t, http.MethodPost, "/sandboxes", `{"templateID":"550E8400-E29B-41D4-A716-446655440000"}`)
	hint, err := buildScheduleHint(request)
	if err != nil {
		t.Fatalf("buildScheduleHint returned error: %v", err)
	}
	hint = server.resolveSnapshotLocalityHint(context.Background(), request, hint)

	keys := localityRequirementKeys(hint.GetLocalityRequirements())
	if len(keys) != 2 || keys[0] != "snapshot/v1/artifacts/550e8400-e29b-41d4-a716-446655440000/vm_state.bin" {
		t.Fatalf("unexpected normalized locality requirements: %v", keys)
	}
}

func TestResolveSnapshotLocalityHintDropsUnresolvedAlias(t *testing.T) {
	node := httptest.NewServer(http.NotFoundHandler())
	defer node.Close()

	server := newTestServer(t, stubSchedulerClient{
		listNodesFunc: func(_ context.Context, _ *schedulerv1.ListNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
			return &schedulerv1.ListNodesResponse{Nodes: []*schedulerv1.Node{{
				NodeId:   "node-a",
				Endpoint: node.URL,
			}}}, nil
		},
	}, 0, 1024)

	request := newHintRequest(t, http.MethodPost, "/sandboxes", `{"templateID":"missing-template"}`)
	hint, err := buildScheduleHint(request)
	if err != nil {
		t.Fatalf("buildScheduleHint returned error: %v", err)
	}
	hint = server.resolveSnapshotLocalityHint(context.Background(), request, hint)
	if got := hint.GetLocalityRequirements(); len(got) != 0 {
		t.Fatalf("unresolved alias produced locality requirements: %v", got)
	}
}
