package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"google.golang.org/grpc"
)

func TestResolveSnapshotLocalityHintNormalizesCanonicalID(t *testing.T) {
	request := newHintRequest(t, http.MethodPost, "/sandboxes", `{"templateID":"550E8400-E29B-41D4-A716-446655440000"}`)
	hint, err := buildScheduleHint(request)
	if err != nil {
		t.Fatalf("buildScheduleHint returned error: %v", err)
	}
	hint = resolveSnapshotLocalityHint(hint)

	want := []string{
		"snapshot/v1/artifacts/550e8400-e29b-41d4-a716-446655440000/vm_state.bin",
		"snapshot/v1/artifacts/550e8400-e29b-41d4-a716-446655440000/firecracker-manifest.json",
	}
	if got := localityRequirementKeys(hint.GetLocalityRequirements()); !equalStrings(got, want) {
		t.Fatalf("locality requirements = %v, want %v", got, want)
	}
}

func TestResolveSnapshotLocalityHintSkipsAliases(t *testing.T) {
	for _, templateID := range []string{
		"warm-template",
		"550e8400e29b41d4a716446655440000",
		"550e8400-e29b-41d4-a716-44665544000z",
	} {
		t.Run(templateID, func(t *testing.T) {
			request := newHintRequest(t, http.MethodPost, "/sandboxes", fmt.Sprintf(`{"templateID":%q}`, templateID))
			hint, err := buildScheduleHint(request)
			if err != nil {
				t.Fatalf("buildScheduleHint returned error: %v", err)
			}
			hint = resolveSnapshotLocalityHint(hint)
			if got := len(hint.GetLocalityRequirements()); got != 0 {
				t.Fatalf("alias %q produced %d locality requirements", templateID, got)
			}
		})
	}
}

func TestHandleProxyAliasDoesNotFanOut(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"sandboxID":"sbx-created"}`)
	}))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(_ context.Context, req *schedulerv1.ScheduleRequest, _ ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			if got := len(req.GetHint().GetLocalityRequirements()); got != 0 {
				return nil, fmt.Errorf("alias produced %d locality requirements", got)
			}
			return &schedulerv1.ScheduleResponse{Node: &schedulerv1.Node{
				NodeId:   "node-a",
				Endpoint: upstream.URL,
			}}, nil
		},
		recordAssignmentFunc: func(_ context.Context, _ *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}, time.Second, 1024)

	gateway := httptest.NewServer(server.Handler())
	defer gateway.Close()

	resp, err := http.Post(
		gateway.URL+"/sandboxes",
		"application/json",
		strings.NewReader(`{"templateID":"warm-template"}`),
	)
	if err != nil {
		t.Fatalf("gateway request failed: %v", err)
	}
	defer resp.Body.Close()
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatalf("read gateway response failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
}
