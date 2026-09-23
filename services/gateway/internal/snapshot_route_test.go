package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
)

func TestSnapshotPlacementForwardsBearerAuthentication(t *testing.T) {
	metadataNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+testAPIKey {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"snapshotType":"distributed"}`))
	}))
	defer metadataNode.Close()
	server := newTestServer(t, stubSchedulerClient{}, time.Second, 1024)
	req := httptest.NewRequest(http.MethodPost, "/snapshots/snap-1/promote", nil)
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	placement, err := server.lookupSnapshotPlacement(context.Background(), req,
		&schedulerv1.Node{Endpoint: metadataNode.URL}, "snap-1")
	if err != nil {
		t.Fatal(err)
	}
	if placement.SnapshotType != "distributed" {
		t.Fatalf("placement = %v, want distributed", placement)
	}
}

func TestLocalSnapshotMutationsRouteToOwner(t *testing.T) {
	for _, test := range []struct {
		method      string
		path        string
		snapshotRef string
	}{
		{http.MethodPost, "/snapshots/snap-1/promote", "snap-1"},
		{http.MethodDelete, "/templates/team%2Fbase%3Av1", "team/base:v1"},
		{http.MethodDelete, "/snapshots/team%2Fbase%3Av1", "team/base:v1"},
	} {
		t.Run(test.method, func(t *testing.T) {
			metadataNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != snapshotPlacementPath || r.URL.Query().Get("snapshotID") != test.snapshotRef {
					t.Errorf("placement request = %s, want reference %q", r.URL, test.snapshotRef)
				}
				_, _ = w.Write([]byte(`{"snapshotType":"local","ownerNodeID":"node-b"}`))
			}))
			t.Cleanup(metadataNode.Close)
			ownerPath := make(chan string, 1)
			ownerNode := ownerPathServer(t, ownerPath, http.StatusNoContent)
			server := newTestServer(t, stubSchedulerClient{
				scheduleFunc: snapshotMetadataNodeSchedule(t, "node-a", metadataNode.URL),
				getNodeFunc: func(context.Context, *schedulerv1.GetNodeRequest, ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
					return observedNodeResponse("node-b", ownerNode.URL, schedulerv1.NodeStatus_NODE_STATUS_READY), nil
				},
			}, time.Second, 1024)

			req := httptest.NewRequest(test.method, "http://gateway.test"+test.path, nil)
			req.Header.Set(headerSandboxID, "sbx-global-header")
			resp := httptest.NewRecorder()
			authenticatedTestHandler(server).ServeHTTP(resp, req)
			if resp.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want 204; body=%q", resp.Code, resp.Body.String())
			}
			if got := <-ownerPath; got != test.path {
				t.Fatalf("owner path = %q, want %q", got, test.path)
			}
		})
	}
}

func TestDistributedSnapshotTypeHintSkipsPlacementLookup(t *testing.T) {
	for _, test := range []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/snapshots/snap-1?type=distributed"},
		{http.MethodDelete, "/snapshots/snap-1?type=distributed"},
		{http.MethodDelete, "/templates/tmpl-1?type=distributed"},
	} {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			metadataNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == snapshotPlacementPath {
					t.Error("type=distributed must skip the placement lookup")
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				if got := r.URL.RequestURI(); got != test.path {
					t.Errorf("proxied request = %q, want the type hint forwarded upstream", got)
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			t.Cleanup(metadataNode.Close)
			server := newTestServer(t, stubSchedulerClient{
				scheduleFunc: snapshotMetadataNodeSchedule(t, "node-a", metadataNode.URL),
			}, time.Second, 1024)

			req := httptest.NewRequest(test.method, "http://gateway.test"+test.path, nil)
			resp := httptest.NewRecorder()
			authenticatedTestHandler(server).ServeHTTP(resp, req)
			if resp.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want 204; body=%q", resp.Code, resp.Body.String())
			}
		})
	}
}

func TestSnapshotSingleRecordResolvesPlacementWithoutDistributedHint(t *testing.T) {
	for _, test := range []struct {
		name        string
		method      string
		path        string
		snapshotRef string
	}{
		{"get without hint", http.MethodGet, "/snapshots/snap-1", "snap-1"},
		{"get local hint", http.MethodGet, "/snapshots/snap-1?type=local", "snap-1"},
		{"delete local hint", http.MethodDelete, "/snapshots/snap-1?type=local", "snap-1"},
		{"template delete without hint", http.MethodDelete, "/templates/tmpl-1", "tmpl-1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			metadataNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != snapshotPlacementPath || r.URL.Query().Get("snapshotID") != test.snapshotRef {
					t.Errorf("placement request = %s, want reference %q", r.URL, test.snapshotRef)
				}
				_, _ = w.Write([]byte(`{"snapshotType":"local","ownerNodeID":"node-b"}`))
			}))
			t.Cleanup(metadataNode.Close)
			ownerPath := make(chan string, 1)
			ownerNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ownerPath <- r.URL.RequestURI()
				w.WriteHeader(http.StatusNoContent)
			}))
			t.Cleanup(ownerNode.Close)
			server := newTestServer(t, stubSchedulerClient{
				scheduleFunc: snapshotMetadataNodeSchedule(t, "node-a", metadataNode.URL),
				getNodeFunc: func(context.Context, *schedulerv1.GetNodeRequest, ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
					return observedNodeResponse("node-b", ownerNode.URL, schedulerv1.NodeStatus_NODE_STATUS_READY), nil
				},
			}, time.Second, 1024)

			req := httptest.NewRequest(test.method, "http://gateway.test"+test.path, nil)
			resp := httptest.NewRecorder()
			authenticatedTestHandler(server).ServeHTTP(resp, req)
			if resp.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want 204; body=%q", resp.Code, resp.Body.String())
			}
			if got := <-ownerPath; got != test.path {
				t.Fatalf("owner request = %q, want %q", got, test.path)
			}
		})
	}
}

func TestDistributedSnapshotPromotionUsesSandboxAgnosticMetadataNode(t *testing.T) {
	originalRequest := make(chan string, 1)
	metadataNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == snapshotPlacementPath {
			_, _ = w.Write([]byte(`{"snapshotType":"distributed"}`))
			return
		}
		originalRequest <- r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer metadataNode.Close()

	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: snapshotMetadataNodeSchedule(t, "node-a", metadataNode.URL),
	}, time.Second, 1024)

	req := httptest.NewRequest(http.MethodPost, "http://gateway.test/snapshots/snap-1/promote", nil)
	resp := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(resp, req)

	if resp.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%q", resp.Code, resp.Body.String())
	}
	if got := <-originalRequest; got != "/snapshots/snap-1/promote" {
		t.Fatalf("scheduled node path = %q", got)
	}
}

func TestSnapshotPromotionPlacementFailureDoesNotProxy(t *testing.T) {
	originalRequest := make(chan struct{}, 1)
	metadataNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == snapshotPlacementPath {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		originalRequest <- struct{}{}
		w.WriteHeader(http.StatusCreated)
	}))
	defer metadataNode.Close()

	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: snapshotMetadataNodeSchedule(t, "node-a", metadataNode.URL),
	}, time.Second, 1024)

	req := httptest.NewRequest(http.MethodPost, "http://gateway.test/snapshots/snap-1/promote", nil)
	resp := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(resp, req)

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.Code)
	}
	if got := resp.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	var envelope struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error envelope: %v; body=%q", err, resp.Body.String())
	}
	if envelope.Code != http.StatusServiceUnavailable || envelope.Message != "snapshot placement unavailable" {
		t.Fatalf("error envelope = %+v", envelope)
	}
	select {
	case <-originalRequest:
		t.Fatal("placement failure forwarded the promotion request")
	default:
	}
}

func TestSnapshotOwnerDialFailureReturnsServiceUnavailable(t *testing.T) {
	metadataNode := snapshotPlacementServer(t, `{"snapshotType":"local","ownerNodeID":"node-b"}`)

	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: snapshotMetadataNodeSchedule(t, "node-a", metadataNode.URL),
		getNodeFunc: func(context.Context, *schedulerv1.GetNodeRequest, ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
			return observedNodeResponse("node-b", "http://127.0.0.1:1", schedulerv1.NodeStatus_NODE_STATUS_READY), nil
		},
	}, time.Second, 1024)

	req := httptest.NewRequest(http.MethodPost, "http://gateway.test/snapshots/snap-1/promote", nil)
	resp := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(resp, req)

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%q", resp.Code, resp.Body.String())
	}
}

func observedNodeResponse(nodeID, endpoint string, nodeStatus schedulerv1.NodeStatus) *schedulerv1.GetNodeResponse {
	return &schedulerv1.GetNodeResponse{Node: &schedulerv1.ObservedNode{
		NodeId:   nodeID,
		Endpoint: endpoint,
		Snapshot: &schedulerv1.NodeSnapshot{Status: nodeStatus},
	}}
}

func snapshotPlacementServer(t *testing.T, response string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != snapshotPlacementPath {
			t.Errorf("metadata node received unexpected path %q", r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(response))
	}))
	t.Cleanup(server.Close)
	return server
}

func ownerPathServer(t *testing.T, path chan<- string, status int) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path <- r.URL.EscapedPath()
		w.WriteHeader(status)
	}))
	t.Cleanup(server.Close)
	return server
}

func snapshotMetadataNodeSchedule(t *testing.T, id, endpoint string) func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
	t.Helper()
	return func(_ context.Context, req *schedulerv1.ScheduleRequest, _ ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
		if !req.GetSandboxAgnostic() {
			t.Error("snapshot metadata node selection must use the sandbox-agnostic schedule fast path")
		}
		return &schedulerv1.ScheduleResponse{Node: &schedulerv1.Node{NodeId: id, Endpoint: endpoint}}, nil
	}
}
