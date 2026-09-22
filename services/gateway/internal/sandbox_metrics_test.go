package gateway

import (
	schedulerv1 "agentenv/services/api/proto"
	"context"
	"encoding/json"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSandboxMetricsQueryValidation(t *testing.T) {
	const id1 = "00000000-0000-0000-0000-000000000001"
	const id2 = "00000000-0000-0000-0000-000000000002"
	for _, q := range []string{"", "sandbox_ids=", "sandbox_ids=not-a-uuid", "sandbox_ids=" + id1 + "," + id1, "sandbox_ids=" + id1 + ",", "sandbox_ids=" + id1 + "&sandbox_ids=" + id2, "sandbox_ids=" + strings.Repeat(id1+",", 100) + id2} {
		if _, err := parseSandboxMetricsIDs(httptest.NewRequest("GET", "/sandboxes/metrics?"+q, nil)); err == nil {
			t.Fatalf("accepted %q", q)
		}
	}
	ids, err := parseSandboxMetricsIDs(httptest.NewRequest("GET", "/sandboxes/metrics?sandbox_ids="+id1+","+id2, nil))
	if err != nil || len(ids) != 2 {
		t.Fatalf("CSV: %v %v", ids, err)
	}
}

func TestSandboxMetricsGatewayGroupsNodesAndPreservesAuth(t *testing.T) {
	const idA = "00000000-0000-0000-0000-00000000000a"
	const idA2 = "00000000-0000-0000-0000-0000000000a2"
	const idB = "00000000-0000-0000-0000-00000000000b"
	const idMissing = "00000000-0000-0000-0000-0000000000ff"
	const idUnavailable = "00000000-0000-0000-0000-0000000000ee"
	var requests atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/sandboxes/metrics" || r.Header.Get(headerAPIKey) != testAPIKey {
			t.Error("incorrect upstream path/auth")
		}
		ids, err := parseSandboxMetricsIDs(r)
		if err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		metrics := map[string]any{"unrequested": map[string]any{"memTotal": 1}}
		for _, id := range ids {
			if id != idUnavailable {
				metrics[id] = map[string]any{"memTotal": int64(8589934592)}
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"sandboxes": metrics})
	})
	a := httptest.NewServer(handler)
	defer a.Close()
	b := httptest.NewServer(handler)
	defer b.Close()
	scheduler := stubSchedulerClient{lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
		if req.SandboxId == idMissing {
			return nil, status.Error(codes.NotFound, "missing")
		}
		node := &schedulerv1.Node{NodeId: "a", Endpoint: a.URL}
		if req.SandboxId == idB {
			node = &schedulerv1.Node{NodeId: "b", Endpoint: b.URL}
		}
		return &schedulerv1.LookupNodeResponse{Node: node}, nil
	}}
	server := newTestServer(t, scheduler, time.Second, 1024)
	w := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(w, httptest.NewRequest("GET", "/sandboxes/metrics?sandbox_ids="+strings.Join([]string{idA, idA2, idB, idMissing, idUnavailable}, ","), nil))
	if w.Code != 200 {
		t.Fatalf("status %d: %s", w.Code, w.Body)
	}
	var result sandboxMetricsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Sandboxes) != 3 || requests.Load() != 2 {
		t.Fatalf("result %s requests %d", w.Body, requests.Load())
	}
	w = httptest.NewRecorder()
	server.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/sandboxes/metrics?sandbox_ids="+idA, nil))
	if w.Code != 401 {
		t.Fatalf("unauthenticated status %d", w.Code)
	}
}

func TestSandboxMetricsGatewaySingleRouteAndNodeFailure(t *testing.T) {
	const id = "00000000-0000-0000-0000-00000000000a"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sandboxes/a/metrics" {
			if r.URL.RawQuery != "start=1&end=2" {
				t.Error("lost range")
			}
			_, _ = w.Write([]byte("[]"))
			return
		}
		w.WriteHeader(500)
	}))
	defer upstream.Close()
	server := newTestServer(t, stubSchedulerClient{lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
		if req.SandboxId != "a" && req.SandboxId != id {
			t.Errorf("lookup %q", req.SandboxId)
		}
		return &schedulerv1.LookupNodeResponse{Node: &schedulerv1.Node{NodeId: "node", Endpoint: upstream.URL}}, nil
	}}, time.Second, 1024)
	for path, want := range map[string]int{"/sandboxes/a/metrics?start=1&end=2": 200, "/sandboxes/metrics?sandbox_ids=" + id: 500, "/sandboxes/metrics?sandbox_ids=" + id + "," + id: 400, "/sandboxes/metrics?sandbox_ids=not-a-uuid": 400} {
		w := httptest.NewRecorder()
		authenticatedTestHandler(server).ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != want {
			t.Errorf("%s: %d %s", path, w.Code, w.Body)
		}
	}
}
