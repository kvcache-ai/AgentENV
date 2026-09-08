package gateway

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type stubSchedulerClient struct {
	scheduleFunc           func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error)
	listNodesFunc          func(context.Context, *schedulerv1.ListNodesRequest, ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error)
	lookupNodeFunc         func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error)
	recordAssignmentFunc   func(context.Context, *schedulerv1.RecordAssignmentRequest, ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error)
	heartbeatFunc          func(context.Context, *schedulerv1.HeartbeatRequest, ...grpc.CallOption) (*schedulerv1.HeartbeatResponse, error)
	reportSandboxEventFunc func(context.Context, *schedulerv1.ReportSandboxEventRequest, ...grpc.CallOption) (*schedulerv1.ReportSandboxEventResponse, error)
	listObservedFunc       func(context.Context, *schedulerv1.ListObservedNodesRequest, ...grpc.CallOption) (*schedulerv1.ListObservedNodesResponse, error)
	listP2pPeersFunc       func(context.Context, *schedulerv1.ListP2PPeersRequest, ...grpc.CallOption) (*schedulerv1.ListP2PPeersResponse, error)
	recordP2pArtifactFunc  func(context.Context, *schedulerv1.RecordP2PArtifactRequest, ...grpc.CallOption) (*schedulerv1.RecordP2PArtifactResponse, error)
	forgetP2pArtifactFunc  func(context.Context, *schedulerv1.ForgetP2PArtifactRequest, ...grpc.CallOption) (*schedulerv1.ForgetP2PArtifactResponse, error)
	lookupP2pArtifactFunc  func(context.Context, *schedulerv1.LookupP2PArtifactRequest, ...grpc.CallOption) (*schedulerv1.LookupP2PArtifactResponse, error)
	getNodeFunc            func(context.Context, *schedulerv1.GetNodeRequest, ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error)
	unregisterNodeFunc     func(context.Context, *schedulerv1.UnregisterNodeRequest, ...grpc.CallOption) (*schedulerv1.UnregisterNodeResponse, error)
}

type trackingReadCloser struct {
	readInvoked *bool
	bodyClosed  *bool
}

func (t trackingReadCloser) Read(_ []byte) (int, error) {
	if t.readInvoked != nil {
		*t.readInvoked = true
	}
	return 0, io.EOF
}

func (t trackingReadCloser) Close() error {
	if t.bodyClosed != nil {
		*t.bodyClosed = true
	}
	return nil
}

func (s stubSchedulerClient) Schedule(ctx context.Context, req *schedulerv1.ScheduleRequest, opts ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
	if s.scheduleFunc == nil {
		return nil, fmt.Errorf("unexpected Schedule call")
	}
	return s.scheduleFunc(ctx, req, opts...)
}

func (s stubSchedulerClient) ListNodes(ctx context.Context, req *schedulerv1.ListNodesRequest, opts ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
	if s.listNodesFunc == nil {
		return nil, fmt.Errorf("unexpected ListNodes call")
	}
	return s.listNodesFunc(ctx, req, opts...)
}

func (s stubSchedulerClient) LookupNode(ctx context.Context, req *schedulerv1.LookupNodeRequest, opts ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
	if s.lookupNodeFunc == nil {
		return nil, fmt.Errorf("unexpected LookupNode call")
	}
	return s.lookupNodeFunc(ctx, req, opts...)
}

func (s stubSchedulerClient) RecordAssignment(ctx context.Context, req *schedulerv1.RecordAssignmentRequest, opts ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
	if s.recordAssignmentFunc == nil {
		return nil, fmt.Errorf("unexpected RecordAssignment call")
	}
	return s.recordAssignmentFunc(ctx, req, opts...)
}

func (s stubSchedulerClient) Heartbeat(ctx context.Context, req *schedulerv1.HeartbeatRequest, opts ...grpc.CallOption) (*schedulerv1.HeartbeatResponse, error) {
	if s.heartbeatFunc == nil {
		return nil, fmt.Errorf("unexpected Heartbeat call")
	}
	return s.heartbeatFunc(ctx, req, opts...)
}

func (s stubSchedulerClient) ListObservedNodes(ctx context.Context, req *schedulerv1.ListObservedNodesRequest, opts ...grpc.CallOption) (*schedulerv1.ListObservedNodesResponse, error) {
	if s.listObservedFunc == nil {
		return nil, fmt.Errorf("unexpected ListObservedNodes call")
	}
	return s.listObservedFunc(ctx, req, opts...)
}

func (s stubSchedulerClient) ReportSandboxEvent(ctx context.Context, req *schedulerv1.ReportSandboxEventRequest, opts ...grpc.CallOption) (*schedulerv1.ReportSandboxEventResponse, error) {
	if s.reportSandboxEventFunc == nil {
		return nil, fmt.Errorf("unexpected ReportSandboxEvent call")
	}
	return s.reportSandboxEventFunc(ctx, req, opts...)
}

func (s stubSchedulerClient) ListP2PPeers(ctx context.Context, req *schedulerv1.ListP2PPeersRequest, opts ...grpc.CallOption) (*schedulerv1.ListP2PPeersResponse, error) {
	if s.listP2pPeersFunc == nil {
		return nil, fmt.Errorf("unexpected ListP2pPeers call")
	}
	return s.listP2pPeersFunc(ctx, req, opts...)
}

func (s stubSchedulerClient) RecordP2PArtifact(ctx context.Context, req *schedulerv1.RecordP2PArtifactRequest, opts ...grpc.CallOption) (*schedulerv1.RecordP2PArtifactResponse, error) {
	if s.recordP2pArtifactFunc == nil {
		return nil, fmt.Errorf("unexpected RecordP2PArtifact call")
	}
	return s.recordP2pArtifactFunc(ctx, req, opts...)
}

func (s stubSchedulerClient) ForgetP2PArtifact(ctx context.Context, req *schedulerv1.ForgetP2PArtifactRequest, opts ...grpc.CallOption) (*schedulerv1.ForgetP2PArtifactResponse, error) {
	if s.forgetP2pArtifactFunc == nil {
		return nil, fmt.Errorf("unexpected ForgetP2PArtifact call")
	}
	return s.forgetP2pArtifactFunc(ctx, req, opts...)
}

func (s stubSchedulerClient) LookupP2PArtifact(ctx context.Context, req *schedulerv1.LookupP2PArtifactRequest, opts ...grpc.CallOption) (*schedulerv1.LookupP2PArtifactResponse, error) {
	if s.lookupP2pArtifactFunc == nil {
		return nil, fmt.Errorf("unexpected LookupP2PArtifact call")
	}
	return s.lookupP2pArtifactFunc(ctx, req, opts...)
}

func (s stubSchedulerClient) GetNode(ctx context.Context, req *schedulerv1.GetNodeRequest, opts ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
	if s.getNodeFunc == nil {
		return nil, fmt.Errorf("unexpected GetNode call")
	}
	return s.getNodeFunc(ctx, req, opts...)
}

func (s stubSchedulerClient) UnregisterNode(ctx context.Context, req *schedulerv1.UnregisterNodeRequest, opts ...grpc.CallOption) (*schedulerv1.UnregisterNodeResponse, error) {
	if s.unregisterNodeFunc == nil {
		return nil, fmt.Errorf("unexpected UnregisterNode call")
	}
	return s.unregisterNodeFunc(ctx, req, opts...)
}

const testAPIKey = "test-api-key"

type testServerOption func(*ServerOptions)

func newTestServer(t *testing.T, schedulerClient schedulerv1.SchedulerClient, timeout time.Duration, maxRespSize int64, opts ...testServerOption) *Server {
	t.Helper()

	options := ServerOptions{
		RequestTimeout:  timeout,
		MaxResponseSize: maxRespSize,
		APIKey:          testAPIKey,
	}
	for _, opt := range opts {
		opt(&options)
	}

	server, err := NewServer(zap.NewNop(), schedulerClient, options)
	if err != nil {
		t.Fatalf("new gateway server failed: %v", err)
	}
	return server
}

func authenticatedTestHandler(server *Server) http.Handler {
	handler := server.Handler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set(headerAPIKey, testAPIKey)
		handler.ServeHTTP(w, r)
	})
}

func TestNewServerRejectsEmptyAPIKey(t *testing.T) {
	_, err := NewServer(zap.NewNop(), stubSchedulerClient{}, ServerOptions{
		RequestTimeout:  time.Second,
		MaxResponseSize: 1024,
	})
	if err == nil {
		t.Fatal("NewServer accepted an empty API key")
	}
}

func TestGatewayRequiresExactAPIKey(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{}, time.Second, 1024)
	handler := server.Handler()
	tests := []struct {
		name       string
		addHeaders func(http.Header)
		wantStatus int
	}{
		{
			name:       "missing",
			addHeaders: func(http.Header) {},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "wrong",
			addHeaders: func(headers http.Header) {
				headers.Set(headerAPIKey, "wrong-key")
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "authorization is application data",
			addHeaders: func(headers http.Header) {
				headers.Set("Authorization", "Bearer "+testAPIKey)
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "valid",
			addHeaders: func(headers http.Header) {
				headers.Set(headerAPIKey, testAPIKey)
			},
			wantStatus: http.StatusBadGateway,
		},
		{
			name: "duplicate",
			addHeaders: func(headers http.Header) {
				headers.Add(headerAPIKey, testAPIKey)
				headers.Add(headerAPIKey, testAPIKey)
			},
			wantStatus: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/nodes", nil)
			tt.addHeaders(req.Header)
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, req)

			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, tt.wantStatus)
			}
		})
	}
}

func TestGatewayMetricsPathDoesNotRequireAPIKey(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{}, time.Second, 1024)
	recorder := httptest.NewRecorder()

	server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusNotFound)
	}
}

func TestGatewayLeavesDataPlaneAuthorizationToRuntime(t *testing.T) {
	const sandboxID = "0191f4d0-7b2a-7c11-9c2d-0123456789ab"
	lookupCalls := 0
	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			lookupCalls++
			return nil, fmt.Errorf("lookup reached")
		},
	}, time.Second, 1024)
	handler := server.Handler()

	for i, tt := range []struct {
		port, header, value string
	}{
		{port: "8080"},
		{port: "49983", header: headerTrafficToken, value: "runtime-validates-this-token"},
		{port: "49983", header: headerTrafficToken, value: "wrong-token"},
		{port: "8080", header: headerEnvdAccessToken, value: "runtime-validates-this-token"},
	} {
		req := httptest.NewRequest(http.MethodGet, "/proxy", nil)
		req.Header.Set(headerE2BSandboxID, sandboxID)
		req.Header.Set(headerE2BTargetPort, tt.port)
		if tt.header != "" {
			req.Header.Set(tt.header, tt.value)
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)
		if recorder.Code == http.StatusUnauthorized || lookupCalls != i+1 {
			t.Fatalf("data plane case %d: status=%d lookup calls=%d", i, recorder.Code, lookupCalls)
		}
	}

	req := httptest.NewRequest(http.MethodPost, "/sandboxes/"+sandboxID+"/pause", nil)
	req.Header.Set(headerE2BSandboxID, sandboxID)
	req.Header.Set(headerTrafficToken, "runtime-validates-this-token")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusUnauthorized || lookupCalls != 4 {
		t.Fatalf("scoped token reached control plane: status=%d lookup calls=%d", recorder.Code, lookupCalls)
	}
}

func TestGatewayRejectsIncompleteProxyRouteBeforeScheduling(t *testing.T) {
	scheduleCalls := 0
	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			scheduleCalls++
			return nil, fmt.Errorf("schedule reached")
		},
	}, time.Second, 1024)
	handler := server.Handler()

	for _, headers := range []http.Header{
		{},
		{headerE2BSandboxID: []string{"sandbox-only"}},
		{headerE2BTargetPort: []string{"8080"}},
	} {
		req := httptest.NewRequest(http.MethodGet, "/proxy", nil)
		req.Header = headers
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, req)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
		}
	}
	if scheduleCalls != 0 {
		t.Fatalf("schedule calls = %d, want 0", scheduleCalls)
	}
}

func withSandboxProxyDomains(domains ...string) testServerOption {
	return func(options *ServerOptions) {
		options.SandboxProxyDomains = domains
	}
}

func withDebugMode(enabled bool) testServerOption {
	return func(options *ServerOptions) {
		options.DebugMode = enabled
	}
}

func withQueryOnlyScheduler(client schedulerv1.SchedulerClient) testServerOption {
	return func(options *ServerOptions) {
		options.QueryOnlySchedulerClient = client
	}
}

func TestSandboxIDFromHeadersPrimary(t *testing.T) {
	h := http.Header{}
	h.Set("x-agentenv-sandbox-id", "abc123")
	id, ok := sandboxIDFromHeaders(h)
	if !ok || id != "abc123" {
		t.Fatalf("expected abc123, got %q (ok=%v)", id, ok)
	}
}

func TestSandboxIDFromHeadersE2B(t *testing.T) {
	h := http.Header{}
	h.Set("e2b-sandbox-id", "xyz")
	id, ok := sandboxIDFromHeaders(h)
	if !ok || id != "xyz" {
		t.Fatalf("expected xyz, got %q (ok=%v)", id, ok)
	}
}

func TestSandboxIDFromHeadersMissing(t *testing.T) {
	_, ok := sandboxIDFromHeaders(http.Header{})
	if ok {
		t.Fatal("expected no sandbox id when headers absent")
	}
}

func TestHasProxyRoutingHeaders(t *testing.T) {
	agentenvSandboxHeader := http.Header{}
	agentenvSandboxHeader.Set(headerSandboxID, "sbx-1")

	e2bPortHeader := http.Header{}
	e2bPortHeader.Set(headerE2BTargetPort, "49983")

	tests := []struct {
		name    string
		headers http.Header
		want    bool
	}{
		{
			name:    "agentenv sandbox header",
			headers: agentenvSandboxHeader,
			want:    true,
		},
		{
			name:    "e2b port header",
			headers: e2bPortHeader,
			want:    true,
		},
		{
			name:    "no proxy headers",
			headers: http.Header{},
			want:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasProxyRoutingHeaders(tc.headers); got != tc.want {
				t.Fatalf("hasProxyRoutingHeaders() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSandboxIDFromPath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
		ok   bool
	}{
		{name: "base sandbox path", path: "/sandboxes/sbx-123", want: "sbx-123", ok: true},
		{name: "sandbox pause path", path: "/sandboxes/sbx-123/pause", want: "sbx-123", ok: true},
		{name: "sandbox timeout path", path: "/sandboxes/sbx-123/timeout", want: "sbx-123", ok: true},
		{name: "sandbox list path", path: "/sandboxes", ok: false},
		{name: "cold sandbox create path", path: "/sandboxes-cold", ok: false},
		{name: "v2 sandbox list path", path: "/v2/sandboxes", ok: false},
		{name: "other resource", path: "/templates/abc", ok: false},
		{name: "empty segment", path: "/sandboxes//pause", ok: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := sandboxIDFromPath(tc.path)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("path %q expected (%q, %v), got (%q, %v)", tc.path, tc.want, tc.ok, got, ok)
			}
		})
	}
}

func TestIsSandboxControlPlaneRequest(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
		want   bool
	}{
		{name: "sandbox detail", method: http.MethodGet, path: "/sandboxes/sbx-123", want: true},
		{name: "sandbox delete", method: http.MethodDelete, path: "/sandboxes/sbx-123", want: true},
		{name: "sandbox pause", method: http.MethodPost, path: "/sandboxes/sbx-123/pause", want: true},
		{name: "sandbox fork", method: http.MethodPost, path: "/sandboxes/sbx-123/fork", want: true},
		{name: "sandbox CPU affinity", method: http.MethodPost, path: "/sandboxes/sbx-123/cpu-affinity", want: true},
		{name: "sandbox CPU affinity wrong method", method: http.MethodGet, path: "/sandboxes/sbx-123/cpu-affinity", want: false},
		{name: "sandbox network update", method: http.MethodPut, path: "/sandboxes/sbx-123/network", want: true},
		{name: "network update wrong method", method: http.MethodPost, path: "/sandboxes/sbx-123/network", want: false},
		{name: "sandbox custom extension params get", method: http.MethodGet, path: "/sandboxes/sbx-123/custom-extension-params", want: true},
		{name: "sandbox custom extension params patch", method: http.MethodPatch, path: "/sandboxes/sbx-123/custom-extension-params", want: true},
		{name: "sandbox custom extension params post", method: http.MethodPost, path: "/sandboxes/sbx-123/custom-extension-params", want: false},
		{name: "data-plane shaped path", method: http.MethodGet, path: "/sandboxes/sbx-123/files", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(tc.method, "http://gateway.test"+tc.path, nil)
			if got := isSandboxControlPlaneRequest(request); got != tc.want {
				t.Fatalf("isSandboxControlPlaneRequest(%s %s) = %v, want %v", tc.method, tc.path, got, tc.want)
			}
		})
	}
}

func TestNodeIDFromPath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
		ok   bool
	}{
		{name: "node detail", path: "/nodes/node-a", want: "node-a", ok: true},
		{name: "node detail trailing slash", path: "/nodes/node-a/", want: "node-a", ok: true},
		{name: "nodes list", path: "/nodes", ok: false},
		{name: "nested path", path: "/nodes/node-a/extra", ok: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := nodeIDFromPath(tc.path)
			if got != tc.want || ok != tc.ok {
				t.Fatalf("nodeIDFromPath(%q) = (%q, %v), want (%q, %v)", tc.path, got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestWriteJSONEncodeErrorReturnsInternalServerError(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{}, time.Second, 1024)
	recorder := httptest.NewRecorder()

	server.writeJSON(recorder, http.StatusOK, map[string]any{
		"unsupported": make(chan int),
	})

	resp := recorder.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status code = %d, want %d", resp.StatusCode, http.StatusInternalServerError)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/plain;") {
		t.Fatalf("content type = %q, want text/plain error response", got)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !strings.Contains(string(body), "failed to encode response") {
		t.Fatalf("body = %q, want encode failure message", string(body))
	}
}

func TestHandleProxyReturnsAggregatedNodesFromScheduler(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{
		listObservedFunc: func(_ context.Context, req *schedulerv1.ListObservedNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListObservedNodesResponse, error) {
			if req.GetClusterId() != "cluster-1" {
				t.Fatalf("unexpected cluster id: %s", req.GetClusterId())
			}

			return &schedulerv1.ListObservedNodesResponse{
				Nodes: []*schedulerv1.ObservedNode{
					{
						NodeId:            "node-a",
						ClusterId:         "cluster-1",
						ServiceInstanceId: "svc-a",
						Version:           "0.1.0",
						Commit:            "abc123",
						MachineInfo: &schedulerv1.MachineInfo{
							CpuFamily:       "6",
							CpuModel:        "158",
							CpuModelName:    "Intel",
							CpuArchitecture: "x86_64",
						},
						Snapshot: &schedulerv1.NodeSnapshot{
							Status:               schedulerv1.NodeStatus_NODE_STATUS_READY,
							SandboxCount:         3,
							SandboxStartingCount: 1,
							AllocatedCpu:         4,
							AllocatedMemoryBytes: 1024,
							CpuPercent:           50,
							CpuCount:             8,
							MemoryUsedBytes:      2048,
							MemoryTotalBytes:     4096,
							CreateSuccesses:      9,
							CreateFails:          2,
						},
					},
				},
			}, nil
		},
	}, 5*time.Second, 4<<20)

	request := httptest.NewRequest(http.MethodGet, "http://gateway.test/nodes?clusterID=cluster-1", nil)
	response := httptest.NewRecorder()

	authenticatedTestHandler(server).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", response.Code)
	}

	var nodes []map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &nodes); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}

	if got := nodes[0]["id"]; got != "node-a" {
		t.Fatalf("unexpected node id field: %v", got)
	}

	if got := nodes[0]["status"]; got != "ready" {
		t.Fatalf("unexpected status field: %v", got)
	}
}

func TestHandleProxyDirectForwardsNodeDetail(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/nodes/node-a" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("clusterID"); got != "cluster-1" {
			t.Fatalf("unexpected clusterID query: %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"node-a"}`))
	}))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		getNodeFunc: func(_ context.Context, req *schedulerv1.GetNodeRequest, _ ...grpc.CallOption) (*schedulerv1.GetNodeResponse, error) {
			if req.GetNodeId() != "node-a" {
				t.Fatalf("unexpected node id: %s", req.GetNodeId())
			}
			if req.GetClusterId() != "cluster-1" {
				t.Fatalf("unexpected cluster id: %s", req.GetClusterId())
			}
			return &schedulerv1.GetNodeResponse{
				Node: &schedulerv1.ObservedNode{NodeId: "node-a", Endpoint: upstream.URL},
			}, nil
		},
	}, 5*time.Second, 4<<20)

	request := httptest.NewRequest(http.MethodGet, "http://gateway.test/nodes/node-a?clusterID=cluster-1", nil)
	response := httptest.NewRecorder()

	authenticatedTestHandler(server).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", response.Code)
	}
	if strings.TrimSpace(response.Body.String()) != `{"id":"node-a"}` {
		t.Fatalf("unexpected response body: %s", response.Body.String())
	}
}

func TestLookupNodeUsesQueryOnlySchedulerClient(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	mainLookupCalled := make(chan struct{}, 1)
	mainScheduler := stubSchedulerClient{
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			mainLookupCalled <- struct{}{}
			return nil, fmt.Errorf("main scheduler lookup should not be used")
		},
	}
	queryScheduler := stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			if req.GetSandboxId() != "sbx-1" {
				return nil, fmt.Errorf("lookup sandbox id = %q, want %q", req.GetSandboxId(), "sbx-1")
			}
			return &schedulerv1.LookupNodeResponse{Node: &schedulerv1.Node{NodeId: "node-1", Endpoint: upstream.URL}}, nil
		},
	}
	server := newTestServer(t, mainScheduler, time.Second, 1024, withQueryOnlyScheduler(queryScheduler))

	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	defer gatewayServer.Close()

	req, err := http.NewRequest(http.MethodGet, gatewayServer.URL+"/health", nil)
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	req.Header.Set(headerSandboxID, "sbx-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
	select {
	case <-mainLookupCalled:
		t.Fatal("main scheduler handled LookupNode; want query-only scheduler")
	default:
	}
}

func TestSandboxIDExtractionPathPreferredOverHeader(t *testing.T) {
	h := http.Header{}
	h.Set("x-agentenv-sandbox-id", "from-header")

	id, ok := sandboxIDFromPath("/sandboxes/from-path/pause")
	if !ok {
		id, ok = sandboxIDFromHeaders(h)
	}

	if !ok || id != "from-path" {
		t.Fatalf("expected path sandbox id to win, got %q (ok=%v)", id, ok)
	}
}

func TestSandboxControlPlaneRequestWithE2BHeadersUsesPathRoute(t *testing.T) {
	type upstreamRequestSnapshot struct {
		path      string
		sandboxID string
	}

	requests := make(chan upstreamRequestSnapshot, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- upstreamRequestSnapshot{
			path:      r.URL.Path,
			sandboxID: r.Header.Get(headerE2BSandboxID),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"sandboxID":"sbx-path"}`))
	}))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			if req.GetSandboxId() != "sbx-path" {
				return nil, fmt.Errorf("lookup sandbox id = %q, want %q", req.GetSandboxId(), "sbx-path")
			}
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{
					NodeId:   "node-1",
					Endpoint: upstream.URL,
				},
			}, nil
		},
	}, time.Second, 1024)

	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	defer gatewayServer.Close()

	req, err := http.NewRequest(http.MethodPost, gatewayServer.URL+"/sandboxes/sbx-path/connect", strings.NewReader(`{"timeout":60}`))
	if err != nil {
		t.Fatalf("build connect request failed: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(headerE2BSandboxID, "sbx-path")
	req.Header.Set(headerE2BTargetPort, "49983")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("connect request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("connect status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	upstreamReq := <-requests
	if upstreamReq.path != "/sandboxes/sbx-path/connect" {
		t.Fatalf("upstream path = %q, want %q", upstreamReq.path, "/sandboxes/sbx-path/connect")
	}
	if upstreamReq.sandboxID != "sbx-path" {
		t.Fatalf("forwarded e2b sandbox id = %q, want %q", upstreamReq.sandboxID, "sbx-path")
	}
}

func TestCPUControlPlaneRequestUsesSandboxLookupAndPreservesUpstreamRequest(t *testing.T) {
	type upstreamRequest struct {
		method string
		path   string
		body   string
	}
	requests := make(chan upstreamRequest, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests <- upstreamRequest{
			method: r.Method,
			path:   r.URL.Path,
			body:   string(body),
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			if req.GetSandboxId() != "sbx-cpu" {
				return nil, fmt.Errorf("lookup sandbox id = %q, want sbx-cpu", req.GetSandboxId())
			}
			return &schedulerv1.LookupNodeResponse{Node: &schedulerv1.Node{
				NodeId:   "node-cpu",
				Endpoint: upstream.URL,
			}}, nil
		},
	}, time.Second, 1024)

	const body = `{"vcpu":"*","core":"2-3"}`
	request := httptest.NewRequest(http.MethodPost, "http://gateway.test/sandboxes/sbx-cpu/cpu-affinity", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusOK, response.Body.String())
	}
	snapshot := <-requests
	if snapshot.method != http.MethodPost || snapshot.path != "/sandboxes/sbx-cpu/cpu-affinity" || snapshot.body != body {
		t.Fatalf("unexpected upstream request: %#v", snapshot)
	}
}

func TestShouldRecordAssignment(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		path       string
		route      routeSource
		hasSandbox bool
		want       bool
	}{
		{name: "create sandbox", method: http.MethodPost, path: "/sandboxes", route: routeSourceSchedule, hasSandbox: false, want: true},
		{name: "create sandbox with trailing slash", method: http.MethodPost, path: "/sandboxes/", route: routeSourceSchedule, hasSandbox: false, want: true},
		{name: "create cold sandbox", method: http.MethodPost, path: "/sandboxes-cold", route: routeSourceSchedule, hasSandbox: false, want: true},
		{name: "create cold sandbox with trailing slash", method: http.MethodPost, path: "/sandboxes-cold/", route: routeSourceSchedule, hasSandbox: false, want: true},
		{name: "fork sandbox records child assignment", method: http.MethodPost, path: "/sandboxes/sbx-1/fork", route: routeSourcePath, hasSandbox: true, want: true},
		{name: "fork-shaped host route is data plane", method: http.MethodPost, path: "/sandboxes/sbx-1/fork", route: routeSourceHost, hasSandbox: true, want: false},
		{name: "fork-shaped header route is data plane", method: http.MethodPost, path: "/sandboxes/sbx-1/fork", route: routeSourceHeader, hasSandbox: true, want: false},
		{name: "list sandboxes", method: http.MethodGet, path: "/sandboxes", route: routeSourceSchedule, hasSandbox: false, want: false},
		{name: "get cold sandbox path", method: http.MethodGet, path: "/sandboxes-cold", route: routeSourceSchedule, hasSandbox: false, want: false},
		{name: "control plane path", method: http.MethodPost, path: "/sandboxes/sbx-1/pause", route: routeSourcePath, hasSandbox: true, want: false},
		{name: "other post path", method: http.MethodPost, path: "/templates", route: routeSourceSchedule, hasSandbox: false, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, tc.path, nil)
			if err != nil {
				t.Fatalf("build request failed: %v", err)
			}
			got := shouldRecordAssignment(req, tc.route, tc.hasSandbox)
			if got != tc.want {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
		})
	}
}

func TestSandboxIDFromHeadersOnResponse(t *testing.T) {
	h := http.Header{}
	h.Set("X-Agentenv-Sandbox-Id", "resp-sbx-1")
	id, ok := sandboxIDFromHeaders(h)
	if !ok || id != "resp-sbx-1" {
		t.Fatalf("expected sandbox id from response header, got %q (ok=%v)", id, ok)
	}
}

func TestExtractSandboxIDFromResponse(t *testing.T) {
	id, ok := extractSandboxIDFromResponse([]byte(`{"sandboxID":"sbx-123"}`))
	if !ok || id != "sbx-123" {
		t.Fatalf("expected sandbox id to be extracted, got %q (ok=%v)", id, ok)
	}
	ids := extractSandboxIDsFromResponse([]byte(`{"sandboxes":[{"sandboxID":"sbx-1"},{"sandboxID":"sbx-2"}]}`))
	if !equalStrings(ids, []string{"sbx-1", "sbx-2"}) {
		t.Fatalf("expected batch sandbox ids, got %#v", ids)
	}
	forkBody := []byte(`[{"sandbox":{"sandboxID":"sbx-child-1"}},{"sandbox":{"sandboxID":"sbx-child-2"}},{"error":{"message":"failed"}}]`)
	if ids := extractSandboxIDsFromResponse(forkBody); len(ids) != 0 {
		t.Fatalf("generic response parser should not infer fork ids, got %#v", ids)
	}
	if ids := extractForkSandboxIDsFromResponse(forkBody); !equalStrings(ids, []string{"sbx-child-1", "sbx-child-2"}) {
		t.Fatalf("expected fork child sandbox ids, got %#v", ids)
	}
}

func TestUpstreamTargetPath(t *testing.T) {
	tests := []struct {
		name        string
		routeSource routeSource
		path        string
		want        string
	}{
		{
			name:        "header route prefixes /proxy",
			routeSource: routeSourceHeader,
			path:        "/sandboxes/sbx-1/files",
			want:        "/proxy/sandboxes/sbx-1/files",
		},
		{
			name:        "host route prefixes /proxy",
			routeSource: routeSourceHost,
			path:        "/readyz",
			want:        "/proxy/readyz",
		},
		{
			name:        "header route root path",
			routeSource: routeSourceHeader,
			path:        "/",
			want:        "/proxy/",
		},
		{
			name:        "path route unchanged",
			routeSource: routeSourcePath,
			path:        "/sandboxes/sbx-1/pause",
			want:        "/sandboxes/sbx-1/pause",
		},
		{
			name:        "schedule route unchanged",
			routeSource: routeSourceSchedule,
			path:        "/sandboxes",
			want:        "/sandboxes",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := upstreamTargetPath(tc.routeSource, tc.path)
			if got != tc.want {
				t.Fatalf("upstreamTargetPath(%q, %q) = %q, want %q", string(tc.routeSource), tc.path, got, tc.want)
			}
		})
	}
}

func TestUpstreamTargetEscapedPath(t *testing.T) {
	tests := []struct {
		name        string
		routeSource routeSource
		path        string
		want        string
	}{
		{
			name:        "header route prefixes /proxy",
			routeSource: routeSourceHeader,
			path:        "/sandboxes/a%2Fb/%2525",
			want:        "/proxy/sandboxes/a%2Fb/%2525",
		},
		{
			name:        "host route prefixes /proxy",
			routeSource: routeSourceHost,
			path:        "/api/foo%2Fbar",
			want:        "/proxy/api/foo%2Fbar",
		},
		{
			name:        "path route unchanged",
			routeSource: routeSourcePath,
			path:        "/sandboxes/a%2Fb/%2525",
			want:        "/sandboxes/a%2Fb/%2525",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := upstreamTargetEscapedPath(tc.routeSource, tc.path)
			if got != tc.want {
				t.Fatalf("upstreamTargetEscapedPath(%q, %q) = %q, want %q", string(tc.routeSource), tc.path, got, tc.want)
			}
		})
	}
}

func TestRequestEscapedPath(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "http://example.test/sandboxes/a%2Fb/%2525?x=1", nil)
	if got := requestEscapedPath(req); got != "/sandboxes/a%2Fb/%2525" {
		t.Fatalf("requestEscapedPath() = %q, want %q", got, "/sandboxes/a%2Fb/%2525")
	}
}

func TestJoinUpstreamPreservesRawEscapedPath(t *testing.T) {
	target, err := joinUpstream(
		"http://upstream:8000/base",
		"/proxy/sandboxes/a/b/%25",
		"/proxy/sandboxes/a%2Fb/%2525",
		"x=1",
	)
	if err != nil {
		t.Fatalf("joinUpstream() error = %v", err)
	}
	parsed, err := url.Parse(target)
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}
	if parsed.EscapedPath() != "/base/proxy/sandboxes/a%2Fb/%2525" {
		t.Fatalf("EscapedPath() = %q, want %q", parsed.EscapedPath(), "/base/proxy/sandboxes/a%2Fb/%2525")
	}
}

func mustListedSandbox(id string, startedAt string, state string, envdVersion string) listedSandbox {
	parsed, err := time.Parse(time.RFC3339Nano, startedAt)
	if err != nil {
		panic(err)
	}
	return listedSandbox{
		TemplateID:  "template",
		SandboxID:   id,
		ClientID:    "client",
		StartedAt:   parsed.UTC(),
		EndAt:       parsed.UTC().Add(time.Hour),
		CPUCount:    1,
		MemoryMB:    128,
		DiskSizeMB:  0,
		Metadata:    map[string]string{"team": "alpha"},
		State:       state,
		EnvdVersion: envdVersion,
	}
}

func decodeListedSandboxResponse(t *testing.T, body io.Reader) []listedSandbox {
	t.Helper()
	var items []listedSandbox
	if err := json.NewDecoder(body).Decode(&items); err != nil {
		t.Fatalf("decode listed sandbox response failed: %v", err)
	}
	return items
}

func sandboxIDs(items []listedSandbox) []string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.SandboxID)
	}
	return ids
}

func TestHandleProxyAggregatesSandboxListAcrossNodes(t *testing.T) {
	type upstreamRequestSnapshot struct {
		query         url.Values
		host          string
		forwardedHost string
		forwardedURI  string
	}

	requests := make(chan upstreamRequestSnapshot, 2)
	newNode := func(items []listedSandbox) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests <- upstreamRequestSnapshot{
				query:         r.URL.Query(),
				host:          r.Host,
				forwardedHost: r.Header.Get("X-Forwarded-Host"),
				forwardedURI:  r.Header.Get("X-Forwarded-URI"),
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(items)
		}))
	}

	nodeA := newNode([]listedSandbox{
		mustListedSandbox("00000000-0000-0000-0000-000000000002", "2026-01-01T00:00:02Z", "running", "envd-a"),
	})
	defer nodeA.Close()
	nodeB := newNode([]listedSandbox{
		mustListedSandbox("00000000-0000-0000-0000-000000000001", "2026-01-01T00:00:02Z", "running", "envd-b"),
		mustListedSandbox("00000000-0000-0000-0000-000000000003", "2026-01-01T00:00:01Z", "running", "envd-c"),
	})
	defer nodeB.Close()

	server := newTestServer(t, stubSchedulerClient{
		listNodesFunc: func(_ context.Context, _ *schedulerv1.ListNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
			return &schedulerv1.ListNodesResponse{
				Nodes: []*schedulerv1.Node{
					{NodeId: "node-a", Endpoint: nodeA.URL},
					{NodeId: "node-b", Endpoint: nodeB.URL},
				},
			}, nil
		},
	}, time.Second, 1024)

	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	defer gatewayServer.Close()

	req, err := http.NewRequest(http.MethodGet, gatewayServer.URL+"/sandboxes?metadata=team%3Dalpha", nil)
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	req.Host = "gateway.test"

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("aggregate sandbox list request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	items := decodeListedSandboxResponse(t, resp.Body)
	if got := sandboxIDs(items); !equalStrings(got, []string{
		"00000000-0000-0000-0000-000000000001",
		"00000000-0000-0000-0000-000000000002",
		"00000000-0000-0000-0000-000000000003",
	}) {
		t.Fatalf("sandbox ids = %v", got)
	}

	for i := 0; i < 2; i++ {
		upstreamReq := <-requests
		if upstreamReq.query.Get("metadata") != "team=alpha" {
			t.Fatalf("metadata query = %q, want %q", upstreamReq.query.Get("metadata"), "team=alpha")
		}
		if upstreamReq.host != "gateway.test" {
			t.Fatalf("upstream host = %q, want %q", upstreamReq.host, "gateway.test")
		}
		if upstreamReq.forwardedHost != "gateway.test" {
			t.Fatalf("X-Forwarded-Host = %q, want %q", upstreamReq.forwardedHost, "gateway.test")
		}
		if upstreamReq.forwardedURI != "/sandboxes?metadata=team%3Dalpha" {
			t.Fatalf("X-Forwarded-URI = %q, want %q", upstreamReq.forwardedURI, "/sandboxes?metadata=team%3Dalpha")
		}
	}
}

func TestHandleProxyAggregatesV2SandboxesWithGlobalPagination(t *testing.T) {
	requests := make(chan url.Values, 4)
	newNode := func(items []listedSandbox) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests <- r.URL.Query()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(items)
		}))
	}

	nodeA := newNode([]listedSandbox{
		mustListedSandbox("00000000-0000-0000-0000-000000000003", "2026-01-01T00:00:03Z", "running", "envd-a"),
	})
	defer nodeA.Close()
	nodeB := newNode([]listedSandbox{
		mustListedSandbox("00000000-0000-0000-0000-000000000002", "2026-01-01T00:00:02Z", "paused", "envd-b"),
		mustListedSandbox("00000000-0000-0000-0000-000000000001", "2026-01-01T00:00:01Z", "running", "envd-c"),
	})
	defer nodeB.Close()

	server := newTestServer(t, stubSchedulerClient{
		listNodesFunc: func(_ context.Context, _ *schedulerv1.ListNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
			return &schedulerv1.ListNodesResponse{
				Nodes: []*schedulerv1.Node{
					{NodeId: "node-a", Endpoint: nodeA.URL},
					{NodeId: "node-b", Endpoint: nodeB.URL},
				},
			}, nil
		},
	}, time.Second, 1024)

	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	defer gatewayServer.Close()

	req, err := http.NewRequest(http.MethodGet, gatewayServer.URL+"/v2/sandboxes?metadata=team%3Dalpha&state=running%2Cpaused&order=desc&startedAfter=2026-01-01T00%3A00%3A00Z&template=tmpl&limit=2", nil)
	if err != nil {
		t.Fatalf("build first page request failed: %v", err)
	}
	req.Host = "gateway.test"

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("first page request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first page status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	pageOne := decodeListedSandboxResponse(t, resp.Body)
	if got := sandboxIDs(pageOne); !equalStrings(got, []string{
		"00000000-0000-0000-0000-000000000003",
		"00000000-0000-0000-0000-000000000002",
	}) {
		t.Fatalf("first page ids = %v", got)
	}

	nextToken := resp.Header.Get("x-next-token")
	if nextToken == "" {
		t.Fatal("expected x-next-token on first page")
	}
	if got := resp.Header.Get("x-total-running"); got != "2" {
		t.Fatalf("first page x-total-running = %q, want 2", got)
	}

	req, err = http.NewRequest(http.MethodGet, gatewayServer.URL+"/v2/sandboxes?metadata=team%3Dalpha&state=running%2Cpaused&order=desc&startedAfter=2026-01-01T00%3A00%3A00Z&template=tmpl&limit=2&nextToken="+url.QueryEscape(nextToken), nil)
	if err != nil {
		t.Fatalf("build second page request failed: %v", err)
	}
	req.Host = "gateway.test"

	respTwo, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("second page request failed: %v", err)
	}
	defer respTwo.Body.Close()

	if respTwo.StatusCode != http.StatusOK {
		t.Fatalf("second page status = %d, want %d", respTwo.StatusCode, http.StatusOK)
	}

	pageTwo := decodeListedSandboxResponse(t, respTwo.Body)
	if got := sandboxIDs(pageTwo); !equalStrings(got, []string{
		"00000000-0000-0000-0000-000000000001",
	}) {
		t.Fatalf("second page ids = %v", got)
	}
	if got := respTwo.Header.Get("x-next-token"); got != "" {
		t.Fatalf("second page x-next-token = %q, want empty", got)
	}
	if got := respTwo.Header.Get("x-total-running"); got != "2" {
		t.Fatalf("second page x-total-running = %q, want 2", got)
	}

	for i := 0; i < 4; i++ {
		query := <-requests
		if query.Get("metadata") != "team=alpha" {
			t.Fatalf("metadata query = %q, want %q", query.Get("metadata"), "team=alpha")
		}
		if query.Get("state") != "running,paused" {
			t.Fatalf("state query = %q, want %q", query.Get("state"), "running,paused")
		}
		if query.Get("order") != "desc" {
			t.Fatalf("order query = %q, want %q", query.Get("order"), "desc")
		}
		if query.Get("startedAfter") != "2026-01-01T00:00:00Z" {
			t.Fatalf("startedAfter query = %q, want %q", query.Get("startedAfter"), "2026-01-01T00:00:00Z")
		}
		if query.Get("template") != "tmpl" {
			t.Fatalf("template query = %q, want %q", query.Get("template"), "tmpl")
		}
		if query.Get("limit") != "" {
			t.Fatalf("limit query = %q, want empty", query.Get("limit"))
		}
		if query.Get("nextToken") != "" {
			t.Fatalf("nextToken query = %q, want empty", query.Get("nextToken"))
		}
	}
}

func TestHandleProxyAggregatesV2SandboxesAscendingPagination(t *testing.T) {
	newNode := func(items []listedSandbox) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if got := r.URL.Query().Get("order"); got != "asc" {
				t.Errorf("order query = %q, want asc", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(items)
		}))
	}

	nodeA := newNode([]listedSandbox{
		mustListedSandbox("00000000-0000-0000-0000-000000000003", "2026-01-01T00:00:02Z", "running", "envd-a"),
	})
	defer nodeA.Close()
	nodeB := newNode([]listedSandbox{
		mustListedSandbox("00000000-0000-0000-0000-000000000001", "2026-01-01T00:00:01Z", "running", "envd-b"),
		mustListedSandbox("00000000-0000-0000-0000-000000000002", "2026-01-01T00:00:02Z", "paused", "envd-c"),
	})
	defer nodeB.Close()

	server := newTestServer(t, stubSchedulerClient{
		listNodesFunc: func(_ context.Context, _ *schedulerv1.ListNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
			return &schedulerv1.ListNodesResponse{
				Nodes: []*schedulerv1.Node{
					{NodeId: "node-a", Endpoint: nodeA.URL},
					{NodeId: "node-b", Endpoint: nodeB.URL},
				},
			}, nil
		},
	}, time.Second, 1024)

	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	defer gatewayServer.Close()

	request := func(nextToken string) (*http.Response, error) {
		query := "order=asc&limit=2"
		if nextToken != "" {
			query += "&nextToken=" + url.QueryEscape(nextToken)
		}
		req, err := http.NewRequest(http.MethodGet, gatewayServer.URL+"/v2/sandboxes?"+query, nil)
		if err != nil {
			return nil, err
		}
		req.Host = "gateway.test"
		return http.DefaultClient.Do(req)
	}

	resp, err := request("")
	if err != nil {
		t.Fatalf("first page request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first page status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	nextToken := resp.Header.Get("x-next-token")
	if nextToken == "" {
		t.Fatal("expected x-next-token on first page")
	}
	if got := resp.Header.Get("x-total-running"); got != "2" {
		t.Fatalf("first page x-total-running = %q, want 2", got)
	}
	pageOne := decodeListedSandboxResponse(t, resp.Body)
	_ = resp.Body.Close()
	if got := sandboxIDs(pageOne); !equalStrings(got, []string{
		"00000000-0000-0000-0000-000000000001",
		"00000000-0000-0000-0000-000000000003",
	}) {
		t.Fatalf("first page ids = %v", got)
	}

	resp, err = request(nextToken)
	if err != nil {
		t.Fatalf("second page request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("second page status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	pageTwo := decodeListedSandboxResponse(t, resp.Body)
	_ = resp.Body.Close()
	if got := sandboxIDs(pageTwo); !equalStrings(got, []string{
		"00000000-0000-0000-0000-000000000002",
	}) {
		t.Fatalf("second page ids = %v", got)
	}
	if got := resp.Header.Get("x-next-token"); got != "" {
		t.Fatalf("second page x-next-token = %q, want empty", got)
	}
	if got := resp.Header.Get("x-total-running"); got != "2" {
		t.Fatalf("second page x-total-running = %q, want 2", got)
	}
}

func TestHandleProxyAggregatesSandboxListDedupsDuplicateSandboxIDs(t *testing.T) {
	newNode := func(items []listedSandbox) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(items)
		}))
	}

	nodeA := newNode([]listedSandbox{
		mustListedSandbox("00000000-0000-0000-0000-000000000001", "2026-01-01T00:00:01Z", "running", "envd-old"),
	})
	defer nodeA.Close()
	nodeB := newNode([]listedSandbox{
		mustListedSandbox("00000000-0000-0000-0000-000000000001", "2026-01-01T00:00:03Z", "running", "envd-new"),
	})
	defer nodeB.Close()

	server := newTestServer(t, stubSchedulerClient{
		listNodesFunc: func(_ context.Context, _ *schedulerv1.ListNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
			return &schedulerv1.ListNodesResponse{
				Nodes: []*schedulerv1.Node{
					{NodeId: "node-a", Endpoint: nodeA.URL},
					{NodeId: "node-b", Endpoint: nodeB.URL},
				},
			}, nil
		},
	}, time.Second, 1024)

	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	defer gatewayServer.Close()

	resp, err := http.Get(gatewayServer.URL + "/v2/sandboxes?limit=10")
	if err != nil {
		t.Fatalf("dedupe request failed: %v", err)
	}
	defer resp.Body.Close()

	items := decodeListedSandboxResponse(t, resp.Body)
	if len(items) != 1 {
		t.Fatalf("expected 1 sandbox after dedupe, got %d", len(items))
	}
	if items[0].EnvdVersion != "envd-new" {
		t.Fatalf("deduped sandbox envdVersion = %q, want %q", items[0].EnvdVersion, "envd-new")
	}
}

func TestHandleProxyClusterListFailsWhenNodeFails(t *testing.T) {
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]listedSandbox{
			mustListedSandbox("00000000-0000-0000-0000-000000000001", "2026-01-01T00:00:01Z", "running", "envd-a"),
		})
	}))
	defer healthy.Close()

	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer failing.Close()

	server := newTestServer(t, stubSchedulerClient{
		listNodesFunc: func(_ context.Context, _ *schedulerv1.ListNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
			return &schedulerv1.ListNodesResponse{
				Nodes: []*schedulerv1.Node{
					{NodeId: "healthy", Endpoint: healthy.URL},
					{NodeId: "failing", Endpoint: failing.URL},
				},
			}, nil
		},
	}, time.Second, 1024)

	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	defer gatewayServer.Close()

	resp, err := http.Get(gatewayServer.URL + "/sandboxes")
	if err != nil {
		t.Fatalf("failing cluster list request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadGateway)
	}
}

func TestHandleProxyClusterListPropagatesUnauthorized(t *testing.T) {
	unauthorized := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "missing auth", http.StatusUnauthorized)
	}))
	defer unauthorized.Close()

	server := newTestServer(t, stubSchedulerClient{
		listNodesFunc: func(_ context.Context, _ *schedulerv1.ListNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
			return &schedulerv1.ListNodesResponse{
				Nodes: []*schedulerv1.Node{
					{NodeId: "node-a", Endpoint: unauthorized.URL},
				},
			}, nil
		},
	}, time.Second, 1024)

	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	defer gatewayServer.Close()

	resp, err := http.Get(gatewayServer.URL + "/sandboxes")
	if err != nil {
		t.Fatalf("unauthorized cluster list request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func equalStrings(got []string, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestIsStreamingRequest(t *testing.T) {
	tests := []struct {
		name    string
		headers http.Header
		want    bool
	}{
		{
			name: "grpc content type",
			headers: http.Header{
				"Content-Type": []string{"application/grpc+proto"},
			},
			want: true,
		},
		{
			name: "connect content type",
			headers: http.Header{
				"Content-Type": []string{"application/connect+proto"},
			},
			want: true,
		},
		{
			name: "connect protocol version header",
			headers: http.Header{
				"Connect-Protocol-Version": []string{"1"},
			},
			want: true,
		},
		{
			name: "grpc-web content type",
			headers: http.Header{
				"Content-Type": []string{"application/grpc-web+proto"},
			},
			want: true,
		},
		{
			name: "sse accept header",
			headers: http.Header{
				"Accept": []string{"text/event-stream"},
			},
			want: true,
		},
		{
			name: "te trailers",
			headers: http.Header{
				"Te": []string{"trailers"},
			},
			want: true,
		},
		{
			name: "normal json request",
			headers: http.Header{
				"Content-Type": []string{"application/json"},
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, "/proxy", nil)
			if err != nil {
				t.Fatalf("build request failed: %v", err)
			}
			req.Header = tc.headers
			if got := isStreamingRequest(req); got != tc.want {
				t.Fatalf("isStreamingRequest() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIsWebSocketRequest(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "/proxy", nil)
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	req.Header.Set("Connection", "keep-alive, Upgrade")
	req.Header.Set("Upgrade", "websocket")

	if !isWebSocketRequest(req) {
		t.Fatal("expected request to be detected as websocket upgrade")
	}

	req.Header.Del("Upgrade")
	if isWebSocketRequest(req) {
		t.Fatal("expected request without upgrade header to be non-websocket")
	}
}

func TestRequestContextForProxy(t *testing.T) {
	t.Run("streaming reuses request context", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPost, "/proxy", nil)
		if err != nil {
			t.Fatalf("build request failed: %v", err)
		}
		routingCtx, cancelRouting := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancelRouting()
		ctx, cancel := requestContextForProxy(req, routingCtx, true)
		defer cancel()
		if req.Context() != ctx {
			t.Fatal("expected streaming request to reuse original context")
		}
	})

	t.Run("non-streaming reuses routing context", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, "/health", nil)
		if err != nil {
			t.Fatalf("build request failed: %v", err)
		}
		routingCtx, cancelRouting := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cancelRouting()
		ctx, cancel := requestContextForProxy(req, routingCtx, false)
		defer cancel()
		if routingCtx != ctx {
			t.Fatal("expected non-streaming request to share routing context")
		}
		<-ctx.Done()
		if ctx.Err() == nil {
			t.Fatal("expected routing timeout context to be canceled")
		}
	})
}

func TestRequestContextNotCanceledWhenStreamingCancelCalled(t *testing.T) {
	baseCtx, baseCancel := context.WithCancel(context.Background())
	defer baseCancel()
	req, err := http.NewRequestWithContext(baseCtx, http.MethodPost, "/proxy", nil)
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	routingCtx, cancelRouting := context.WithTimeout(context.Background(), time.Second)
	defer cancelRouting()
	ctx, cancel := requestContextForProxy(req, routingCtx, true)
	cancel()
	if err := ctx.Err(); err != nil {
		t.Fatalf("streaming cancel function should be no-op, got context err: %v", err)
	}
}

func TestSetXForwardedFor(t *testing.T) {
	h := http.Header{}
	setXForwardedFor(h, "10.0.0.2:12345")
	if got := h.Get("X-Forwarded-For"); got != "10.0.0.2" {
		t.Fatalf("X-Forwarded-For = %q, want %q", got, "10.0.0.2")
	}

	setXForwardedFor(h, "10.0.0.3:8080")
	if got := h.Get("X-Forwarded-For"); got != "10.0.0.3" {
		t.Fatalf("X-Forwarded-For overwrite = %q, want %q", got, "10.0.0.3")
	}
}

func TestInjectForwardedHeadersSetsXForwardedFor(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://gateway.test/sandboxes", nil)
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	req.RemoteAddr = "192.168.1.10:5000"
	req.Host = "gateway.test"

	h := http.Header{}
	h.Set("X-Forwarded-For", "10.0.0.1")
	injectForwardedHeaders(h, req)

	if got := h.Get("X-Forwarded-For"); got != "192.168.1.10" {
		t.Fatalf("X-Forwarded-For = %q, want %q", got, "192.168.1.10")
	}
}

func TestRecordAssignmentFromResponseUsesHeaderWithoutReadingBody(t *testing.T) {
	readInvoked := false
	bodyClosed := false

	recorded := make(chan *schedulerv1.RecordAssignmentRequest, 1)
	server := newTestServer(t, stubSchedulerClient{
		recordAssignmentFunc: func(_ context.Context, req *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			recorded <- req
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}, time.Second, 1024)

	resp := &http.Response{
		StatusCode: http.StatusCreated,
		Header:     http.Header{},
		Body:       trackingReadCloser{readInvoked: &readInvoked, bodyClosed: &bodyClosed},
	}
	resp.Header.Set(headerSandboxID, "sbx-from-header")

	node := &schedulerv1.Node{NodeId: "node-1", Endpoint: "http://node"}
	if err := server.recordAssignmentFromResponse(context.Background(), resp, node, "/sandboxes/sbx-from-header"); err != nil {
		t.Fatalf("recordAssignmentFromResponse returned error: %v", err)
	}

	if readInvoked {
		t.Fatal("expected response body to remain unread when sandbox id header is present")
	}
	if bodyClosed {
		t.Fatal("expected response body to remain open when sandbox id header is present")
	}

	req := <-recorded
	if req.GetSandboxId() != "sbx-from-header" {
		t.Fatalf("recorded sandbox id = %q, want %q", req.GetSandboxId(), "sbx-from-header")
	}
}

func TestReadBodyWithLimitWithinLimit(t *testing.T) {
	body, truncated, err := readBodyWithLimit(strings.NewReader("hello"), 5)
	if err != nil {
		t.Fatalf("readBodyWithLimit returned error: %v", err)
	}
	if truncated {
		t.Fatal("expected body to fit limit")
	}
	if string(body) != "hello" {
		t.Fatalf("unexpected body: %q", string(body))
	}
}

func TestReadBodyWithLimitDetectsOverflow(t *testing.T) {
	_, truncated, err := readBodyWithLimit(strings.NewReader("hello!"), 5)
	if err != nil {
		t.Fatalf("readBodyWithLimit returned error: %v", err)
	}
	if !truncated {
		t.Fatal("expected overflow to be detected")
	}
}

func TestRecordAssignmentTimeout(t *testing.T) {
	if got := recordAssignmentTimeout(2 * time.Second); got != 2*time.Second {
		t.Fatalf("expected timeout to keep smaller value, got %s", got)
	}
	if got := recordAssignmentTimeout(30 * time.Second); got != maxRecordAssignmentTimeout {
		t.Fatalf("expected timeout to be capped at %s, got %s", maxRecordAssignmentTimeout, got)
	}
	if got := recordAssignmentTimeout(0); got != maxRecordAssignmentTimeout {
		t.Fatalf("expected zero timeout to fall back to %s, got %s", maxRecordAssignmentTimeout, got)
	}
}

func TestFlushInterval(t *testing.T) {
	if got := flushInterval(true); got != -1 {
		t.Fatalf("flushInterval(true) = %s, want -1ns", got)
	}
	if got := flushInterval(false); got != 0 {
		t.Fatalf("flushInterval(false) = %s, want 0s", got)
	}
}

func TestMetricsEndpointReturnsNotFoundWithoutProxyRouting(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{}, time.Second, 1024)
	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	resp, err := http.Get(gatewayServer.URL + "/metrics")
	if err != nil {
		t.Fatalf("gateway metrics request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("gateway metrics status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestHealthEndpointReturnsGatewayHealthWithoutProxyHeaders(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{}, time.Second, 1024)
	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	defer gatewayServer.Close()

	resp, err := http.Get(gatewayServer.URL + "/health")
	if err != nil {
		t.Fatalf("gateway health request failed: %v", err)
	}
	defer resp.Body.Close()

	_, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read gateway health body failed: %v", err)
	}

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("gateway health status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
}

func TestHealthAndMetricsEndpointsWithSandboxHeadersProxyToSandbox(t *testing.T) {
	type upstreamRequestSnapshot struct {
		path         string
		targetPort   string
		forwardedURI string
	}

	requests := make(chan upstreamRequestSnapshot, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- upstreamRequestSnapshot{
			path:         r.URL.Path,
			targetPort:   r.Header.Get(headerTargetPort),
			forwardedURI: r.Header.Get("X-Forwarded-URI"),
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			if req.GetSandboxId() != "sbx-service" {
				return nil, fmt.Errorf("unexpected sandbox id lookup: %q", req.GetSandboxId())
			}
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{
					NodeId:   "node-1",
					Endpoint: upstream.URL,
				},
			}, nil
		},
	}, time.Second, 1024)

	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	defer gatewayServer.Close()

	for _, path := range []string{"/health", "/metrics"} {
		t.Run(path, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, gatewayServer.URL+path, nil)
			if err != nil {
				t.Fatalf("build proxy request failed: %v", err)
			}
			req.Header.Set(headerSandboxID, "sbx-service")
			req.Header.Set(headerTargetPort, "49983")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("proxy request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusNoContent {
				t.Fatalf("proxy status = %d, want %d", resp.StatusCode, http.StatusNoContent)
			}

			upstreamReq := <-requests
			if upstreamReq.path != "/proxy"+path {
				t.Fatalf("upstream path = %q, want %q", upstreamReq.path, "/proxy"+path)
			}
			if upstreamReq.targetPort != "49983" {
				t.Fatalf("target port header = %q, want %q", upstreamReq.targetPort, "49983")
			}
			if upstreamReq.forwardedURI != path {
				t.Fatalf("X-Forwarded-URI = %q, want %q", upstreamReq.forwardedURI, path)
			}
		})
	}
}

func TestHealthAndMetricsEndpointsWithProxyHeadersMissingSandboxIDReturnBadRequest(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{}, time.Second, 1024)
	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	defer gatewayServer.Close()

	for _, path := range []string{"/health", "/metrics"} {
		t.Run(path, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, gatewayServer.URL+path, nil)
			if err != nil {
				t.Fatalf("build malformed proxy request failed: %v", err)
			}
			req.Header.Set(headerTargetPort, "49983")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("malformed proxy request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("malformed proxy status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
			}
		})
	}
}

func TestHealthAndMetricsEndpointsWithHostRoutingProxyToSandbox(t *testing.T) {
	type upstreamRequestSnapshot struct {
		path       string
		sandboxID  string
		targetPort string
	}

	requests := make(chan upstreamRequestSnapshot, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- upstreamRequestSnapshot{
			path:       r.URL.Path,
			sandboxID:  r.Header.Get(headerSandboxID),
			targetPort: r.Header.Get(headerTargetPort),
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			if req.GetSandboxId() != "sbx-service" {
				return nil, fmt.Errorf("unexpected sandbox id lookup: %q", req.GetSandboxId())
			}
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{
					NodeId:   "node-1",
					Endpoint: upstream.URL,
				},
			}, nil
		},
	}, time.Second, 1024, withSandboxProxyDomains("sandbox-proxy.example.invalid"))
	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	defer gatewayServer.Close()

	for _, path := range []string{"/health", "/metrics"} {
		t.Run(path, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, gatewayServer.URL+path, nil)
			if err != nil {
				t.Fatalf("build host-routed request failed: %v", err)
			}
			req.Host = "40988-sbx-service.sandbox-proxy.example.invalid"

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("host-routed request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusAccepted {
				t.Fatalf("proxy status = %d, want %d", resp.StatusCode, http.StatusAccepted)
			}

			upstreamReq := <-requests
			if upstreamReq.path != "/proxy"+path {
				t.Fatalf("upstream path = %q, want %q", upstreamReq.path, "/proxy"+path)
			}
			if upstreamReq.sandboxID != "sbx-service" {
				t.Fatalf("sandbox routing header = %q, want %q", upstreamReq.sandboxID, "sbx-service")
			}
			if upstreamReq.targetPort != "40988" {
				t.Fatalf("target port header = %q, want 40988", upstreamReq.targetPort)
			}
		})
	}
}

func TestHandleProxyHostBasedRoutingForwardsToSandboxProxy(t *testing.T) {
	const sandboxID = "018ff4b7-8c0d-7f7c-9a99-123456789abc"

	type upstreamRequestSnapshot struct {
		path          string
		rawQuery      string
		host          string
		sandboxID     string
		targetPort    string
		forwardedHost string
		forwardedURI  string
	}

	requests := make(chan upstreamRequestSnapshot, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- upstreamRequestSnapshot{
			path:          r.URL.Path,
			rawQuery:      r.URL.RawQuery,
			host:          r.Host,
			sandboxID:     r.Header.Get(headerSandboxID),
			targetPort:    r.Header.Get(headerTargetPort),
			forwardedHost: r.Header.Get("X-Forwarded-Host"),
			forwardedURI:  r.Header.Get("X-Forwarded-URI"),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			if req.GetSandboxId() != sandboxID {
				return nil, fmt.Errorf("unexpected sandbox id lookup: %q", req.GetSandboxId())
			}
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{
					NodeId:   "node-1",
					Endpoint: upstream.URL,
				},
			}, nil
		},
	}, time.Second, 1024, withSandboxProxyDomains("sandbox-proxy.example.invalid"))

	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	defer gatewayServer.Close()

	req, err := http.NewRequest(http.MethodGet, gatewayServer.URL+"/readyz?x=1", nil)
	if err != nil {
		t.Fatalf("build host-routed request failed: %v", err)
	}
	req.Host = "40988-" + sandboxID + ".sandbox-proxy.example.invalid"
	req.Header.Set(headerSandboxID, "sbx-from-header")
	req.Header.Set(headerTargetPort, "1234")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("host-routed request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200; body = %s", resp.StatusCode, string(body))
	}

	upstreamReq := <-requests
	if upstreamReq.path != "/proxy/readyz" {
		t.Fatalf("upstream path = %q, want %q", upstreamReq.path, "/proxy/readyz")
	}
	if upstreamReq.rawQuery != "x=1" {
		t.Fatalf("raw query = %q, want x=1", upstreamReq.rawQuery)
	}
	if upstreamReq.host != req.Host {
		t.Fatalf("upstream host = %q, want %q", upstreamReq.host, req.Host)
	}
	if upstreamReq.sandboxID != sandboxID {
		t.Fatalf("sandbox routing header = %q, want %q", upstreamReq.sandboxID, sandboxID)
	}
	if upstreamReq.targetPort != "40988" {
		t.Fatalf("target port header = %q, want 40988", upstreamReq.targetPort)
	}
	if upstreamReq.forwardedHost != req.Host {
		t.Fatalf("X-Forwarded-Host = %q, want %q", upstreamReq.forwardedHost, req.Host)
	}
	if upstreamReq.forwardedURI != "/readyz?x=1" {
		t.Fatalf("X-Forwarded-URI = %q, want %q", upstreamReq.forwardedURI, "/readyz?x=1")
	}
}

func TestHandleProxyHostBasedRoutingRejectsInvalidHost(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{}, time.Second, 1024, withSandboxProxyDomains("sandbox-proxy.example.invalid"))
	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	defer gatewayServer.Close()

	req, err := http.NewRequest(http.MethodGet, gatewayServer.URL+"/readyz", nil)
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	req.Host = "40988-sbx_bad.sandbox-proxy.example.invalid"

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body failed: %v", err)
	}
	if got, want := strings.TrimSpace(string(body)), "invalid sandbox data-plane host: sandbox id is invalid"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

func TestHandleProxyHTTPForwardingAndRecordAssignment(t *testing.T) {
	type upstreamRequestSnapshot struct {
		method          string
		path            string
		rawQuery        string
		host            string
		contentType     string
		body            string
		forwardedHost   string
		forwardedProto  string
		forwardedMethod string
		forwardedURI    string
	}

	requests := make(chan upstreamRequestSnapshot, 1)
	recorded := make(chan *schedulerv1.RecordAssignmentRequest, 1)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read upstream request body failed: %v", err)
		}
		requests <- upstreamRequestSnapshot{
			method:          r.Method,
			path:            r.URL.Path,
			rawQuery:        r.URL.RawQuery,
			host:            r.Host,
			contentType:     r.Header.Get("Content-Type"),
			body:            string(payload),
			forwardedHost:   r.Header.Get("X-Forwarded-Host"),
			forwardedProto:  r.Header.Get("X-Forwarded-Proto"),
			forwardedMethod: r.Header.Get("X-Forwarded-Method"),
			forwardedURI:    r.Header.Get("X-Forwarded-URI"),
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"sandboxID":"sbx-created"}`))
	}))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(_ context.Context, req *schedulerv1.ScheduleRequest, _ ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			if req.GetHint().GetNewSandbox() == nil {
				return nil, fmt.Errorf("unexpected schedule hint: %v", req.GetHint())
			}
			return &schedulerv1.ScheduleResponse{
				Node: &schedulerv1.Node{
					NodeId:   "node-1",
					Endpoint: upstream.URL,
				},
			}, nil
		},
		recordAssignmentFunc: func(_ context.Context, req *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			recorded <- req
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}, time.Second, 1024, withDebugMode(true))

	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	defer gatewayServer.Close()

	req, err := http.NewRequest(http.MethodPost, gatewayServer.URL+"/sandboxes", strings.NewReader(`{"template":"base"}`))
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	req.Host = "gateway.test"
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body failed: %v", err)
	}

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("unexpected response status: %d", resp.StatusCode)
	}
	if got := resp.Header.Get(headerNodeID); got != "node-1" {
		t.Fatalf("response %s = %q, want %q", headerNodeID, got, "node-1")
	}
	if string(body) != `{"sandboxID":"sbx-created"}` {
		t.Fatalf("unexpected response body %q", string(body))
	}

	upstreamReq := <-requests
	if upstreamReq.method != http.MethodPost {
		t.Fatalf("upstream method = %q, want %q", upstreamReq.method, http.MethodPost)
	}
	if upstreamReq.path != "/sandboxes" {
		t.Fatalf("upstream path = %q, want %q", upstreamReq.path, "/sandboxes")
	}
	if upstreamReq.rawQuery != "" {
		t.Fatalf("upstream raw query = %q, want empty", upstreamReq.rawQuery)
	}
	if upstreamReq.host != "gateway.test" {
		t.Fatalf("upstream host = %q, want %q", upstreamReq.host, "gateway.test")
	}
	if upstreamReq.contentType != "application/json" {
		t.Fatalf("upstream content type = %q, want %q", upstreamReq.contentType, "application/json")
	}
	if upstreamReq.body != `{"template":"base"}` {
		t.Fatalf("upstream body = %q, want %q", upstreamReq.body, `{"template":"base"}`)
	}
	if upstreamReq.forwardedHost != "gateway.test" {
		t.Fatalf("X-Forwarded-Host = %q, want %q", upstreamReq.forwardedHost, "gateway.test")
	}
	if upstreamReq.forwardedProto != "http" {
		t.Fatalf("X-Forwarded-Proto = %q, want %q", upstreamReq.forwardedProto, "http")
	}
	if upstreamReq.forwardedMethod != http.MethodPost {
		t.Fatalf("X-Forwarded-Method = %q, want %q", upstreamReq.forwardedMethod, http.MethodPost)
	}
	if upstreamReq.forwardedURI != "/sandboxes" {
		t.Fatalf("X-Forwarded-URI = %q, want %q", upstreamReq.forwardedURI, "/sandboxes")
	}

	recordReq := <-recorded
	if recordReq.GetSandboxId() != "sbx-created" {
		t.Fatalf("recorded sandbox id = %q, want %q", recordReq.GetSandboxId(), "sbx-created")
	}
	if recordReq.GetNode().GetNodeId() != "node-1" {
		t.Fatalf("recorded node id = %q, want %q", recordReq.GetNode().GetNodeId(), "node-1")
	}
	if recordReq.GetNode().GetEndpoint() != upstream.URL {
		t.Fatalf("recorded node endpoint = %q, want %q", recordReq.GetNode().GetEndpoint(), upstream.URL)
	}
}

func TestHandleProxyColdSandboxCreateRecordsAssignment(t *testing.T) {
	type upstreamRequestSnapshot struct {
		method string
		path   string
		body   string
	}

	requests := make(chan upstreamRequestSnapshot, 1)
	recorded := make(chan *schedulerv1.RecordAssignmentRequest, 1)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read upstream request body failed: %v", err)
		}
		requests <- upstreamRequestSnapshot{
			method: r.Method,
			path:   r.URL.Path,
			body:   string(payload),
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"sandboxID":"sbx-cold"}`))
	}))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(_ context.Context, req *schedulerv1.ScheduleRequest, _ ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			cold := req.GetHint().GetNewColdSandbox()
			if cold == nil {
				return nil, fmt.Errorf("unexpected schedule hint: %v", req.GetHint())
			}
			if len(cold.GetImages()) != 1 || cold.GetImages()[0] != "ubuntu:24.04" {
				return nil, fmt.Errorf("unexpected cold sandbox images: %v", cold.GetImages())
			}
			return &schedulerv1.ScheduleResponse{
				Node: &schedulerv1.Node{
					NodeId:   "node-1",
					Endpoint: upstream.URL,
				},
			}, nil
		},
		recordAssignmentFunc: func(_ context.Context, req *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			recorded <- req
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}, time.Second, 1024, withDebugMode(true))

	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	defer gatewayServer.Close()

	req, err := http.NewRequest(http.MethodPost, gatewayServer.URL+"/sandboxes-cold", strings.NewReader(`{"image":"ubuntu:24.04"}`))
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("unexpected response status: %d", resp.StatusCode)
	}

	upstreamReq := <-requests
	if upstreamReq.method != http.MethodPost {
		t.Fatalf("upstream method = %q, want %q", upstreamReq.method, http.MethodPost)
	}
	if upstreamReq.path != "/sandboxes-cold" {
		t.Fatalf("upstream path = %q, want %q", upstreamReq.path, "/sandboxes-cold")
	}
	if upstreamReq.body != `{"image":"ubuntu:24.04"}` {
		t.Fatalf("upstream body = %q", upstreamReq.body)
	}

	recordReq := <-recorded
	if recordReq.GetSandboxId() != "sbx-cold" {
		t.Fatalf("recorded sandbox id = %q, want %q", recordReq.GetSandboxId(), "sbx-cold")
	}
	if recordReq.GetNode().GetNodeId() != "node-1" {
		t.Fatalf("recorded node id = %q, want %q", recordReq.GetNode().GetNodeId(), "node-1")
	}
}

func TestHandleProxyWebSocketForwarding(t *testing.T) {
	type upstreamRequestSnapshot struct {
		path            string
		rawQuery        string
		host            string
		sandboxID       string
		upgrade         string
		connection      string
		forwardedHost   string
		forwardedProto  string
		forwardedMethod string
		forwardedURI    string
	}

	const websocketKey = "dGhlIHNhbXBsZSBub25jZQ=="

	requests := make(chan upstreamRequestSnapshot, 1)
	upstreamDone := make(chan error, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- upstreamRequestSnapshot{
			path:            r.URL.Path,
			rawQuery:        r.URL.RawQuery,
			host:            r.Host,
			sandboxID:       r.Header.Get(headerSandboxID),
			upgrade:         r.Header.Get("Upgrade"),
			connection:      r.Header.Get("Connection"),
			forwardedHost:   r.Header.Get("X-Forwarded-Host"),
			forwardedProto:  r.Header.Get("X-Forwarded-Proto"),
			forwardedMethod: r.Header.Get("X-Forwarded-Method"),
			forwardedURI:    r.Header.Get("X-Forwarded-URI"),
		}

		if !isWebSocketRequest(r) {
			upstreamDone <- fmt.Errorf("expected websocket upgrade request")
			http.Error(w, "expected websocket", http.StatusBadRequest)
			return
		}

		hj, ok := w.(http.Hijacker)
		if !ok {
			upstreamDone <- fmt.Errorf("response writer does not support hijacking")
			http.Error(w, "hijacking unsupported", http.StatusInternalServerError)
			return
		}

		conn, bufrw, err := hj.Hijack()
		if err != nil {
			upstreamDone <- err
			return
		}
		defer conn.Close()

		if _, err := fmt.Fprintf(bufrw, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: %s\r\n\r\n", websocketAccept(websocketKey)); err != nil {
			upstreamDone <- err
			return
		}
		if err := bufrw.Flush(); err != nil {
			upstreamDone <- err
			return
		}

		payload := make([]byte, 4)
		if _, err := io.ReadFull(conn, payload); err != nil {
			upstreamDone <- err
			return
		}
		if string(payload) != "ping" {
			upstreamDone <- fmt.Errorf("unexpected websocket payload %q", string(payload))
			return
		}
		if _, err := conn.Write([]byte("pong")); err != nil {
			upstreamDone <- err
			return
		}

		upstreamDone <- nil
	}))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			if req.GetSandboxId() != "sbx-1" {
				return nil, fmt.Errorf("unexpected sandbox id lookup: %q", req.GetSandboxId())
			}
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{
					NodeId:   "node-1",
					Endpoint: upstream.URL,
				},
			}, nil
		},
	}, time.Second, 1024)

	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	defer gatewayServer.Close()

	gatewayURL, err := url.Parse(gatewayServer.URL)
	if err != nil {
		t.Fatalf("parse gateway url failed: %v", err)
	}

	conn, err := net.Dial("tcp", gatewayURL.Host)
	if err != nil {
		t.Fatalf("dial gateway failed: %v", err)
	}
	defer conn.Close()

	if _, err := fmt.Fprintf(conn, "GET /socket?token=abc HTTP/1.1\r\nHost: gateway.test\r\nConnection: keep-alive, Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n%s: sbx-1\r\n\r\n", websocketKey, headerSandboxID); err != nil {
		t.Fatalf("write websocket handshake failed: %v", err)
	}

	handshakeReq, err := http.NewRequest(http.MethodGet, gatewayServer.URL+"/socket?token=abc", nil)
	if err != nil {
		t.Fatalf("build handshake request failed: %v", err)
	}
	respReader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(respReader, handshakeReq)
	if err != nil {
		t.Fatalf("read websocket handshake failed: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("unexpected handshake status: %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Sec-WebSocket-Accept"); got != websocketAccept(websocketKey) {
		t.Fatalf("unexpected websocket accept header: %q", got)
	}

	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write websocket payload failed: %v", err)
	}
	reply := make([]byte, 4)
	if _, err := io.ReadFull(respReader, reply); err != nil {
		t.Fatalf("read websocket payload failed: %v", err)
	}
	if string(reply) != "pong" {
		t.Fatalf("unexpected websocket reply %q", string(reply))
	}

	req := <-requests
	if req.path != "/proxy/socket" {
		t.Fatalf("upstream path = %q, want %q", req.path, "/proxy/socket")
	}
	if req.rawQuery != "token=abc" {
		t.Fatalf("upstream raw query = %q, want %q", req.rawQuery, "token=abc")
	}
	if req.host != "gateway.test" {
		t.Fatalf("upstream host = %q, want %q", req.host, "gateway.test")
	}
	if req.sandboxID != "sbx-1" {
		t.Fatalf("sandbox routing header = %q, want %q", req.sandboxID, "sbx-1")
	}
	if !strings.EqualFold(req.upgrade, "websocket") {
		t.Fatalf("upgrade header = %q, want websocket", req.upgrade)
	}
	if !strings.Contains(strings.ToLower(req.connection), "upgrade") {
		t.Fatalf("connection header = %q, want token upgrade", req.connection)
	}
	if req.forwardedHost != "gateway.test" {
		t.Fatalf("X-Forwarded-Host = %q, want %q", req.forwardedHost, "gateway.test")
	}
	if req.forwardedProto != "http" {
		t.Fatalf("X-Forwarded-Proto = %q, want %q", req.forwardedProto, "http")
	}
	if req.forwardedMethod != http.MethodGet {
		t.Fatalf("X-Forwarded-Method = %q, want %q", req.forwardedMethod, http.MethodGet)
	}
	if req.forwardedURI != "/socket?token=abc" {
		t.Fatalf("X-Forwarded-URI = %q, want %q", req.forwardedURI, "/socket?token=abc")
	}

	if err := <-upstreamDone; err != nil {
		t.Fatal(err)
	}
}

func TestHandleProxyPreservesEncodedPathSegments(t *testing.T) {
	type upstreamRequestSnapshot struct {
		rawURI      string
		rawQuery    string
		escapedPath string
	}

	requests := make(chan upstreamRequestSnapshot, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- upstreamRequestSnapshot{
			rawURI:      r.RequestURI,
			rawQuery:    r.URL.RawQuery,
			escapedPath: r.URL.EscapedPath(),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			if req.GetSandboxId() != "sbx-enc" {
				return nil, fmt.Errorf("unexpected sandbox id lookup: %q", req.GetSandboxId())
			}
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{
					NodeId:   "node-1",
					Endpoint: upstream.URL,
				},
			}, nil
		},
	}, time.Second, 1024)

	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	defer gatewayServer.Close()

	tests := []struct {
		name            string
		rawRequestURI   string
		wantEscapedPath string
		wantRawQuery    string
	}{
		{
			name:            "encoded slash in path segment",
			rawRequestURI:   "/v0/files/%2F?list=true",
			wantEscapedPath: "/proxy/v0/files/%2F",
			wantRawQuery:    "list=true",
		},
		{
			name:            "double encoded percent",
			rawRequestURI:   "/v0/files/%252Fhome%252Fuser?list=true",
			wantEscapedPath: "/proxy/v0/files/%252Fhome%252Fuser",
			wantRawQuery:    "list=true",
		},
		{
			name:            "encoded space in path",
			rawRequestURI:   "/v0/files/my%20file.txt",
			wantEscapedPath: "/proxy/v0/files/my%20file.txt",
			wantRawQuery:    "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Use raw TCP to ensure the encoded path is sent exactly as-is,
			// since http.NewRequest decodes %2F into / in URL.Path.
			gatewayURL, err := url.Parse(gatewayServer.URL)
			if err != nil {
				t.Fatalf("parse gateway url: %v", err)
			}

			conn, err := net.Dial("tcp", gatewayURL.Host)
			if err != nil {
				t.Fatalf("dial gateway: %v", err)
			}
			defer conn.Close()

			rawHTTP := fmt.Sprintf(
				"GET %s HTTP/1.1\r\nHost: gateway.test\r\n%s: sbx-enc\r\nConnection: close\r\n\r\n",
				tc.rawRequestURI,
				headerSandboxID,
			)
			if _, err := conn.Write([]byte(rawHTTP)); err != nil {
				t.Fatalf("write request: %v", err)
			}

			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Fatalf("read response: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("status = %d, want 200; body = %s", resp.StatusCode, string(body))
			}

			upstreamReq := <-requests
			if upstreamReq.escapedPath != tc.wantEscapedPath {
				t.Fatalf("upstream escaped path = %q, want %q", upstreamReq.escapedPath, tc.wantEscapedPath)
			}
			if upstreamReq.rawQuery != tc.wantRawQuery {
				t.Fatalf("upstream raw query = %q, want %q", upstreamReq.rawQuery, tc.wantRawQuery)
			}
		})
	}
}

func websocketAccept(key string) string {
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func TestProxyRequestContextDeadlineReturnsGatewayTimeout(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{}, time.Second, 1024)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	proxyReq := httptest.NewRequest(http.MethodGet, "/sandboxes/sbx-timeout", nil).WithContext(ctx)
	recorder := httptest.NewRecorder()

	server.proxyRequest(
		recorder,
		proxyReq,
		context.Background(),
		"http://127.0.0.1:1/sandboxes/sbx-timeout",
		&schedulerv1.Node{NodeId: "node-1", Endpoint: "http://127.0.0.1:1"},
		proxyRequestOptions{},
	)

	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusGatewayTimeout)
	}
}

func TestGatewayClassifiesClientCanceledProxyErrors(t *testing.T) {
	streamInputReq := httptest.NewRequest(http.MethodPost, "/process.Process/StreamInput", nil)
	if !isStreamInputProxyRequest(streamInputReq) {
		t.Fatalf("POST StreamInput should be classified as stream input")
	}

	otherReq := httptest.NewRequest(http.MethodPost, "/process.Process/Connect", nil)
	if isStreamInputProxyRequest(otherReq) {
		t.Fatalf("POST Connect should not be classified as stream input")
	}

	getReq := httptest.NewRequest(http.MethodGet, "/process.Process/StreamInput", nil)
	if isStreamInputProxyRequest(getReq) {
		t.Fatalf("GET StreamInput should not be classified as stream input")
	}
}
