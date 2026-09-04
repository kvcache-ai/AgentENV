package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGatewayRouteLabelIncludesV2Sandboxes(t *testing.T) {
	if got := gatewayRouteLabel("/v2/sandboxes"); got != "/v2/sandboxes" {
		t.Fatalf("gatewayRouteLabel() = %q, want %q", got, "/v2/sandboxes")
	}
}

func TestGatewayRouteLabelSandboxActions(t *testing.T) {
	cases := map[string]string{
		"/sandboxes/abc/pause":                   "/sandboxes/{sandbox_id}/pause",
		"/sandboxes/abc/resume":                  "/sandboxes/{sandbox_id}/resume",
		"/sandboxes/abc/fork":                    "/sandboxes/{sandbox_id}/fork",
		"/sandboxes/abc/custom-extension-params": "/sandboxes/{sandbox_id}/custom-extension-params",
	}
	for path, want := range cases {
		if got := gatewayRouteLabel(path); got != want {
			t.Fatalf("gatewayRouteLabel(%q) = %q, want %q", path, got, want)
		}
	}
}

func TestStatusRecorderRouteSourceLabel(t *testing.T) {
	recorder := &statusRecorder{}
	if got := recorder.routeSourceLabel(); got != "unknown" {
		t.Fatalf("routeSourceLabel() = %q, want unknown", got)
	}

	setGatewayRouteSource(recorder, routeSourceHost)
	if got := recorder.routeSourceLabel(); got != string(routeSourceHost) {
		t.Fatalf("routeSourceLabel() = %q, want %q", got, string(routeSourceHost))
	}
}

func TestInstrumentGatewayHTTPSkipsOnlyLocalEndpoints(t *testing.T) {
	server := newTestServer(
		t,
		stubSchedulerClient{},
		time.Second,
		1024,
		withSandboxProxyDomains("sandbox-proxy.example.invalid"),
	)

	tests := []struct {
		name        string
		path        string
		host        string
		sandboxID   string
		targetPort  string
		wantWrapped bool
	}{
		{name: "local health", path: "/health"},
		{name: "local metrics", path: "/metrics"},
		{
			name:        "header-routed metrics",
			path:        "/metrics",
			sandboxID:   "sbx-metrics",
			targetPort:  "49983",
			wantWrapped: true,
		},
		{
			name:        "host-routed metrics",
			path:        "/metrics",
			host:        "49983-sbx-metrics.sandbox-proxy.example.invalid",
			wantWrapped: true,
		},
		{name: "ordinary gateway route", path: "/sandboxes", wantWrapped: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wrapped := false
			handler := server.instrumentGatewayHTTP(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, wrapped = w.(*statusRecorder)
				w.WriteHeader(http.StatusNoContent)
			}))
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if tt.host != "" {
				req.Host = tt.host
			}
			if tt.sandboxID != "" {
				req.Header.Set(headerSandboxID, tt.sandboxID)
			}
			if tt.targetPort != "" {
				req.Header.Set(headerTargetPort, tt.targetPort)
			}
			handler.ServeHTTP(httptest.NewRecorder(), req)
			if wrapped != tt.wantWrapped {
				t.Fatalf("instrumentation wrapper present = %t, want %t", wrapped, tt.wantWrapped)
			}
		})
	}
}

func TestHTTPStatusLabel(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name   string
		status int // status written to the recorder; 0 means none written
		ctx    context.Context
		want   string
	}{
		{name: "ok", status: 200, ctx: context.Background(), want: "2xx"},
		{name: "created", status: 201, ctx: context.Background(), want: "2xx"},
		{name: "redirect", status: 302, ctx: context.Background(), want: "3xx"},
		{name: "client error", status: 404, ctx: context.Background(), want: "4xx"},
		{name: "server error", status: 502, ctx: context.Background(), want: "5xx"},
		{name: "unwritten defaults to 2xx", status: 0, ctx: context.Background(), want: "2xx"},
		{name: "unwritten and cancelled is client_closed", status: 0, ctx: cancelled, want: "client_closed"},
		{name: "written status wins over cancel", status: 500, ctx: cancelled, want: "5xx"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := &statusRecorder{ResponseWriter: httptest.NewRecorder()}
			if tt.status != 0 {
				recorder.WriteHeader(tt.status)
			}
			if got := httpStatusLabel(recorder, tt.ctx); got != tt.want {
				t.Fatalf("httpStatusLabel() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestHTTPStatusLabelNonRecorder(t *testing.T) {
	if got := httpStatusLabel(httptest.NewRecorder(), context.Background()); got != "other" {
		t.Fatalf("httpStatusLabel() = %q, want other", got)
	}
}
