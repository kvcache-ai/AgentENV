package gateway

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"agentenv/services/shared/observability"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	gatewayHTTPDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agentenv_gateway_http_request_duration_seconds",
			Help:    "Gateway HTTP request duration by method, route, route source, and status.",
			Buckets: observability.DurationBuckets,
		},
		[]string{"method", "route", "route_source", "status"},
	)
	gatewayUpstreamProxyDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agentenv_gateway_upstream_proxy_duration_seconds",
			Help:    "Gateway upstream reverse proxy duration by route and status.",
			Buckets: observability.DurationBuckets,
		},
		[]string{"route", "status"},
	)
	gatewaySchedulerRPCDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agentenv_gateway_scheduler_rpc_duration_seconds",
			Help:    "Gateway scheduler RPC duration by RPC and status.",
			Buckets: observability.DurationBuckets,
		},
		[]string{"rpc", "status"},
	)
)

type statusRecorder struct {
	http.ResponseWriter
	status      int
	routeSource routeSource
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.status != 0 {
		return
	}
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(body []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(body)
}

func (r *statusRecorder) Flush() {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("response writer does not support hijack")
	}
	conn, brw, err := hijacker.Hijack()
	if err == nil && r.status == 0 {
		r.status = http.StatusSwitchingProtocols
	}
	return conn, brw, err
}

func (r *statusRecorder) statusCode() int {
	if r.status == 0 {
		return http.StatusOK
	}
	return r.status
}

func (r *statusRecorder) setRouteSource(source routeSource) {
	r.routeSource = source
}

func (r *statusRecorder) routeSourceLabel() string {
	if r.routeSource == "" {
		return "unknown"
	}
	return string(r.routeSource)
}

type routeSourceRecorder interface {
	setRouteSource(routeSource)
}

func setGatewayRouteSource(w http.ResponseWriter, source routeSource) {
	if recorder, ok := w.(routeSourceRecorder); ok {
		recorder.setRouteSource(source)
	}
}

func (s *Server) instrumentGatewayHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.isLocalGatewayEndpointRequest(r) {
			next.ServeHTTP(w, r)
			return
		}
		// Sandbox-routed /health and /metrics requests are user traffic, so
		// keep them instrumented like any other proxy call.

		method := gatewayMethodLabel(r.Method)
		route := gatewayRouteLabel(r.URL.Path)
		recorder := &statusRecorder{ResponseWriter: w}
		start := time.Now()
		next.ServeHTTP(recorder, r)
		status := httpStatusLabel(recorder, r.Context())

		gatewayHTTPDuration.WithLabelValues(method, route, recorder.routeSourceLabel(), status).Observe(time.Since(start).Seconds())
	})
}

func (s *Server) isLocalGatewayEndpointRequest(r *http.Request) bool {
	if (r.URL.Path != "/health" && r.URL.Path != "/metrics") || hasProxyRoutingHeaders(r.Header) {
		return false
	}
	hostRoute, hostRouteErr := parseHostRoute(r.Host, s.sandboxProxyDomains)
	return hostRoute == nil && hostRouteErr == nil
}

func recordGatewaySchedulerRPC(rpc string, start time.Time, err error) {
	status := observability.GRPCStatusLabel(err)
	gatewaySchedulerRPCDuration.WithLabelValues(rpc, status).Observe(time.Since(start).Seconds())
}

func recordGatewayUpstreamProxy(route string, start time.Time, w http.ResponseWriter, ctx context.Context) {
	gatewayUpstreamProxyDuration.WithLabelValues(route, httpStatusLabel(w, ctx)).Observe(time.Since(start).Seconds())
}

func gatewayMethodLabel(method string) string {
	switch method {
	case http.MethodGet:
		return "GET"
	case http.MethodPost:
		return "POST"
	case http.MethodPut:
		return "PUT"
	case http.MethodPatch:
		return "PATCH"
	case http.MethodDelete:
		return "DELETE"
	case http.MethodHead:
		return "HEAD"
	case http.MethodOptions:
		return "OPTIONS"
	default:
		return "OTHER"
	}
}

// httpStatusLabel derives the status bucket for a recorded response. ctx must
// be the inbound request context (r.Context()): its cancellation means the
// client disconnected, not that the gateway timed out (an upstream timeout is
// context.DeadlineExceeded and is turned into a 504 by the proxy ErrorHandler).
// So when no status was written and ctx was cancelled, it reports
// "client_closed" (nginx-style 499) instead of counting the request as 2xx.
func httpStatusLabel(w http.ResponseWriter, ctx context.Context) string {
	recorder, ok := w.(*statusRecorder)
	if !ok {
		return "other"
	}
	if recorder.status == 0 && errors.Is(ctx.Err(), context.Canceled) {
		return "client_closed"
	}
	status := recorder.statusCode()
	switch {
	case status >= 100 && status < 200:
		return "1xx"
	case status >= 200 && status < 300:
		return "2xx"
	case status >= 300 && status < 400:
		return "3xx"
	case status >= 400 && status < 500:
		return "4xx"
	case status >= 500 && status < 600:
		return "5xx"
	default:
		return "other"
	}
}

func gatewayRouteLabel(path string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(path), "/")
	if trimmed == "" {
		trimmed = "/"
	}

	switch trimmed {
	case "/sandboxes", "/sandboxes-cold", "/v2/sandboxes", "/nodes":
		return trimmed
	}

	parts := strings.Split(strings.Trim(trimmed, "/"), "/")
	if len(parts) == 0 {
		return "unmatched"
	}
	switch parts[0] {
	case "sandboxes":
		if len(parts) == 2 {
			return "/sandboxes/{sandbox_id}"
		}
		if len(parts) == 3 {
			switch parts[2] {
			case "snapshots", "custom-extension-params", "pause", "resume", "fork":
				return "/sandboxes/{sandbox_id}/" + parts[2]
			}
		}
	case "nodes":
		if len(parts) == 2 {
			return "/nodes/{node_id}"
		}
	case "proxy":
		return "/proxy/*"
	}
	return "unmatched"
}
