package gateway

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	headerAPIKey               = "X-API-Key"
	headerTrafficToken         = "e2b-traffic-access-token"
	headerEnvdAccessToken      = "X-Access-Token"
	headerSandboxID            = "x-agentenv-sandbox-id"
	headerE2BSandboxID         = "e2b-sandbox-id"
	headerTargetPort           = "x-agentenv-target-port"
	headerE2BTargetPort        = "e2b-sandbox-port"
	headerNodeID               = "x-agentenv-node-id"
	maxRecordAssignmentTimeout = 5 * time.Second
)

type routeSource string

const (
	routeSourceHeader   routeSource = "header"
	routeSourceHost     routeSource = "host"
	routeSourcePath     routeSource = "path"
	routeSourceSchedule routeSource = "schedule"
	routeSourceGateway  routeSource = "gateway"
)

type ServerOptions struct {
	APIKey                   string
	RequestTimeout           time.Duration
	MaxResponseSize          int64
	DebugMode                bool
	SandboxProxyDomains      []string
	QueryOnlySchedulerClient schedulerv1.SchedulerClient
}

type Server struct {
	logger             *zap.Logger
	scheduler          schedulerv1.SchedulerClient
	queryOnlyScheduler schedulerv1.SchedulerClient
	httpClient         *http.Client
	apiKey             []byte
	requestTimeout     time.Duration
	maxRespSize        int64
	// debugMode, when true, enables debug-only behaviors such as exposing
	// the backend node id on proxied responses via the x-agentenv-node-id
	// header. Off by default; toggled via GatewayConfig.DebugMode.
	debugMode           bool
	sandboxProxyDomains []string
}

func NewServer(logger *zap.Logger, schedulerClient schedulerv1.SchedulerClient, options ServerOptions) (*Server, error) {
	if options.APIKey == "" {
		return nil, errors.New("API key is required")
	}
	sandboxProxyDomains, err := normalizeProxyDomains(options.SandboxProxyDomains)
	if err != nil {
		return nil, err
	}

	queryOnlyScheduler := options.QueryOnlySchedulerClient
	if queryOnlyScheduler == nil {
		queryOnlyScheduler = schedulerClient
	}

	return &Server{
		logger:              logger,
		scheduler:           schedulerClient,
		queryOnlyScheduler:  queryOnlyScheduler,
		httpClient:          &http.Client{},
		requestTimeout:      options.RequestTimeout,
		maxRespSize:         options.MaxResponseSize,
		apiKey:              []byte(options.APIKey),
		debugMode:           options.DebugMode,
		sandboxProxyDomains: sandboxProxyDomains,
	}, nil
}

func (s *Server) SandboxProxyDomains() []string {
	return s.sandboxProxyDomains
}

func (s *Server) Handler() http.Handler {
	// We avoid http.ServeMux because it normalizes request paths (e.g.
	// decoding %2F → / and issuing 301 redirects), which breaks proxy
	// forwarding of percent-encoded path segments such as /files/%2F.
	core := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isExplicitProxyPath(r.URL.Path) && !hasCompleteProxyRouteHeaders(r.Header) {
			setGatewayRouteSource(w, routeSourceHeader)
			if _, hasSandbox := sandboxIDFromHeaders(r.Header); !hasSandbox {
				http.Error(w, "sandbox id header required", http.StatusBadRequest)
				return
			}
			http.Error(w, "target port header required", http.StatusBadRequest)
			return
		}
		if r.URL.Path == "/health" || r.URL.Path == "/metrics" {
			hostRoute, hostRouteErr := parseHostRoute(r.Host, s.sandboxProxyDomains)
			if hostRoute != nil || hostRouteErr != nil {
				s.handleProxy(w, r)
				return
			}
			if hasProxyRoutingHeaders(r.Header) {
				if _, hasSandbox := sandboxIDFromHeaders(r.Header); !hasSandbox {
					setGatewayRouteSource(w, routeSourceHeader)
					http.Error(w, "sandbox id header required", http.StatusBadRequest)
					return
				}
				s.handleProxy(w, r)
				return
			}
			if r.URL.Path == "/health" {
				// Keep load balancer health checks local when they are not sandbox-routed.
				w.WriteHeader(http.StatusNoContent)
			} else {
				// Gateway Prometheus metrics use the separate metrics listener. Keep
				// this path unavailable on the public HTTP listener unless it is
				// explicitly routed to a sandbox.
				http.NotFound(w, r)
			}
			return
		}
		s.handleProxy(w, r)
	})
	return s.instrumentGatewayHTTP(s.authenticate(core))
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, value any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(value); err != nil {
		s.logger.Warn("encode json response failed",
			zap.Error(err),
			zap.Int("status", status),
		)
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := buf.WriteTo(w); err != nil {
		s.logger.Warn("write json response failed",
			zap.Error(err),
			zap.Int("status", status),
		)
	}
}

func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {
	websocket := isWebSocketRequest(r)
	streaming := isStreamingRequest(r)
	longLived := streaming || websocket
	routingCtx, cancelRouting := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancelRouting()

	hostRoute, hostRouteErr := parseHostRoute(r.Host, s.sandboxProxyDomains)
	if hostRouteErr != nil {
		setGatewayRouteSource(w, routeSourceHost)
		s.logger.Debug("host routing rejected",
			zap.String("host", r.Host),
			zap.Error(hostRouteErr),
			zap.Int("status", http.StatusBadRequest),
		)
		http.Error(w, hostRouteErr.Error(), http.StatusBadRequest)
		return
	}

	if hostRoute == nil && !hasProxyRoutingHeaders(r.Header) {
		if isClusterListRequest(r) {
			setGatewayRouteSource(w, routeSourceGateway)
			s.handleClusterList(w, r, routingCtx)
			return
		} else if isNodeListRequest(r) {
			setGatewayRouteSource(w, routeSourceGateway)
			s.handleNodeList(w, r, routingCtx)
			return
		} else if nodeID, ok := isNodeDetailRequest(r); ok {
			setGatewayRouteSource(w, routeSourcePath)
			s.handleNodeDetail(w, r, routingCtx, nodeID, longLived)
			return
		}
	}

	sandboxID, hasSandbox := "", false
	routeSource := routeSourceHeader
	if hostRoute != nil {
		s.logHostRoutingHeaderConflicts(r, hostRoute)
		sandboxID = hostRoute.sandboxID
		hasSandbox = true
		routeSource = routeSourceHost
	} else if isSandboxControlPlaneRequest(r) {
		sandboxID, hasSandbox = sandboxIDFromPath(r.URL.Path)
		routeSource = routeSourcePath
	} else {
		sandboxID, hasSandbox = sandboxIDFromHeaders(r.Header)
	}
	if !hasSandbox {
		routeSource = routeSourceSchedule
	}
	setGatewayRouteSource(w, routeSource)
	var node *schedulerv1.Node

	if hasSandbox {
		rpcStart := time.Now()
		resp, err := s.queryOnlyScheduler.LookupNode(routingCtx, &schedulerv1.LookupNodeRequest{SandboxId: sandboxID})
		recordGatewaySchedulerRPC("LookupNode", rpcStart, err)
		if err != nil {
			s.writeSchedulerError(w, err)
			return
		}
		node = resp.GetNode()
	} else {
		hint, err := buildScheduleHint(r)
		if err != nil {
			// this only happens it cannot read request body, so the request cannot continue
			s.logger.Warn("Fatal error when building schedule hint",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Error(err),
			)
			http.Error(w, "failed to read request body", http.StatusBadRequest)
			return
		}
		rpcStart := time.Now()
		resp, err := s.scheduler.Schedule(routingCtx, &schedulerv1.ScheduleRequest{
			Hint: hint,
		})
		recordGatewaySchedulerRPC("Schedule", rpcStart, err)
		if err != nil {
			s.writeSchedulerError(w, err)
			return
		}
		node = resp.GetNode()
	}

	s.logger.Debug("gateway routed request",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.String("route_source", string(routeSource)),
		zap.String("sandbox_id", sandboxID),
		zap.String("node_id", node.GetNodeId()),
		zap.String("upstream_endpoint", node.GetEndpoint()),
	)

	decodedPath := upstreamTargetPath(routeSource, r.URL.Path)
	escapedPath := upstreamTargetEscapedPath(routeSource, requestEscapedPath(r))
	upstreamURL, err := joinUpstream(node.GetEndpoint(), decodedPath, escapedPath, r.URL.RawQuery)
	if err != nil {
		http.Error(w, "invalid upstream endpoint", http.StatusBadGateway)
		return
	}

	upstreamCtx, cancelUpstream := requestContextForProxy(r, routingCtx, longLived)
	defer cancelUpstream()

	s.proxyRequest(
		w,
		r.Clone(upstreamCtx),
		r.Context(),
		upstreamURL,
		node,
		proxyRequestOptions{
			recordAssignment: shouldRecordAssignment(r, routeSource, hasSandbox),
			hostRoute:        hostRoute,
			flushImmediately: longLived,
		},
	)
}

func (s *Server) writeSchedulerError(w http.ResponseWriter, err error) {
	st, ok := status.FromError(err)
	if !ok {
		http.Error(w, "scheduler unavailable", http.StatusBadGateway)
		return
	}
	switch st.Code() {
	case codes.InvalidArgument:
		http.Error(w, st.Message(), http.StatusBadRequest)
	case codes.NotFound:
		http.Error(w, st.Message(), http.StatusNotFound)
	case codes.Unavailable:
		http.Error(w, st.Message(), http.StatusServiceUnavailable)
	default:
		http.Error(w, "scheduler error", http.StatusBadGateway)
	}
}

type proxyRequestOptions struct {
	recordAssignment bool
	hostRoute        *hostRoute
	flushImmediately bool
}

func (s *Server) proxyRequest(
	w http.ResponseWriter,
	proxyReq *http.Request,
	originalCtx context.Context,
	target string,
	node *schedulerv1.Node,
	options proxyRequestOptions,
) {
	upstreamURL, err := url.Parse(target)
	if err != nil {
		http.Error(w, "invalid upstream endpoint", http.StatusBadGateway)
		return
	}

	proxy := &httputil.ReverseProxy{
		Rewrite: func(req *httputil.ProxyRequest) {
			req.Out.URL.Scheme = upstreamURL.Scheme
			req.Out.URL.Host = upstreamURL.Host
			req.Out.URL.Path = upstreamURL.Path
			req.Out.URL.RawPath = upstreamURL.RawPath
			req.Out.URL.RawQuery = upstreamURL.RawQuery
			req.Out.Host = req.In.Host
			injectForwardedHeaders(req.Out.Header, req.In)
			if options.hostRoute != nil {
				req.Out.Header.Set(headerSandboxID, options.hostRoute.sandboxID)
				req.Out.Header.Set(headerTargetPort, strconv.Itoa(options.hostRoute.targetPort))
			}
		},
		FlushInterval: flushInterval(options.flushImmediately),
		ModifyResponse: func(resp *http.Response) error {
			// In debug mode, expose the upstream node id on the response so
			// operators can tell which backend node served a given request.
			// This is purely for debugging/observability and is not consumed
			// by the client.
			if s.debugMode {
				if nodeID := node.GetNodeId(); nodeID != "" {
					resp.Header.Set(headerNodeID, nodeID)
				}
			}
			if !options.recordAssignment || resp.StatusCode < 200 || resp.StatusCode >= 300 {
				return nil
			}
			return s.recordAssignmentFromResponse(originalCtx, resp, node, proxyReq.URL.Path)
		},
		ErrorHandler: func(rw http.ResponseWriter, _ *http.Request, err error) {
			if errors.Is(err, context.Canceled) {
				logLevel := zap.WarnLevel
				if isStreamInputProxyRequest(proxyReq) {
					logLevel = zap.DebugLevel
				}
				s.logger.Log(logLevel, "proxy request closed by client",
					zap.Error(err),
					zap.String("node", node.GetNodeId()),
					zap.String("path", proxyReq.URL.Path),
					zap.String("target", upstreamURL.String()),
				)
				return
			}

			if errors.Is(err, context.DeadlineExceeded) || errors.Is(proxyReq.Context().Err(), context.DeadlineExceeded) {
				s.logger.Warn("proxy request timed out",
					zap.Error(err),
					zap.String("node", node.GetNodeId()),
					zap.String("path", proxyReq.URL.Path),
					zap.String("target", upstreamURL.String()),
				)
				http.Error(rw, "upstream timeout", http.StatusGatewayTimeout)
				return
			}

			var proxyErr *proxyResponseError
			if errors.As(err, &proxyErr) {
				http.Error(rw, proxyErr.message, proxyErr.statusCode)
				return
			}

			s.logger.Warn("proxy request failed",
				zap.Error(err),
				zap.String("node", node.GetNodeId()),
				zap.String("path", proxyReq.URL.Path),
				zap.String("target", upstreamURL.String()),
			)
			http.Error(rw, "upstream unavailable", http.StatusBadGateway)
		},
	}

	proxyStart := time.Now()
	route := gatewayRouteLabel(proxyReq.URL.Path)
	proxy.ServeHTTP(w, proxyReq)
	recordGatewayUpstreamProxy(route, proxyStart, w, proxyReq.Context())
}

func isStreamInputProxyRequest(r *http.Request) bool {
	return r.Method == http.MethodPost && r.URL.Path == "/process.Process/StreamInput"
}

type proxyResponseError struct {
	statusCode int
	message    string
	cause      error
}

func (e *proxyResponseError) Error() string {
	if e.cause != nil {
		return e.cause.Error()
	}
	return e.message
}

func (s *Server) recordAssignmentFromResponse(ctx context.Context, resp *http.Response, node *schedulerv1.Node, requestPath string) error {
	recordCtx, cancelRecord := context.WithTimeout(ctx, recordAssignmentTimeout(s.requestTimeout))
	defer cancelRecord()

	if sandboxID, ok := sandboxIDFromHeaders(resp.Header); ok {
		s.recordAssignment(recordCtx, sandboxID, node, "response_header")
		return nil
	}

	body, truncated, err := readBodyWithLimit(resp.Body, s.maxRespSize)
	if err != nil {
		return &proxyResponseError{
			statusCode: http.StatusBadGateway,
			message:    "failed to read upstream response",
			cause:      err,
		}
	}
	if truncated {
		s.logger.Warn("upstream response exceeded configured forwarding limit",
			zap.Int64("max_response_size_bytes", s.maxRespSize),
			zap.Int64("upstream_content_length", resp.ContentLength),
			zap.String("content_type", resp.Header.Get("Content-Type")),
		)
		return &proxyResponseError{
			statusCode: http.StatusBadGateway,
			message:    "upstream response too large",
		}
	}
	_ = resp.Body.Close()

	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	if resp.Header == nil {
		resp.Header = make(http.Header)
	}
	resp.Header.Set("Content-Length", strconv.Itoa(len(body)))

	ids := extractSandboxIDsFromResponse(body)
	if isForkPath(requestPath) {
		ids = extractForkSandboxIDsFromResponse(body)
	}
	for _, sandboxID := range ids {
		s.recordAssignment(recordCtx, sandboxID, node, "response_body")
	}
	return nil
}

func (s *Server) recordAssignment(ctx context.Context, sandboxID string, node *schedulerv1.Node, source string) {
	rpcStart := time.Now()
	_, err := s.scheduler.RecordAssignment(ctx, &schedulerv1.RecordAssignmentRequest{SandboxId: sandboxID, Node: node})
	recordGatewaySchedulerRPC("RecordAssignment", rpcStart, err)
	if err != nil {
		s.logger.Warn("record assignment failed", zap.Error(err), zap.String("sandbox_id", sandboxID), zap.String("node_id", node.GetNodeId()))
		return
	}

	s.logger.Debug("gateway recorded sandbox assignment",
		zap.String("sandbox_id", sandboxID),
		zap.String("node_id", node.GetNodeId()),
		zap.String("source", source),
	)
}

func readBodyWithLimit(src io.Reader, limit int64) ([]byte, bool, error) {
	if limit <= 0 {
		body, err := io.ReadAll(src)
		return body, false, err
	}
	body, err := io.ReadAll(io.LimitReader(src, limit+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(body)) > limit {
		return nil, true, nil
	}
	return body, false, nil
}

func recordAssignmentTimeout(requestTimeout time.Duration) time.Duration {
	if requestTimeout <= 0 {
		return maxRecordAssignmentTimeout
	}
	if requestTimeout < maxRecordAssignmentTimeout {
		return requestTimeout
	}
	return maxRecordAssignmentTimeout
}

func flushInterval(flushImmediately bool) time.Duration {
	if flushImmediately {
		return -1
	}
	return 0
}

func shouldRecordAssignment(r *http.Request, routeSource routeSource, hasSandbox bool) bool {
	if r.Method != http.MethodPost {
		return false
	}
	path := strings.TrimRight(r.URL.Path, "/")
	if !hasSandbox {
		return path == "/sandboxes" || path == "/sandboxes-cold"
	}
	if routeSource != routeSourcePath {
		return false
	}

	// Fork is routed by the source sandbox but creates child sandbox assignments.
	return isForkPath(path)
}

func isForkPath(path string) bool {
	parts := strings.Split(strings.Trim(strings.TrimRight(path, "/"), "/"), "/")
	return len(parts) == 3 && parts[0] == "sandboxes" && strings.TrimSpace(parts[1]) != "" && parts[2] == "fork"
}

func sandboxIDFromHeaders(h http.Header) (string, bool) {
	for _, name := range []string{headerSandboxID, headerE2BSandboxID} {
		v := strings.TrimSpace(h.Get(name))
		if v != "" {
			return v, true
		}
	}
	return "", false
}

func hasProxyRoutingHeaders(h http.Header) bool {
	for _, name := range []string{
		headerSandboxID,
		headerE2BSandboxID,
		headerTargetPort,
		headerE2BTargetPort,
	} {
		if strings.TrimSpace(h.Get(name)) != "" {
			return true
		}
	}
	return false
}

func hasCompleteProxyRouteHeaders(h http.Header) bool {
	_, hasSandbox := sandboxIDFromHeaders(h)
	_, hasTargetPort := targetPortFromHeaders(h)
	return hasSandbox && hasTargetPort
}

func targetPortFromHeaders(h http.Header) (string, bool) {
	for _, name := range []string{headerTargetPort, headerE2BTargetPort} {
		v := strings.TrimSpace(h.Get(name))
		if v != "" {
			return v, true
		}
	}
	return "", false
}

func sandboxIDFromPath(path string) (string, bool) {
	const marker = "/sandboxes/"
	rest, found := strings.CutPrefix(path, marker)
	if !found {
		_, rest, found = strings.Cut(path, marker)
	}
	if !found {
		return "", false
	}
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return "", false
	}
	if id, _, hasSlash := strings.Cut(rest, "/"); hasSlash {
		rest = id
	}
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return "", false
	}
	return rest, true
}

func isSandboxControlPlaneRequest(r *http.Request) bool {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 || parts[0] != "sandboxes" || strings.TrimSpace(parts[1]) == "" {
		return false
	}

	if len(parts) == 2 {
		return r.Method == http.MethodGet || r.Method == http.MethodDelete
	}
	if len(parts) != 3 {
		return false
	}

	switch parts[2] {
	case "pause", "resume", "fork", "connect", "timeout", "refreshes", "snapshots":
		return r.Method == http.MethodPost
	case "cpu-affinity":
		return r.Method == http.MethodPost
	case "network":
		return r.Method == http.MethodPut
	case "custom-extension-params":
		return r.Method == http.MethodGet || r.Method == http.MethodPatch
	default:
		return false
	}
}

func (s *Server) logHostRoutingHeaderConflicts(r *http.Request, route *hostRoute) {
	headerSandboxIDValue, hasHeaderSandboxID := sandboxIDFromHeaders(r.Header)
	headerTargetPortValue, hasHeaderTargetPort := targetPortFromHeaders(r.Header)

	hostTargetPortValue := strconv.Itoa(route.targetPort)
	sandboxIDConflict := hasHeaderSandboxID && headerSandboxIDValue != route.sandboxID
	targetPortConflict := hasHeaderTargetPort && headerTargetPortValue != hostTargetPortValue
	if !sandboxIDConflict && !targetPortConflict {
		return
	}

	s.logger.Debug("host routing overrides conflicting routing headers",
		zap.String("host", r.Host),
		zap.String("host_sandbox_id", route.sandboxID),
		zap.String("host_target_port", hostTargetPortValue),
		zap.String("header_sandbox_id", headerSandboxIDValue),
		zap.String("header_target_port", headerTargetPortValue),
		zap.Bool("sandbox_id_conflict", sandboxIDConflict),
		zap.Bool("target_port_conflict", targetPortConflict),
	)
}

func isDataPlaneRouteSource(routeSource routeSource) bool {
	return routeSource == routeSourceHeader || routeSource == routeSourceHost
}

// upstreamTargetPath returns the path to use when forwarding to the upstream
// node. Requests routed via sandbox proxy host or routing headers are forwarded
// to the /proxy sub-tree on the upstream, while control-plane and scheduled
// requests are forwarded as-is.
func upstreamTargetPath(routeSource routeSource, originalPath string) string {
	if isDataPlaneRouteSource(routeSource) {
		return "/proxy" + originalPath
	}
	return originalPath
}

func upstreamTargetEscapedPath(routeSource routeSource, originalEscapedPath string) string {
	if isDataPlaneRouteSource(routeSource) {
		return "/proxy" + originalEscapedPath
	}
	return originalEscapedPath
}

func joinUpstream(endpoint string, path string, escapedPath string, rawQuery string) (string, error) {
	base, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	if base.Scheme == "" || base.Host == "" {
		return "", errors.New("endpoint must include scheme and host")
	}
	baseEscapedPath := base.EscapedPath()
	base.Path = joinURLPath(base.Path, path)
	if escapedPath != "" {
		base.RawPath = joinURLPath(baseEscapedPath, escapedPath)
	}
	base.RawQuery = rawQuery
	return base.String(), nil
}

func requestEscapedPath(r *http.Request) string {
	if raw := r.URL.RawPath; raw != "" {
		return raw
	}
	if uri := strings.TrimSpace(r.RequestURI); uri != "" {
		if parsed, err := url.ParseRequestURI(uri); err == nil {
			if escaped := parsed.EscapedPath(); escaped != "" {
				return escaped
			}
		}
	}
	if escaped := r.URL.EscapedPath(); escaped != "" {
		return escaped
	}
	return "/"
}

func joinURLPath(basePath, path string) string {
	switch {
	case strings.HasSuffix(basePath, "/") && strings.HasPrefix(path, "/"):
		return basePath + strings.TrimPrefix(path, "/")
	case !strings.HasSuffix(basePath, "/") && !strings.HasPrefix(path, "/"):
		if basePath == "" {
			return "/" + path
		}
		return basePath + "/" + path
	default:
		if basePath == "" {
			return "/" + strings.TrimPrefix(path, "/")
		}
		return basePath + path
	}
}

func injectForwardedHeaders(h http.Header, r *http.Request) {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	setXForwardedFor(h, r.RemoteAddr)
	h.Set("X-Forwarded-Host", r.Host)
	h.Set("X-Forwarded-Proto", scheme)
	h.Set("X-Forwarded-Method", r.Method)
	h.Set("X-Forwarded-URI", r.URL.RequestURI())
}

func setXForwardedFor(h http.Header, remoteAddr string) {
	host := strings.TrimSpace(remoteAddr)
	if parsedHost, _, err := net.SplitHostPort(remoteAddr); err == nil {
		host = parsedHost
	}
	if host == "" {
		h.Del("X-Forwarded-For")
		return
	}
	h.Set("X-Forwarded-For", host)
}

func requestContextForProxy(r *http.Request, routingCtx context.Context, streaming bool) (context.Context, context.CancelFunc) {
	if streaming {
		return r.Context(), func() {}
	}
	return routingCtx, func() {}
}

func isStreamingRequest(r *http.Request) bool {
	contentType := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	if strings.HasPrefix(contentType, "application/grpc") {
		return true
	}
	if strings.HasPrefix(contentType, "application/connect+") {
		return true
	}
	if strings.TrimSpace(r.Header.Get("Connect-Protocol-Version")) != "" {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(r.Header.Get("Accept")), "text/event-stream") {
		return true
	}
	if headerContainsToken(r.Header, "Te", "trailers") {
		return true
	}
	return false
}

func isWebSocketRequest(r *http.Request) bool {
	return strings.EqualFold(strings.TrimSpace(r.Header.Get("Upgrade")), "websocket") &&
		headerContainsToken(r.Header, "Connection", "upgrade")
}

func headerContainsToken(h http.Header, name string, want string) bool {
	for _, v := range h.Values(name) {
		for _, token := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(token), want) {
				return true
			}
		}
	}
	return false
}

func extractSandboxIDFromResponse(body []byte) (string, bool) {
	ids := extractSandboxIDsFromResponse(body)
	if len(ids) == 0 {
		return "", false
	}
	return ids[0], true
}

func extractSandboxIDsFromResponse(body []byte) []string {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil
	}
	var ids []string
	appendSandboxID := func(value any) {
		if id, ok := value.(string); ok && strings.TrimSpace(id) != "" {
			ids = append(ids, id)
		}
	}
	for _, key := range []string{"sandboxID", "sandboxId", "sandbox_id"} {
		appendSandboxID(payload[key])
	}
	if data, ok := payload["data"].(map[string]any); ok {
		for _, key := range []string{"sandboxID", "sandboxId", "sandbox_id"} {
			appendSandboxID(data[key])
		}
	}
	appendSandboxIDsFromArray := func(value any) {
		items, ok := value.([]any)
		if !ok {
			return
		}
		for _, item := range items {
			object, ok := item.(map[string]any)
			if !ok {
				continue
			}
			for _, key := range []string{"sandboxID", "sandboxId", "sandbox_id"} {
				appendSandboxID(object[key])
			}
		}
	}
	appendSandboxIDsFromArray(payload["sandboxes"])
	if data, ok := payload["data"].(map[string]any); ok {
		appendSandboxIDsFromArray(data["sandboxes"])
	}
	if len(ids) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(ids))
	unique := ids[:0]
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	return unique
}

type forkAssignmentResult struct {
	Sandbox *struct {
		SandboxID string `json:"sandboxID"`
	} `json:"sandbox"`
}

func extractForkSandboxIDsFromResponse(body []byte) []string {
	var results []forkAssignmentResult
	if err := json.Unmarshal(body, &results); err != nil {
		return nil
	}
	ids := make([]string, 0, len(results))
	seen := make(map[string]struct{}, len(results))
	for _, result := range results {
		if result.Sandbox == nil {
			continue
		}
		id := strings.TrimSpace(result.Sandbox.SandboxID)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	return ids
}

func singleHeaderMatches(headers http.Header, name string, expected []byte) bool {
	values := headers.Values(name)
	if len(values) != 1 || len(values[0]) != len(expected) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(values[0]), expected) == 1
}

func (s *Server) isSandboxDataPlaneRequest(r *http.Request) bool {
	if isExplicitProxyPath(r.URL.Path) {
		// The explicit proxy prefix cannot dispatch to a control-plane handler.
		// Let the core handler return a stable 400 for incomplete routing data.
		return true
	}

	hostRoute, err := parseHostRoute(r.Host, s.sandboxProxyDomains)
	if hostRoute != nil {
		return true
	}
	if err != nil {
		return false
	}

	return !isSandboxControlPlaneRequest(r) && hasCompleteProxyRouteHeaders(r.Header)
}

func isExplicitProxyPath(path string) bool {
	return path == "/proxy" || strings.HasPrefix(path, "/proxy/")
}

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dataPlane := s.isSandboxDataPlaneRequest(r)
		if dataPlane || r.URL.Path == "/health" || r.URL.Path == "/metrics" {
			// Sandbox-scoped ingress and envd authorization depend on runtime
			// metadata and are enforced by the owning runtime node.
			next.ServeHTTP(w, r)
			return
		}

		if !singleHeaderMatches(r.Header, headerAPIKey, s.apiKey) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}
