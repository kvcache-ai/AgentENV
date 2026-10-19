package gateway

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
)

// connectAuthority returns the CONNECT request-target authority
// (CONNECT host:port). The Go HTTP server mirrors the authority into both
// URL.Host and Host for CONNECT requests; prefer URL.Host since that is the
// literal request-target.
func connectAuthority(r *http.Request) string {
	if r.URL != nil {
		if host := strings.TrimSpace(r.URL.Host); host != "" {
			return host
		}
	}
	return r.Host
}

// relayDataPlaneConnect handles HTTP CONNECT data-plane requests by relaying
// the tunnel to the owning node. httputil.ReverseProxy only hijacks and
// bridges 101 (upgrade) responses, so CONNECT tunnels need an explicit relay:
// hijack the client connection, replay the CONNECT request with sandbox
// routing headers to the node, forward the node's response head, then bridge
// raw bytes bidirectionally. The routing timeout still bounds node lookup and
// the initial dial, but the established tunnel is exempt from it.
func (s *Server) relayDataPlaneConnect(
	w http.ResponseWriter,
	r *http.Request,
	routingCtx context.Context,
	node *schedulerv1.Node,
	hostRoute *hostRoute,
) {
	if r.ProtoMajor != 1 || r.ProtoMinor != 1 {
		http.Error(w, "CONNECT requires HTTP/1.1", http.StatusNotImplemented)
		return
	}

	upstreamURL, err := url.Parse(node.GetEndpoint())
	if err != nil || upstreamURL.Scheme != "http" || upstreamURL.Host == "" {
		s.logger.Warn("CONNECT relay got unusable node endpoint",
			zap.String("endpoint", node.GetEndpoint()),
			zap.String("node", node.GetNodeId()),
		)
		http.Error(w, "invalid upstream endpoint", http.StatusBadGateway)
		return
	}

	proxyStart := time.Now()
	defer func() {
		recordGatewayUpstreamProxy("/proxy/*", proxyStart, w, r.Context())
	}()

	dialAddress := upstreamURL.Host
	if upstreamURL.Port() == "" {
		dialAddress = net.JoinHostPort(upstreamURL.Hostname(), "80")
	}
	dialer := &net.Dialer{}
	nodeConn, err := dialer.DialContext(routingCtx, "tcp", dialAddress)
	if err != nil {
		s.logger.Warn("CONNECT relay failed to dial node",
			zap.Error(err),
			zap.String("node", node.GetNodeId()),
			zap.String("endpoint", upstreamURL.Host),
		)
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}
	// Bound the request replay and response-head read by the routing timeout;
	// the deadline is cleared once the tunnel is established, so the tunnel
	// itself stays exempt.
	if deadline, ok := routingCtx.Deadline(); ok {
		nodeConn.SetDeadline(deadline)
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		nodeConn.Close()
		s.logger.Error("CONNECT relay cannot hijack client connection")
		http.Error(w, "CONNECT tunneling unsupported", http.StatusInternalServerError)
		return
	}
	clientConn, clientRW, err := hijacker.Hijack()
	if err != nil {
		nodeConn.Close()
		s.logger.Warn("CONNECT relay failed to hijack client connection", zap.Error(err))
		return
	}

	sandboxID, _ := sandboxIDFromHeaders(r.Header)
	targetPort, hasTargetPort := targetPortFromHeaders(r.Header)
	if hostRoute != nil {
		sandboxID = hostRoute.sandboxID
		targetPort = strconv.Itoa(hostRoute.targetPort)
		hasTargetPort = true
	}

	nodeRW := bufio.NewReadWriter(bufio.NewReader(nodeConn), bufio.NewWriter(nodeConn))
	if err := writeConnectRequest(nodeRW, r, sandboxID, targetPort, hasTargetPort); err != nil {
		s.logger.Warn("CONNECT relay failed to forward request to node",
			zap.Error(err),
			zap.String("node", node.GetNodeId()),
		)
		overrideGatewayResponseStatus(w, http.StatusBadGateway)
		writeRawConnectResponse(clientRW, "502 Bad Gateway", "upstream unavailable")
		closeConnectTunnel(clientConn, nodeConn)
		return
	}

	resp, err := http.ReadResponse(nodeRW.Reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		s.logger.Warn("CONNECT relay failed to read node response",
			zap.Error(err),
			zap.String("node", node.GetNodeId()),
		)
		overrideGatewayResponseStatus(w, http.StatusBadGateway)
		writeRawConnectResponse(clientRW, "502 Bad Gateway", "upstream unavailable")
		closeConnectTunnel(clientConn, nodeConn)
		return
	}
	overrideGatewayResponseStatus(w, resp.StatusCode)

	if resp.StatusCode/100 != 2 {
		// Tunnel rejected: forward the node's HTTP error response verbatim.
		if err := resp.Write(clientRW); err != nil {
			s.logger.Debug("CONNECT relay failed to forward node error response", zap.Error(err))
		}
		resp.Body.Close()
		clientRW.Flush()
		closeConnectTunnel(clientConn, nodeConn)
		return
	}
	// Tunnel established: lift the establishment deadline so the tunnel
	// itself is exempt from the routing timeout.
	nodeConn.SetDeadline(time.Time{})
	// resp.Body of a 2xx CONNECT response is delimited by connection close and
	// aliases nodeRW.Reader; leave it untouched so no tunneled byte is consumed.

	// After a 2xx CONNECT response the connection becomes a raw tunnel; body
	// framing and hop-by-hop headers no longer apply and must not be forwarded.
	resp.Header.Del("Content-Length")
	resp.Header.Del("Transfer-Encoding")
	resp.Header.Del("Connection")
	_, err = fmt.Fprintf(clientRW, "HTTP/1.1 %s\r\n", resp.Status)
	if err == nil {
		err = resp.Header.Write(clientRW)
	}
	if err == nil {
		_, err = clientRW.WriteString("\r\n")
	}
	if err == nil {
		err = clientRW.Flush()
	}
	if err != nil {
		s.logger.Debug("CONNECT relay failed to deliver tunnel response to client", zap.Error(err))
		closeConnectTunnel(clientConn, nodeConn)
		return
	}

	s.logger.Debug("CONNECT tunnel established",
		zap.String("node", node.GetNodeId()),
		zap.String("sandbox_id", sandboxID),
	)

	// Bridge raw bytes in both directions, propagating half-closes: when one
	// direction reaches EOF, the peer's write side is closed while the other
	// direction keeps flowing, so EOF-delimited tunnel protocols still
	// receive the pending response (matching the node side's
	// copy_bidirectional semantics). The buffered readers are used as copy
	// sources so bytes read ahead during hijack/response parsing are not lost.
	copyDone := make(chan struct{}, 2)
	go func() {
		if _, err := io.Copy(nodeConn, clientRW); err != nil {
			closeConnectTunnel(clientConn, nodeConn)
		} else {
			closeConnectWrite(nodeConn)
		}
		copyDone <- struct{}{}
	}()
	go func() {
		if _, err := io.Copy(clientConn, nodeRW.Reader); err != nil {
			closeConnectTunnel(clientConn, nodeConn)
		} else {
			closeConnectWrite(clientConn)
		}
		copyDone <- struct{}{}
	}()
	<-copyDone
	<-copyDone
	closeConnectTunnel(clientConn, nodeConn)

	s.logger.Debug("CONNECT tunnel closed",
		zap.String("node", node.GetNodeId()),
		zap.String("sandbox_id", sandboxID),
	)
}

func closeConnectTunnel(clientConn net.Conn, nodeConn net.Conn) {
	clientConn.Close()
	nodeConn.Close()
}

// closeConnectWrite propagates a tunnel half-close: the read side stays open
// for the peer's remaining bytes while its write side observes EOF.
func closeConnectWrite(conn net.Conn) {
	if writer, ok := conn.(interface{ CloseWrite() error }); ok {
		writer.CloseWrite()
	}
}

// writeConnectRequest replays the client's CONNECT request to the node. The
// node ignores the request-target and routes purely on the sandbox routing
// headers, which are (re)injected here from the resolved route, mirroring the
// hostRoute header injection done by the reverse-proxy Rewrite hook.
func writeConnectRequest(
	w *bufio.ReadWriter,
	r *http.Request,
	sandboxID string,
	targetPort string,
	hasTargetPort bool,
) error {
	authority := strings.TrimSpace(r.Host)
	if authority == "" {
		authority = "connect.invalid"
	}
	if _, err := fmt.Fprintf(w, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n", authority, authority); err != nil {
		return err
	}

	header := r.Header.Clone()
	removeConnectHopByHopHeaders(header)
	header.Set(headerSandboxID, sandboxID)
	if hasTargetPort {
		header.Set(headerTargetPort, targetPort)
	}
	if err := header.Write(w); err != nil {
		return err
	}
	if _, err := w.WriteString("\r\n"); err != nil {
		return err
	}
	return w.Flush()
}

// removeConnectHopByHopHeaders strips connection-scoped headers (including
// Connection-nominated ones) before the CONNECT request is replayed to the
// node, matching what httputil.ReverseProxy does on the regular proxy path.
func removeConnectHopByHopHeaders(header http.Header) {
	for _, value := range header.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			if token = strings.TrimSpace(token); token != "" {
				header.Del(token)
			}
		}
	}
	for _, name := range []string{
		"Connection",
		"Proxy-Connection",
		"Keep-Alive",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"Te",
		"Trailer",
		"Transfer-Encoding",
		"Content-Length",
		"Upgrade",
	} {
		header.Del(name)
	}
}

// writeRawConnectResponse emits a minimal HTTP error response on a hijacked
// client connection when the tunnel cannot be established.
func writeRawConnectResponse(w *bufio.ReadWriter, status string, message string) {
	body := message + "\n"
	fmt.Fprintf(
		w,
		"HTTP/1.1 %s\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s",
		status,
		len(body),
		body,
	)
	w.Flush()
}
