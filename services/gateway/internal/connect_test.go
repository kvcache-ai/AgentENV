package gateway

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
)

// startConnectEchoNode emulates a node's CONNECT-aware data-plane proxy: it
// reads the CONNECT request head (captured for assertions), replies 200, and
// echoes everything it receives afterwards.
func startConnectEchoNode(t *testing.T) (endpoint string, heads <-chan string) {
	t.Helper()
	return startConnectNode(t, func(_ string, conn net.Conn, reader *bufio.Reader) {
		if _, err := conn.Write([]byte("HTTP/1.1 200 OK\r\n\r\n")); err != nil {
			return
		}
		io.Copy(conn, reader)
	})
}

// startConnectNode emulates a node data-plane proxy: each accepted connection
// sends its CONNECT request head to the returned channel and is then handed to
// handle.
func startConnectNode(
	t *testing.T,
	handle func(head string, conn net.Conn, reader *bufio.Reader),
) (endpoint string, heads <-chan string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	headCh := make(chan string, 8)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				head, err := readRawRequestHead(reader)
				if err != nil {
					return
				}
				headCh <- head
				handle(head, conn, reader)
			}(conn)
		}
	}()
	t.Cleanup(func() { listener.Close() })
	return "http://" + listener.Addr().String(), headCh
}

func readRawRequestHead(reader *bufio.Reader) (string, error) {
	var head strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return "", err
		}
		head.WriteString(line)
		if line == "\r\n" {
			return head.String(), nil
		}
	}
}

func receiveNodeHead(t *testing.T, heads <-chan string) string {
	t.Helper()
	select {
	case head := <-heads:
		return head
	case <-time.After(5 * time.Second):
		t.Fatal("node did not receive a CONNECT request")
		return ""
	}
}

func dialConnectGateway(t *testing.T, gatewayURL string, request string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(gatewayURL, "http://"), 5*time.Second)
	if err != nil {
		t.Fatalf("dial gateway failed: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	reader := bufio.NewReader(conn)
	if _, err := conn.Write([]byte(request)); err != nil {
		t.Fatalf("write CONNECT request failed: %v", err)
	}
	return conn, reader
}

func readConnectResponse(t *testing.T, reader *bufio.Reader) *http.Response {
	t.Helper()
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatalf("read CONNECT response failed: %v", err)
	}
	return resp
}

func assertTunnelEcho(t *testing.T, conn net.Conn, reader *bufio.Reader) {
	t.Helper()
	for _, payload := range []string{"ping", "second message"} {
		if _, err := conn.Write([]byte(payload)); err != nil {
			t.Fatalf("tunnel write failed: %v", err)
		}
		echoed := make([]byte, len(payload))
		if _, err := io.ReadFull(reader, echoed); err != nil {
			t.Fatalf("tunnel read failed: %v", err)
		}
		if string(echoed) != payload {
			t.Fatalf("tunnel echo = %q, want %q", echoed, payload)
		}
	}
}

func connectNodeScheduler(t *testing.T, wantSandboxID string, endpoint string) stubSchedulerClient {
	return stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			if req.GetSandboxId() != wantSandboxID {
				return nil, fmt.Errorf("lookup sandbox id = %q, want %q", req.GetSandboxId(), wantSandboxID)
			}
			return &schedulerv1.LookupNodeResponse{
				Node: &schedulerv1.Node{NodeId: "node-1", Endpoint: endpoint},
			}, nil
		},
	}
}

func TestDataPlaneConnectTunnelsRawTCP(t *testing.T) {
	nodeEndpoint, heads := startConnectEchoNode(t)
	server := newTestServer(t, connectNodeScheduler(t, "sbx-connect", nodeEndpoint), time.Second, 1024)

	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	conn, reader := dialConnectGateway(t, gatewayServer.URL,
		"CONNECT ignored.example:443 HTTP/1.1\r\n"+
			"Host: ignored.example:443\r\n"+
			http.CanonicalHeaderKey(headerSandboxID)+": sbx-connect\r\n"+
			http.CanonicalHeaderKey(headerTargetPort)+": 8080\r\n"+
			"\r\n")

	resp := readConnectResponse(t, reader)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	assertTunnelEcho(t, conn, reader)

	head := receiveNodeHead(t, heads)
	if !strings.HasPrefix(head, "CONNECT ignored.example:443 HTTP/1.1\r\n") {
		t.Fatalf("node CONNECT request line missing or wrong, head:\n%s", head)
	}
	if !strings.Contains(head, http.CanonicalHeaderKey(headerSandboxID)+": sbx-connect\r\n") {
		t.Fatalf("node head missing sandbox routing header, head:\n%s", head)
	}
	if !strings.Contains(head, http.CanonicalHeaderKey(headerTargetPort)+": 8080\r\n") {
		t.Fatalf("node head missing target port routing header, head:\n%s", head)
	}
}

func TestDataPlaneConnectTunnelsRawTCPViaHostRoute(t *testing.T) {
	nodeEndpoint, heads := startConnectEchoNode(t)
	server := newTestServer(
		t,
		connectNodeScheduler(t, "sbx-host", nodeEndpoint),
		time.Second,
		1024,
		withSandboxProxyDomains("sandbox.example.invalid"),
	)

	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	authority := "8080-sbx-host.sandbox.example.invalid"
	conn, reader := dialConnectGateway(t, gatewayServer.URL,
		"CONNECT "+authority+" HTTP/1.1\r\nHost: "+authority+"\r\n\r\n")

	resp := readConnectResponse(t, reader)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	assertTunnelEcho(t, conn, reader)

	head := receiveNodeHead(t, heads)
	if !strings.Contains(head, http.CanonicalHeaderKey(headerSandboxID)+": sbx-host\r\n") {
		t.Fatalf("node head missing host-route sandbox header, head:\n%s", head)
	}
	if !strings.Contains(head, http.CanonicalHeaderKey(headerTargetPort)+": 8080\r\n") {
		t.Fatalf("node head missing host-route port header, head:\n%s", head)
	}
}

func TestDataPlaneConnectTunnelsRawTCPViaHostRouteWithAuthorityPort(t *testing.T) {
	nodeEndpoint, heads := startConnectEchoNode(t)
	server := newTestServer(
		t,
		connectNodeScheduler(t, "sbx-host", nodeEndpoint),
		time.Second,
		1024,
		withSandboxProxyDomains("sandbox.example.invalid"),
	)

	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	authority := "8080-sbx-host.sandbox.example.invalid:8080"
	conn, reader := dialConnectGateway(t, gatewayServer.URL,
		"CONNECT "+authority+" HTTP/1.1\r\nHost: "+authority+"\r\n\r\n")

	resp := readConnectResponse(t, reader)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	assertTunnelEcho(t, conn, reader)

	head := receiveNodeHead(t, heads)
	if !strings.Contains(head, http.CanonicalHeaderKey(headerSandboxID)+": sbx-host\r\n") {
		t.Fatalf("node head missing host-route sandbox header, head:\n%s", head)
	}
	if !strings.Contains(head, http.CanonicalHeaderKey(headerTargetPort)+": 8080\r\n") {
		t.Fatalf("node head missing host-route port header, head:\n%s", head)
	}
}

func TestDataPlaneConnectIgnoresMismatchedAuthorityPort(t *testing.T) {
	nodeEndpoint, heads := startConnectEchoNode(t)
	server := newTestServer(
		t,
		connectNodeScheduler(t, "sbx-host", nodeEndpoint),
		time.Second,
		1024,
		withSandboxProxyDomains("sandbox.example.invalid"),
	)

	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	authority := "8080-sbx-host.sandbox.example.invalid:9090"
	conn, reader := dialConnectGateway(t, gatewayServer.URL,
		"CONNECT "+authority+" HTTP/1.1\r\nHost: "+authority+"\r\n\r\n")

	resp := readConnectResponse(t, reader)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	assertTunnelEcho(t, conn, reader)

	head := receiveNodeHead(t, heads)
	if !strings.Contains(head, http.CanonicalHeaderKey(headerSandboxID)+": sbx-host\r\n") {
		t.Fatalf("node head missing host-route sandbox header, head:\n%s", head)
	}
	if !strings.Contains(head, http.CanonicalHeaderKey(headerTargetPort)+": 8080\r\n") {
		t.Fatalf("node head missing host-route port header, head:\n%s", head)
	}
}

func TestDataPlaneConnectForwardsNodeErrorResponse(t *testing.T) {
	nodeEndpoint, _ := startConnectNode(t, func(_ string, conn net.Conn, _ *bufio.Reader) {
		body := "upstream unavailable\n"
		fmt.Fprintf(
			conn,
			"HTTP/1.1 502 Bad Gateway\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: %d\r\n\r\n%s",
			len(body),
			body,
		)
	})
	server := newTestServer(t, connectNodeScheduler(t, "sbx-connect", nodeEndpoint), time.Second, 1024)

	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	_, reader := dialConnectGateway(t, gatewayServer.URL,
		"CONNECT ignored.example:443 HTTP/1.1\r\n"+
			"Host: ignored.example:443\r\n"+
			http.CanonicalHeaderKey(headerSandboxID)+": sbx-connect\r\n"+
			http.CanonicalHeaderKey(headerTargetPort)+": 8080\r\n"+
			"\r\n")

	resp := readConnectResponse(t, reader)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("CONNECT status = %d, want %d", resp.StatusCode, http.StatusBadGateway)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read error response body failed: %v", err)
	}
	if string(body) != "upstream unavailable\n" {
		t.Fatalf("error response body = %q, want %q", body, "upstream unavailable\n")
	}
}

func TestDataPlaneConnectTunnelOutlivesRoutingTimeout(t *testing.T) {
	nodeEndpoint, _ := startConnectEchoNode(t)
	server := newTestServer(t, connectNodeScheduler(t, "sbx-connect", nodeEndpoint), 500*time.Millisecond, 1024)

	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	conn, reader := dialConnectGateway(t, gatewayServer.URL,
		"CONNECT ignored.example:443 HTTP/1.1\r\n"+
			"Host: ignored.example:443\r\n"+
			http.CanonicalHeaderKey(headerSandboxID)+": sbx-connect\r\n"+
			http.CanonicalHeaderKey(headerTargetPort)+": 8080\r\n"+
			"\r\n")

	resp := readConnectResponse(t, reader)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	// The routing timeout only bounds node lookup and the initial dial; an
	// established tunnel must not be cancelled by it.
	time.Sleep(time.Second)
	assertTunnelEcho(t, conn, reader)
}

func TestDataPlaneConnectNodeStallTimesOut(t *testing.T) {
	nodeEndpoint, _ := startConnectNode(t, func(_ string, _ net.Conn, _ *bufio.Reader) {
		// Stall: never send a response head. The gateway must fail the
		// request at the routing timeout instead of hanging forever.
		time.Sleep(3 * time.Second)
	})
	server := newTestServer(t, connectNodeScheduler(t, "sbx-connect", nodeEndpoint), 300*time.Millisecond, 1024)

	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	_, reader := dialConnectGateway(t, gatewayServer.URL,
		"CONNECT ignored.example:443 HTTP/1.1\r\n"+
			"Host: ignored.example:443\r\n"+
			headerSandboxID+": sbx-connect\r\n"+
			headerTargetPort+": 8080\r\n"+
			"\r\n")

	start := time.Now()
	resp := readConnectResponse(t, reader)
	defer resp.Body.Close()
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("stalled node was not cut off at the routing timeout (took %v)", elapsed)
	}
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("CONNECT status = %d, want %d", resp.StatusCode, http.StatusBadGateway)
	}
}

func TestDataPlaneConnectPropagatesHalfClose(t *testing.T) {
	nodeEndpoint, _ := startConnectNode(t, func(_ string, conn net.Conn, reader *bufio.Reader) {
		if _, err := conn.Write([]byte("HTTP/1.1 200 OK\r\n\r\n")); err != nil {
			return
		}
		// Echo until the client half-closes, then send a final response that
		// must still reach the client (EOF-delimited request/response).
		io.Copy(conn, reader)
		conn.Write([]byte("bye"))
	})
	server := newTestServer(t, connectNodeScheduler(t, "sbx-connect", nodeEndpoint), time.Second, 1024)

	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	conn, reader := dialConnectGateway(t, gatewayServer.URL,
		"CONNECT ignored.example:443 HTTP/1.1\r\n"+
			"Host: ignored.example:443\r\n"+
			headerSandboxID+": sbx-connect\r\n"+
			headerTargetPort+": 8080\r\n"+
			"\r\n")

	resp := readConnectResponse(t, reader)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("tunnel write failed: %v", err)
	}
	echoed := make([]byte, 4)
	if _, err := io.ReadFull(reader, echoed); err != nil {
		t.Fatalf("tunnel read failed: %v", err)
	}
	if string(echoed) != "ping" {
		t.Fatalf("tunnel echo = %q, want %q", echoed, "ping")
	}

	// Half-close the write side; the node's post-EOF response must not be
	// truncated by the gateway tearing the tunnel down.
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		t.Fatalf("client conn is %T, want *net.TCPConn", conn)
	}
	if err := tcpConn.CloseWrite(); err != nil {
		t.Fatalf("half-close failed: %v", err)
	}
	rest, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read after half-close failed: %v", err)
	}
	if string(rest) != "bye" {
		t.Fatalf("post-half-close response = %q, want %q", rest, "bye")
	}
}

func TestDataPlaneConnectRequiresSandboxRoute(t *testing.T) {
	nodeEndpoint, _ := startConnectEchoNode(t)
	server := newTestServer(t, connectNodeScheduler(t, "sbx-connect", nodeEndpoint), time.Second, 1024)

	gatewayServer := httptest.NewServer(server.Handler())
	defer gatewayServer.Close()

	// No sandbox routing header and no host route: the request falls through
	// to the control plane, which has no CONNECT handler.
	_, reader := dialConnectGateway(t, gatewayServer.URL,
		"CONNECT ignored.example:443 HTTP/1.1\r\nHost: ignored.example:443\r\n\r\n")

	resp := readConnectResponse(t, reader)
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatal("CONNECT without sandbox route unexpectedly succeeded")
	}
}
