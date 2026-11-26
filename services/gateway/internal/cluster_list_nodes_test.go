package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
)

const (
	sandboxOnNodeA = "00000000-0000-0000-0000-00000000000a"
	sandboxOnNodeB = "00000000-0000-0000-0000-00000000000b"

	// The request timeout is far above the node timeout, so a list that ends
	// in a fraction of it was ended by the node timeout.
	clusterListTestRequestTimeout = 30 * time.Second
	clusterListTestNodeTimeout    = 50 * time.Millisecond
)

func withClusterListNodeTimeout(timeout time.Duration) testServerOption {
	return func(options *ServerOptions) {
		options.ClusterListNodeTimeout = timeout
	}
}

func newListNode(t *testing.T, items ...listedSandbox) *httptest.Server {
	t.Helper()
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(items)
	}))
	t.Cleanup(node.Close)
	return node
}

func newStatusNode(t *testing.T, status int) *httptest.Server {
	t.Helper()
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, http.StatusText(status), status)
	}))
	t.Cleanup(node.Close)
	return node
}

// newSilentNode starts a node that accepts a list call and never answers it.
func newSilentNode(t *testing.T) *httptest.Server {
	t.Helper()
	release := make(chan struct{})
	node := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() {
		close(release)
		node.Close()
	})
	return node
}

// closedNodeEndpoint returns an endpoint that refuses connections.
func closedNodeEndpoint(t *testing.T) string {
	t.Helper()
	node := httptest.NewServer(http.NotFoundHandler())
	endpoint := node.URL
	node.Close()
	return endpoint
}

func newClusterListGateway(t *testing.T, nodes ...*schedulerv1.Node) *httptest.Server {
	t.Helper()
	server := newTestServer(t, stubSchedulerClient{
		listNodesFunc: func(_ context.Context, _ *schedulerv1.ListNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
			return &schedulerv1.ListNodesResponse{Nodes: nodes}, nil
		},
	}, clusterListTestRequestTimeout, 1024, withClusterListNodeTimeout(clusterListTestNodeTimeout))
	gatewayServer := httptest.NewServer(authenticatedTestHandler(server))
	t.Cleanup(gatewayServer.Close)
	return gatewayServer
}

func getClusterList(t *testing.T, target string, allowPartial string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		t.Fatalf("build cluster list request failed: %v", err)
	}
	if allowPartial != "" {
		req.Header.Set(headerAllowPartial, allowPartial)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("cluster list request failed: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestClusterListFailsWhenNodeDoesNotAnswer(t *testing.T) {
	healthy := newListNode(t, mustListedSandbox(sandboxOnNodeA, "2026-01-01T00:00:01Z", "running", "envd-a"))
	silent := newSilentNode(t)
	gatewayServer := newClusterListGateway(t,
		&schedulerv1.Node{NodeId: "node-a", Endpoint: healthy.URL},
		&schedulerv1.Node{NodeId: "node-silent", Endpoint: silent.URL},
	)

	started := time.Now()
	resp := getClusterList(t, gatewayServer.URL+"/sandboxes", "")

	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadGateway)
	}
	if elapsed := time.Since(started); elapsed > clusterListTestRequestTimeout/2 {
		t.Fatalf("list ended after %s, want the node timeout of %s to end it", elapsed, clusterListTestNodeTimeout)
	}
	if got := resp.Header.Get(headerUnreachableNodeCount); got != "" {
		t.Fatalf("%s = %q on a failed list, want none", headerUnreachableNodeCount, got)
	}
}

func TestClusterListPartialLeavesOutUnreachableNodes(t *testing.T) {
	healthy := newListNode(t, mustListedSandbox(sandboxOnNodeA, "2026-01-01T00:00:01Z", "running", "envd-a"))
	failing := newStatusNode(t, http.StatusInternalServerError)
	silent := newSilentNode(t)
	gatewayServer := newClusterListGateway(t,
		&schedulerv1.Node{NodeId: "node-silent", Endpoint: silent.URL},
		&schedulerv1.Node{NodeId: "node-a", Endpoint: healthy.URL},
		&schedulerv1.Node{NodeId: "node-failing", Endpoint: failing.URL},
		&schedulerv1.Node{NodeId: "node-closed", Endpoint: closedNodeEndpoint(t)},
	)

	started := time.Now()
	resp := getClusterList(t, gatewayServer.URL+"/sandboxes", "true")

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if elapsed := time.Since(started); elapsed > clusterListTestRequestTimeout/2 {
		t.Fatalf("list ended after %s, want the node timeout of %s to end the silent node's call", elapsed, clusterListTestNodeTimeout)
	}
	if got := sandboxIDs(decodeListedSandboxResponse(t, resp.Body)); !equalStrings(got, []string{sandboxOnNodeA}) {
		t.Fatalf("sandboxes = %v, want only the sandbox of the node that answered", got)
	}
	if got := resp.Header.Get(headerUnreachableNodeCount); got != "3" {
		t.Fatalf("%s = %q, want %q", headerUnreachableNodeCount, got, "3")
	}
	if got, want := resp.Header.Get(headerUnreachableNodes), "node-closed,node-failing,node-silent"; got != want {
		t.Fatalf("%s = %q, want %q", headerUnreachableNodes, got, want)
	}
}

func TestClusterListPartialOfAnsweringNodesIsComplete(t *testing.T) {
	nodeA := newListNode(t, mustListedSandbox(sandboxOnNodeA, "2026-01-01T00:00:01Z", "running", "envd-a"))
	nodeB := newListNode(t, mustListedSandbox(sandboxOnNodeB, "2026-01-01T00:00:02Z", "running", "envd-b"))
	gatewayServer := newClusterListGateway(t,
		&schedulerv1.Node{NodeId: "node-a", Endpoint: nodeA.URL},
		&schedulerv1.Node{NodeId: "node-b", Endpoint: nodeB.URL},
	)

	resp := getClusterList(t, gatewayServer.URL+"/sandboxes", "true")

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if got := sandboxIDs(decodeListedSandboxResponse(t, resp.Body)); !equalStrings(got, []string{sandboxOnNodeB, sandboxOnNodeA}) {
		t.Fatalf("sandboxes = %v, want both nodes' sandboxes, newest first", got)
	}
	for _, header := range []string{headerUnreachableNodeCount, headerUnreachableNodes} {
		if values := resp.Header.Values(header); len(values) != 0 {
			t.Fatalf("%s = %q on a complete list, want none", header, values)
		}
	}
}

func TestClusterListPartialPropagatesClientError(t *testing.T) {
	healthy := newListNode(t, mustListedSandbox(sandboxOnNodeA, "2026-01-01T00:00:01Z", "running", "envd-a"))
	failing := newStatusNode(t, http.StatusInternalServerError)
	unauthorized := newStatusNode(t, http.StatusUnauthorized)
	gatewayServer := newClusterListGateway(t,
		&schedulerv1.Node{NodeId: "node-a", Endpoint: healthy.URL},
		&schedulerv1.Node{NodeId: "node-failing", Endpoint: failing.URL},
		&schedulerv1.Node{NodeId: "node-unauthorized", Endpoint: unauthorized.URL},
	)

	resp := getClusterList(t, gatewayServer.URL+"/sandboxes", "true")

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestClusterListPartialFailsWhenNoNodeAnswers(t *testing.T) {
	failing := newStatusNode(t, http.StatusInternalServerError)
	silent := newSilentNode(t)
	gatewayServer := newClusterListGateway(t,
		&schedulerv1.Node{NodeId: "node-failing", Endpoint: failing.URL},
		&schedulerv1.Node{NodeId: "node-silent", Endpoint: silent.URL},
	)

	resp := getClusterList(t, gatewayServer.URL+"/sandboxes", "true")

	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadGateway)
	}
	if got := resp.Header.Get(headerUnreachableNodeCount); got != "" {
		t.Fatalf("%s = %q on a failed list, want none", headerUnreachableNodeCount, got)
	}
}

func TestClusterListPartialV2MarksEveryPage(t *testing.T) {
	nodeA := newListNode(t, mustListedSandbox(sandboxOnNodeA, "2026-01-01T00:00:01Z", "running", "envd-a"))
	nodeB := newListNode(t, mustListedSandbox(sandboxOnNodeB, "2026-01-01T00:00:02Z", "running", "envd-b"))
	failing := newStatusNode(t, http.StatusInternalServerError)
	gatewayServer := newClusterListGateway(t,
		&schedulerv1.Node{NodeId: "node-a", Endpoint: nodeA.URL},
		&schedulerv1.Node{NodeId: "node-b", Endpoint: nodeB.URL},
		&schedulerv1.Node{NodeId: "node-failing", Endpoint: failing.URL},
	)

	first := getClusterList(t, gatewayServer.URL+"/v2/sandboxes?limit=1", "true")
	nextToken := first.Header.Get("x-next-token")
	second := getClusterList(t, gatewayServer.URL+"/v2/sandboxes?limit=1&nextToken="+nextToken, "true")

	if got := sandboxIDs(decodeListedSandboxResponse(t, first.Body)); !equalStrings(got, []string{sandboxOnNodeB}) {
		t.Fatalf("first page = %v, want the newest sandbox", got)
	}
	if got := sandboxIDs(decodeListedSandboxResponse(t, second.Body)); !equalStrings(got, []string{sandboxOnNodeA}) {
		t.Fatalf("second page = %v, want the older sandbox", got)
	}
	for page, resp := range []*http.Response{first, second} {
		if got := resp.Header.Get(headerUnreachableNodes); got != "node-failing" {
			t.Fatalf("page %d: %s = %q, want %q", page+1, headerUnreachableNodes, got, "node-failing")
		}
		// Only the nodes that answered are counted.
		if got := resp.Header.Get("x-total-running"); got != "2" {
			t.Fatalf("page %d: x-total-running = %q, want %q", page+1, got, "2")
		}
	}
}

func TestClusterListRejectsInvalidAllowPartial(t *testing.T) {
	healthy := newListNode(t)
	gatewayServer := newClusterListGateway(t, &schedulerv1.Node{NodeId: "node-a", Endpoint: healthy.URL})

	resp := getClusterList(t, gatewayServer.URL+"/sandboxes", "sometimes")

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestParseClusterListAllowPartial(t *testing.T) {
	for value, want := range map[string]bool{"": false, "false": false, "true": true, " TRUE ": true} {
		r := httptest.NewRequest(http.MethodGet, "/sandboxes", nil)
		r.Header.Set(headerAllowPartial, value)
		got, err := parseClusterListAllowPartial(r)
		if err != nil || got != want {
			t.Fatalf("%s %q = %v, %v; want %v", headerAllowPartial, value, got, err, want)
		}
	}
}

func TestSetUnreachableNodeHeadersCapsAndEscapesNodeIDs(t *testing.T) {
	nodeIDs := []string{"rack 1,node/a"}
	for i := 1; i < maxUnreachableNodeIDs+8; i++ {
		nodeIDs = append(nodeIDs, "node-"+strconv.Itoa(i))
	}
	header := http.Header{}

	setUnreachableNodeHeaders(header, nodeIDs)

	if got, want := header.Get(headerUnreachableNodeCount), strconv.Itoa(maxUnreachableNodeIDs+8); got != want {
		t.Fatalf("%s = %q, want the exact count %q", headerUnreachableNodeCount, got, want)
	}
	listed := splitCommaList(header.Get(headerUnreachableNodes))
	if len(listed) != maxUnreachableNodeIDs {
		t.Fatalf("%s lists %d node IDs, want %d", headerUnreachableNodes, len(listed), maxUnreachableNodeIDs)
	}
	if listed[0] != "rack%201%2Cnode%2Fa" {
		t.Fatalf("first node ID = %q, want it percent-encoded", listed[0])
	}
}

func splitCommaList(value string) []string {
	var parts []string
	start := 0
	for i := 0; i <= len(value); i++ {
		if i == len(value) || value[i] == ',' {
			parts = append(parts, value[start:i])
			start = i + 1
		}
	}
	return parts
}
