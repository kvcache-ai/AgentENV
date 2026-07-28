package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"google.golang.org/grpc"
)

func TestResolveSnapshotLocalityHintResolvesTemplateAlias(t *testing.T) {
	const canonicalID = "550e8400-e29b-41d4-a716-446655440000"

	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/templates/aliases/warm-template" {
			t.Fatalf("unexpected alias lookup: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"templateID": canonicalID,
			"public":     false,
		})
	}))
	defer node.Close()

	server := newTestServer(t, stubSchedulerClient{
		listNodesFunc: func(_ context.Context, _ *schedulerv1.ListNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
			return &schedulerv1.ListNodesResponse{Nodes: []*schedulerv1.Node{{
				NodeId:   "node-a",
				Endpoint: node.URL,
			}}}, nil
		},
	}, 0, 1024)

	request := newHintRequest(t, http.MethodPost, "/sandboxes", `{"templateID":"warm-template"}`)
	hint, err := buildScheduleHint(request)
	if err != nil {
		t.Fatalf("buildScheduleHint returned error: %v", err)
	}
	hint = server.resolveSnapshotLocalityHint(context.Background(), request, hint)

	want := []string{
		"snapshot/v1/artifacts/" + canonicalID + "/vm_state.bin",
		"snapshot/v1/artifacts/" + canonicalID + "/firecracker-manifest.json",
	}
	if got := localityRequirementKeys(hint.GetLocalityRequirements()); !equalStrings(got, want) {
		t.Fatalf("locality requirements = %v, want %v", got, want)
	}
}

func TestResolveSnapshotLocalityHintNormalizesCanonicalID(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{}, 0, 1024)
	request := newHintRequest(t, http.MethodPost, "/sandboxes", `{"templateID":"550E8400-E29B-41D4-A716-446655440000"}`)
	hint, err := buildScheduleHint(request)
	if err != nil {
		t.Fatalf("buildScheduleHint returned error: %v", err)
	}
	hint = server.resolveSnapshotLocalityHint(context.Background(), request, hint)

	keys := localityRequirementKeys(hint.GetLocalityRequirements())
	if len(keys) != 2 || keys[0] != "snapshot/v1/artifacts/550e8400-e29b-41d4-a716-446655440000/vm_state.bin" {
		t.Fatalf("unexpected normalized locality requirements: %v", keys)
	}
}

func TestResolveSnapshotLocalityHintDropsUnresolvedAlias(t *testing.T) {
	node := httptest.NewServer(http.NotFoundHandler())
	defer node.Close()

	server := newTestServer(t, stubSchedulerClient{
		listNodesFunc: func(_ context.Context, _ *schedulerv1.ListNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
			return &schedulerv1.ListNodesResponse{Nodes: []*schedulerv1.Node{{
				NodeId:   "node-a",
				Endpoint: node.URL,
			}}}, nil
		},
	}, 0, 1024)

	request := newHintRequest(t, http.MethodPost, "/sandboxes", `{"templateID":"missing-template"}`)
	hint, err := buildScheduleHint(request)
	if err != nil {
		t.Fatalf("buildScheduleHint returned error: %v", err)
	}
	hint = server.resolveSnapshotLocalityHint(context.Background(), request, hint)
	if got := hint.GetLocalityRequirements(); len(got) != 0 {
		t.Fatalf("unresolved alias produced locality requirements: %v", got)
	}
}

func TestResolveSnapshotLocalityHintSkipsWhenGlobalResolutionBusy(t *testing.T) {
	var listNodesCalls atomic.Int32
	server := newTestServer(t, stubSchedulerClient{
		listNodesFunc: func(_ context.Context, _ *schedulerv1.ListNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
			listNodesCalls.Add(1)
			return nil, fmt.Errorf("unexpected ListNodes call")
		},
	}, 0, 1024)
	server.templateAliasResolutionSlots = make(chan struct{}, 1)
	server.templateAliasResolutionSlots <- struct{}{}
	defer func() { <-server.templateAliasResolutionSlots }()

	request := newHintRequest(t, http.MethodPost, "/sandboxes", `{"templateID":"busy-template"}`)
	hint, err := buildScheduleHint(request)
	if err != nil {
		t.Fatalf("buildScheduleHint returned error: %v", err)
	}
	hint = server.resolveSnapshotLocalityHint(context.Background(), request, hint)

	if got := len(hint.GetLocalityRequirements()); got != 0 {
		t.Fatalf("busy resolver produced locality requirements: %d", got)
	}
	if got := listNodesCalls.Load(); got != 0 {
		t.Fatalf("ListNodes calls = %d, want 0 while global resolver is busy", got)
	}
}

func TestHandleProxyContinuesSchedulingAfterTemplateAliasLookupTimeout(t *testing.T) {
	lookupStarted := make(chan struct{})
	aliasNode := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(lookupStarted)
		<-r.Context().Done()
	}))
	defer aliasNode.Close()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"sandboxID":"sbx-created"}`))
	}))
	defer upstream.Close()

	type scheduleObservation struct {
		ctxErr        error
		remaining     time.Duration
		hasDeadline   bool
		localityCount int
	}
	scheduled := make(chan scheduleObservation, 1)
	server := newTestServer(t, stubSchedulerClient{
		listNodesFunc: func(ctx context.Context, _ *schedulerv1.ListNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return &schedulerv1.ListNodesResponse{Nodes: []*schedulerv1.Node{{
				NodeId:   "alias-node",
				Endpoint: aliasNode.URL,
			}}}, nil
		},
		scheduleFunc: func(ctx context.Context, req *schedulerv1.ScheduleRequest, _ ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			deadline, hasDeadline := ctx.Deadline()
			scheduled <- scheduleObservation{
				ctxErr:        ctx.Err(),
				remaining:     time.Until(deadline),
				hasDeadline:   hasDeadline,
				localityCount: len(req.GetHint().GetLocalityRequirements()),
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return &schedulerv1.ScheduleResponse{Node: &schedulerv1.Node{
				NodeId:   "node-1",
				Endpoint: upstream.URL,
			}}, nil
		},
		recordAssignmentFunc: func(_ context.Context, _ *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}, time.Second, 1024)
	server.templateAliasLookupTimeout = 50 * time.Millisecond
	server.templateAliasScheduleReserve = 20 * time.Millisecond

	gateway := httptest.NewServer(server.Handler())
	defer gateway.Close()

	req, err := http.NewRequest(http.MethodPost, gateway.URL+"/sandboxes", strings.NewReader(`{"templateID":"hanging-template"}`))
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("gateway request failed: %v", err)
	}
	defer resp.Body.Close()
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatalf("read gateway response failed: %v", err)
	}

	select {
	case <-lookupStarted:
	default:
		t.Fatal("template alias lookup did not reach the hanging node")
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}

	observation := <-scheduled
	if observation.localityCount != 0 {
		t.Fatalf("locality requirements = %d, want 0 after lookup timeout", observation.localityCount)
	}
	if err := observation.ctxErr; err != nil {
		t.Fatalf("schedule context already canceled: %v", err)
	}
	if !observation.hasDeadline || observation.remaining <= server.templateAliasScheduleReserve {
		t.Fatalf("schedule context did not retain a useful deadline: %s", observation.remaining)
	}
}

func TestResolveTemplateAliasBoundsConcurrentNodeLookups(t *testing.T) {
	const (
		nodeCount   = 5
		concurrency = 2
	)

	var active atomic.Int32
	var maxActive atomic.Int32
	servers := make([]*httptest.Server, 0, nodeCount)
	nodes := make([]*schedulerv1.Node, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		node := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			current := active.Add(1)
			for {
				previous := maxActive.Load()
				if current <= previous || maxActive.CompareAndSwap(previous, current) {
					break
				}
			}
			defer active.Add(-1)
			<-r.Context().Done()
		}))
		servers = append(servers, node)
		nodes = append(nodes, &schedulerv1.Node{NodeId: nodeID, Endpoint: node.URL})
	}
	defer func() {
		for _, node := range servers {
			node.Close()
		}
	}()

	server := newTestServer(t, stubSchedulerClient{
		listNodesFunc: func(_ context.Context, _ *schedulerv1.ListNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
			return &schedulerv1.ListNodesResponse{Nodes: nodes}, nil
		},
	}, 0, 1024)
	server.templateAliasLookupTimeout = 50 * time.Millisecond
	server.templateAliasLookupConcurrency = concurrency

	request := newHintRequest(t, http.MethodPost, "/sandboxes", `{"templateID":"slow-template"}`)
	hint, err := buildScheduleHint(request)
	if err != nil {
		t.Fatalf("buildScheduleHint returned error: %v", err)
	}
	hint = server.resolveSnapshotLocalityHint(context.Background(), request, hint)

	if got := len(hint.GetLocalityRequirements()); got != 0 {
		t.Fatalf("timed-out alias produced locality requirements: %d", got)
	}
	if got := maxActive.Load(); got == 0 || got > concurrency {
		t.Fatalf("maximum concurrent alias lookups = %d, want between 1 and %d", got, concurrency)
	}
}
