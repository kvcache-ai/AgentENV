package scheduler

import (
	"testing"
	"time"
)

func TestBindingStoreExpiresBindingsOnLookup(t *testing.T) {
	store := NewInMemoryBindingStore(time.Second)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}
	base := time.Unix(100, 0)

	store.Record("sbx-1", node, base)

	if got, ok, err := store.Get("sbx-1", base.Add(500*time.Millisecond)); err != nil || !ok || got.ID != node.ID {
		t.Fatalf("expected binding before ttl expiry, got (%+v, %v, %v)", got, ok, err)
	}
	if _, ok, err := store.Get("sbx-1", base.Add(time.Second)); err != nil || ok {
		t.Fatalf("expected binding to expire at ttl boundary, got ok=%v err=%v", ok, err)
	}
	if _, ok, err := store.Get("sbx-1", base.Add(2*time.Second)); err != nil || ok {
		t.Fatalf("expected expired binding to be removed, got ok=%v err=%v", ok, err)
	}
}

func TestBindingStoreReconcileNodeRefreshesAndRemovesBindings(t *testing.T) {
	store := NewInMemoryBindingStore(10 * time.Second)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}
	base := time.Unix(200, 0)

	store.Record("sbx-1", node, base)
	store.Record("sbx-2", node, base)

	store.ReconcileNode(node, []string{"sbx-2", "sbx-3", "sbx-3", ""}, base.Add(time.Second))

	if _, ok, err := store.Get("sbx-1", base.Add(2*time.Second)); err != nil || ok {
		t.Fatalf("expected reconcile to remove sandbox missing from heartbeat roster, got ok=%v err=%v", ok, err)
	}
	for _, sandboxID := range []string{"sbx-2", "sbx-3"} {
		got, ok, err := store.Get(sandboxID, base.Add(2*time.Second))
		if err != nil || !ok || got.ID != node.ID {
			t.Fatalf("expected %s to resolve to node-a, got (%+v, %v, %v)", sandboxID, got, ok, err)
		}
	}
	if _, ok, err := store.Get("sbx-2", base.Add(11*time.Second)); err != nil || ok {
		t.Fatalf("expected reconcile to refresh ttl from latest heartbeat time, got ok=%v err=%v", ok, err)
	}
}

func TestBindingStoreMovesBindingBetweenNodes(t *testing.T) {
	store := NewInMemoryBindingStore(10 * time.Second)
	base := time.Unix(300, 0)

	store.Record("sbx-1", Node{ID: "node-a", Endpoint: "http://node-a"}, base)
	store.Record("sbx-1", Node{ID: "node-b", Endpoint: "http://node-b"}, base.Add(time.Second))
	store.ReconcileNode(Node{ID: "node-a", Endpoint: "http://node-a"}, nil, base.Add(2*time.Second))

	got, ok, err := store.Get("sbx-1", base.Add(3*time.Second))
	if err != nil || !ok {
		t.Fatalf("expected moved binding to remain available, got ok=%v err=%v", ok, err)
	}
	if got.ID != "node-b" {
		t.Fatalf("expected binding to move to node-b, got %q", got.ID)
	}
}

func TestArtifactStoreRecordsForgetsAndRemovesNode(t *testing.T) {
	store := NewInMemoryArtifactStore(10, 0)

	store.Record("cluster-1", "backend", "artifact-a", "node-a")
	store.Record("cluster-1", "backend", "artifact-a", "node-b")
	store.Record("cluster-1", "other", "artifact-a", "node-c")

	nodes := store.Lookup("cluster-1", "backend", "artifact-a")
	if len(nodes) != 2 {
		t.Fatalf("expected 2 iroh providers, got %v", nodes)
	}

	store.Forget("cluster-1", "backend", "artifact-a", "node-a")
	nodes = store.Lookup("cluster-1", "backend", "artifact-a")
	if len(nodes) != 1 || nodes[0] != "node-b" {
		t.Fatalf("expected only node-b after forget, got %v", nodes)
	}

	store.ForgetNode("node-b")
	if nodes := store.Lookup("cluster-1", "backend", "artifact-a"); len(nodes) != 0 {
		t.Fatalf("expected node removal to clear artifact, got %v", nodes)
	}
	if nodes := store.Lookup("cluster-1", "other", "artifact-a"); len(nodes) != 1 || nodes[0] != "node-c" {
		t.Fatalf("expected other backend provider to remain, got %v", nodes)
	}
}

func TestArtifactStoreCapacityCountsArtifactKeys(t *testing.T) {
	store := NewInMemoryArtifactStore(2, 0)

	store.Record("cluster-1", "backend", "artifact-a", "node-a")
	store.Record("cluster-1", "backend", "artifact-a", "node-b")
	store.Record("cluster-1", "backend", "artifact-b", "node-c")

	nodes := store.Lookup("cluster-1", "backend", "artifact-a")
	if len(nodes) != 2 {
		t.Fatalf("expected artifact-a providers to share one capacity entry, got %v", nodes)
	}
	if nodes := store.Lookup("cluster-1", "backend", "artifact-b"); len(nodes) != 1 || nodes[0] != "node-c" {
		t.Fatalf("expected artifact-b to remain, got %v", nodes)
	}
}

func TestArtifactStoreEvictsLeastRecentlyUsedArtifactKey(t *testing.T) {
	store := NewInMemoryArtifactStore(2, 0)

	store.Record("cluster-1", "backend", "artifact-a", "node-a")
	store.Record("cluster-1", "backend", "artifact-a", "node-b")
	store.Record("cluster-1", "backend", "artifact-b", "node-c")
	store.Record("cluster-1", "backend", "artifact-c", "node-d")

	if nodes := store.Lookup("cluster-1", "backend", "artifact-a"); len(nodes) != 0 {
		t.Fatalf("expected artifact-a to be evicted with all providers, got %v", nodes)
	}
	if nodes := store.Lookup("cluster-1", "backend", "artifact-b"); len(nodes) != 1 || nodes[0] != "node-c" {
		t.Fatalf("expected artifact-b to remain, got %v", nodes)
	}
	if nodes := store.Lookup("cluster-1", "backend", "artifact-c"); len(nodes) != 1 || nodes[0] != "node-d" {
		t.Fatalf("expected artifact-c to remain, got %v", nodes)
	}

	store.ForgetNode("node-a")
	if nodes := store.Lookup("cluster-1", "backend", "artifact-b"); len(nodes) != 1 || nodes[0] != "node-c" {
		t.Fatalf("expected evicted node reverse index to be cleaned, got %v", nodes)
	}
}

func TestArtifactStoreLookupRefreshesLRU(t *testing.T) {
	store := NewInMemoryArtifactStore(2, 0)

	store.Record("cluster-1", "backend", "artifact-a", "node-a")
	store.Record("cluster-1", "backend", "artifact-b", "node-b")
	if nodes := store.Lookup("cluster-1", "backend", "artifact-a"); len(nodes) != 1 || nodes[0] != "node-a" {
		t.Fatalf("expected artifact-a before eviction, got %v", nodes)
	}
	store.Record("cluster-1", "backend", "artifact-c", "node-c")

	if nodes := store.Lookup("cluster-1", "backend", "artifact-a"); len(nodes) != 1 || nodes[0] != "node-a" {
		t.Fatalf("expected lookup to refresh artifact-a, got %v", nodes)
	}
	if nodes := store.Lookup("cluster-1", "backend", "artifact-b"); len(nodes) != 0 {
		t.Fatalf("expected artifact-b to be evicted, got %v", nodes)
	}
}

func TestArtifactStoreRecordRefreshesLRU(t *testing.T) {
	store := NewInMemoryArtifactStore(2, 0)

	store.Record("cluster-1", "backend", "artifact-a", "node-a")
	store.Record("cluster-1", "backend", "artifact-b", "node-b")
	store.Record("cluster-1", "backend", "artifact-a", "node-c")
	store.Record("cluster-1", "backend", "artifact-c", "node-d")

	nodes := store.Lookup("cluster-1", "backend", "artifact-a")
	if len(nodes) != 2 {
		t.Fatalf("expected record to refresh artifact-a, got %v", nodes)
	}
	if nodes := store.Lookup("cluster-1", "backend", "artifact-b"); len(nodes) != 0 {
		t.Fatalf("expected artifact-b to be evicted, got %v", nodes)
	}
}

func TestArtifactStoreForgetRemovesKeysFromLRU(t *testing.T) {
	store := NewInMemoryArtifactStore(2, 0)

	store.Record("cluster-1", "backend", "artifact-a", "node-a")
	store.Record("cluster-1", "backend", "artifact-b", "node-b")
	store.Forget("cluster-1", "backend", "artifact-a", "node-a")
	store.Record("cluster-1", "backend", "artifact-c", "node-c")

	if nodes := store.Lookup("cluster-1", "backend", "artifact-b"); len(nodes) != 1 || nodes[0] != "node-b" {
		t.Fatalf("expected artifact-b to remain after forgetting artifact-a, got %v", nodes)
	}
	if nodes := store.Lookup("cluster-1", "backend", "artifact-c"); len(nodes) != 1 || nodes[0] != "node-c" {
		t.Fatalf("expected artifact-c to remain, got %v", nodes)
	}
}

func TestArtifactStoreForgetNodeRemovesKeysFromLRU(t *testing.T) {
	store := NewInMemoryArtifactStore(2, 0)

	store.Record("cluster-1", "backend", "artifact-a", "node-a")
	store.Record("cluster-1", "backend", "artifact-b", "node-b")
	store.ForgetNode("node-a")
	store.Record("cluster-1", "backend", "artifact-c", "node-c")

	if nodes := store.Lookup("cluster-1", "backend", "artifact-b"); len(nodes) != 1 || nodes[0] != "node-b" {
		t.Fatalf("expected artifact-b to remain after forgetting node-a, got %v", nodes)
	}
	if nodes := store.Lookup("cluster-1", "backend", "artifact-c"); len(nodes) != 1 || nodes[0] != "node-c" {
		t.Fatalf("expected artifact-c to remain, got %v", nodes)
	}
}

func TestArtifactStoreLookupLimitsReturnedNodes(t *testing.T) {
	store := NewInMemoryArtifactStore(10, 2)

	store.Record("cluster-1", "backend", "artifact-a", "node-a")
	store.Record("cluster-1", "backend", "artifact-a", "node-b")
	store.Record("cluster-1", "backend", "artifact-a", "node-c")

	nodes := store.Lookup("cluster-1", "backend", "artifact-a")
	if len(nodes) != 2 {
		t.Fatalf("expected lookup to return 2 nodes, got %v", nodes)
	}
	for _, nodeID := range nodes {
		if nodeID != "node-a" && nodeID != "node-b" && nodeID != "node-c" {
			t.Fatalf("lookup returned unexpected node %q", nodeID)
		}
	}
}

func TestArtifactStoreLookupAllIgnoresReturnedNodeLimit(t *testing.T) {
	store := NewInMemoryArtifactStore(10, 1)
	for _, nodeID := range []string{"node-a", "node-b", "node-c"} {
		store.Record("cluster", "backend", "key", nodeID)
	}

	if got := len(store.LookupAll("cluster", "backend", "key")); got != 3 {
		t.Fatalf("LookupAll returned %d nodes, want 3", got)
	}
}

func TestArtifactStoreLookupReturnsAllNodesWhenLimitIsNonPositive(t *testing.T) {
	for _, limit := range []int{0, -1} {
		store := NewInMemoryArtifactStore(10, limit)

		store.Record("cluster-1", "backend", "artifact-a", "node-a")
		store.Record("cluster-1", "backend", "artifact-a", "node-b")
		store.Record("cluster-1", "backend", "artifact-a", "node-c")

		if nodes := store.Lookup("cluster-1", "backend", "artifact-a"); len(nodes) != 3 {
			t.Fatalf("expected limit %d to return all nodes, got %v", limit, nodes)
		}
	}
}
