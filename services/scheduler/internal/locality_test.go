package scheduler

import (
	"testing"

	schedulerv1 "agentenv/services/api/proto"
)

func TestPreferLocalNodesSelectsHighestCoverage(t *testing.T) {
	store := NewInMemoryArtifactStore(10, 1)
	for _, key := range []string{"key-a", "key-b", "key-c"} {
		store.Record("cluster", "iroh", key, "node-a")
	}
	for _, key := range []string{"key-a", "key-b"} {
		store.Record("cluster", "iroh", key, "node-b")
	}

	preferred, stats := PreferLocalNodes(
		localityTestNodes("cluster", "iroh", "node-a", "node-b"),
		localityTestRequirements("key-a", "key-b", "key-c"),
		store,
	)
	assertLocalityNodeIDs(t, preferred, "node-a")
	if stats.RequirementCount != 3 || stats.ProviderNodeCount != 2 || stats.MaxCoverage != 3 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestPreferLocalNodesRetainsHighestCoverageTies(t *testing.T) {
	store := NewInMemoryArtifactStore(10, 0)
	for _, nodeID := range []string{"node-a", "node-b"} {
		store.Record("cluster", "iroh", "key", nodeID)
	}

	preferred, _ := PreferLocalNodes(
		localityTestNodes("cluster", "iroh", "node-a", "node-b", "node-c"),
		localityTestRequirements("key"),
		store,
	)
	assertLocalityNodeIDs(t, preferred, "node-a", "node-b")
}

func TestPreferLocalNodesPreservesCandidatesWithoutUsefulLocality(t *testing.T) {
	tests := []struct {
		name         string
		requirements []*schedulerv1.LocalityRequirement
		record       func(*InMemoryArtifactStore)
	}{
		{name: "no requirements"},
		{name: "empty requirements", requirements: localityTestRequirements("", "  ")},
		{name: "no inventory", requirements: localityTestRequirements("key")},
		{
			name:         "only non-candidate provider",
			requirements: localityTestRequirements("key"),
			record: func(store *InMemoryArtifactStore) {
				store.Record("cluster", "iroh", "key", "removed-node")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewInMemoryArtifactStore(10, 0)
			if test.record != nil {
				test.record(store)
			}
			nodes := localityTestNodes("cluster", "iroh", "node-a", "node-b")
			preferred, _ := PreferLocalNodes(nodes, test.requirements, store)
			assertLocalityNodeIDs(t, preferred, "node-a", "node-b")
		})
	}
}

func TestPreferLocalNodesIsolatesClusterAndBackend(t *testing.T) {
	store := NewInMemoryArtifactStore(10, 0)
	store.Record("cluster-a", "iroh", "key", "node-a")
	store.Record("cluster-b", "iroh", "key", "node-b")
	store.Record("cluster-a", "other", "key", "node-c")

	nodes := []RichNode{
		{Node: Node{ID: "node-a"}, ClusterID: "cluster-a", P2pBackend: "iroh"},
		{Node: Node{ID: "node-b"}, ClusterID: "cluster-a", P2pBackend: "iroh"},
		{Node: Node{ID: "node-c"}, ClusterID: "cluster-a", P2pBackend: "iroh"},
	}
	preferred, _ := PreferLocalNodes(nodes, localityTestRequirements("key"), store)
	assertLocalityNodeIDs(t, preferred, "node-a")
}

func TestPreferLocalNodesDeduplicatesRequirements(t *testing.T) {
	store := NewInMemoryArtifactStore(10, 0)
	store.Record("cluster", "iroh", "key-a", "node-a")
	store.Record("cluster", "iroh", "key-b", "node-b")

	preferred, stats := PreferLocalNodes(
		localityTestNodes("cluster", "iroh", "node-a", "node-b"),
		localityTestRequirements("key-a", "key-a", " key-a ", "key-b"),
		store,
	)
	assertLocalityNodeIDs(t, preferred, "node-a", "node-b")
	if stats.RequirementCount != 2 || stats.MaxCoverage != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestPreferLocalNodesSkipsCandidatesWithoutLocalityContext(t *testing.T) {
	store := NewInMemoryArtifactStore(10, 0)
	store.Record("cluster", "iroh", "key", "node-a")
	nodes := []RichNode{
		{Node: Node{ID: "node-a"}},
		{Node: Node{ID: "node-b"}, ClusterID: "cluster", P2pBackend: "iroh"},
	}

	preferred, stats := PreferLocalNodes(nodes, localityTestRequirements("key"), store)
	assertLocalityNodeIDs(t, preferred, "node-a", "node-b")
	if stats.MaxCoverage != 0 {
		t.Fatalf("unexpected locality hit without node context: %+v", stats)
	}
}

func TestPreferLocalNodesCountsRequirementsBeforeEarlyReturn(t *testing.T) {
	_, stats := PreferLocalNodes(
		nil,
		localityTestRequirements("key-a", "key-a", "key-b"),
		NewInMemoryArtifactStore(10, 0),
	)
	if stats.RequirementCount != 2 {
		t.Fatalf("requirement count = %d, want 2", stats.RequirementCount)
	}
}

type duplicateArtifactProvider struct{}

func (duplicateArtifactProvider) LookupEligible(
	string,
	string,
	string,
	map[string]struct{},
) []string {
	return []string{"node-a", "node-a"}
}

func TestPreferLocalNodesDeduplicatesProviderNodeIDs(t *testing.T) {
	preferred, stats := PreferLocalNodes(
		localityTestNodes("cluster", "iroh", "node-a"),
		localityTestRequirements("key-a"),
		duplicateArtifactProvider{},
	)
	assertLocalityNodeIDs(t, preferred, "node-a")
	if stats.MaxCoverage != 1 || stats.ProviderNodeCount != 1 {
		t.Fatalf("unexpected duplicate provider stats: %+v", stats)
	}
}

func localityTestNodes(clusterID, backend string, nodeIDs ...string) []RichNode {
	nodes := make([]RichNode, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		nodes = append(nodes, RichNode{
			Node:       Node{ID: nodeID},
			ClusterID:  clusterID,
			P2pBackend: backend,
		})
	}
	return nodes
}

func localityTestRequirements(keys ...string) []*schedulerv1.LocalityRequirement {
	requirements := make([]*schedulerv1.LocalityRequirement, 0, len(keys))
	for _, key := range keys {
		requirements = append(requirements, &schedulerv1.LocalityRequirement{Key: key})
	}
	return requirements
}

func assertLocalityNodeIDs(t *testing.T, nodes []RichNode, want ...string) {
	t.Helper()
	if len(nodes) != len(want) {
		t.Fatalf("node count = %d, want %d: %+v", len(nodes), len(want), nodes)
	}
	for i, node := range nodes {
		if node.ID != want[i] {
			t.Fatalf("node[%d] = %q, want %q", i, node.ID, want[i])
		}
	}
}
