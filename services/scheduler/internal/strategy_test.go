package scheduler

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	schedulerv1 "agentenv/services/api/proto"
)

func TestRoundRobin(t *testing.T) {
	s := &RoundRobinStrategy{}
	nodes := []RichNode{{Node: Node{ID: "a"}}, {Node: Node{ID: "b"}}, {Node: Node{ID: "c"}}}

	got1, _ := s.Select(nodes, nil)
	got2, _ := s.Select(nodes, nil)
	got3, _ := s.Select(nodes, nil)
	got4, _ := s.Select(nodes, nil)

	if got1.ID != "a" || got2.ID != "b" || got3.ID != "c" || got4.ID != "a" {
		t.Fatalf("unexpected order: %s %s %s %s", got1.ID, got2.ID, got3.ID, got4.ID)
	}
}

func TestRandomNoNodes(t *testing.T) {
	s := NewRandomStrategy()
	_, err := s.Select(nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestWeightedImageAffinityUsesDigestAcrossRegistries(t *testing.T) {
	strategy := NewWeightedImageAffinityStrategy(map[string]float64{
		"host-a": 1,
		"host-b": 1,
		"host-c": 1,
	})
	nodes := []RichNode{
		{Node: Node{ID: "pod-a", AffinityID: "host-a"}},
		{Node: Node{ID: "pod-b", AffinityID: "host-b"}},
		{Node: Node{ID: "pod-c", AffinityID: "host-c"}},
	}
	digest := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	first, err := strategy.Select(nodes, coldSandboxHint("registry-a/repo@sha256:"+digest))
	if err != nil {
		t.Fatal(err)
	}
	second, err := strategy.Select(nodes, coldSandboxHint("registry-b/mirror@sha256:"+digest))
	if err != nil {
		t.Fatal(err)
	}
	if first.AffinityID != second.AffinityID {
		t.Fatalf("same digest mapped to %q and %q", first.AffinityID, second.AffinityID)
	}
}

func TestWeightedImageAffinityHonorsReservedRuntime(t *testing.T) {
	strategy := NewWeightedImageAffinityStrategy(map[string]float64{"host-a": 1, "host-b": 1})
	nodes := []RichNode{
		{Node: Node{ID: "pod-a", AffinityID: "host-a"}},
		{Node: Node{ID: "pod-b", AffinityID: "host-b"}},
	}
	hint := coldSandboxHint("repo@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	hint.GetNewColdSandbox().Metadata = map[string]string{reservedRuntimeMetadataKey: "pod-b"}

	node, err := strategy.Select(nodes, hint)
	if err != nil {
		t.Fatal(err)
	}
	if node.ID != "pod-b" {
		t.Fatalf("selected %q instead of reserved runtime", node.ID)
	}
}

func TestWeightedImageAffinityRejectsUnavailableReservedRuntime(t *testing.T) {
	strategy := NewWeightedImageAffinityStrategy(map[string]float64{"host-a": 1})
	hint := coldSandboxHint("repo")
	hint.GetNewColdSandbox().Metadata = map[string]string{reservedRuntimeMetadataKey: "missing"}

	if _, err := strategy.Select([]RichNode{{Node: Node{ID: "pod-a"}}}, hint); !errors.Is(err, ErrNoNodes) {
		t.Fatalf("expected ErrNoNodes, got %v", err)
	}
}

func TestWeightedImageAffinitySurvivesPodIdentityChange(t *testing.T) {
	strategy := NewWeightedImageAffinityStrategy(map[string]float64{
		"host-a": 1,
		"host-b": 3,
	})
	oldPods := []RichNode{
		{Node: Node{ID: "pod-a-old", AffinityID: "host-a"}},
		{Node: Node{ID: "pod-b-old", AffinityID: "host-b"}},
	}
	newPods := []RichNode{
		{Node: Node{ID: "pod-b-new", AffinityID: "host-b"}},
		{Node: Node{ID: "pod-a-new", AffinityID: "host-a"}},
	}
	for i := 0; i < 1000; i++ {
		hint := coldSandboxHint(fmt.Sprintf("repo@sha256:%064x", i))
		before, err := strategy.Select(oldPods, hint)
		if err != nil {
			t.Fatal(err)
		}
		after, err := strategy.Select(newPods, hint)
		if err != nil {
			t.Fatal(err)
		}
		if before.AffinityID != after.AffinityID {
			t.Fatalf("image %d moved from %q to %q", i, before.AffinityID, after.AffinityID)
		}
	}
}

func TestWeightedImageAffinityHonorsWeights(t *testing.T) {
	strategy := NewWeightedImageAffinityStrategy(map[string]float64{
		"small": 1,
		"large": 3,
	})
	nodes := []RichNode{
		{Node: Node{ID: "small-pod", AffinityID: "small"}},
		{Node: Node{ID: "large-pod", AffinityID: "large"}},
	}
	counts := map[string]int{}
	for i := 0; i < 10000; i++ {
		node, err := strategy.Select(nodes, coldSandboxHint(fmt.Sprintf("repo@sha256:%064x", i)))
		if err != nil {
			t.Fatal(err)
		}
		counts[node.AffinityID]++
	}
	if counts["small"] < 2200 || counts["small"] > 2800 {
		t.Fatalf("unexpected weighted distribution: %#v", counts)
	}
}

func TestWeightedImageAffinityFallsBackToRoundRobinWithoutImage(t *testing.T) {
	strategy := NewWeightedImageAffinityStrategy(map[string]float64{"host-a": 1, "host-b": 1})
	nodes := []RichNode{
		{Node: Node{ID: "pod-a", AffinityID: "host-a"}},
		{Node: Node{ID: "pod-b", AffinityID: "host-b"}},
	}
	first, err := strategy.Select(nodes, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := strategy.Select(nodes, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != "pod-a" || second.ID != "pod-b" {
		t.Fatalf("unexpected fallback order: %q %q", first.ID, second.ID)
	}
}

func TestWeightedImageAffinitySkipsUnconfiguredNodes(t *testing.T) {
	strategy := NewWeightedImageAffinityStrategy(map[string]float64{"configured": 1})
	nodes := []RichNode{
		{Node: Node{ID: "unknown-pod", AffinityID: "unknown"}},
		{Node: Node{ID: "configured-pod", AffinityID: "configured"}},
	}
	node, err := strategy.Select(nodes, coldSandboxHint("repo@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	if node.AffinityID != "configured" {
		t.Fatalf("selected unconfigured node %q", node.AffinityID)
	}
}

func coldSandboxHint(image string) *schedulerv1.ScheduleRequestHint {
	return &schedulerv1.ScheduleRequestHint{
		Kind: &schedulerv1.ScheduleRequestHint_NewColdSandbox{
			NewColdSandbox: &schedulerv1.NewColdSandboxHint{Images: []string{image}},
		},
	}
}

func TestNewStrategyNormalizesNamesAndRejectsUnknown(t *testing.T) {
	for _, name := range []string{"round_robin", "random", "weighted_image_affinity"} {
		strategy, err := NewStrategy("  "+strings.ToUpper(name)+"  ", map[string]float64{"host": 1})
		if err != nil || strategy.Name() != name {
			t.Fatalf("strategy %q: got %v, %v", name, strategy, err)
		}
	}
	for _, name := range []string{"", "round-robin", "weighted_image_affinty"} {
		if strategy, err := NewStrategy(name); err == nil || strategy != nil {
			t.Fatalf("unknown strategy %q: got %v, %v", name, strategy, err)
		}
	}
}

func TestWeightedImageAffinityDoesNotFallBackWhenConfiguredHostsAreUnavailable(t *testing.T) {
	strategy := NewWeightedImageAffinityStrategy(map[string]float64{"configured": 1})
	nodes := []RichNode{{Node: Node{ID: "pod", AffinityID: "unconfigured"}}}
	if _, err := strategy.Select(nodes, coldSandboxHint("repo:latest")); !errors.Is(err, ErrNoNodes) {
		t.Fatalf("expected ErrNoNodes for unmatched weights, got %v", err)
	}
	// Requests without an image key intentionally use all eligible nodes.
	if node, err := strategy.Select(nodes, nil); err != nil || node.ID != "pod" {
		t.Fatalf("no-image fallback: got %v, %v", node, err)
	}
}
