package scheduler

import (
	"testing"
	"time"
)

func roundRobinTestNodes(ids ...string) []RichNode {
	nodes := make([]RichNode, len(ids))
	for i, id := range ids {
		nodes[i] = RichNode{Node: Node{ID: id}}
	}
	return nodes
}

func TestRoundRobin(t *testing.T) {
	s := &RoundRobinStrategy{}
	nodes := roundRobinTestNodes("a", "b", "c")

	got1, _ := s.Select(nodes, nil)
	got2, _ := s.Select(nodes, nil)
	got3, _ := s.Select(nodes, nil)
	got4, _ := s.Select(nodes, nil)

	if got1.ID != "a" || got2.ID != "b" || got3.ID != "c" || got4.ID != "a" {
		t.Fatalf("unexpected order: %s %s %s %s", got1.ID, got2.ID, got3.ID, got4.ID)
	}
}

func TestRoundRobinNoNodes(t *testing.T) {
	s := &RoundRobinStrategy{}
	if _, err := s.Select(nil, nil); err != ErrNoNodes {
		t.Fatalf("expected ErrNoNodes, got %v", err)
	}
}

func TestRoundRobinFilteredNodeResumesFairly(t *testing.T) {
	clock := time.Unix(1_700_000_000, 0)
	s := &RoundRobinStrategy{now: func() time.Time { return clock }}
	all := roundRobinTestNodes("a", "b", "c")
	ab := all[:2]

	assertSelect := func(nodes []RichNode, want string) {
		t.Helper()
		clock = clock.Add(time.Second)
		got, err := s.Select(nodes, nil)
		if err != nil {
			t.Fatalf("Select: %v", err)
		}
		if got.ID != want {
			t.Fatalf("expected %s, got %s", want, got.ID)
		}
	}

	assertSelect(all, "a")
	// c is filtered out for a while; a and b alternate.
	assertSelect(ab, "b")
	assertSelect(ab, "a")
	assertSelect(ab, "b")
	// c rejoins: never picked, so it is selected immediately.
	assertSelect(all, "c")
	// Then the normal cycle resumes.
	assertSelect(all, "a")
	assertSelect(all, "b")
}

func TestRoundRobinNewNodePickedFirst(t *testing.T) {
	clock := time.Unix(1_700_000_000, 0)
	s := &RoundRobinStrategy{now: func() time.Time { return clock }}
	nodes := roundRobinTestNodes("a", "b")

	if _, err := s.Select(nodes, nil); err != nil {
		t.Fatalf("Select: %v", err)
	}
	if _, err := s.Select(nodes, nil); err != nil {
		t.Fatalf("Select: %v", err)
	}

	// A node that joins later has never been picked and wins immediately.
	joined := roundRobinTestNodes("a", "b", "c")
	got, err := s.Select(joined, nil)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if got.ID != "c" {
		t.Fatalf("expected new node c, got %s", got.ID)
	}
}

func TestRoundRobinDeterministicForInputOrder(t *testing.T) {
	clock := time.Unix(1_700_000_000, 0)
	s1 := &RoundRobinStrategy{now: func() time.Time { return clock }}
	s2 := &RoundRobinStrategy{now: func() time.Time { return clock }}
	fwd := roundRobinTestNodes("a", "b", "c")
	rev := roundRobinTestNodes("c", "b", "a")

	for i := 0; i < 6; i++ {
		got1, err := s1.Select(fwd, nil)
		if err != nil {
			t.Fatalf("Select: %v", err)
		}
		got2, err := s2.Select(rev, nil)
		if err != nil {
			t.Fatalf("Select: %v", err)
		}
		if got1.ID != got2.ID {
			t.Fatalf("iteration %d: input order changed the pick: %s vs %s", i, got1.ID, got2.ID)
		}
	}
}

func TestRoundRobinExpiresStaleEntries(t *testing.T) {
	clock := time.Unix(1_700_000_000, 0)
	s := &RoundRobinStrategy{now: func() time.Time { return clock }}
	nodes := roundRobinTestNodes("a", "b")

	if _, err := s.Select(nodes, nil); err != nil {
		t.Fatalf("Select: %v", err)
	}
	if _, err := s.Select(nodes, nil); err != nil {
		t.Fatalf("Select: %v", err)
	}

	// After the expiry, both entries are swept and both nodes count as never
	// picked again; the tie breaks to the smaller node ID.
	clock = clock.Add(roundRobinEntryExpiry + time.Hour)
	got, err := s.Select(nodes, nil)
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	if got.ID != "a" {
		t.Fatalf("expected a after expiry, got %s", got.ID)
	}
	if len(s.entries) != 1 {
		t.Fatalf("expected stale entries swept, got %d entries", len(s.entries))
	}
}

func TestRoundRobinExpirySweepThrottled(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	clock := start
	s := &RoundRobinStrategy{now: func() time.Time { return clock }}
	nodes := roundRobinTestNodes("a")

	// First Select runs a sweep and records lastSweep.
	if _, err := s.Select(nodes, nil); err != nil {
		t.Fatalf("Select: %v", err)
	}
	// Inject an already-stale entry right after the sweep.
	s.entries["old"] = start.Add(-roundRobinEntryExpiry - time.Second)

	// Within the sweep interval the stale entry survives.
	clock = start.Add(30 * time.Second)
	if _, err := s.Select(nodes, nil); err != nil {
		t.Fatalf("Select: %v", err)
	}
	if _, ok := s.entries["old"]; !ok {
		t.Fatal("sweep ran before the interval elapsed")
	}

	// Once the interval elapses the stale entry is swept.
	clock = start.Add(roundRobinExpirySweepInterval)
	if _, err := s.Select(nodes, nil); err != nil {
		t.Fatalf("Select: %v", err)
	}
	if _, ok := s.entries["old"]; ok {
		t.Fatal("stale entry not swept after the interval elapsed")
	}
}

func TestRandomNoNodes(t *testing.T) {
	s := NewRandomStrategy()
	_, err := s.Select(nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}
