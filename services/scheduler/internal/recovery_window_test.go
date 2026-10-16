package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Recovery-window and sync-node-snapshots tests (#259, test list group D).
//
// These tests pin the contract ahead of the Phase 3 business code; the ones
// covering still-unimplemented rules are expected to fail (red) until it
// lands.

func newTestService(t *testing.T, nodeIDs []string) (*Service, *AtomicNodeRegistry, *InMemoryBindingStore) {
	t.Helper()
	nodes := make([]Node, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		nodes = append(nodes, Node{ID: id, Endpoint: "http://" + id + ":8080"})
	}
	registry := NewAtomicNodeRegistry(nodes, 30*time.Second)
	store := NewInMemoryBindingStore(30 * time.Second)
	svc := NewService(nil, registry, NewRandomStrategy(), store)
	return svc, registry, store
}

func heartbeatFor(nodeID, instanceID string, sandboxIDs ...string) *schedulerv1.HeartbeatRequest {
	return &schedulerv1.HeartbeatRequest{
		NodeId:            nodeID,
		ServiceInstanceId: instanceID,
		SandboxIds:        sandboxIDs,
	}
}

func reportFor(nodeID, instanceID string, sandboxIDs ...string) *nodeReport {
	return nodeReportFromProto(heartbeatFor(nodeID, instanceID, sandboxIDs...))
}

// fixedClock is a controllable time source for window tests.
type fixedClock struct{ now time.Time }

func (c *fixedClock) Now() time.Time { return c.now }

// newLeaderTestService builds a service with leadership attached (recovery
// window = recoveryTTL) and a fixed clock the caller can advance.
func newLeaderTestService(t *testing.T, nodeIDs []string, recoveryTTL time.Duration, at, acquireAt time.Time) (*Service, *AtomicNodeRegistry, *fixedClock) {
	t.Helper()
	nodes := make([]Node, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		nodes = append(nodes, Node{ID: id, Endpoint: "http://" + id + ":8080"})
	}
	registry := NewAtomicNodeRegistry(nodes, 30*time.Second)
	store := NewInMemoryBindingStore(30 * time.Second)
	clock := &fixedClock{now: at}
	svc := NewService(nil, registry, NewStrategy("round_robin"), store,
		WithLeadership(acquiredLeadershipSnapshot(recoveryTTL, acquireAt, nodeIDs...)),
		WithClock(clock.Now),
	)
	return svc, registry, clock
}

// Case D3: the pulled-snapshot ingest path is the same code path as Heartbeat.
// Sync-node-snapshots feeds admin /nodes responses through ingestNodeReport;
// this test pins the contract: both entry points update observations and
// reconcile bindings identically.
func TestIngestNodeReportSharedWithHeartbeat(t *testing.T) {
	svc, registry, store := newTestService(t, []string{"node-a"})

	// Entry point 1: the Heartbeat RPC.
	if _, err := svc.Heartbeat(context.Background(), heartbeatFor("node-a", "inst-1", "sbx-1")); err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
	if snap := registry.PeekObserved("node-a"); snap == nil {
		t.Fatal("heartbeat must record an observation")
	}
	node, ok, err := store.Get("sbx-1", time.Now())
	if err != nil || !ok || node.ID != "node-a" {
		t.Fatalf("heartbeat must reconcile bindings: ok=%v node=%v err=%v", ok, node, err)
	}

	// Entry point 2: direct ingest (what sync-node-snapshots calls with a
	// pulled admin /nodes snapshot). Same observable effects.
	if _, err := svc.ingestNodeReport(reportFor("node-a", "inst-1", "sbx-2"), time.Now()); err != nil {
		t.Fatalf("direct ingest failed: %v", err)
	}
	if snap := registry.PeekObserved("node-a"); snap == nil {
		t.Fatal("direct ingest must record an observation")
	}
	node, ok, err = store.Get("sbx-2", time.Now())
	if err != nil || !ok || node.ID != "node-a" {
		t.Fatalf("direct ingest must reconcile bindings: ok=%v node=%v err=%v", ok, node, err)
	}

	// Both entry points also stamp the same freshness marker the recovery
	// window reads.
	if _, ok := registry.LastReportAt("node-a"); !ok {
		t.Fatal("ingest must stamp LastReportAt for the recovery window")
	}
}

// Case D1: fresh-observations-only scheduling inside the recovery window.
//
// node-b's only observation predates acquisition (stale); node-a reported
// after (fresh). Round-robin over both candidates would alternate
// node-a → node-b on consecutive calls; the fresh-observations-only filter must pin every
// pick to node-a.
func TestScheduleFreshOnlyDuringRecoveryWindow(t *testing.T) {
	t0 := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	svc, _, _ := newLeaderTestService(t, []string{"node-a", "node-b"}, 30*time.Second, t0.Add(2*time.Second), t0)

	// Stale observation: reported before acquisition.
	if _, err := svc.ingestNodeReport(reportFor("node-b", "inst-b"), t0.Add(-10*time.Second)); err != nil {
		t.Fatalf("stale ingest failed: %v", err)
	}
	// Fresh observation: reported after acquisition.
	if _, err := svc.ingestNodeReport(reportFor("node-a", "inst-a"), t0.Add(time.Second)); err != nil {
		t.Fatalf("fresh ingest failed: %v", err)
	}

	for i := 0; i < 2; i++ {
		resp, err := svc.Schedule(context.Background(), &schedulerv1.ScheduleRequest{})
		if err != nil {
			t.Fatalf("schedule %d failed: %v", i, err)
		}
		if got := resp.GetNode().GetNodeId(); got != "node-a" {
			t.Fatalf("schedule %d picked %q; want node-a — node-b's observation predates acquisition and must not be a candidate", i, got)
		}
	}
}

// Case D2: zero fresh observations — Schedule must return Unavailable and
// never fall back to unobserved nodes (#191: no snapshot means "unknown",
// not "unlimited").
func TestScheduleZeroFreshObservationsReturnsUnavailable(t *testing.T) {
	t0 := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	svc, _, _ := newLeaderTestService(t, []string{"node-a"}, 30*time.Second, t0.Add(time.Second), t0)

	// The only observation predates acquisition: nothing is fresh.
	if _, err := svc.ingestNodeReport(reportFor("node-a", "inst-a"), t0.Add(-10*time.Second)); err != nil {
		t.Fatalf("stale ingest failed: %v", err)
	}

	_, err := svc.Schedule(context.Background(), &schedulerv1.ScheduleRequest{})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("want Unavailable with zero fresh observations, got %v", err)
	}
}

// Case D4: a node whose rebuild pull fails stays unobserved (and is kept out
// of scheduling by the fresh-observations-only rule) until its next report; successful
// pulls ingest through the shared path.
func TestPullFailureKeepsNodeUnobserved(t *testing.T) {
	t0 := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	svc, registry, _ := newLeaderTestService(t, []string{"node-a", "node-b"}, 30*time.Second, t0.Add(2*time.Second), t0)

	fetchCalls := 0
	fetch := func(_ context.Context, node Node) (*nodeReport, error) {
		fetchCalls++
		if node.ID == "node-b" {
			return nil, errors.New("admin endpoint unreachable")
		}
		return reportFor("node-a", "inst-a"), nil
	}
	ingest := func(report *nodeReport, now time.Time) error {
		_, err := svc.ingestNodeReport(report, now)
		return err
	}
	refresher := NewConcurrentNodeSnapshotRefresher(nil, ingest, fetch, 2)
	refresher.Refresh(context.Background(), registry.Snapshot(false))

	if fetchCalls != 2 {
		t.Fatalf("rebuild must pull every node exactly once; got %d fetches for 2 nodes", fetchCalls)
	}
	if _, ok := registry.LastReportAt("node-a"); !ok {
		t.Fatal("a successful pull must observe node-a")
	}
	if _, ok := registry.LastReportAt("node-b"); ok {
		t.Fatal("a failed pull must leave node-b unobserved")
	}

	// Fresh-only scheduling: only node-a is a candidate.
	resp, err := svc.Schedule(context.Background(), &schedulerv1.ScheduleRequest{})
	if err != nil {
		t.Fatalf("schedule failed: %v", err)
	}
	if got := resp.GetNode().GetNodeId(); got != "node-a" {
		t.Fatalf("schedule picked %q; want node-a — node-b's pull failed and it must stay unobserved", got)
	}
}

// Case D5: LookupNode/GetNode semantics during vs after the recovery window.
// A missing binding/observation is "not yet rebuilt" (Unavailable) while the
// window is open, and "does not exist" (NotFound) once it closes.
func TestLookupNodeWindowSemantics(t *testing.T) {
	t0 := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	svc, _, clock := newLeaderTestService(t, []string{"node-a"}, 30*time.Second, t0.Add(time.Second), t0)

	// Window open: missing entries are "not yet rebuilt".
	clock.now = t0.Add(time.Second)
	_, err := svc.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: "sbx-ghost"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("window open: LookupNode want Unavailable, got %v", err)
	}
	_, err = svc.GetNode(context.Background(), &schedulerv1.GetNodeRequest{NodeId: "node-ghost"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("window open: GetNode want Unavailable, got %v", err)
	}

	// Window closed (acquired more than the recovery TTL ago): missing
	// entries are "does not exist".
	clock.now = t0.Add(31 * time.Second)
	_, err = svc.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: "sbx-ghost"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("window closed: LookupNode want NotFound, got %v", err)
	}
	_, err = svc.GetNode(context.Background(), &schedulerv1.GetNodeRequest{NodeId: "node-ghost"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("window closed: GetNode want NotFound, got %v", err)
	}
}

// Case F4 regression (#341 review): a snapshot pull carries no roster, so
// pulling must never delete existing bindings — including bindings for
// paused sandboxes that no list endpoint would report.
func TestPullIngestPreservesBindingsWithoutRoster(t *testing.T) {
	svc, registry, store := newTestService(t, []string{"node-a"})

	// A binding exists, e.g. for a paused sandbox.
	if err := store.Record("sbx-paused", Node{ID: "node-a", Endpoint: "http://node-a:8080"}, time.Now()); err != nil {
		t.Fatalf("seed binding failed: %v", err)
	}

	// A pull-shaped report: observations present, roster unknown (nil).
	pulled := &nodeReport{nodeID: "node-a", serviceInstanceID: "inst-a"}
	if _, err := svc.ingestNodeReport(pulled, time.Now()); err != nil {
		t.Fatalf("pull ingest failed: %v", err)
	}

	if registry.PeekObserved("node-a") == nil {
		t.Fatal("pull must still record the observation")
	}
	node, ok, err := store.Get("sbx-paused", time.Now())
	if err != nil || !ok || node.ID != "node-a" {
		t.Fatalf("pull without a roster must preserve bindings: ok=%v node=%v err=%v", ok, node, err)
	}
}

// Case: #341 review (Copilot, service.go) — a node known at acquisition
// whose pull failed and whose reporter is still backing off must stay
// excluded after the recovery TTL expires; a node that joined after the
// acquisition keeps the steady-state fail-open admission.
func TestKnownUnobservedNodeStaysExcludedPastRecoveryTTL(t *testing.T) {
	t0 := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	svc, registry, clock := newLeaderTestService(t, []string{"node-old"}, 30*time.Second, t0.Add(2*time.Second), t0)

	// node-old was known at acquisition but never reports (pull failed,
	// reporter backing off up to 60s). node-new is registered later, so it
	// is not in the acquisition-time recovery-pending set.
	registry.Set([]Node{
		{ID: "node-old", Endpoint: "http://node-old:8080"},
		{ID: "node-new", Endpoint: "http://node-new:8080"},
	}, nil)

	// Well past the recovery TTL: the timer alone must not readmit node-old.
	clock.now = t0.Add(90 * time.Second)
	resp, err := svc.Schedule(context.Background(), &schedulerv1.ScheduleRequest{})
	if err != nil {
		t.Fatalf("schedule failed: %v", err)
	}
	if got := resp.GetNode().GetNodeId(); got != "node-new" {
		t.Fatalf("past the TTL, node-old must still be excluded; got %q", got)
	}

	// Once node-old finally reports, it becomes a candidate again.
	if _, err := svc.Heartbeat(context.Background(), heartbeatFor("node-old", "inst-o")); err != nil {
		t.Fatalf("late heartbeat failed: %v", err)
	}
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		resp, err := svc.Schedule(context.Background(), &schedulerv1.ScheduleRequest{})
		if err != nil {
			t.Fatalf("schedule %d failed: %v", i, err)
		}
		seen[resp.GetNode().GetNodeId()] = true
	}
	if !seen["node-old"] {
		t.Fatalf("after its late report, node-old must be schedulable; seen %v", seen)
	}
}
