package scheduler

import (
	"sync/atomic"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
)

// guozy18's review (#341): a pull that started before a heartbeat in the
// same millisecond passed the freshness guard — LastReportAt is
// millisecond-truncated, so the same-ms heartbeat compared as "not after"
// the pull's capture time — and the stale pull overwrote the heartbeat's
// observation and reconciled its stale roster, deleting bindings the
// heartbeat had just affirmed. Deterministic; no goroutines needed.
func TestStalePullSameMillisecondMustNotOverwriteHeartbeat(t *testing.T) {
	base := time.UnixMilli(1_760_000_000_000)
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL)
	store := NewInMemoryBindingStore(defaultObservedReportTTL)
	service := NewService(zap.NewNop(), registry, NewStrategy("round_robin"), store)

	// The pull captures its data at T+100µs.
	fetchedAt := base.Add(100 * time.Microsecond)

	// A heartbeat lands at T+200µs: newer data, same millisecond. Its
	// roster carries the freshly created sandbox.
	heartbeat := nodeReportFromProto(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ServiceInstanceId: "svc-1",
		SandboxIds:        []string{"sbx-new"},
		Snapshot: &schedulerv1.NodeSnapshot{
			Status:       schedulerv1.NodeStatus_NODE_STATUS_READY,
			SandboxCount: 100,
		},
	})
	if _, err := service.ingestNodeReport(heartbeat, base.Add(200*time.Microsecond)); err != nil {
		t.Fatalf("heartbeat ingest failed: %v", err)
	}

	// The stale pull response arrives: captured before the heartbeat, it
	// holds an older snapshot and a roster without sbx-new.
	stale := &nodeReport{
		nodeID:            "node-a",
		serviceInstanceID: "svc-1",
		fetchedAt:         fetchedAt,
		sandboxIDs:        []string{},
		snapshot: &schedulerv1.NodeSnapshot{
			Status:       schedulerv1.NodeStatus_NODE_STATUS_READY,
			SandboxCount: 1,
		},
	}
	if _, err := service.ingestNodeReport(stale, base.Add(300*time.Microsecond)); err != nil {
		t.Fatalf("stale pull ingest failed: %v", err)
	}

	if got := registry.PeekObserved("node-a").GetSandboxCount(); got != 100 {
		t.Fatalf("stale pull overwrote the heartbeat observation: sandbox count = %d, want 100", got)
	}
	// The binding the heartbeat's roster affirmed must survive the stale pull.
	if _, found, err := store.Get("sbx-new", base.Add(time.Second)); err != nil || !found {
		t.Fatalf("stale pull deleted a fresh binding (found=%v, err=%v)", found, err)
	}
}

// The accepted path must keep working: a pull captured after the node's
// latest report ingests and reconciles (heartbeat-parity rebuild, #259).
func TestFreshPullStillIngestsAndReconciles(t *testing.T) {
	base := time.UnixMilli(1_760_000_000_000)
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL)
	store := NewInMemoryBindingStore(defaultObservedReportTTL)
	service := NewService(zap.NewNop(), registry, NewStrategy("round_robin"), store)

	heartbeat := nodeReportFromProto(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ServiceInstanceId: "svc-1",
		SandboxIds:        []string{"sbx-old"},
		Snapshot: &schedulerv1.NodeSnapshot{
			Status:       schedulerv1.NodeStatus_NODE_STATUS_READY,
			SandboxCount: 1,
		},
	})
	if _, err := service.ingestNodeReport(heartbeat, base); err != nil {
		t.Fatalf("heartbeat ingest failed: %v", err)
	}

	// Pull captured a second later: newer roster (sbx-old gone, sbx-new up).
	fresh := &nodeReport{
		nodeID:            "node-a",
		serviceInstanceID: "svc-1",
		fetchedAt:         base.Add(time.Second),
		sandboxIDs:        []string{"sbx-new"},
		snapshot: &schedulerv1.NodeSnapshot{
			Status:       schedulerv1.NodeStatus_NODE_STATUS_READY,
			SandboxCount: 1,
		},
	}
	if _, err := service.ingestNodeReport(fresh, base.Add(2*time.Second)); err != nil {
		t.Fatalf("fresh pull ingest failed: %v", err)
	}

	if _, found, _ := store.Get("sbx-new", base.Add(3*time.Second)); !found {
		t.Fatal("fresh pull must reconcile its roster: sbx-new binding missing")
	}
	if _, found, _ := store.Get("sbx-old", base.Add(3*time.Second)); found {
		t.Fatal("fresh pull must reconcile its roster: sbx-old binding should be gone")
	}
}

// guozy18's wrap-up (#341): an accepted pull that already landed in the
// registry can still lose its roster reconcile to a newer heartbeat — the
// heartbeat updates Redis, then the pull's older roster reconcile deletes
// the heartbeat's bindings (brief routing drops). Report ingestion and
// binding reconciliation must be ordered per node. The hook parks the pull
// between registry ingest and reconcile, forcing the exact interleave.
func TestPullReconcileCannotOvertakeNewerHeartbeat(t *testing.T) {
	base := time.UnixMilli(1_760_000_000_000)
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL)
	store := NewInMemoryBindingStore(defaultObservedReportTTL)
	service := NewService(zap.NewNop(), registry, NewStrategy("round_robin"), store)

	// Seed the node so the pull is a rebuild, not a first report.
	seed := nodeReportFromProto(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ServiceInstanceId: "svc-1",
		SandboxIds:        []string{"sbx-old"},
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY, SandboxCount: 1},
	})
	if _, err := service.ingestNodeReport(seed, base); err != nil {
		t.Fatalf("seed heartbeat: %v", err)
	}

	// Park the FIRST ingest (the pull) between registry update and
	// reconcile. The gate must be a non-blocking CAS, not a sync.Once: a
	// second Once.Do blocks while the first closure runs, which would park
	// the heartbeat behind the pull and mimic the very ordering under test.
	pullParked := make(chan struct{})
	releasePull := make(chan struct{})
	var hookArmed atomic.Bool
	hookArmed.Store(true)
	service.beforeReconcile = func() {
		if hookArmed.CompareAndSwap(true, false) {
			close(pullParked)
			<-releasePull
		}
	}

	// The pull captured an empty roster at T2 (sbx-new did not exist yet)
	// and passes the freshness check.
	pull := &nodeReport{
		nodeID:            "node-a",
		serviceInstanceID: "svc-1",
		fetchedAt:         base.Add(time.Second),
		sandboxIDs:        []string{},
		snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY, SandboxCount: 0},
	}
	pullDone := make(chan error, 1)
	go func() {
		_, err := service.ingestNodeReport(pull, base.Add(2*time.Second))
		pullDone <- err
	}()
	<-pullParked

	// The heartbeat races in at T3 carrying both sandboxes.
	heartbeat := nodeReportFromProto(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ServiceInstanceId: "svc-1",
		SandboxIds:        []string{"sbx-old", "sbx-new"},
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY, SandboxCount: 2},
	})
	hbDone := make(chan error, 1)
	go func() {
		_, err := service.ingestNodeReport(heartbeat, base.Add(3*time.Second))
		hbDone <- err
	}()
	// Without per-node ordering the heartbeat completes while the pull is
	// parked; wait for that explicitly so releasing the pull deterministically
	// lands its stale roster after the heartbeat's Redis update. With
	// ordering, the heartbeat still waits for the pull's section when the
	// probe times out, and the release lets both run in order.
	hbFinished := false
	select {
	case err := <-hbDone:
		hbFinished = true
		if err != nil {
			t.Fatalf("heartbeat ingest: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
	}
	close(releasePull)
	if err := <-pullDone; err != nil {
		t.Fatalf("pull ingest: %v", err)
	}
	if !hbFinished {
		if err := <-hbDone; err != nil {
			t.Fatalf("heartbeat ingest: %v", err)
		}
	}

	if _, found, _ := store.Get("sbx-new", base.Add(4*time.Second)); !found {
		t.Fatal("stale pull reconcile deleted a binding the newer heartbeat affirmed")
	}
}
