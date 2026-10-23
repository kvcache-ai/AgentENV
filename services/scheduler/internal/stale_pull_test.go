package scheduler

import (
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
)

// guozy18's review (#341): a pull that started before a heartbeat in the
// same millisecond passed the freshness guard — the report timestamp was
// millisecond-truncated, so the same-ms heartbeat compared as "not after"
// the pull's capture time — and the stale pull overwrote the heartbeat's
// observation. Deterministic; no goroutines needed. (Bindings are no longer
// at stake: pulls never reconcile, yingdi-shan's observations-only rule.)
func TestStalePullSameMillisecondMustNotOverwriteHeartbeat(t *testing.T) {
	base := time.UnixMilli(1_760_000_000_000)
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL)
	store := NewInMemoryBindingStore(defaultObservedReportTTL)
	service := NewService(zap.NewNop(), registry, NewStrategy("round_robin"), store)

	// The pull captures its data at T+100µs.
	fetchedAt := base.Add(100 * time.Microsecond)

	// A heartbeat lands at T+200µs: newer data, same millisecond. Its
	// roster carries the freshly created sandbox.
	heartbeat := &schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ServiceInstanceId: "svc-1",
		SandboxIds:        []string{"sbx-new"},
		Snapshot: &schedulerv1.NodeSnapshot{
			Status:       schedulerv1.NodeStatus_NODE_STATUS_READY,
			SandboxCount: 100,
		},
	}
	if _, err := service.ingestNodeReport(heartbeat, time.Time{}, base.Add(200*time.Microsecond)); err != nil {
		t.Fatalf("heartbeat ingest failed: %v", err)
	}

	// The stale pull response arrives: captured before the heartbeat, it
	// holds an older snapshot and a roster without sbx-new.
	stale := &schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ServiceInstanceId: "svc-1",
		SandboxIds:        []string{},
		Snapshot: &schedulerv1.NodeSnapshot{
			Status:       schedulerv1.NodeStatus_NODE_STATUS_READY,
			SandboxCount: 1,
		},
	}
	if _, err := service.ingestNodeReport(stale, fetchedAt, base.Add(300*time.Microsecond)); err != nil {
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
// latest report still refreshes the observation — but never reconciles
// bindings, even a fresh one (observations-only pulls, #341 review).
func TestFreshPullStillIngests(t *testing.T) {
	base := time.UnixMilli(1_760_000_000_000)
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL)
	store := NewInMemoryBindingStore(defaultObservedReportTTL)
	service := NewService(zap.NewNop(), registry, NewStrategy("round_robin"), store)

	heartbeat := &schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ServiceInstanceId: "svc-1",
		SandboxIds:        []string{"sbx-old"},
		Snapshot: &schedulerv1.NodeSnapshot{
			Status:       schedulerv1.NodeStatus_NODE_STATUS_READY,
			SandboxCount: 1,
		},
	}
	if _, err := service.ingestNodeReport(heartbeat, time.Time{}, base); err != nil {
		t.Fatalf("heartbeat ingest failed: %v", err)
	}

	// Pull captured a second later: it wins the freshness check and updates
	// the observation, but its roster is ignored.
	fresh := &schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ServiceInstanceId: "svc-1",
		SandboxIds:        []string{"sbx-new"},
		Snapshot: &schedulerv1.NodeSnapshot{
			Status:       schedulerv1.NodeStatus_NODE_STATUS_READY,
			SandboxCount: 7,
		},
	}
	if _, err := service.ingestNodeReport(fresh, base.Add(time.Second), base.Add(2*time.Second)); err != nil {
		t.Fatalf("fresh pull ingest failed: %v", err)
	}

	if got := registry.PeekObserved("node-a").GetSandboxCount(); got != 7 {
		t.Fatalf("fresh pull must refresh the observation: sandbox count = %d, want 7", got)
	}
	if _, found, _ := store.Get("sbx-new", base.Add(3*time.Second)); found {
		t.Fatal("pulls never reconcile: sbx-new binding must not be created by a pull")
	}
	if _, found, _ := store.Get("sbx-old", base.Add(3*time.Second)); !found {
		t.Fatal("pulls never reconcile: sbx-old binding must survive the pull")
	}
}
