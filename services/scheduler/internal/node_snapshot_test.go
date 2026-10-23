package scheduler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

// Sync-node-snapshots fetcher tests (#259): the production pull assembles a
// heartbeat-shaped request from GET /nodes + GET /sandboxes, honoring the
// x-api-key header.

func adminTestServer(t *testing.T, wantKey string, nodeJSON, sandboxesJSON string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/nodes", func(w http.ResponseWriter, r *http.Request) {
		if wantKey != "" && r.Header.Get("x-api-key") != wantKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(nodeJSON))
	})
	mux.HandleFunc("/sandboxes", func(w http.ResponseWriter, r *http.Request) {
		if wantKey != "" && r.Header.Get("x-api-key") != wantKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(sandboxesJSON))
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

const adminNodeFixture = `[{
  "version": "0.2.0",
  "commit": "abc123",
  "id": "node-a",
  "serviceInstanceID": "inst-1",
  "clusterID": "cluster-1",
  "sandboxCount": 2,
  "createSuccesses": 10,
  "createFails": 1,
  "sandboxStartingCount": 1,
  "sandboxPausedCount": 3,
  "machineInfo": {"cpuFamily": "6", "cpuModel": "85", "cpuModelName": "Xeon", "cpuArchitecture": "x86_64", "cpuConfigJSON": "{}"},
  "metrics": {
    "allocatedCPU": 4,
    "allocatedMemoryBytes": 8589934592,
    "cpuPercent": 55,
    "cpuCount": 16,
    "memoryUsedBytes": 17179869184,
    "memoryTotalBytes": 34359738368,
    "pausedAllocatedCPU": 2,
    "pausedAllocatedMemoryBytes": 4294967296,
    "disks": [{"mountPoint": "/", "device": "/dev/ublkb0", "filesystemType": "ext4", "usedBytes": 1024, "totalBytes": 4096}]
  }
}]`

const adminSandboxesFixture = `[{"sandboxID": "sbx-1"}, {"sandboxID": "sbx-2"}]`

func TestAdminSnapshotFetcherAssemblesHeartbeatShape(t *testing.T) {
	srv := adminTestServer(t, "secret", adminNodeFixture, adminSandboxesFixture)
	fetch := NewAdminSnapshotFetcher("secret")

	req, _, err := fetch(context.Background(), Node{ID: "node-a", Endpoint: srv.URL})
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}

	if req.GetNodeId() != "node-a" || req.GetServiceInstanceId() != "inst-1" || req.GetClusterId() != "cluster-1" {
		t.Fatalf("identity fields wrong: %v", req)
	}
	snap := req.GetSnapshot()
	if snap == nil {
		t.Fatal("snapshot must be populated")
	}
	if snap.GetAllocatedCpu() != 4 || snap.GetCpuPercent() != 55 || snap.GetSandboxCount() != 2 {
		t.Fatalf("metrics mapping wrong: %+v", snap)
	}
	if snap.GetPausedSandboxCount() != 3 || snap.GetPausedAllocatedCpu() != 2 {
		t.Fatalf("paused mapping wrong: %+v", snap)
	}
	if len(snap.GetDisks()) != 1 || snap.GetDisks()[0].GetDevice() != "/dev/ublkb0" {
		t.Fatalf("disk mapping wrong: %+v", snap.GetDisks())
	}
	// Pulls carry observations only: the roster is never mapped (#341
	// review, yingdi-shan).
	if req.GetSandboxIds() != nil {
		t.Fatalf("pull must leave SandboxIds nil, got %v", req.GetSandboxIds())
	}
}

const adminParityNodeFixture = `[{
  "version": "0.2.0",
  "commit": "abc123",
  "id": "node-a",
  "serviceInstanceID": "inst-1",
  "clusterID": "cluster-1",
  "sandboxCount": 2,
  "createSuccesses": 10,
  "createFails": 1,
  "sandboxStartingCount": 1,
  "sandboxPausedCount": 3,
  "p2pEndpoint": {"backend": "iroh", "address": "node-a.p2p.local"},
  "machineInfo": {"cpuFamily": "6", "cpuModel": "85", "cpuModelName": "Xeon", "cpuArchitecture": "x86_64", "cpuConfigJSON": "{\"ht\":true}"},
  "metrics": {
    "allocatedCPU": 4,
    "allocatedMemoryBytes": 8589934592,
    "cpuPercent": 55,
    "cpuCount": 16,
    "memoryUsedBytes": 17179869184,
    "memoryTotalBytes": 34359738368,
    "pausedAllocatedCPU": 2,
    "pausedAllocatedMemoryBytes": 4294967296,
    "disks": [{"mountPoint": "/", "device": "/dev/ublkb0", "filesystemType": "ext4", "usedBytes": 1024, "totalBytes": 4096}]
  }
}]`

// Parity path (#341 review): when the admin API exposes the heartbeat fields
// the pull keeps (p2pEndpoint, cpuConfigJSON), it maps them through.
func TestAdminSnapshotFetcherParityFields(t *testing.T) {
	srv := adminTestServer(t, "", adminParityNodeFixture, "[]")
	fetch := NewAdminSnapshotFetcher("")

	req, _, err := fetch(context.Background(), Node{ID: "node-a", Endpoint: srv.URL})
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if req.GetP2PEndpoint().GetBackend() != "iroh" || req.GetP2PEndpoint().GetAddress() != "node-a.p2p.local" {
		t.Fatalf("parity p2p mapping wrong: %v", req.GetP2PEndpoint())
	}
	if req.GetMachineInfo().GetCpuConfigJson() != `{"ht":true}` {
		t.Fatalf("parity cpuConfigJSON mapping wrong: %q", req.GetMachineInfo().GetCpuConfigJson())
	}
}

func TestAdminSnapshotFetcherAuthFailure(t *testing.T) {
	srv := adminTestServer(t, "secret", adminNodeFixture, adminSandboxesFixture)
	fetch := NewAdminSnapshotFetcher("wrong-key")

	if _, _, err := fetch(context.Background(), Node{ID: "node-a", Endpoint: srv.URL}); err == nil {
		t.Fatal("fetch with a wrong key must fail")
	}
}

func TestAdminSnapshotFetcherHeartbeatIngestCompatibility(t *testing.T) {
	srv := adminTestServer(t, "", adminNodeFixture, adminSandboxesFixture)
	fetch := NewAdminSnapshotFetcher("")

	svc, registry, store := newTestService(t, []string{"node-a"})
	req, fetchedAt, err := fetch(context.Background(), Node{ID: "node-a", Endpoint: srv.URL})
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if _, err := svc.ingestNodeReport(req, fetchedAt, time.Now()); err != nil {
		t.Fatalf("pulled snapshot must ingest through the shared path: %v", err)
	}
	if registry.PeekObserved("node-a") == nil {
		t.Fatal("pulled snapshot must record an observation")
	}
	// The pull refreshes observations only; bindings are refreshed by
	// heartbeats, so nothing may be written to the store here.
	if _, ok, err := store.Get("sbx-1", time.Now()); err != nil || ok {
		t.Fatalf("pull-ingest must not touch bindings: ok=%v err=%v", ok, err)
	}
}

// Freshness guard (#341 review): the pull path skips reports older than
// the node's latest observation — a pull captured before a heartbeat must
// not overwrite it, a pull captured after is applied, and heartbeat-shaped
// reports are never skipped.
func TestPulledIngestSkipsStaleReports(t *testing.T) {
	svc, registry, _ := newTestService(t, []string{"node-a"})
	t0 := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

	report := func(count uint32) *schedulerv1.HeartbeatRequest {
		return &schedulerv1.HeartbeatRequest{
			NodeId:            "node-a",
			ServiceInstanceId: "inst-1",
			Snapshot:          &schedulerv1.NodeSnapshot{SandboxCount: count},
		}
	}

	// A heartbeat lands at t0+1s.
	if _, err := svc.ingestNodeReport(report(100), time.Time{}, t0.Add(time.Second)); err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	// A pull captured before that heartbeat must not overwrite it.
	if _, err := svc.ingestNodeReport(report(1), t0, t0.Add(2*time.Second)); err != nil {
		t.Fatalf("stale pull ingest failed: %v", err)
	}
	if got := registry.PeekObserved("node-a").GetSandboxCount(); got != 100 {
		t.Fatalf("stale pull must not overwrite the newer heartbeat: sandbox count = %d, want 100", got)
	}

	// A pull captured after the heartbeat is applied.
	if _, err := svc.ingestNodeReport(report(7), t0.Add(3*time.Second), t0.Add(3*time.Second)); err != nil {
		t.Fatalf("fresh pull ingest failed: %v", err)
	}
	if got := registry.PeekObserved("node-a").GetSandboxCount(); got != 7 {
		t.Fatalf("fresh pull must be applied: sandbox count = %d, want 7", got)
	}

	// Heartbeat-shaped reports (zero fetchedAt) always apply.
	if _, err := svc.ingestNodeReport(report(9), time.Time{}, t0.Add(4*time.Second)); err != nil {
		t.Fatalf("heartbeat-shaped ingest failed: %v", err)
	}
	if got := registry.PeekObserved("node-a").GetSandboxCount(); got != 9 {
		t.Fatalf("heartbeat-shaped report must apply: sandbox count = %d, want 9", got)
	}
}
