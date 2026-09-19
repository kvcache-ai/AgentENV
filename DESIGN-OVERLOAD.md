# Overload, skew, and multitenancy design (fork patch stack)

Status: agreed design, 2026-09-19 (ryanng). Implementation language: Rust
only — the Go gateway stays untouched (it proxies node responses opaquely);
the Go scheduler gets replaced, not patched. Companion to `PATCHES.md`.

## Problem

- Round-robin placement ignores load; bindings pin a sandbox to its birth
  node forever, so skew accrues and is never corrected.
- Resume is an ungated side effect of a data-plane touch: a burst of
  resumes on one node can overcommit memory (cgroup OOM kills individual
  VMs today — blast-radius containment, not admission).
- kubelet cannot see the NVMe hostPath disks, so nothing sheds load when
  fc-work (disk 6, running sandboxes' writable state) or snapshots (disk 7,
  paused population) fill.
- One `X-API-Key` value is accepted from anyone: no attribution, no quota.

## Snapshot tiering: node-local by default, export as a pressure valve

Pause writes the snapshot to node disk 7 and stops there. No upload by
default; durability is a side effect of pressure, not a promise (a snapshot
that never crossed the export watermark dies with the node, same as
Capsule).

Watermarks on the snapshots disk (chart knobs):

- `< export_watermark` (default 0.70): nothing happens.
- `70–95%`: async LRU drain. Exporter walks snapshots coldest-first
  (least-recently-resumed `touched_at`), uploads to the snapshot store,
  verifies, deletes the local copy, flips the binding
  `PAUSED_LOCAL -> DURABLE_ONLY`.
- `>= blocking_watermark` (default 0.95): emergency flush. New pause writes
  block (429 + Retry-After) and the exporter flushes with priority until
  the disk drops below 95%. Running sandboxes and local resumes are
  unaffected; only new snapshot bytes wait.

One mechanism, two speeds; there is no separate eviction pass. A snapshot
is deletable locally iff durable; no live state ever has zero copies.

### Snapshot store

Reuse the existing publish machinery, don't invent a transport: snapshot
publishing already commits artifacts under `snapshot/v1/artifacts/
{snapshot_id}/...` with an OSS object-storage backend and P2P-first
resolution (`src/snapshot/p2p.rs`, `SnapshotManager::publish*`; overlaybd
layer identity via `src/overlaybd/p2p/artifact.rs` — snapshot publishing
must reuse that helper). Our addition is POLICY, not plumbing: the
watermark-driven exporter decides WHEN to publish and when the local copy
may be deleted. Target bucket: dedicated `agentenv-snapshots` (never the
shared swef registry bucket — its GC is frozen and snapshots are
high-churn). Tenant/class/ttl ride the artifact metadata; durable TTL =
bucket lifecycle rules per class prefix (no code); a small quota job
handles per-tenant snapshot-GB later.

### The "DB"

No new database. Hot path: the scheduler's Redis binding row grows
`{state: LOCAL(node) | LOCAL+DURABLE(node) | DURABLE_ONLY | RESUMING(node',
lease) | TOMBSTONE, artifact_ref}` — one CAS-guarded row per sandbox is the
single-flight point. Durable truth: the store manifests; Redis is
reconstructible from them.

## Resume flow

Clients never talk to a node directly; all data plane goes through the
gateway by `x-agentenv-sandbox-id`, which is what lets admission apply.

1. `RUNNING` -> proxy (unchanged).
2. `PAUSED_LOCAL(N)`, N within resume budget -> same-node resume (~0.24s),
   gateway holds the request briefly. The only fast path.
3. Slow path — exactly two triggers: local snapshot evicted, or birth node
   over budget with a durable copy. Scheduler re-places through the same
   admission as create (P2C + watermarks + tenant quota); target node
   restores from the store (lazy overlaybd disk, eager memory file);
   gateway answers 202/429 + Retry-After instead of holding the request;
   `RESUMING` lease (~120s) gives single-flight and re-places on lease
   expiry.
4. Over budget, not yet durable -> 429 + Retry-After; exporter prioritizes
   that sandbox.
5. No admissible placement -> typed `503 NoCapacity` (fleet) or
   `429 QuotaExceeded` (tenant) — space is not guaranteed; the durable copy
   waits. `TOMBSTONE` -> `410 Gone` (permanent sample error).
6. Explicit `POST /sandboxes/{id}/resume` lets agent frameworks prewarm
   during model generation; implicit touch-resume remains the fallback.

## Admission and placement

- Create: watermark filter (fc-work <=85%, snapshots <=95%, memory within
  budget), then power-of-two-choices on resident committed memory among
  survivors. Empty survivor set -> typed NoCapacity.
- Resume: node-local memory-budget semaphore
  (`resident_mem_committed + vm_mem <= max_resident_mem_bytes`) -> 429.
  Caps worst-case node RSS regardless of tenant behavior.
- Capacity truth: node heartbeat grows a vector (resident mem committed,
  VM count, per-disk headroom, paused snapshot bytes) + Prometheus gauges.

## Multitenancy (simple)

- Rung 1: the `X-API-Key` value IS the tenant name by convention
  (`eval`, `rl`, `adhoc`); gateway-forwarded in the schedule hint; quota
  table in chart values (max resident GB / active sandboxes / snapshot GB /
  create rate; unknown -> tiny `default`). No verification: internal fleet,
  honest-tenant model.
- Rung 2 (when spoofing matters): same header, per-tenant secret key,
  string-compare map — same distribution pattern as the registry reader
  credential. OIDC/SA-tokens only if this ever leaves the trusted fleet.
- Blunt instrument: tenant -> node-pool mapping via the chart's
  poolSelector labels (e.g. 8 nodes eval/rl, 2 adhoc).

## Implementation phases (all Rust)

- **R0 — node-runtime only, no control-plane changes** (acute fixes; the
  Go gateway passes node 429s through untouched):
  resume budget gate before `SnapshotManager::load_runnable`; create
  rejection at watermark (crude shedding: client retries, RR lands
  elsewhere); capacity gauges in `src/orchestrator/metrics.rs` +
  `src/observability/prometheus.rs`; `src/snapshot/exporter.rs` state
  machine (Idle/Draining/Blocking) mirroring the image-cache GC watermark
  mechanics (`src/image/cache/service.rs`); exports ride the existing
  `SnapshotManager::publish*` path; LRU `touched_at` persists in the shared
  `LocalKvStore` (`src/local_store.rs`, the repo convention for node-local
  catalogs). `make fmt` + `make clippy -D warnings` gate every patch.
- **R1 — Rust scheduler drop-in**: `crates/scheduler` implementing the
  existing gRPC proto + Redis schema + EndpointSlice discovery; parity
  first (RR, bindings, heartbeats, GetNode), image swap in the chart.
- **R2 — features on the Rust scheduler**: P2C + watermark-filtered
  placement consuming the heartbeat vector; tenant quotas; binding states +
  restore-on-load (`RestoreSandbox` node RPC + RESUMING lease).

Client side (yarl, already partially landed on `rn/aenv-p1`): 429/202
Retry-After handling with bounded jitter; detached long-run execution so
multi-hour agentic commands survive relocation and gateway churn.

Validation: Rust unit tests for the exporter state machine and resume
gate; e2e via `x/ryanng/aenv` smoke extensions (pause-storm past 70/95 on
a canary node asserting drain -> blocking -> 429; evict-then-resume
round-trip through re-placement).
