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
   `RESUMING` lease (~120s) gives single-flight and re-places on lease
   expiry. While a restore is in flight the client sees `503 NoCapacity`
   with Retry-After — same signal, same behavior.
4. Over budget, not yet durable -> `503 NoCapacity` + Retry-After (the
   exporter prioritizes that sandbox so the next retry relocates).
5. No admissible placement (fleet full, or tenant over quota) ->
   `503 NoCapacity` with a `reason` field (`fleet` | `quota` | `restoring`)
   — one client behavior, reasons kept for metrics. `TOMBSTONE` ->
   `410 Gone` (fail the rollout as a sandbox failure).
6. Explicit `POST /sandboxes/{id}/resume` lets agent frameworks prewarm
   during model generation; implicit touch-resume remains the fallback.

## Admission and placement

- Create: watermark filter (fc-work <=85%, snapshots <=95%, memory within
  budget), then power-of-two-choices on resident committed memory among
  survivors. Empty survivor set -> typed NoCapacity.
- Resume: node-local memory-budget semaphore
  (`resident_mem_committed + vm_mem <= max_resident_mem_bytes`); a refusal
  is a node->scheduler signal (relocate or report fleet-full), surfaced to
  the client only as `503 NoCapacity`. Caps worst-case node RSS regardless
  of tenant behavior.
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
- **R1 — Rust scheduler drop-in**: see "Rust scheduler design" below.
- **R2 — features on the Rust scheduler**: P2C + watermark-filtered
  placement consuming the heartbeat vector; tenant quotas; binding states +
  restore-on-load (`RestoreSandbox` node RPC + RESUMING lease).

## Actor contract and W&B observability

Actors see exactly TWO signals — anything richer invites divergent client
behavior:

- `503 NoCapacity` (+ Retry-After, + `reason: fleet|quota|restoring`):
  retryable. The actor backs off with bounded jitter and keeps the rollout
  leased. Node-level refusals (resume budget, create watermark, blocking
  flush) NEVER reach the client as 429s — they are node->scheduler signals
  the scheduler absorbs by re-placing or by answering 503 when the fleet
  genuinely has no space. There is no client 429 codepath.
- `410 Gone` (tombstoned/destroyed sandbox): fatal to the rollout — the
  actor marks it a sandbox failure immediately (no retry loop, no deadline
  burn).

Per-step W&B tallies (actor side): a thread-safe counter struct in the
session layer increments on every backpressure/lifecycle event —

    sandbox/no_capacity_retries      (503s seen, by reason)
    sandbox/no_capacity_wait_s       (total backoff seconds)
    sandbox/gone_failures            (410 rollout failures)
    sandbox/create_count / create_p50_s / create_p95_s
    sandbox/resume_count / resume_p50_s
    sandbox/exec_transport_retries   (poll-path transient errors)

Transport is the two pipes that already exist — no new RPC, no heartbeat
change:

- RL: counters attach PER ROLLOUT via `Rollout.metadata["sandbox"]`
  (`yarl/src/yarl/boundary.py` — free-form dict that travels
  actor -> buffer -> driver and is never sent to the trainer). The session
  objects a rollout used (verifier session per sample, agentic session per
  episode) accumulate their events and the actor folds them into the
  rollout at finalize; raw durations ride as small lists so the driver can
  compute true per-step p50/p95 across the rollouts it consumed that
  optimizer step, then logs `sandbox/*` next to loss/KL — the exact
  precedent is `full_sampling_entropies`, per-rollout values aggregated
  driver-side.
- Eval sidecar: per-task `auxiliary_metrics` on the task summary
  (`yarl/src/yarl/eval/report.py`, flattened to
  `eval/<task>/auxiliary/<name>`) — the sidecar snapshots its session
  counters at task end into `sandbox_no_capacity_retries`,
  `sandbox_gone_failures`, `sandbox_create_p50_s`, ...

Scoping rule: counters live on session instances, which belong to exactly
one rollout — so concurrent rollouts in one actor process never race a
shared struct, and a rollout's tallies are attributable to its sample.
Fleet-side Prometheus gauges (R0) stay the operator view;
the W&B tallies are the per-run view so a step with sandbox pressure is
visible next to its loss/pass@1 without cross-referencing Datadog.

Detached long-run execution (landed on `rn/aenv-p1`) keeps multi-hour
agentic commands alive across relocation and gateway churn.

Validation: Rust unit tests for the exporter state machine and resume
gate; e2e via `x/ryanng/aenv` smoke extensions (pause-storm past 70/95 on
a canary node asserting drain -> blocking -> 429; evict-then-resume
round-trip through re-placement).

## Rust scheduler design (R1 drop-in, R2 features)

Why replace rather than patch: the language policy is Rust-only for our
code, the scheduler is where every remaining control-plane feature lands
(placement, quotas, binding states, restore), and the Go service is small
enough (~5.1k lines, 13 RPCs) that a faithful port is cheaper than
maintaining a Go patch stack we cannot own.

### Parity contract (R1)

Drop-in means bit-compatible at three seams, so the gateway, node runtimes,
Redis contents, and the chart cannot tell the difference:

1. **gRPC surface** — implement `services/api/proto/scheduler.proto`
   verbatim (tonic codegen from the same file): `Schedule`, `ListNodes`,
   `LookupNode`, `RecordAssignment`, `Heartbeat`, `ReportSandboxEvent`,
   `ListObservedNodes`, `GetNode`, `UnregisterNode`, and the four P2P index
   RPCs (`ListP2pPeers`, `Record/Forget/LookupP2pArtifact`). Same status
   codes per error path (table-driven from the Go tests).
2. **Redis schema** — same keys (`agentenv:scheduler:bindings:<sandbox>`,
   node keys) and the same atomic multi-key binding write (port the Lua/
   MULTI semantics exactly); an R1 scheduler must be able to adopt a live
   Redis written by the Go one mid-flight and vice versa (this is also the
   rollback story).
3. **Discovery + policy behaviors** — EndpointSlice informer (kube-rs
   watcher) with the same active/lingering node classification; the
   round-robin strategy; and the existing `filter.go` / `cpu_template.go`
   semantics ported with their test tables transplanted 1:1.

Crate shape: `crates/scheduler` in the patched tree (ships via the normal
patch stack), tonic + redis-rs + kube-rs; state held as today (Redis +
in-memory registry rebuilt from heartbeats), no new stores. Heartbeats
already carry a `NodeSnapshot` proto — the R0 capacity vector extends that
message, so R1 consumes capacity with no extra plumbing.

### Validation and rollout

- Port the Go unit tables (strategy, filter, cpu_template, redis store,
  registry) as Rust tests — behavior parity is asserted where it was
  already specified.
- **Shadow phase**: run the Rust scheduler as a second deployment against
  the same Redis in read-mostly mode — it receives mirrored `Schedule`
  calls (gateway shadow flag or a replay harness from gateway logs) and
  logs its decision without writing; diff placements/bindings against the
  Go scheduler for a day of eval traffic.
- Cutover = chart image swap (scheduler Deployment only, gateway/nodes
  untouched); rollback = swap back — safe because of the shared-Redis
  adoption property above.
- e2e gate: the existing smoke + stress batteries against the Rust
  scheduler, plus one pipeflush.

### R2 features (land only on the Rust scheduler)

- **Admission + P2C placement**: `admissible(node)` watermark filter over
  the heartbeat capacity vector, then power-of-two-choices on resident
  committed memory; typed `NO_CAPACITY` when the survivor set is empty.
- **Tenant quotas**: tenant label from the schedule hint; per-tenant
  resident/active/snapshot/create-rate counters in Redis; typed
  `QUOTA_EXCEEDED`.
- **Binding state machine**: extend the binding row with
  `LOCAL / LOCAL+DURABLE / DURABLE_ONLY / RESUMING(lease) / TOMBSTONE` +
  `artifact_ref`; node reports offload via a new `ReportSnapshotOffloaded`
  RPC; `GetNode` on `DURABLE_ONLY` kicks `RestoreSandbox` on an admitted
  node under a CAS lease (single-flight, lease-expiry retry) and returns
  UNAVAILABLE+retry-hint until ready — the gateway needs no change.

Proto additions (capacity fields, `ReportSnapshotOffloaded`,
`RestoreSandbox`) are additive and gated so a mixed fleet (old nodes, new
scheduler or the reverse) degrades to R1 behavior rather than failing.
