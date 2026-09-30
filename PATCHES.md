# Prometheus patch stack

Base: upstream `875faee`. The same stack on the previously deployed base
`6e8deea` stays on branch `prometheus-main` (last build `63a6d1d`).
Branch `main` carries our patches on top of upstream. The `push` ruleset makes
it append-only (PRs with one approval, no force-push), so
`git log --no-merges upstream/main..main` is always the exact stack. New
patches land as PRs against `main`, merged with "Rebase and merge".
Upstream catch-up: open a PR that merges a chosen upstream ref into `main`
("Create a merge commit"), then re-run CI and roll pods per the OnDelete
runbook.

Builds: every push to `main` runs `build-images`, which pushes
`agentenv-{runtime,gateway,scheduler}:<short-sha>` to OCIR and records the
build on the `gh-pages` builds page. `.github/scripts/build-od5.sh` builds the
same images from a local checkout on the od5 buildkit lane. Deployments pin
the tag in fl2024008/prometheus `infra/agentenv/chart/values.yaml`.

| Patch | Why |
|---|---|
| `feat: route native registry reads through mirrors with origin fallback` | Native OverlayBD reads use authenticated OCI mirrors with bounded origin fallback (from prometheus `infra/agentenv/patches/0001-native-registry-mirrors.patch`, #22629). |

## Imported runtime and control-plane changes

[🤖] The remaining source changes from Prometheus PRs
[#23941](https://github.com/fl2024008/prometheus/pull/23941) and
[#24053](https://github.com/fl2024008/prometheus/pull/24053) now live here as
ordinary code, including their tests. Builds no longer need to apply that
patch stack. The registry mirror change above was already present.

- Disk-pressure admission, concurrency limits for conversion/pause/resume,
  durable cleanup obligations, generation pruning, and paused-image layer pins.
- Pause rollback and recovery, bounded resume preparation and housekeeping,
  Firecracker API deadlines, and guest readiness fixes.
- Weighted image affinity, runtime reservation hints, stale-heartbeat exclusion,
  binding retention for unready nodes, and larger gateway fleet responses.
- Missing-layer blob fetches, lifecycle metrics, bounded guest probes, and
  bounded guest telemetry (see [tools-image notes](tools-image/envd-bounded-telemetry.md)).

### Admission and recovery behavior

`[disk_policy]` defaults to enabled: admission closes at 80% used and reopens
at 70%; 85% is the hard limit. Pressure is measured on the fullest monitored
filesystem, covering home, image cache, snapshot cache, Firecracker work, and
configured POSIX snapshot-store paths. These are observations, not byte
reservations: concurrent writers can still overshoot the thresholds.

Filesystem usage is sampled off Tokio on one dedicated worker per sampler
(runtime policy and image conversion), with no overlapping probes. Sampling
runs every second; missing, failed, or older-than-five-second samples refuse
admission with HTTP 503. Startup waits at most one second for the first runtime
sample, then stays closed until fresh data arrives. Heartbeats also expose
stale samples as non-accepting. Sampling failure preserves pressure hysteresis.
The cleanup loop's separate `poll_interval_secs` still defaults to 30 seconds.
A hung kernel probe can retain its one worker, but request traffic cannot
start additional probes or occupy Tokio's blocking pool.

API pauses use ordinary admission. Timeout pauses may proceed in the cleanup
band, but not at the hard limit or without fresh disk data. Shutdown pauses
bypass admission to preserve VM state. Image conversion retains its separate
minimum-free-space check, using its image-cache filesystem sample.

If a pause fails terminally and a last durable checkpoint exists, the sandbox
immediately becomes Paused at that checkpoint, with the same state after a
restart. The pause operation still fails and explicitly reports the rollback:
progress after the last checkpoint is lost. A failed persistence attempt whose
backend also cannot resume uses the same recovery path. If no checkpoint exists,
the stopped sandbox is removed; an unreadable checkpoint is preserved, with
image GC disabled and no resumable runtime state advertised.

### Migration and limits

Drain running and paused sandboxes before upgrading from the old paused-record
format, and drain external capacity leases before replacing nodes. Legacy
records lack the durable layer closure required for safe resume and cache GC;
this is an empty-fleet cutover, not an in-place migration. Cleanup journal space
must be allocated before the disk fills; its configured size must match an
existing journal. Keep previous images/configuration for rollback after draining.

Cleanup reclaims obsolete generations and unused cached layers, never valid
paused checkpoints. Automatic snapshot export, cross-node recovery under
pressure, disk byte reservations, load-aware placement, and tenant quotas remain
future work in [DESIGN-OVERLOAD.md](DESIGN-OVERLOAD.md). Affinity is still a
placement preference, not load balancing; nodes with no heartbeat retain the
existing discovery fallback. Tools-image changes require a separate immutable
artifact build and canary; changing the runtime image does not replace guest
tools or migrate already paused guests.
