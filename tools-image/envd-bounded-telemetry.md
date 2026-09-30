# Bounded envd remote telemetry

[🤖] The source files under `envd/internal/logs/exporter/` replace those in envd 0.5.15 from e2b-dev/infra 2026.17, commit 9c3b7c5dbd181ba819084276b8228f70936e911e.

AgentENV supplies an empty MMDS log collector address. Upstream envd only starts its HTTP log consumer when that address is nonempty, while command-output telemetry is enqueued regardless. Missing or slow collectors can therefore retain unbounded output; a goroutine per write adds another unbounded backlog.

The exporter caps pending remote telemetry at 4MiB and 1,024 records, drops new records once full, and copies admitted records synchronously under the queue lock. The consumer has at most one additional bounded batch in flight. Serialization has temporary per-record allocations; these are not part of the pending-byte count. This is a telemetry buffer bound, not a bound on all envd memory.

The writer returns the full input length even when telemetry is dropped so the existing MultiWriter still sends every byte to local stdout. Actual command stdout/stderr streams, exit events, and cleanup semantics are unchanged. Once full without a collector, later remote records are intentionally dropped.

Regression tests cover 64MiB input without MMDS, a blocked collector, concurrent writes, record count, oversized records, copy ownership/order, queue reuse, and complete local output. Run from the envd module after copying in the replacement files with `GOWORK=off go test -race ./internal/logs/exporter`.

Build as a distinct tools artifact using `make -C tools-image build`; the new default tools version is 0.1.1-envd-bounded.1. The envd commit marker includes bounded-telemetry.1. Runtime/gateway image builds alone do not replace tools drives. Existing configured tools versions and live runtimes are unchanged. Publish under a new immutable tag, select that exact artifact for an isolated canary, and verify guest output/exit/pause/resume before any separately authorized fleet rollout. Existing snapshots may retain the original envd process and require explicit migration planning.

The tools build checks the exact upstream commit before copying these files; updating `ENVD_REF` requires reviewing the source replacement against the new version. The upstream license is retained at `envd/LICENSE`.
