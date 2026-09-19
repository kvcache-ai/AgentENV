# Prometheus patch stack

Base: upstream `6e8deea` (the ref deployed in fl2024008/prometheus PR #21749).
Branch `prometheus-main` carries our patches, rebased (never merged) so
`git log upstream/main..prometheus-main` is always the exact stack.
Upstream catch-up: rebase `prometheus-main` onto a new upstream ref
deliberately, then re-run CI and roll pods per the OnDelete runbook.

| Patch | Why |
|---|---|
| (none yet) | pipeline proof-of-life builds pristine 6e8deea |

Planned (design: `DESIGN-OVERLOAD.md`; Rust-only, phases R0 -> R1 -> R2):
R0 node-runtime overload safety (resume memory gate, create watermark
rejection, capacity gauges, lazy snapshot exporter with LRU drain at 70%
and blocking flush at 95%); R1 Rust scheduler drop-in; R2 P2C placement,
tenant quotas, snapshot restore-on-load.
