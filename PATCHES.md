# Prometheus patch stack

Base: upstream `6e8deea` (the ref deployed in fl2024008/prometheus PR #21749).
Branch `prometheus-main` carries our patches, rebased (never merged) so
`git log upstream/main..prometheus-main` is always the exact stack.
Upstream catch-up: rebase `prometheus-main` onto a new upstream ref
deliberately, then re-run CI and roll pods per the OnDelete runbook.

| Patch | Why |
|---|---|
| (none yet) | pipeline proof-of-life builds pristine 6e8deea |

Planned: disk-full admission reserve (capsule #21376 semantics), resume
memory accounting, disk-aware placement strategy, S3/OSS snapshot backend
enablement.
