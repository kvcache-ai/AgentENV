# Prometheus patch stack

Base: upstream `6e8deea` (the ref deployed in fl2024008/prometheus PR #21749).
Branch `prometheus-main` carries our patches, rebased (never merged) so
`git log upstream/main..prometheus-main` is always the exact stack. New
patches land as PRs against `prometheus-main`.
Upstream catch-up: rebase `prometheus-main` onto a new upstream ref
deliberately, then re-run CI and roll pods per the OnDelete runbook.

Builds: every push to `prometheus-main` runs `build-images`, which pushes
`agentenv-{runtime,gateway,scheduler}:<short-sha>` to OCIR and records the
build on the `gh-pages` builds page. `.github/scripts/build-od5.sh` builds the
same images from a local checkout on the od5 buildkit lane. Deployments pin
the tag in fl2024008/prometheus `infra/agentenv/chart/values.yaml`.

| Patch | Why |
|---|---|
| `feat: route native registry reads through mirrors with origin fallback` | Native OverlayBD reads use authenticated OCI mirrors with bounded origin fallback (from prometheus `infra/agentenv/patches/0001-native-registry-mirrors.patch`, #22629). |

Planned (design: `DESIGN-OVERLOAD.md`; Rust-only, phases R0 -> R1 -> R2):
R0 node-runtime overload safety (resume memory gate, create watermark
rejection, capacity gauges, lazy snapshot exporter with LRU drain at 70%
and blocking flush at 95%), plus the pause restack EXDEV copy-fallback /
fail-safe (destructive on fabricated-disk nodes, see stress battery
findings); R1 Rust scheduler drop-in; R2 P2C placement, tenant quotas,
snapshot restore-on-load.
