# AgentENV mincore/KVM pre-fault: final Madsys experiment

Date: 2026-08-30.  This report supersedes the earlier diagnostic tables.

## Provenance and validity

- Host: Madsys-4-2.
- AgentENV worktree: `26544dd5833633cad653c9dbc948923eefff156e`, with the in-scope uncommitted convergence changes recorded in its final status.
- Firecracker: temporary benchmark-only candidate built from the converged Firecracker worktree at `9f50bedd7286b5db8c17b67a6e12cffe9444dee7`; SHA-256 `661958b8f7c0192c2caeeda7bd4416065c6dbbf3706ebb240858cb087f7a4ab4`; reports `v1.15.1-patch-v2`.
- The candidate was selected only by `--firecracker-binary`; no system binary, product config, PR, or Release was changed.  The normal AgentENV maximum remains 256 MiB; 512 MiB used an explicit benchmark-only `--max-prefault-bytes 536870912`.
- No temporary uBlk or OverlayBD diagnostic daemon was used.  Resource-cold means no holder sandbox; it is not a claim of physically cold host page cache.

## Functional correctness

`KVM_PRE_FAULT_MEMORY` now repeats an ioctl until the kernel-updated `size` is zero, accumulating completed bytes and ioctl count.  API completion is emitted only after every successfully-dispatched worker response is drained.  A send failure drains only workers that were successfully signalled, avoiding a dead-vCPU receive hang.

AgentENV validates the returned stats before accepting an enabled sample: exact range count and bytes, `requested == completed`, aggregate and each-worker `remaining == 0`, and nonempty workers.  Missing stats or any incomplete result fails the sample.  The fixed-set helper canonicalizes GPA ranges by sorting, merging contiguous entries, rejecting overlap, and checking the exact total.

## Size sanity: one vCPU, pre-fault only

All rows requested/completed exactly the stated bytes and had zero remaining bytes.

| Working set | Firecracker wall |
| --- | ---: |
| 4 MiB | 4.540 ms |
| 16 MiB | 15.139 ms |
| 64 MiB | 37.774 ms |
| 256 MiB | 136.080 ms |
| 512 MiB | 289.834 ms |

This invalidates the old 0.x-ms 512-MiB result: that result did not represent complete synchronous pre-faulting and must not be cited.

## Fixed 512 MiB microbenchmark

Exactly one canonical GPA range `[128 MiB, 640 MiB)` was split across vCPUs.  Each group has ten valid samples; the number is Firecracker aggregate pre-fault wall, in ms.

| vCPU/workers | Mean | Median | SD | p95 | Min–max |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 257.329 | 257.458 | 34.139 | 304.543 | 204.260–304.543 |
| 2 | 292.257 | 294.133 | 25.564 | 330.328 | 253.324–330.328 |
| 4 | 234.023 | 231.807 | 30.820 | 283.063 | 195.396–283.063 |
| 8 | 195.458 | 192.945 | 25.413 | 255.471 | 166.067–255.471 |

Raw samples (microseconds):

- 1: 204260, 211798, 236672, 241669, 253884, 261031, 279326, 285183, 294925, 304543
- 2: 253324, 258217, 276296, 281389, 292894, 295372, 304810, 305350, 324593, 330328
- 4: 195396, 195906, 200260, 224903, 226324, 237289, 254924, 259544, 262621, 283063
- 8: 166067, 173532, 174269, 188369, 190456, 195434, 195978, 202429, 212578, 255471

Conclusion: workers do execute concurrently (established in the prior timeline diagnosis), but Madsys does not show near-linear scaling.  Two workers regress by about 13.6% versus one; four and eight workers improve the mean by about 9.1% and 24.0%, respectively.  Ten samples establish that limited result, not a general scaling law.

## Product snapshot-resume A/B

The real benchmark profiles the production `envd-ready` cutoff, then restores the same profiled snapshot in existing ABBA order (`disabled, enabled, enabled, disabled`).  It produced twenty samples per arm per mode.  The final profile contained 110 ranges / 32,395,264 bytes.  Every enabled sample had exact requested/completed bytes and zero remaining bytes.

| Mode | Arm | Total mean / median | Explicit pre-fault mean | envd-ready mean | Total min–max |
| --- | --- | ---: | ---: | ---: | ---: |
| resource-cold | disabled | 142.237 / 139.102 ms | 0.004 ms | 75.441 ms | 112.050–166.949 ms |
| resource-cold | enabled | 123.965 / 121.102 ms | 53.379 ms | 12.569 ms | 106.675–156.879 ms |
| hot | disabled | 85.217 / 85.637 ms | 0.004 ms | 32.310 ms | 74.796–92.065 ms |
| hot | enabled | 85.645 / 85.640 ms | 20.891 ms | 16.117 ms | 77.995–91.255 ms |

Thus resource-cold end-to-end mean improves 12.8% (18.272 ms), despite explicit pre-fault cost, because envd-ready falls by about 62.9 ms.  In this hot-holder definition the end-to-end result is statistically close and slightly worse by 0.5% (0.429 ms); do not claim a hot-path benefit.

Per-sample raw phase rows are emitted by the finalized benchmark as `*_sample` lines and saved as `docs/src/internals/agentenv-prefault-product-raw-20260830.log`; the full fixed-512 output is `docs/src/internals/agentenv-prefault-fixed512-raw-20260830.log`.  Product result interpretation is limited to this host, image, cache/holder contract, and envd-ready cutoff.

## Historical correction and limitation

- Earlier 512-MiB 0.x-ms data is invalid and removed from interpretation.
- Experiments whose vCPU configurations had naturally different working sets cannot answer pure worker scaling; only the fixed 512-MiB test above can.
- `UBLK_U_IO_COMMIT_AND_FETCH_REQ` timing includes the next fetch wait and is not per-request completion latency.
- Diagnostic evidence says concurrent pre-fault produces more/smaller uBlk reads and raises comparable-size OverlayBD base-read service cost.  It does not establish uBlk serialization, an io_uring cause, an LSMT cause, a specific lock, or a completion-tail claim.
