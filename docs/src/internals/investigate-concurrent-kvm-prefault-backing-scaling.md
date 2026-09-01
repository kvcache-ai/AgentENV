# Follow-up draft: Investigate concurrent KVM pre-fault backing-path scaling

## Observation

On Madsys, a fixed, correct 512-MiB `KVM_PRE_FAULT_MEMORY` workload does not scale linearly from 1 to 2/4/8 Firecracker vCPU workers.  Ten-sample Firecracker wall means were 257.3, 292.3, 234.0, and 195.5 ms respectively.

## Established evidence

- Workers overlap in wall-clock time; this is not Firecracker userspace dispatch or join serialization.
- Completion accounting is exact: requested equals completed and remaining is zero for aggregate and per-worker stats.
- Two workers produce more and smaller uBlk reads for nearly the same total bytes.
- Diagnostic runs showed higher `ImageBaseRead` / `OverlaybdTarget::handle_read` service time in comparable request-size buckets with two workers.

## Not established

The investigation did not establish a lower-level cause in LSMT indexing, local-file or io_uring service, cache/backing residency, uBlk queueing, KVM locking, or completion handling.  In particular, `UBLK_U_IO_COMMIT_AND_FETCH_REQ` includes waiting for the next fetch and must not be treated as a request-completion latency.

## Highest-information next experiment

Use a low-intrusion profile that separates backend read service from request formation while retaining worker identity, then compare one and two workers with the same fixed 512-MiB range and documented cache state.  Do not block the current correctness/product PR on this work.
