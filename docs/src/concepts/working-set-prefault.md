# Working-set Profiling and Pre-fault

AgentENV can optionally collect the memory pages exercised while a template
snapshot is started, then ask KVM to pre-fault those guest physical ranges
before a later restore resumes. Both controls are disabled by default and do
not add capabilities to the Firecracker process.

```toml
[template_profiling]
enabled = true
# Defaults shown here.
max_prefault_bytes = 268435456
max_range_count = 4096
max_guest_memory_ratio_percent = 50

[restore_prefault]
enabled = true
```

The controls are independent. Enable profiling first to publish metadata for
new snapshots, and enable restore pre-fault only when those metadata should be
used at restore time.

## What is collected

For a template build, AgentENV launches a dedicated, disposable profiler from
the snapshot in the paused state. It records the initial `mincore` residency,
resumes through normal guest initialization, pauses it again, and reads the
final resident ranges. Firecracker supplies the mapping between snapshot-image
offsets and guest physical addresses (GPA); AgentENV validates and records the
resulting GPA ranges in the snapshot manifest.

The profiler has its own memory device. It does not replace the eventual
published snapshot and does not turn on Linux idle-page tracking or
`CAP_DAC_OVERRIDE`.

At restore, AgentENV loads the snapshot paused, validates the optional
working-set metadata against the restored guest-memory regions and configured
limits, calls Firecracker's KVM pre-fault API for valid ranges, then resumes
the guest. The metadata is a performance hint: a snapshot without it, invalid
metadata, or an HTTP pre-fault API rejection resumes normally. A Firecracker
socket/transport failure remains a restore error.

## Limits and operational notes

The byte, range-count, and guest-memory-ratio limits reject an oversized
working set rather than silently truncating it. Set values that are appropriate
for the VM size and the startup latency you want to optimize.

Host swap does not block profiling. However, if guest-backed pages are swapped
out while `mincore` is sampled, the recorded working set can be incomplete and
the later pre-fault benefit can be smaller. This does not change snapshot
correctness or recovery behavior.

The feature does not change Firecracker's snapshot serialization format. Older
snapshots that lack working-set metadata remain compatible; they simply have no
pre-fault ranges to apply.
