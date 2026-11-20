package scheduler

import "agentenv/services/shared/config"

// FilterByResourceLimit removes nodes that exceed any configured resource
// threshold. Nodes without a heartbeat snapshot are always kept (they have no
// metrics to evaluate). A nil limit disables all filtering.
func FilterByResourceLimit(nodes []RichNode, limit *config.NodeResourceLimit) []RichNode {
	if limit == nil {
		return nodes
	}

	result := make([]RichNode, 0, len(nodes))
	for _, n := range nodes {
		if n.Snapshot == nil {
			// No heartbeat yet — cannot evaluate limits; keep the node.
			result = append(result, n)
			continue
		}
		if !withinLimit(n, limit) {
			continue
		}
		result = append(result, n)
	}
	return result
}

func withinLimit(n RichNode, limit *config.NodeResourceLimit) bool {
	s := n.Snapshot

	if limit.MaxSandboxCount != nil && s.SandboxCount > *limit.MaxSandboxCount {
		return false
	}
	if limit.MaxSandboxStartingCount != nil && s.SandboxStartingCount > *limit.MaxSandboxStartingCount {
		return false
	}
	if limit.MaxCPUUsedPercent != nil && s.CpuPercent > *limit.MaxCPUUsedPercent {
		return false
	}
	if limit.MaxCPUAllocatedPercent != nil {
		if s.CpuCount > 0 {
			allocatedPercent := s.AllocatedCpu * 100 / s.CpuCount
			if allocatedPercent > *limit.MaxCPUAllocatedPercent {
				return false
			}
		}
	}
	if limit.MaxMemoryUsedPercent != nil {
		if s.MemoryTotalBytes > 0 {
			usedPercent := uint32(s.MemoryUsedBytes * 100 / s.MemoryTotalBytes)
			if usedPercent > *limit.MaxMemoryUsedPercent {
				return false
			}
		}
	}
	if limit.MaxMemoryAllocatedPercent != nil {
		if s.MemoryTotalBytes > 0 {
			allocatedPercent := uint32(s.AllocatedMemoryBytes * 100 / s.MemoryTotalBytes)
			if allocatedPercent > *limit.MaxMemoryAllocatedPercent {
				return false
			}
		}
	}

	// "Including paused" ceilings sum the active running set with the paused
	// reservations reported in the snapshot. A node exceeding any of these is
	// dropped from scheduling candidates regardless of whether the active-only
	// counters are within limits.
	if limit.MaxSandboxCountIncludingPaused != nil {
		total := s.SandboxCount + s.PausedSandboxCount
		if total > *limit.MaxSandboxCountIncludingPaused {
			return false
		}
	}
	if limit.MaxAllocatedCPUIncludingPaused != nil {
		total := s.AllocatedCpu + s.PausedAllocatedCpu
		if total > *limit.MaxAllocatedCPUIncludingPaused {
			return false
		}
	}
	if limit.MaxAllocatedMemoryBytesIncludingPaused != nil {
		total := s.AllocatedMemoryBytes + s.PausedAllocatedMemoryBytes
		if total > *limit.MaxAllocatedMemoryBytesIncludingPaused {
			return false
		}
	}
	return true
}
