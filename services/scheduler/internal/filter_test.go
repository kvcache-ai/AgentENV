package scheduler

import (
	"testing"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"
)

func uint32Ptr(v uint32) *uint32 { return &v }
func uint64Ptr(v uint64) *uint64 { return &v }

func TestFilterNilLimitKeepsAll(t *testing.T) {
	nodes := []RichNode{
		{Node: Node{ID: "a"}},
		{Node: Node{ID: "b"}},
	}
	result := FilterByResourceLimit(nodes, nil)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
}

func TestFilterExcludesAdmissionBlockedNodesAndKeepsLegacyNodes(t *testing.T) {
	nodes := []RichNode{
		{
			Node: Node{ID: "ready"},
			Snapshot: &schedulerv1.NodeSnapshot{
				Status:             schedulerv1.NodeStatus_NODE_STATUS_READY,
				AcceptingSandboxes: boolPtr(true),
			},
		},
		{
			Node: Node{ID: "blocked"},
			Snapshot: &schedulerv1.NodeSnapshot{
				Status:             schedulerv1.NodeStatus_NODE_STATUS_READY,
				AcceptingSandboxes: boolPtr(false),
			},
		},
		{
			Node:     Node{ID: "legacy"},
			Snapshot: &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
		},
	}

	result := FilterByResourceLimit(nodes, nil)
	if len(result) != 2 || result[0].ID != "ready" || result[1].ID != "legacy" {
		t.Fatalf("expected [ready legacy], got %v", result)
	}
}

func TestFilterKeepsNodesWithoutSnapshot(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxSandboxCount: uint32Ptr(5)}
	nodes := []RichNode{
		{Node: Node{ID: "no-heartbeat"}, Snapshot: nil},
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 1 {
		t.Fatalf("expected node without snapshot to be kept, got %d", len(result))
	}
}

func TestFilterMaxSandboxCount(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxSandboxCount: uint32Ptr(10)}
	nodes := []RichNode{
		{Node: Node{ID: "ok"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 5}},
		{Node: Node{ID: "at-limit"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 10}},
		{Node: Node{ID: "over"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 11}},
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0].ID != "ok" || result[1].ID != "at-limit" {
		t.Fatalf("unexpected nodes: %s, %s", result[0].ID, result[1].ID)
	}
}

func TestFilterMaxSandboxStartingCount(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxSandboxStartingCount: uint32Ptr(2)}
	nodes := []RichNode{
		{Node: Node{ID: "ok"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxStartingCount: 1}},
		{Node: Node{ID: "over"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxStartingCount: 3}},
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 1 || result[0].ID != "ok" {
		t.Fatalf("expected [ok], got %v", result)
	}
}

func TestFilterMaxCPUUsedPercent(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxCPUUsedPercent: uint32Ptr(80)}
	nodes := []RichNode{
		{Node: Node{ID: "ok"}, Snapshot: &schedulerv1.NodeSnapshot{CpuPercent: 50}},
		{Node: Node{ID: "over"}, Snapshot: &schedulerv1.NodeSnapshot{CpuPercent: 95}},
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 1 || result[0].ID != "ok" {
		t.Fatalf("expected [ok], got %v", result)
	}
}

func TestFilterMaxCPUAllocatedPercent(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxCPUAllocatedPercent: uint32Ptr(50)}
	nodes := []RichNode{
		{Node: Node{ID: "ok"}, Snapshot: &schedulerv1.NodeSnapshot{AllocatedCpu: 2, CpuCount: 8}},       // 25%
		{Node: Node{ID: "over"}, Snapshot: &schedulerv1.NodeSnapshot{AllocatedCpu: 6, CpuCount: 8}},     // 75%
		{Node: Node{ID: "zero-cpu"}, Snapshot: &schedulerv1.NodeSnapshot{AllocatedCpu: 5, CpuCount: 0}}, // skip check
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0].ID != "ok" || result[1].ID != "zero-cpu" {
		t.Fatalf("unexpected: %s, %s", result[0].ID, result[1].ID)
	}
}

func TestFilterMaxMemoryUsedPercent(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxMemoryUsedPercent: uint32Ptr(70)}
	nodes := []RichNode{
		{Node: Node{ID: "ok"}, Snapshot: &schedulerv1.NodeSnapshot{MemoryUsedBytes: 60, MemoryTotalBytes: 100}},   // 60%
		{Node: Node{ID: "over"}, Snapshot: &schedulerv1.NodeSnapshot{MemoryUsedBytes: 80, MemoryTotalBytes: 100}}, // 80%
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 1 || result[0].ID != "ok" {
		t.Fatalf("expected [ok], got %v", result)
	}
}

func TestFilterMaxMemoryAllocatedPercent(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxMemoryAllocatedPercent: uint32Ptr(50)}
	nodes := []RichNode{
		{Node: Node{ID: "ok"}, Snapshot: &schedulerv1.NodeSnapshot{AllocatedMemoryBytes: 40, MemoryTotalBytes: 100}},   // 40%
		{Node: Node{ID: "over"}, Snapshot: &schedulerv1.NodeSnapshot{AllocatedMemoryBytes: 60, MemoryTotalBytes: 100}}, // 60%
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 1 || result[0].ID != "ok" {
		t.Fatalf("expected [ok], got %v", result)
	}
}

func TestFilterMultipleLimits(t *testing.T) {
	limit := &config.NodeResourceLimit{
		MaxSandboxCount:   uint32Ptr(10),
		MaxCPUUsedPercent: uint32Ptr(80),
	}
	nodes := []RichNode{
		{Node: Node{ID: "both-ok"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 5, CpuPercent: 50}},
		{Node: Node{ID: "sandbox-over"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 15, CpuPercent: 50}},
		{Node: Node{ID: "cpu-over"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 5, CpuPercent: 90}},
		{Node: Node{ID: "both-over"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 15, CpuPercent: 90}},
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 1 || result[0].ID != "both-ok" {
		t.Fatalf("expected [both-ok], got %v", result)
	}
}

func TestFilterAllExcludedReturnsEmpty(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxSandboxCount: uint32Ptr(0)}
	nodes := []RichNode{
		{Node: Node{ID: "a"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 1}},
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 0 {
		t.Fatalf("expected empty, got %d", len(result))
	}
}

func TestFilterMaxSandboxCountIncludingPaused(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxSandboxCountIncludingPaused: uint32Ptr(5)}
	nodes := []RichNode{
		// 2 running + 3 paused = 5, at limit, kept.
		{Node: Node{ID: "at-limit"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 2, PausedSandboxCount: 3}},
		// 3 running + 3 paused = 6, over.
		{Node: Node{ID: "over"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 3, PausedSandboxCount: 3}},
		// 0 running + 0 paused, kept.
		{Node: Node{ID: "empty"}, Snapshot: &schedulerv1.NodeSnapshot{}},
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0].ID != "at-limit" || result[1].ID != "empty" {
		t.Fatalf("unexpected nodes: %s, %s", result[0].ID, result[1].ID)
	}
}

func TestFilterMaxAllocatedCPUIncludingPaused(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxAllocatedCPUIncludingPaused: uint32Ptr(8)}
	nodes := []RichNode{
		// 4 active + 4 paused = 8, at limit.
		{Node: Node{ID: "at-limit"}, Snapshot: &schedulerv1.NodeSnapshot{AllocatedCpu: 4, PausedAllocatedCpu: 4}},
		// 4 active + 5 paused = 9, over.
		{Node: Node{ID: "over"}, Snapshot: &schedulerv1.NodeSnapshot{AllocatedCpu: 4, PausedAllocatedCpu: 5}},
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 1 || result[0].ID != "at-limit" {
		t.Fatalf("expected [at-limit], got %v", result)
	}
}

func TestFilterMaxAllocatedMemoryBytesIncludingPaused(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxAllocatedMemoryBytesIncludingPaused: uint64Ptr(1000)}
	nodes := []RichNode{
		// 400 + 600 = 1000, at limit.
		{Node: Node{ID: "at-limit"}, Snapshot: &schedulerv1.NodeSnapshot{AllocatedMemoryBytes: 400, PausedAllocatedMemoryBytes: 600}},
		// 500 + 600 = 1100, over.
		{Node: Node{ID: "over"}, Snapshot: &schedulerv1.NodeSnapshot{AllocatedMemoryBytes: 500, PausedAllocatedMemoryBytes: 600}},
		// 0 + 0 = 0, kept.
		{Node: Node{ID: "empty"}, Snapshot: &schedulerv1.NodeSnapshot{}},
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0].ID != "at-limit" || result[1].ID != "empty" {
		t.Fatalf("unexpected nodes: %s, %s", result[0].ID, result[1].ID)
	}
}

// A node that fits active limits but exceeds an "including paused" limit must
// still be excluded.
func TestFilterIncludingPausedExcludesNodeWithinActiveLimits(t *testing.T) {
	limit := &config.NodeResourceLimit{
		MaxSandboxCount:                uint32Ptr(10),
		MaxSandboxCountIncludingPaused: uint32Ptr(5),
	}
	nodes := []RichNode{
		// Within active ceiling (2 <= 10) but over including-paused (2 + 4 = 6 > 5).
		{Node: Node{ID: "over-paused"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 2, PausedSandboxCount: 4}},
		// Within both.
		{Node: Node{ID: "ok"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 2, PausedSandboxCount: 2}},
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 1 || result[0].ID != "ok" {
		t.Fatalf("expected [ok], got %v", result)
	}
}
