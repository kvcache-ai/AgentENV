package scheduler

import (
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

func TestFleetPlannerKeepsOneWarmNodeAndScalesForCapacity(t *testing.T) {
	registry := NewHeartbeatNodeRegistry(time.Minute)
	now := time.Unix(100, 0)
	registerFleetNode(t, registry, "node-a", "service-a", 0, 0, 10, 100, now)
	planner := mustFleetPlanner(t, registry, FleetPolicy{
		MinNodes: 1, MaxNodes: 250, WarmNodes: 1,
		MaxSandboxesPerNode: 24, MaxMemoryUsedPercent: 85,
		EmptyGrace: time.Minute, DrainGrace: time.Minute, DemandTTL: time.Minute,
	})

	plan := planner.Plan([]string{"node-a"}, now)
	if plan.DesiredNodes != 1 {
		t.Fatalf("idle desired nodes = %d, want 1", plan.DesiredNodes)
	}

	registerFleetNode(t, registry, "node-a", "service-a", 24, 0, 70, 100, now.Add(time.Second))
	plan = planner.Plan([]string{"node-a"}, now.Add(time.Second))
	if plan.DesiredNodes != 2 {
		t.Fatalf("full desired nodes = %d, want 2", plan.DesiredNodes)
	}
}

func TestFleetPlannerCountsBootingMemberWithoutOverScaling(t *testing.T) {
	registry := NewHeartbeatNodeRegistry(time.Minute)
	now := time.Unix(100, 0)
	registerFleetNode(t, registry, "node-a", "service-a", 24, 0, 70, 100, now)
	planner := mustFleetPlanner(t, registry, FleetPolicy{
		MinNodes: 1, MaxNodes: 250, WarmNodes: 1,
		MaxSandboxesPerNode: 24, MaxMemoryUsedPercent: 85,
		EmptyGrace: time.Minute, DrainGrace: time.Minute, DemandTTL: time.Minute,
	})

	plan := planner.Plan([]string{"node-a", "node-booting"}, now)
	if plan.DesiredNodes != 2 || plan.ProvisioningNodes != 1 {
		t.Fatalf("plan with booting member = %#v, want desired=2 provisioning=1", plan)
	}
}

func TestFleetPlannerRecentDemandAddsCapacity(t *testing.T) {
	registry := NewHeartbeatNodeRegistry(time.Minute)
	now := time.Unix(100, 0)
	registerFleetNode(t, registry, "node-a", "service-a", 0, 0, 10, 100, now)
	planner := mustFleetPlanner(t, registry, FleetPolicy{
		MinNodes: 1, MaxNodes: 250, WarmNodes: 1,
		MaxSandboxesPerNode: 24, MaxMemoryUsedPercent: 85,
		EmptyGrace: time.Minute, DrainGrace: time.Minute, DemandTTL: time.Minute,
	})
	planner.RecordDemand(now)

	plan := planner.Plan([]string{"node-a"}, now.Add(time.Second))
	if plan.DesiredNodes != 2 {
		t.Fatalf("recent demand desired nodes = %d, want 2", plan.DesiredNodes)
	}
}

func TestFleetPlannerRecentDemandDoesNotRunAwayWhileCapacityArrives(t *testing.T) {
	registry := NewHeartbeatNodeRegistry(time.Minute)
	now := time.Unix(100, 0)
	registerFleetNode(t, registry, "node-a", "service-a", 0, 0, 10, 100, now)
	planner := mustFleetPlanner(t, registry, FleetPolicy{
		MinNodes: 1, MaxNodes: 250, WarmNodes: 1,
		MaxSandboxesPerNode: 24, MaxMemoryUsedPercent: 85,
		EmptyGrace: time.Minute, DrainGrace: time.Minute, DemandTTL: time.Minute,
	})
	planner.RecordDemand(now)

	booting := planner.Plan([]string{"node-a", "node-b"}, now.Add(time.Second))
	if booting.DesiredNodes != 2 {
		t.Fatalf("booting desired nodes = %d, want 2", booting.DesiredNodes)
	}
	registerFleetNode(t, registry, "node-b", "service-b", 0, 0, 10, 100, now.Add(2*time.Second))
	ready := planner.Plan([]string{"node-a", "node-b"}, now.Add(3*time.Second))
	if ready.DesiredNodes != 2 {
		t.Fatalf("ready desired nodes = %d, want fixed demand target 2", ready.DesiredNodes)
	}
}

func TestFleetPlannerMemoryPressureAddsOnlyOneNodePerPressureEpisode(t *testing.T) {
	registry := NewHeartbeatNodeRegistry(time.Minute)
	now := time.Unix(100, 0)
	registerFleetNode(t, registry, "node-a", "service-a", 0, 0, 90, 100, now)
	planner := mustFleetPlanner(t, registry, FleetPolicy{
		MinNodes: 1, MaxNodes: 250, WarmNodes: 1,
		MaxSandboxesPerNode: 24, MaxMemoryUsedPercent: 85,
		EmptyGrace: time.Minute, DrainGrace: time.Minute, DemandTTL: time.Minute,
	})

	booting := planner.Plan([]string{"node-a", "node-b"}, now.Add(time.Second))
	if booting.DesiredNodes != 2 {
		t.Fatalf("booting desired nodes = %d, want 2", booting.DesiredNodes)
	}
	registerFleetNode(t, registry, "node-b", "service-b", 0, 0, 10, 100, now.Add(2*time.Second))
	ready := planner.Plan([]string{"node-a", "node-b"}, now.Add(3*time.Second))
	if ready.DesiredNodes != 2 {
		t.Fatalf("ready desired nodes = %d, want fixed pressure target 2", ready.DesiredNodes)
	}
}

func TestFleetPlannerCordonsThenDeletesExactEmptyNode(t *testing.T) {
	registry := NewHeartbeatNodeRegistry(time.Hour)
	start := time.Unix(100, 0)
	registerFleetNode(t, registry, "node-a", "service-a", 1, 0, 10, 100, start)
	registerFleetNode(t, registry, "node-b", "service-b", 0, 0, 10, 100, start)
	planner := mustFleetPlanner(t, registry, FleetPolicy{
		MinNodes: 1, MaxNodes: 250, WarmNodes: 0,
		MaxSandboxesPerNode: 24, MaxMemoryUsedPercent: 85,
		EmptyGrace: time.Minute, DrainGrace: time.Minute, DemandTTL: time.Minute,
	})

	_ = planner.Plan([]string{"node-a", "node-b"}, start)
	plan := planner.Plan([]string{"node-a", "node-b"}, start.Add(time.Minute))
	if len(plan.CordonCandidates) != 1 || plan.CordonCandidates[0].NodeID != "node-b" {
		t.Fatalf("cordon candidates = %#v, want node-b", plan.CordonCandidates)
	}
	if err := registry.SetCordoned("node-b", "service-b", true); err != nil {
		t.Fatalf("cordon failed: %v", err)
	}
	planner.MarkCordoned("node-b", start.Add(time.Minute))

	plan = planner.Plan([]string{"node-a", "node-b"}, start.Add(2*time.Minute))
	if len(plan.DeleteCandidates) != 1 || plan.DeleteCandidates[0].NodeID != "node-b" || plan.DeleteCandidates[0].ServiceInstanceID != "service-b" {
		t.Fatalf("delete candidates = %#v, want exact node-b generation", plan.DeleteCandidates)
	}
}

func TestFleetPlannerNeverDeletesNodeWithPausedSandbox(t *testing.T) {
	registry := NewHeartbeatNodeRegistry(time.Hour)
	start := time.Unix(100, 0)
	registerFleetNode(t, registry, "node-a", "service-a", 1, 0, 10, 100, start)
	registerFleetNode(t, registry, "node-b", "service-b", 0, 1, 10, 100, start)
	planner := mustFleetPlanner(t, registry, FleetPolicy{
		MinNodes: 1, MaxNodes: 250, WarmNodes: 0,
		MaxSandboxesPerNode: 24, MaxMemoryUsedPercent: 85,
		EmptyGrace: time.Second, DrainGrace: time.Second, DemandTTL: time.Minute,
	})

	_ = planner.Plan([]string{"node-a", "node-b"}, start)
	plan := planner.Plan([]string{"node-a", "node-b"}, start.Add(time.Minute))
	if len(plan.CordonCandidates) != 0 || len(plan.DeleteCandidates) != 0 {
		t.Fatalf("paused node became removable: %#v", plan)
	}
}

func TestFleetPlannerClampsHugeWorkloadBeforeAddingWarmCapacity(t *testing.T) {
	registry := NewHeartbeatNodeRegistry(time.Minute)
	now := time.Unix(100, 0)
	registerFleetNode(t, registry, "node-a", "service-a", ^uint32(0), 0, 10, 100, now)
	planner := mustFleetPlanner(t, registry, FleetPolicy{
		MinNodes: 1, MaxNodes: 250, WarmNodes: 1,
		MaxSandboxesPerNode: 1, MaxMemoryUsedPercent: 85,
		EmptyGrace: time.Minute, DrainGrace: time.Minute, DemandTTL: time.Minute,
	})

	plan := planner.Plan([]string{"node-a"}, now)
	if plan.DesiredNodes != 250 {
		t.Fatalf("desired nodes = %d, want max 250", plan.DesiredNodes)
	}
}

func registerFleetNode(t *testing.T, registry *AtomicNodeRegistry, nodeID, serviceID string, active, paused uint32, used, total uint64, now time.Time) {
	t.Helper()
	_, _, err := registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId: nodeID, Endpoint: "http://" + nodeID + ":8000", ClusterId: "cluster-a",
		ServiceInstanceId: serviceID,
		Snapshot: &schedulerv1.NodeSnapshot{
			Status:       schedulerv1.NodeStatus_NODE_STATUS_READY,
			SandboxCount: active, PausedSandboxCount: paused,
			MemoryUsedBytes: used, MemoryTotalBytes: total,
		},
	}, now)
	if err != nil {
		t.Fatalf("register %s: %v", nodeID, err)
	}
}

func mustFleetPlanner(t *testing.T, registry NodeRegistry, policy FleetPolicy) *FleetPlanner {
	t.Helper()
	planner, err := NewFleetPlanner(registry, policy)
	if err != nil {
		t.Fatalf("new fleet planner: %v", err)
	}
	return planner
}
