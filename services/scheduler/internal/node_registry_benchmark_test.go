package scheduler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

// hostCpuConfig builds a CPU configuration with the shape of a
// cpu-template-helper dump from one x86 host: 63 CPUID leaves with four
// registers each and 47 MSRs, indented, about 39 KB. seed changes the bitmaps,
// so two seeds stand for two kinds of host.
func hostCpuConfig(tb testing.TB, seed uint32) string {
	tb.Helper()
	cpuid := make([]cpuidModifier, 63)
	for leaf := range cpuid {
		modifiers := make([]registerMod, 0, 4)
		for i, register := range []string{"eax", "ebx", "ecx", "edx"} {
			modifiers = append(modifiers, registerMod{
				Register: register,
				Bitmap:   bm32(0x9e3779b9*uint32(leaf+1) + uint32(i) + seed),
			})
		}
		cpuid[leaf] = cpuidModifier{Leaf: fmt.Sprintf("0x%x", leaf), Subleaf: "0x0", Modifiers: modifiers}
	}
	msrs := make([]msrModifier, 47)
	for i := range msrs {
		msrs[i] = msrModifier{Addr: fmt.Sprintf("0x%x", 0x10+i), Bitmap: bm64(0x9e3779b97f4a7c15*uint64(i+1) + uint64(seed))}
	}
	var indented bytes.Buffer
	if err := json.Indent(&indented, []byte(buildConfig(nil, cpuid, msrs)), "", "  "); err != nil {
		tb.Fatal(err)
	}
	return indented.String()
}

// reportCpuConfig sends one heartbeat. It copies the configuration first,
// because every heartbeat that the scheduler decodes has its own string.
func reportCpuConfig(tb testing.TB, registry *AtomicNodeRegistry, nodeID, clusterID, cpuConfigJSON string) string {
	tb.Helper()
	_, intersection, err := registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            nodeID,
		ClusterId:         clusterID,
		ServiceInstanceId: "instance-1",
		MachineInfo:       &schedulerv1.MachineInfo{CpuConfigJson: string([]byte(cpuConfigJSON))},
	}, time.Unix(100, 0))
	if err != nil {
		tb.Fatalf("heartbeat from %s: %v", nodeID, err)
	}
	return intersection
}

func registryWithReportedNodes(tb testing.TB, nodeCount int, cpuConfigFor func(node int) string) *AtomicNodeRegistry {
	tb.Helper()
	nodes := make([]Node, nodeCount)
	for i := range nodes {
		nodes[i] = Node{ID: fmt.Sprintf("node-%04d", i), Endpoint: fmt.Sprintf("http://node-%04d", i)}
	}
	registry := NewAtomicNodeRegistry(nodes, defaultObservedReportTTL)
	for i, node := range nodes {
		reportCpuConfig(tb, registry, node.ID, "cluster-1", cpuConfigFor(i))
	}
	return registry
}

// restartRuntime replays what the registry sees when one runtime restarts:
// the node unregisters, another node sends a heartbeat, and the node reports
// again. The CPU intersection is computed twice.
func restartRuntime(tb testing.TB, registry *AtomicNodeRegistry, cpuConfigJSON string) {
	tb.Helper()
	if err := registry.UnregisterObserved("node-0000", "instance-1"); err != nil {
		tb.Fatal(err)
	}
	if got := reportCpuConfig(tb, registry, "node-0004", "cluster-1", cpuConfigJSON); got == "" {
		tb.Fatal("no CPU intersection after the node unregistered")
	}
	if got := reportCpuConfig(tb, registry, "node-0000", "cluster-1", cpuConfigJSON); got == "" {
		tb.Fatal("no CPU intersection after the node reported again")
	}
}

// Measure one runtime restart in a fleet whose nodes share one or four CPU
// configurations.
func BenchmarkRuntimeRestartRecomputesCpuIntersection(b *testing.B) {
	for _, nodeCount := range []int{32, 1240} {
		for _, hostKinds := range []int{1, 4} {
			configs := make([]string, hostKinds)
			for kind := range configs {
				configs[kind] = hostCpuConfig(b, uint32(kind))
			}
			// A restart leaves the registry as it found it, so one registry
			// serves every run of the sub-benchmark.
			registry := registryWithReportedNodes(b, nodeCount, func(node int) string {
				return configs[node%hostKinds]
			})
			b.Run(fmt.Sprintf("nodes=%d/host_kinds=%d", nodeCount, hostKinds), func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					restartRuntime(b, registry, configs[0])
				}
			})
		}
	}
}

// Measure the first report of every node of a fleet on one kind of host. Each
// first report computes the CPU intersection again.
func BenchmarkFirstReportsRecomputeCpuIntersection(b *testing.B) {
	cpuConfigJSON := hostCpuConfig(b, 0)
	for _, nodeCount := range []int{32, 1240} {
		b.Run(fmt.Sprintf("nodes=%d", nodeCount), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				registryWithReportedNodes(b, nodeCount, func(int) string { return cpuConfigJSON })
			}
		})
	}
}
