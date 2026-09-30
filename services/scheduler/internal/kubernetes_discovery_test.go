package scheduler

import (
	"context"
	"strings"
	"testing"
	"time"

	"agentenv/services/shared/config"
	"go.uber.org/zap"

	schedulerv1 "agentenv/services/api/proto"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
)

var defaultDiscoveryCfg = config.SchedulerDiscoveryKubernetesConfig{
	Namespace:   "agentenv-system",
	ServiceName: "agentenv-nodes",
	Port:        8000,
	Scheme:      "http",
}

func TestNodesFromEndpointSlicesServingEndpointIsActive(t *testing.T) {
	active, lingering := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(8000, servingEndpoint("agentenv-node-a", "10.0.0.1")),
	}, defaultDiscoveryCfg)

	if len(active) != 1 {
		t.Fatalf("expected 1 active node, got %d", len(active))
	}
	if len(lingering) != 0 {
		t.Fatalf("expected 0 lingering nodes, got %d", len(lingering))
	}
	if got := active[0].ID; got != "agentenv-node-a" {
		t.Fatalf("expected node id agentenv-node-a, got %q", got)
	}
	if got := active[0].Endpoint; got != "http://10.0.0.1:8000" {
		t.Fatalf("expected endpoint http://10.0.0.1:8000, got %q", got)
	}
	if got := active[0].AffinityID; got != "agentenv-node-a" {
		t.Fatalf("expected pod-name affinity fallback, got %q", got)
	}
}

func TestNodesFromEndpointSlicesUsesStableKubernetesNodeForAffinity(t *testing.T) {
	endpoint := servingEndpoint("agentenv-node-a", "10.0.0.1")
	endpoint.NodeName = stringPtr("od5-node-1")
	active, _ := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(8000, endpoint),
	}, defaultDiscoveryCfg)

	if len(active) != 1 {
		t.Fatalf("expected 1 active node, got %d", len(active))
	}
	if got := active[0].AffinityID; got != "od5-node-1" {
		t.Fatalf("expected stable kubernetes node affinity id, got %q", got)
	}
}

func TestNodesFromEndpointSlicesNotServingIsLingering(t *testing.T) {
	active, lingering := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(8000, notServingEndpoint("agentenv-node-a", "10.0.0.1")),
	}, defaultDiscoveryCfg)

	if len(active) != 0 {
		t.Fatalf("expected 0 active nodes, got %d", len(active))
	}
	if len(lingering) != 1 || lingering[0].ID != "agentenv-node-a" {
		t.Fatalf("expected non-serving node to linger, got %v", lingering)
	}
}

func TestNodesFromEndpointSlicesTerminatingIsLingering(t *testing.T) {
	active, lingering := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(8000, terminatingEndpoint("agentenv-node-a", "10.0.0.1")),
	}, defaultDiscoveryCfg)

	if len(active) != 0 {
		t.Fatalf("expected 0 active nodes, got %d", len(active))
	}
	if len(lingering) != 1 {
		t.Fatalf("expected 1 lingering node, got %d", len(lingering))
	}
	if got := lingering[0].ID; got != "agentenv-node-a" {
		t.Fatalf("expected lingering node agentenv-node-a, got %q", got)
	}
}

func TestFilterNodesByPodLabelsNoScheduleIsLingering(t *testing.T) {
	discovery := newDiscoveryWithPodLabelSelectors(t, "", "agentenv.io/scheduler-state=no-schedule",
		newPod("agentenv-node-a", map[string]string{"agentenv.io/scheduler-state": "no-schedule"}),
	)

	active, lingering := discovery.filterNodesByPodLabels(
		[]Node{{ID: "agentenv-node-a", Endpoint: "http://10.0.0.1:8000"}},
		nil,
	)

	if len(active) != 0 {
		t.Fatalf("expected 0 active nodes, got %d", len(active))
	}
	if len(lingering) != 1 {
		t.Fatalf("expected 1 lingering node, got %d", len(lingering))
	}
	if got := lingering[0].ID; got != "agentenv-node-a" {
		t.Fatalf("expected lingering node agentenv-node-a, got %q", got)
	}
}

func TestFilterNodesByPodLabelsIgnoreIsExcluded(t *testing.T) {
	discovery := newDiscoveryWithPodLabelSelectors(t, "agentenv.io/discovery=ignore", "",
		newPod("agentenv-node-a", map[string]string{"agentenv.io/discovery": "ignore"}),
	)

	active, lingering := discovery.filterNodesByPodLabels(
		[]Node{{ID: "agentenv-node-a", Endpoint: "http://10.0.0.1:8000"}},
		[]Node{{ID: "agentenv-node-a", Endpoint: "http://10.0.0.1:8000"}},
	)

	if len(active) != 0 || len(lingering) != 0 {
		t.Fatalf("expected ignored pod to be excluded, got active=%d lingering=%d", len(active), len(lingering))
	}
}

func TestValidateOptionalPodSelector(t *testing.T) {
	if err := validateOptionalPodSelector("", "ignore_pod_selector"); err != nil {
		t.Fatalf("expected empty selector to be valid, got %v", err)
	}
	if err := validateOptionalPodSelector("agentenv.io/scheduler-state in (draining,no-schedule)", "no_schedule_pod_selector"); err != nil {
		t.Fatalf("expected selector to be valid, got %v", err)
	}
	if err := validateOptionalPodSelector("agentenv.io/scheduler-state in (", "no_schedule_pod_selector"); err == nil {
		t.Fatal("expected invalid selector to be rejected")
	}
}

func TestNodesFromEndpointSlicesNotServingTerminatingIsLingering(t *testing.T) {
	ep := discoveryv1.Endpoint{
		Addresses: []string{"10.0.0.1"},
		Conditions: discoveryv1.EndpointConditions{
			Serving:     boolPtr(false),
			Terminating: boolPtr(true),
		},
		TargetRef: &corev1.ObjectReference{Kind: "Pod", Name: "agentenv-node-a"},
	}
	active, lingering := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(8000, ep),
	}, defaultDiscoveryCfg)

	if len(active) != 0 || len(lingering) != 1 {
		t.Fatalf("expected not-serving+terminating node to linger, got active=%d lingering=%d", len(active), len(lingering))
	}
}

func TestReadinessTransitionsPreserveSandboxRouting(t *testing.T) {
	registry := NewAtomicNodeRegistry(nil, defaultObservedReportTTL)
	service := NewService(zap.NewNop(), registry, NewStrategy("round_robin"), NewInMemoryBindingStore(defaultObservedReportTTL))
	endpoint := servingEndpoint("node-a", "10.0.0.1")
	for _, serving := range []bool{true, false, true} {
		endpoint.Conditions.Serving = boolPtr(serving)
		active, lingering := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
			newEndpointSlice(8000, endpoint),
		}, defaultDiscoveryCfg)
		registry.Set(active, lingering)
		_, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
			NodeId: "node-a", ServiceInstanceId: "worker-a", SandboxIds: []string{"sandbox-a"},
			Snapshot: &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
		})
		if err != nil {
			t.Fatalf("serving=%v: heartbeat rejected: %v", serving, err)
		}
		binding, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: "sandbox-a"})
		if err != nil || binding.GetNode().GetNodeId() != "node-a" {
			t.Fatalf("serving=%v: sandbox route lost: %v, %v", serving, binding, err)
		}
		if admitted := len(registry.Snapshot(false)) > 0; admitted != serving {
			t.Fatalf("serving=%v: new placement eligibility=%v", serving, admitted)
		}
	}
	registry.Set(nil, nil)
	if _, ok := registry.Resolve("node-a"); ok {
		t.Fatal("removed endpoint must leave the registry")
	}
}

func TestNodesFromEndpointSlicesFormatsIPv6Endpoint(t *testing.T) {
	active, _ := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(8000, servingEndpoint("agentenv-node-v6", "2001:db8::10")),
	}, defaultDiscoveryCfg)

	if len(active) != 1 {
		t.Fatalf("expected 1 node, got %d", len(active))
	}
	if got := active[0].Endpoint; got != "http://[2001:db8::10]:8000" {
		t.Fatalf("expected endpoint http://[2001:db8::10]:8000, got %q", got)
	}
}

func TestNodesFromEndpointSlicesSkipsInvalidAddresses(t *testing.T) {
	active, _ := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(8000, endpointWithAddresses("agentenv-node-a", []string{"not-an-ip"})),
	}, defaultDiscoveryCfg)

	if len(active) != 0 {
		t.Fatalf("expected no nodes, got %d", len(active))
	}
}

func TestNodesFromEndpointSlicesUsesFirstValidAddress(t *testing.T) {
	active, _ := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(8000, endpointWithAddresses("agentenv-node-a", []string{"not-an-ip", "10.0.0.3"})),
	}, defaultDiscoveryCfg)

	if len(active) != 1 {
		t.Fatalf("expected 1 node, got %d", len(active))
	}
	if got := active[0].Endpoint; got != "http://10.0.0.3:8000" {
		t.Fatalf("expected endpoint http://10.0.0.3:8000, got %q", got)
	}
}

func TestNodesFromEndpointSlicesIgnoresSlicesWithoutMatchingPort(t *testing.T) {
	active, _ := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(9000, servingEndpoint("agentenv-node-a", "10.0.0.1")),
	}, defaultDiscoveryCfg)

	if len(active) != 0 {
		t.Fatalf("expected no nodes, got %d", len(active))
	}
}

func TestNodeRegistryReflectsEndpointRemovalAcrossSyncs(t *testing.T) {
	registry := NewAtomicNodeRegistry(nil, 30*time.Second)
	now := time.Unix(100, 0)

	active1, _ := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(8000,
			servingEndpoint("agentenv-node-a", "10.0.0.1"),
			servingEndpoint("agentenv-node-b", "10.0.0.2"),
		),
	}, defaultDiscoveryCfg)
	registry.Set(active1, nil)
	// Both are active; heartbeat them so they become ready for Snapshot.
	heartbeatNode(registry, "agentenv-node-a", "http://10.0.0.1:8000", now)
	heartbeatNode(registry, "agentenv-node-b", "http://10.0.0.2:8000", now)

	if got := len(registry.Snapshot( /* allowLingering */ false)); got != 2 {
		t.Fatalf("expected 2 nodes after initial sync, got %d", got)
	}

	active2, _ := nodesFromEndpointSlices([]*discoveryv1.EndpointSlice{
		newEndpointSlice(8000, servingEndpoint("agentenv-node-b", "10.0.0.2")),
	}, defaultDiscoveryCfg)
	registry.Set(active2, nil)

	snapshot := registry.Snapshot( /* allowLingering */ false)
	if len(snapshot) != 1 {
		t.Fatalf("expected 1 node after removal sync, got %d", len(snapshot))
	}
	if got := snapshot[0].ID; got != "agentenv-node-b" {
		t.Fatalf("expected remaining node agentenv-node-b, got %q", got)
	}
}

func TestSnapshotFiltersLingeringNodes(t *testing.T) {
	registry := NewAtomicNodeRegistry(nil, 30*time.Second)

	// node-a: active
	// node-b: active
	// node-c: lingering
	active := []Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}
	lingering := []Node{
		{ID: "node-c", Endpoint: "http://node-c"},
	}
	registry.Set(active, lingering)

	// Without lingering: only active nodes
	noLingering := registry.Snapshot( /* allowLingering */ false)
	if len(noLingering) != 2 {
		t.Fatalf("expected 2 active nodes, got %v", nodeIDs(noLingering))
	}

	// With lingering: all nodes
	withLingering := registry.Snapshot( /* allowLingering */ true)
	if len(withLingering) != 3 {
		t.Fatalf("expected 3 nodes with lingering, got %v", nodeIDs(withLingering))
	}
}

func TestLingeringNodeGetsNoScheduleStatusInObservedView(t *testing.T) {
	registry := NewAtomicNodeRegistry(nil, 30*time.Second)
	now := time.Unix(100, 0)

	registry.Set(nil, []Node{{ID: "node-a", Endpoint: "http://node-a"}})
	heartbeatNode(registry, "node-a", "http://node-a", now)

	observed, ok := registry.GetObserved("node-a", "", now)
	if !ok {
		t.Fatal("expected observed node")
	}
	if got := observed.GetSnapshot().GetStatus(); got != schedulerv1.NodeStatus_NODE_STATUS_LINGERING {
		t.Fatalf("expected NO_SCHEDULE for lingering node, got %v", got)
	}
}

func TestActiveNodeGetsReadyStatusInObservedView(t *testing.T) {
	registry := NewAtomicNodeRegistry(nil, 30*time.Second)
	now := time.Unix(100, 0)

	registry.Set([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, nil)
	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}, now)

	observed, ok := registry.GetObserved("node-a", "", now)
	if !ok {
		t.Fatal("expected observed node")
	}
	if got := observed.GetSnapshot().GetStatus(); got != schedulerv1.NodeStatus_NODE_STATUS_READY {
		t.Fatalf("expected READY for active node, got %v", got)
	}
}

func TestSetRemovesObservedNodesMissingFromDiscovery(t *testing.T) {
	registry := NewAtomicNodeRegistry(nil, 30*time.Second)
	now := time.Unix(100, 0)

	registry.Set([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, nil)
	heartbeatNode(registry, "node-a", "http://node-a", now)
	registry.Set(nil, nil) // remove from discovery

	if _, ok := registry.GetObserved("node-a", "", now); ok {
		t.Fatal("expected observed node to be removed from registry")
	}
	if nodes := registry.ListObserved("", now); len(nodes) != 0 {
		t.Fatalf("expected no observed nodes after removal, got %d", len(nodes))
	}
}

// ── test helpers ──

func heartbeatNode(registry *AtomicNodeRegistry, nodeID, endpoint string, now time.Time) {
	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            nodeID,
		ClusterId:         "cluster-test",
		ServiceInstanceId: "svc-" + nodeID,
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}, now)
}

func nodeIDs(nodes []Node) []string {
	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID
	}
	return ids
}

func newDiscoveryWithPodLabelSelectors(t *testing.T, ignoreSelector string, noScheduleSelector string, pods ...*corev1.Pod) *KubernetesDiscovery {
	t.Helper()

	var ignoreInformer cache.SharedIndexInformer
	if strings.TrimSpace(ignoreSelector) != "" {
		ignoreInformer = newPodStoreInformer(t, ignoreSelector, pods...)
	}
	var noScheduleInformer cache.SharedIndexInformer
	if strings.TrimSpace(noScheduleSelector) != "" {
		noScheduleInformer = newPodStoreInformer(t, noScheduleSelector, pods...)
	}

	return &KubernetesDiscovery{
		config:                defaultDiscoveryCfg,
		ignorePodInformer:     ignoreInformer,
		noSchedulePodInformer: noScheduleInformer,
	}
}

func newPodStoreInformer(t *testing.T, selectorText string, pods ...*corev1.Pod) cache.SharedIndexInformer {
	t.Helper()

	selector, err := labels.Parse(selectorText)
	if err != nil {
		t.Fatalf("parse selector: %v", err)
	}

	informer := cache.NewSharedIndexInformer(&cache.ListWatch{}, &corev1.Pod{}, 0, cache.Indexers{})
	for _, pod := range pods {
		if selector.Matches(labels.Set(pod.Labels)) {
			if err := informer.GetStore().Add(pod); err != nil {
				t.Fatalf("add pod to informer store: %v", err)
			}
		}
	}
	return informer
}

func newPod(name string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: defaultDiscoveryCfg.Namespace,
			Labels:    labels,
		},
	}
}

func newEndpointSlice(port int32, endpoints ...discoveryv1.Endpoint) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "agentenv-nodes-slice",
			Namespace: "agentenv-system",
			Labels: map[string]string{
				discoveryv1.LabelServiceName: "agentenv-nodes",
			},
		},
		Ports: []discoveryv1.EndpointPort{
			{Port: int32Ptr(port)},
		},
		Endpoints: endpoints,
	}
}

func servingEndpoint(name string, address string) discoveryv1.Endpoint {
	return discoveryv1.Endpoint{
		Addresses: []string{address},
		Conditions: discoveryv1.EndpointConditions{
			Serving: boolPtr(true),
		},
		TargetRef: &corev1.ObjectReference{
			Kind: "Pod",
			Name: name,
		},
	}
}

func notServingEndpoint(name string, address string) discoveryv1.Endpoint {
	return discoveryv1.Endpoint{
		Addresses: []string{address},
		Conditions: discoveryv1.EndpointConditions{
			Serving: boolPtr(false),
		},
		TargetRef: &corev1.ObjectReference{
			Kind: "Pod",
			Name: name,
		},
	}
}

func terminatingEndpoint(name string, address string) discoveryv1.Endpoint {
	return discoveryv1.Endpoint{
		Addresses: []string{address},
		Conditions: discoveryv1.EndpointConditions{
			Serving:     boolPtr(true),
			Terminating: boolPtr(true),
		},
		TargetRef: &corev1.ObjectReference{
			Kind: "Pod",
			Name: name,
		},
	}
}

func endpointWithAddresses(name string, addresses []string) discoveryv1.Endpoint {
	return discoveryv1.Endpoint{
		Addresses: addresses,
		Conditions: discoveryv1.EndpointConditions{
			Serving: boolPtr(true),
		},
		TargetRef: &corev1.ObjectReference{
			Kind: "Pod",
			Name: name,
		},
	}
}

func int32Ptr(v int32) *int32 {
	return &v
}

func boolPtr(v bool) *bool {
	return &v
}

func stringPtr(v string) *string {
	return &v
}
