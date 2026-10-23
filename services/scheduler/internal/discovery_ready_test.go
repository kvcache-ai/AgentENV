package scheduler

import (
	"context"
	"testing"
	"time"

	"agentenv/services/shared/config"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// Issue ④ (#341 maintainer review): the discovery ready signal must not fire
// before the registry actually holds the discovered nodes. Leader election
// waits on this signal; if it fires early, an early acquisition captures an
// empty registry and recovers nothing.

func discoveryReadyFixture() *discoveryv1.EndpointSlice {
	serving := true
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "agentenv-nodes-abc",
			Namespace: "default",
			Labels:    map[string]string{discoveryv1.LabelServiceName: "agentenv-nodes"},
		},
		Ports: []discoveryv1.EndpointPort{{Port: ptrInt32(8000)}},
		Endpoints: []discoveryv1.Endpoint{{
			Addresses:  []string{"10.0.0.9"},
			Conditions: discoveryv1.EndpointConditions{Serving: &serving},
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Name: "node-a"},
		}},
	}
}

func ptrInt32(v int32) *int32 { return &v }

// Hold the registry's write lock so syncFromStore's Set is blocked. With the
// bug (close before syncFromStore), ready still fires in that window — a
// deterministic red. With the fix (close after syncFromStore), ready cannot
// fire while Set is blocked; it fires only after we release the lock, with
// the registry already populated.
func TestDiscoveryReadyFiresAfterRegistryPopulated(t *testing.T) {
	clientset := fake.NewSimpleClientset(discoveryReadyFixture())
	registry := NewAtomicNodeRegistry(nil, 30*time.Second)
	ready := make(chan struct{})

	d, err := newKubernetesDiscoveryWithClient(
		zap.NewNop(),
		config.SchedulerDiscoveryKubernetesConfig{
			Namespace:   "default",
			ServiceName: "agentenv-nodes",
			Port:        8000,
		},
		registry,
		clientset,
		ready,
	)
	if err != nil {
		t.Fatalf("build discovery: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registry.mu.Lock()
	go func() { _ = d.Run(ctx) }()

	select {
	case <-ready:
		registry.mu.Unlock()
		t.Fatal("ready fired while the registry was still unpopulated (Set blocked by the test lock)")
	case <-time.After(5 * time.Second):
		// Correct behavior: ready cannot fire until syncFromStore completes.
		registry.mu.Unlock()
	}

	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("ready never fired after releasing the lock")
	}
	if got := len(registry.Snapshot(false)); got == 0 {
		t.Fatal("ready fired but the registry is empty")
	}
}
