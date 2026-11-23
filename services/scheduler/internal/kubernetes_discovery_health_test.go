package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
)

func TestInformerHealthTracksSuccessfulListAndWatchOpen(t *testing.T) {
	now := time.Unix(1_000, 0)
	health := newInformerHealth(func() time.Time { return now })
	var watchTimeout *int64
	wrapped := health.wrap("endpointslices", &cache.ListWatch{
		ListFunc: func(metav1.ListOptions) (runtime.Object, error) {
			return &corev1.PodList{}, nil
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			watchTimeout = options.TimeoutSeconds
			return watch.NewFake(), nil
		},
	})

	if health.Healthy() {
		t.Fatal("discovery is healthy before any informer synchronized")
	}
	if _, err := wrapped.ListFunc(metav1.ListOptions{}); err != nil {
		t.Fatalf("list: %v", err)
	}
	if !health.Healthy() {
		t.Fatal("successful initial list did not make discovery healthy")
	}

	now = now.Add(kubernetesDiscoveryMaxStaleness + time.Second)
	if health.Healthy() {
		t.Fatal("an informer with no successful list or watch open stayed healthy")
	}
	watcher, err := wrapped.WatchFunc(metav1.ListOptions{})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	watcher.Stop()
	if watchTimeout == nil || *watchTimeout != int64(kubernetesDiscoveryWatchTimeout/time.Second) {
		t.Fatalf("watch timeout = %v, want %s", watchTimeout, kubernetesDiscoveryWatchTimeout)
	}
	if !health.Healthy() {
		t.Fatal("a successful watch open did not restore health without any events")
	}
}

func TestInformerHealthUsesStalestInformerAndRejectsFailedRefresh(t *testing.T) {
	now := time.Unix(2_000, 0)
	health := newInformerHealth(func() time.Time { return now })
	list := func(metav1.ListOptions) (runtime.Object, error) { return &corev1.PodList{}, nil }
	ignoreListFails := false
	endpointSlices := health.wrap("endpointslices", &cache.ListWatch{ListFunc: list})
	ignorePods := health.wrap("ignore-pods", &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			if ignoreListFails {
				return nil, errors.New("synthetic apiserver failure")
			}
			return list(options)
		},
	})

	if _, err := endpointSlices.ListFunc(metav1.ListOptions{}); err != nil {
		t.Fatalf("list endpointslices: %v", err)
	}
	if health.Healthy() {
		t.Fatal("one successful informer hid an informer that never synchronized")
	}
	if _, err := ignorePods.ListFunc(metav1.ListOptions{}); err != nil {
		t.Fatalf("list ignore-pods: %v", err)
	}
	if !health.Healthy() {
		t.Fatal("all enabled informers synchronized but discovery is unhealthy")
	}

	now = now.Add(2 * time.Minute)
	if _, err := endpointSlices.ListFunc(metav1.ListOptions{}); err != nil {
		t.Fatalf("refresh endpointslices: %v", err)
	}
	now = now.Add(time.Minute + time.Second)
	if health.Healthy() {
		t.Fatal("a fresh endpointslices informer hid the stale pod informer")
	}
	ignoreListFails = true
	if _, err := ignorePods.ListFunc(metav1.ListOptions{}); err == nil {
		t.Fatal("expected synthetic list failure")
	}
	if health.Healthy() {
		t.Fatal("a failed list refreshed discovery health")
	}
	ignoreListFails = false
	if _, err := ignorePods.ListFunc(metav1.ListOptions{}); err != nil {
		t.Fatalf("recover ignore-pods: %v", err)
	}
	if !health.Healthy() {
		t.Fatal("successful recovery did not restore discovery health")
	}
}

func TestDiscoveryHealthCallbackReportsCurrentFreshness(t *testing.T) {
	discovery, err := newKubernetesDiscoveryWithClient(
		zap.NewNop(),
		defaultDiscoveryCfg,
		NewAtomicNodeRegistry(nil, 30*time.Second),
		fake.NewSimpleClientset(),
		nil,
	)
	if err != nil {
		t.Fatalf("build discovery: %v", err)
	}
	var statuses []bool
	discovery.SetHealthCallback(func(serving bool) {
		statuses = append(statuses, serving)
	})
	discovery.reportHealth(context.Background())
	discovery.health.mark("endpointslices")
	discovery.reportHealth(context.Background())
	if len(statuses) != 2 || statuses[0] || !statuses[1] {
		t.Fatalf("health callbacks = %v, want [false true]", statuses)
	}
}

func TestDiscoveryRunReportsInitialAndSynchronizedHealth(t *testing.T) {
	discovery, err := newKubernetesDiscoveryWithClient(
		zap.NewNop(),
		defaultDiscoveryCfg,
		NewAtomicNodeRegistry(nil, 30*time.Second),
		fake.NewSimpleClientset(discoveryReadyFixture()),
		nil,
	)
	if err != nil {
		t.Fatalf("build discovery: %v", err)
	}
	statuses := make(chan bool, 4)
	discovery.SetHealthCallback(func(serving bool) {
		statuses <- serving
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- discovery.Run(ctx) }()
	for _, want := range []bool{false, true} {
		select {
		case got := <-statuses:
			if got != want {
				t.Fatalf("health status = %v, want %v", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for health status %v", want)
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
}

func TestDiscoveryHealthCallbackSkipsCanceledContext(t *testing.T) {
	discovery, err := newKubernetesDiscoveryWithClient(
		zap.NewNop(),
		defaultDiscoveryCfg,
		NewAtomicNodeRegistry(nil, 30*time.Second),
		fake.NewSimpleClientset(),
		nil,
	)
	if err != nil {
		t.Fatalf("build discovery: %v", err)
	}
	called := false
	discovery.SetHealthCallback(func(bool) { called = true })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	discovery.reportHealth(ctx)
	if called {
		t.Fatal("health callback ran after shutdown cancellation")
	}
}
