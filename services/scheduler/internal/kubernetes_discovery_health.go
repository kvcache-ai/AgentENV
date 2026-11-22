package scheduler

import (
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

const (
	kubernetesDiscoveryMaxStaleness      = 3 * time.Minute
	kubernetesDiscoveryWatchTimeout      = time.Minute
	kubernetesDiscoveryHealthCheckPeriod = 30 * time.Second
)

// informerHealth tracks successful list and watch opens, not object events.
// A quiet cluster still reopens watches at the server-side timeout; a wedged
// reflector cannot keep the process healthy without refreshing this state.
type informerHealth struct {
	now         func() time.Time
	mu          sync.RWMutex
	lastSuccess map[string]time.Time
}

func newInformerHealth(now func() time.Time) *informerHealth {
	return &informerHealth{now: now, lastSuccess: make(map[string]time.Time)}
}

func (h *informerHealth) wrap(name string, listWatch *cache.ListWatch) *cache.ListWatch {
	h.mu.Lock()
	h.lastSuccess[name] = time.Time{}
	h.mu.Unlock()

	wrapped := *listWatch
	wrapped.ListFunc = func(options metav1.ListOptions) (runtime.Object, error) {
		object, err := listWatch.ListFunc(options)
		if err == nil {
			h.mark(name)
		}
		return object, err
	}
	wrapped.WatchFunc = func(options metav1.ListOptions) (watch.Interface, error) {
		timeoutSeconds := int64(kubernetesDiscoveryWatchTimeout / time.Second)
		options.TimeoutSeconds = &timeoutSeconds
		watcher, err := listWatch.WatchFunc(options)
		if err == nil {
			h.mark(name)
		}
		return watcher, err
	}
	return &wrapped
}

func (h *informerHealth) mark(name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lastSuccess[name] = h.now()
}

func (h *informerHealth) Healthy() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.lastSuccess) == 0 {
		return false
	}
	now := h.now()
	for _, lastSuccess := range h.lastSuccess {
		if lastSuccess.IsZero() || now.Sub(lastSuccess) > kubernetesDiscoveryMaxStaleness {
			return false
		}
	}
	return true
}
