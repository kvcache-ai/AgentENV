package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agentenv/services/shared/config"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// ============================================================

// Leadership contract (#259): the immutable state values and the
// recovery-window boundaries that lookup/schedule semantics read.
// Transitions are owned by LeadershipManager and covered in its tests.

func TestLeadershipStates(t *testing.T) {
	standby := newLeadershipSnapshot(30 * time.Second)
	if standby.IsLeader() {
		t.Fatal("a fresh replica must not be the leader")
	}
	if _, ok := standby.LeaderSince(); ok {
		t.Fatal("LeaderSince must be false before acquisition")
	}

	t0 := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	leader := acquiredLeadershipSnapshot(30*time.Second, t0)
	if !leader.IsLeader() {
		t.Fatal("acquiredLeadership must be the leader")
	}
	since, ok := leader.LeaderSince()
	if !ok || !since.Equal(t0) {
		t.Fatalf("LeaderSince after acquisition = (%v, %v); want (%v, true)", since, ok, t0)
	}
}

func TestLeadershipRecoveryWindowBoundaries(t *testing.T) {
	const ttl = 30 * time.Second

	if newLeadershipSnapshot(ttl).InRecoveryWindow(time.Now()) {
		t.Fatal("a replica that never acquired leadership is never in the recovery window")
	}

	t0 := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	l := acquiredLeadershipSnapshot(ttl, t0)

	if !l.InRecoveryWindow(t0.Add(time.Second)) {
		t.Fatal("the window must be open right after acquisition")
	}
	if !l.InRecoveryWindow(t0.Add(ttl - time.Nanosecond)) {
		t.Fatal("the window must stay open for the whole TTL")
	}
	if l.InRecoveryWindow(t0.Add(ttl)) {
		t.Fatal("the window must close exactly at the TTL boundary")
	}
	if l.InRecoveryWindow(t0.Add(ttl + time.Minute)) {
		t.Fatal("the window must stay closed after the TTL")
	}

	if newLeadershipSnapshot(ttl).InRecoveryWindow(t0.Add(time.Second)) {
		t.Fatal("a released replica is never in the recovery window")
	}
}

// ============================================================

func newTestManager() *leadershipManager {
	return newLeadershipManager(nil, config.SchedulerConfig{
		ReportTTL:      30 * time.Second,
		RedisAddr:      "localhost:6379", // shared store → standbys serve reads
		LeaderElection: config.SchedulerLeaderElectionConfig{Enabled: true},
	})
}

// The leader readiness service is NOT_SERVING from registration, flips
// SERVING on acquisition, and flips back on loss/shutdown (#259).
func TestLeadershipManagerHealthTransitions(t *testing.T) {
	lifecycle := newTestManager()
	hs := health.NewServer()
	lifecycle.RegisterHealth(hs)

	check := func() grpc_health_v1.HealthCheckResponse_ServingStatus {
		resp, err := hs.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{Service: LeaderHealthService})
		if err != nil {
			t.Fatalf("leader health check failed: %v", err)
		}
		return resp.GetStatus()
	}

	if got := check(); got != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("initial leader health = %v; want NOT_SERVING until the lease is won", got)
	}

	lifecycle.onStartedLeading(context.Background())
	if got := check(); got != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("leader health after acquisition = %v; want SERVING", got)
	}

	lifecycle.MarkNotServing()
	if got := check(); got != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("leader health after MarkNotServing = %v; want NOT_SERVING", got)
	}
}

// Construction-order guard: onStartedLeading before BindRuntime must not
// panic or dereference the unbound service/registry; it logs and skips the
// sync-node-snapshots retrieval.
func TestLeadershipManagerAcquireBeforeBindRuntimeIsSafe(t *testing.T) {
	lifecycle := newTestManager()
	lifecycle.onStartedLeading(context.Background())
}

// ServiceOption attaches the lifecycle's Leadership to the Service, enabling
// the recovery-window rules only when election is on.
func TestLeadershipManagerServiceOptionAttachesLeadership(t *testing.T) {
	lifecycle := newTestManager()
	svc := NewService(nil, nil, NewRandomStrategy(), NewInMemoryBindingStore(0),
		lifecycle.ServiceOption(),
	)
	if svc.leadership == nil {
		t.Fatal("ServiceOption must attach the lifecycle's Leadership to the Service")
	}
}

// Leader gate tests (#259, test list group B, unit cases B1-B4).

func invokeGate(t *testing.T, isLeader, serveReads bool, method string) (bool, error) {
	t.Helper()
	l := &leadershipManager{
		sharedBindingStore: serveReads,
	}
	l.state.Store(newLeadershipSnapshot(30 * time.Second))
	if isLeader {
		l.state.Store(acquiredLeadershipSnapshot(30*time.Second, time.Now()))
	}
	interceptor := l.GateInterceptor()
	handlerCalled := false
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return "served", nil
	}
	_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: method}, handler)
	return handlerCalled, err
}

// Case B1: a standby without a shared store rejects writes.
func TestGateStandbyWithoutStoreRejectsWrites(t *testing.T) {
	called, err := invokeGate(t, false, false, "/scheduler.v1.Scheduler/Schedule")
	if called {
		t.Fatal("write handler must not run on a standby")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}
	if status.Convert(err).Message() != "not the leader" {
		t.Fatalf("unexpected message: %v", err)
	}
}

// Case B2: a standby without a shared store rejects reads too
// (degraded mode: reads retry until they land on the leader).
func TestGateStandbyWithoutStoreRejectsReads(t *testing.T) {
	called, err := invokeGate(t, false, false, "/scheduler.v1.Scheduler/LookupNode")
	if called {
		t.Fatal("read handler must not run on a standby without a shared store")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", err)
	}
}

// Case B3: a standby with a shared Redis store serves reads from it,
// while writes stay gated.
func TestGateStandbyWithStoreServesReads(t *testing.T) {
	called, err := invokeGate(t, false, true, "/scheduler.v1.Scheduler/LookupNode")
	if !called || err != nil {
		t.Fatalf("standby with a shared store must serve LookupNode: called=%v err=%v", called, err)
	}

	// GetNode reads replica-local observations, which are always empty on a
	// standby — it stays gated even with a shared store (#341 review, F2).
	called, err = invokeGate(t, false, true, "/scheduler.v1.Scheduler/GetNode")
	if called || status.Code(err) != codes.Unavailable {
		t.Fatalf("GetNode must stay gated on standbys (observations are leader-local): called=%v err=%v", called, err)
	}

	called, err = invokeGate(t, false, true, "/scheduler.v1.Scheduler/RecordAssignment")
	if called || status.Code(err) != codes.Unavailable {
		t.Fatalf("writes must stay gated even with a shared store: called=%v err=%v", called, err)
	}

	called, err = invokeGate(t, false, true, "/scheduler.v1.Scheduler/Heartbeat")
	if called || status.Code(err) != codes.Unavailable {
		t.Fatalf("heartbeats must stay gated even with a shared store: called=%v err=%v", called, err)
	}
}

// Case B4: health-check RPCs bypass the gate (else probes deadlock).
func TestGateHealthChecksBypass(t *testing.T) {
	for _, method := range []string{
		"/grpc.health.v1.Health/Check",
		"/grpc.health.v1.Health/Watch",
	} {
		called, err := invokeGate(t, false, false, method)
		if !called || err != nil {
			t.Fatalf("health check %s must bypass the gate: called=%v err=%v", method, called, err)
		}
	}
}

// The leader serves everything, including with no shared store.
func TestGateLeaderServesEverything(t *testing.T) {
	for _, method := range []string{
		"/scheduler.v1.Scheduler/Schedule",
		"/scheduler.v1.Scheduler/LookupNode",
		"/scheduler.v1.Scheduler/Heartbeat",
	} {
		called, err := invokeGate(t, true, false, method)
		if !called || err != nil {
			t.Fatalf("leader must serve %s: called=%v err=%v", method, called, err)
		}
	}
}

// ============================================================

// Leader-election behavior tests against a fake clientset (#259, group C).
// The fake API server handles Lease CRUD and renewal failures, so these run
// offline in seconds without envtest/Kind. A real-apiserver partition test
// stays in Kind/CI.

var electionTiming = config.SchedulerLeaderElectionConfig{
	Enabled:        true,
	LeaseName:      "agentenv-scheduler",
	LeaseNamespace: "agentenv-system",
	LeaseDuration:  2 * time.Second,
	RenewDeadline:  1500 * time.Millisecond,
	RetryPeriod:    300 * time.Millisecond,
}

type runnerHandle struct {
	state   atomic.Pointer[leadershipSnapshot]
	started atomic.Int32
	stopped atomic.Int32
	cancel  context.CancelFunc
}

func startRunner(t *testing.T, clientset *fake.Clientset, identity string) *runnerHandle {
	t.Helper()
	h := &runnerHandle{}
	h.state.Store(newLeadershipSnapshot(30 * time.Second))
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	runner := newLeaderElector(nil, electionTiming, clientset, identity,
		func(context.Context) {
			h.state.Store(acquiredLeadershipSnapshot(30*time.Second, time.Now()))
			h.started.Add(1)
		},
		func() {
			h.state.Store(newLeadershipSnapshot(30 * time.Second))
			h.stopped.Add(1)
		},
	)
	go func() {
		if err := runner.Run(ctx); err != nil {
			t.Errorf("runner %s failed: %v", identity, err)
		}
	}()
	t.Cleanup(cancel)
	return h
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// Case C1: exactly one leader among two replicas competing for one Lease.
func TestElectionExactlyOneLeader(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	a := startRunner(t, clientset, "replica-a")
	b := startRunner(t, clientset, "replica-b")

	eventually(t, 5*time.Second, "one leader", func() bool {
		return a.state.Load().IsLeader() != b.state.Load().IsLeader()
	})
	eventually(t, 2*time.Second, "leader remains unique", func() bool {
		return a.state.Load().IsLeader() != b.state.Load().IsLeader()
	})
}

// Case C2: graceful leader loss (context cancel) — the standby takes over
// within the lease + retry budget, and the ex-leader's fencing callback runs.
func TestElectionFailoverAfterCancel(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	a := startRunner(t, clientset, "replica-a")
	b := startRunner(t, clientset, "replica-b")

	eventually(t, 5*time.Second, "one leader", func() bool {
		return a.state.Load().IsLeader() != b.state.Load().IsLeader()
	})

	leader, standby := a, b
	if b.state.Load().IsLeader() {
		leader, standby = b, a
	}

	leader.cancel()

	eventually(t, 5*time.Second, "ex-leader fencing callback", func() bool {
		return leader.stopped.Load() >= 1 && !leader.state.Load().IsLeader()
	})
	eventually(t, 5*time.Second, "standby takeover", func() bool {
		return standby.state.Load().IsLeader() && standby.started.Load() >= 1
	})
}

// Case C3: fencing under renewal failure — when the leader can no longer
// renew the Lease (partition equivalent), it must call OnStoppedLeading
// within renew_deadline and the standby must take over. No dual-primary
// window: the standby only starts after the Lease actually expires.
func TestElectionFencingOnRenewFailure(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	a := startRunner(t, clientset, "replica-a")
	b := startRunner(t, clientset, "replica-b")

	eventually(t, 5*time.Second, "one leader", func() bool {
		return a.state.Load().IsLeader() != b.state.Load().IsLeader()
	})

	leader, standby := a, b
	leaderID := "replica-a"
	if b.state.Load().IsLeader() {
		leader, standby = b, a
		leaderID = "replica-b"
	}

	// Simulate a partition: Lease updates by the current holder now fail.
	clientset.PrependReactor("update", "leases", func(action ktesting.Action) (bool, runtime.Object, error) {
		lease, ok := action.(ktesting.UpdateAction).GetObject().(*coordinationv1.Lease)
		if !ok || lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != leaderID {
			return false, nil, nil
		}
		return true, nil, errors.New("apiserver unreachable (simulated partition)")
	})

	eventually(t, 5*time.Second, "ex-leader stops serving within renew_deadline", func() bool {
		return leader.stopped.Load() >= 1 && !leader.state.Load().IsLeader()
	})
	eventually(t, 5*time.Second, "standby takes over after expiry", func() bool {
		return standby.state.Load().IsLeader()
	})
}

// Case C3-bis: callback ordering — the ex-leader's OnStoppedLeading must fire
// before the standby's OnStartedLeading. This is the callback-level proof of
// the no-dual-primary window.
func TestElectionCallbackOrderNoDualPrimary(t *testing.T) {
	type event struct {
		kind string
		id   string
	}
	var mu sync.Mutex
	var seq []event
	record := func(kind, id string) {
		mu.Lock()
		seq = append(seq, event{kind: kind, id: id})
		mu.Unlock()
	}

	clientset := fake.NewSimpleClientset()
	start := func(id string) *atomic.Pointer[leadershipSnapshot] {
		state := &atomic.Pointer[leadershipSnapshot]{}
		state.Store(newLeadershipSnapshot(30 * time.Second))
		ctx, cancel := context.WithCancel(context.Background())
		runner := newLeaderElector(nil, electionTiming, clientset, id,
			func(context.Context) {
				state.Store(acquiredLeadershipSnapshot(30*time.Second, time.Now()))
				record("start", id)
			},
			func() {
				state.Store(newLeadershipSnapshot(30 * time.Second))
				record("stop", id)
			},
		)
		go func() { _ = runner.Run(ctx) }()
		t.Cleanup(cancel)
		return state
	}

	a := start("replica-a")
	b := start("replica-b")

	eventually(t, 5*time.Second, "one leader", func() bool {
		return a.Load().IsLeader() != b.Load().IsLeader()
	})

	mu.Lock()
	var firstStart, lastStop, takeOver int = -1, -1, -1
	for i, e := range seq {
		switch {
		case e.kind == "start" && firstStart == -1:
			firstStart = i
		case e.kind == "stop":
			lastStop = i
		case e.kind == "start" && i > firstStart:
			takeOver = i
		}
	}
	mu.Unlock()

	// Kill the leader (cancel via cleanup happens later; simulate loss by
	// renewing failure is C3's job — here we only assert ordering of the
	// events seen so far and after a forced loss).
	// Force a loss: prepend the same renew-failure reactor as C3.
	leaderID := "replica-a"
	if b.Load().IsLeader() {
		leaderID = "replica-b"
	}
	clientset.PrependReactor("update", "leases", func(action ktesting.Action) (bool, runtime.Object, error) {
		lease, ok := action.(ktesting.UpdateAction).GetObject().(*coordinationv1.Lease)
		if !ok || lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != leaderID {
			return false, nil, nil
		}
		return true, nil, errors.New("apiserver unreachable (simulated partition)")
	})

	eventually(t, 5*time.Second, "stop then takeover ordering", func() bool {
		mu.Lock()
		defer mu.Unlock()
		lastStop, takeOver = -1, -1
		for i, e := range seq {
			if e.kind == "stop" {
				lastStop = i
			}
			if e.kind == "start" && i != firstStart {
				takeOver = i
			}
		}
		return lastStop != -1 && takeOver != -1
	})

	mu.Lock()
	defer mu.Unlock()
	if lastStop >= takeOver {
		t.Fatalf("dual-primary window: OnStoppedLeading (idx %d) must precede takeover OnStartedLeading (idx %d); seq=%v", lastStop, takeOver, seq)
	}
}
