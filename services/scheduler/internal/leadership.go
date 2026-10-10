package scheduler

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// leadershipView is the narrow read-only seam the Service uses for
// recovery-window semantics (#259). It is intentionally unexported: outside
// this package, everything goes through the Leadership facade.
type leadershipView interface {
	InRecoveryWindow(now time.Time) bool
	LeaderSince() (time.Time, bool)
	// RecoveryPending reports whether nodeID was known at acquisition and
	// has not yet produced a post-acquisition report.
	// RecoveryPending reports whether nodeID was known at acquisition and
	// has not yet produced a post-acquisition report. A nil lastReportAt
	// means the node never reported.
	RecoveryPending(nodeID string, lastReportAt *time.Time) bool
}

// Leadership is the single external contract for the scheduler's
// leader-election behavior (#259). Callers (main, the Service) depend only
// on this interface and never on a concrete implementation. Two
// implementations exist: LeadershipManager (election enabled) and
// nonLeadership (a no-op for election disabled), chosen by NewLeadership.
type Leadership interface {
	leadershipView

	// ServiceOption attaches the recovery-window view to a Service under
	// construction.
	ServiceOption() ServiceOption
	// GateInterceptor returns the read/write gate for the gRPC chain;
	// a pass-through when election is disabled.
	GateInterceptor() grpc.UnaryServerInterceptor
	// RegisterHealth registers the leader readiness service; no-op when
	// election is disabled.
	RegisterHealth(hs *health.Server)
	// BindRuntime attaches the post-construction dependencies for
	// sync-node-snapshots; no-op when election is disabled.
	BindRuntime(svc *Service, registry NodeRegistry)
	// Run drives leader election until ctx is done; returns immediately
	// when election is disabled.
	Run(ctx context.Context, onStop func()) error
	// MarkNotServing flips leader readiness off; no-op when disabled.
	MarkNotServing()
}

// NewLeadership builds the Leadership implementation for the given config:
// LeadershipManager when leader election is enabled, a no-op otherwise.
// Callers never branch on the config themselves.
func NewLeadership(logger *zap.Logger, cfg config.SchedulerConfig) Leadership {
	if !cfg.LeaderElection.Enabled {
		return nonLeadership{}
	}
	return newLeadershipManager(logger, cfg)
}

// nonLeadership is the null-object Leadership: election disabled. Every
// method is a no-op or a conservative default, so callers behave exactly as
// a single-writer scheduler without any conditional wiring.
type nonLeadership struct{}

func (nonLeadership) InRecoveryWindow(time.Time) bool         { return false }
func (nonLeadership) LeaderSince() (time.Time, bool)          { return time.Time{}, false }
func (nonLeadership) RecoveryPending(string, *time.Time) bool { return false }
func (nonLeadership) ServiceOption() ServiceOption            { return func(*Service) {} }
func (nonLeadership) RegisterHealth(*health.Server)           {}
func (nonLeadership) BindRuntime(*Service, NodeRegistry)      {}
func (nonLeadership) MarkNotServing()                         {}
func (nonLeadership) GateInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(ctx, req)
	}
}
func (nonLeadership) Run(ctx context.Context, _ func()) error {
	<-ctx.Done()
	return nil
}

// ============================================================

// leadershipSnapshot is an immutable snapshot of this replica's leadership state:
// whether it currently holds the leader lease and, if so, when it acquired
// it. The acquisition time drives the recovery window: right after winning,
// observations and bindings are still rebuilding, so scheduling must only
// consider nodes observed after that point (#259).
//
// The value object carries no lock and no mutators. Transitions are owned by
// LeadershipManager, which atomically swaps the current instance; readers
// load the current snapshot and compute against it.
type leadershipSnapshot struct {
	leader      bool
	since       time.Time
	recoveryTTL time.Duration
	// knownNodes is the node set known at acquisition. A known node with no
	// post-acquisition report stays excluded from scheduling until it
	// reports — a timer alone must not readmit it while its reporter is
	// still backing off (#341 review). Nodes that join later are not in the
	// set and keep the steady-state fail-open behavior.
	knownNodes map[string]bool
}

// newLeadership is the not-leading state. recoveryTTL bounds the recovery
// window after each acquisition; it is the scheduler report TTL: after it,
// every live node has either reported or been pulled, so a missing binding
// means "does not exist", not "not yet rebuilt".
func newLeadershipSnapshot(recoveryTTL time.Duration) *leadershipSnapshot {
	return &leadershipSnapshot{recoveryTTL: recoveryTTL}
}

// acquiredLeadershipSnapshot is the leading state, acquired at now, with the
// node set known at that moment (the recovery-pending set).
func acquiredLeadershipSnapshot(recoveryTTL time.Duration, now time.Time, knownNodes ...string) *leadershipSnapshot {
	known := make(map[string]bool, len(knownNodes))
	for _, id := range knownNodes {
		known[id] = true
	}
	return &leadershipSnapshot{leader: true, since: now, recoveryTTL: recoveryTTL, knownNodes: known}
}

// RecoveryPending reports whether nodeID was known at acquisition and has
// not yet produced a post-acquisition report (lastReportAt is the node's
// latest report time, ok=false when it never reported).
func (l *leadershipSnapshot) RecoveryPending(nodeID string, lastReportAt *time.Time) bool {
	if !l.leader || !l.knownNodes[nodeID] {
		return false
	}
	if lastReportAt == nil {
		return true
	}
	// Millisecond-precision comparison, matching the scheduler's freshness
	// rule (#341 review): a report in the same millisecond as the
	// acquisition counts as post-acquisition.
	return lastReportAt.UnixMilli() < l.since.UnixMilli()
}

// InRecoveryWindow reports whether the recovery window following leadership
// acquisition is still open at now. While open, LookupNode/GetNode must
// return Unavailable for missing entries instead of NotFound ("not yet
// rebuilt" is not "does not exist"). A replica that is not the leader is
// never in the window.
func (l *leadershipSnapshot) InRecoveryWindow(now time.Time) bool {
	if !l.leader {
		return false
	}
	return now.Before(l.since.Add(l.recoveryTTL))
}

func (l *leadershipSnapshot) IsLeader() bool {
	return l.leader
}

// LeaderSince reports when leadership was acquired. The second return value
// is false while this replica is not the leader.
func (l *leadershipSnapshot) LeaderSince() (time.Time, bool) {
	return l.since, l.leader
}

// ============================================================

// LeaderHealthService is the readiness health service name for leader
// election (#259): SERVING only while this replica holds the lease, so the
// K8S Service's endpoints contain only the leader. The deployment's
// readiness probe points here; liveness keeps reporting the overall ""
// status. Single definition point: deploy manifests reference the same
// string, and it is derived from the generated service descriptor so a proto
// package rename cannot drift.
var LeaderHealthService = schedulerv1.Scheduler_ServiceDesc.ServiceName + "/leader"

// leadershipManager owns every leader-election policy the scheduler process
// applies at runtime: the shared Leadership state (the single source for
// "who leads and since when"), the leader-aware gate, the leader readiness
// health service, and the elector callbacks (readiness flip +
// sync-node-snapshots on acquire; readiness flip + stop hook on loss).
//
// Construction is two-phase to cut a construction cycle: the Service needs
// the leadership state early (ServiceOption), while the post-acquisition
// retrieval needs the Service and registry late (BindRuntime). Call order:
//
//	NewLeadershipManager → ServiceOption / GateInterceptor → (build Service)
//	→ BindRuntime → RegisterHealth → Run
type leadershipManager struct {
	logger             *zap.Logger
	cfg                config.SchedulerConfig
	state              atomic.Pointer[leadershipSnapshot]
	sharedBindingStore bool
	health             *health.Server
	svc                *Service
	registry           NodeRegistry
}

// NewLeadershipManager builds the manager and its Leadership state. It must
// be created before the Service so the recovery window can be attached via
// ServiceOption.
func newLeadershipManager(logger *zap.Logger, cfg config.SchedulerConfig) *leadershipManager {
	if logger == nil {
		logger = zap.NewNop()
	}
	l := &leadershipManager{
		logger: logger,
		cfg:    cfg,
		// Whether standbys may serve reads depends on the binding store:
		// shared (Redis) → reads served; in-memory → reads rejected too.
		sharedBindingStore: strings.TrimSpace(cfg.RedisAddr) != "",
	}
	l.state.Store(newLeadershipSnapshot(cfg.ReportTTL))
	logger.Info("scheduler leader election enabled",
		zap.String("lease", cfg.LeaderElection.LeaseNamespace+"/"+cfg.LeaderElection.LeaseName),
	)
	if strings.TrimSpace(cfg.NodeAdminAPIKey) == "" {
		logger.Warn("sync-node-snapshots pulls will fail auth without scheduler.node_admin_api_key; nodes stay unobserved until heartbeats")
	}
	return l
}

// ServiceOption attaches the recovery-window view to the Service under
// construction. The Service receives the manager itself as a narrow
// leadershipView — the internal leadership object is never handed out.
func (l *leadershipManager) ServiceOption() ServiceOption {
	return WithLeadership(l)
}

// InRecoveryWindow delegates to the internal leadership state: the only
// recovery-window read the Service needs (#259).
func (l *leadershipManager) InRecoveryWindow(now time.Time) bool {
	return l.state.Load().InRecoveryWindow(now)
}

// LeaderSince delegates to the internal leadership state.
func (l *leadershipManager) LeaderSince() (time.Time, bool) {
	return l.state.Load().LeaderSince()
}

// RecoveryPending delegates to the internal leadership state.
func (l *leadershipManager) RecoveryPending(nodeID string, lastReportAt *time.Time) bool {
	return l.state.Load().RecoveryPending(nodeID, lastReportAt)
}

// standbyReadableMethods classifies every Scheduler RPC: true = a non-leader
// replica sharing the binding store (Redis) may serve it; false = gated
// behind leadership. The table lists all methods explicitly so a proto
// change adding an RPC cannot silently fall into "not a read, reject":
// init panics on any unclassified method (fail fast at startup, #259).
//
// Notes on the reads:
//   - LookupNode reads bindings from the shared store: correct on standbys.
//   - GetNode reads replica-local observations, which are always empty on
//     standbys (heartbeats/pulls only reach the leader), so serving it there
//     would return a terminal NotFound for real nodes. Gated (#341 review).
//   - ListNodes reads the informer-backed registry (warm on every replica)
//     and is semantically standby-serveable, but stays gated for now —
//     deferred to a PR decision (#259 review).
var standbyReadableMethods = map[string]bool{
	"/scheduler.v1.Scheduler/Schedule":           false,
	"/scheduler.v1.Scheduler/ListNodes":          false,
	"/scheduler.v1.Scheduler/LookupNode":         true,
	"/scheduler.v1.Scheduler/RecordAssignment":   false,
	"/scheduler.v1.Scheduler/Heartbeat":          false,
	"/scheduler.v1.Scheduler/ReportSandboxEvent": false,
	"/scheduler.v1.Scheduler/ListObservedNodes":  false,
	"/scheduler.v1.Scheduler/ListP2pPeers":       false,
	"/scheduler.v1.Scheduler/RecordP2pArtifact":  false,
	"/scheduler.v1.Scheduler/ForgetP2pArtifact":  false,
	"/scheduler.v1.Scheduler/LookupP2pArtifact":  false,
	"/scheduler.v1.Scheduler/GetNode":            false,
	"/scheduler.v1.Scheduler/UnregisterNode":     false,
}

func init() {
	serviceName := schedulerv1.Scheduler_ServiceDesc.ServiceName
	for _, method := range schedulerv1.Scheduler_ServiceDesc.Methods {
		fullMethod := "/" + serviceName + "/" + method.MethodName
		if _, ok := standbyReadableMethods[fullMethod]; !ok {
			panic("scheduler: unclassified RPC in standbyReadableMethods: " + fullMethod)
		}
	}
}

// GateInterceptor returns the read/write gate to chain into the gRPC server
// after the metrics interceptor. gRPC registration must happen before
// Serve, so gating is an interceptor, not late registration. Behaviour:
//
//   - health-check RPCs are never gated (liveness must work on standbys);
//   - the leader serves everything;
//   - a standby with a shared binding store serves the read RPCs above from
//     that store and rejects the rest with Unavailable("not the leader");
//   - a standby without a shared store (in-memory bindings) rejects reads
//     too, and lookups reach the leader via client retry (degraded mode).
//
// When leader election is disabled the lifecycle (and this interceptor) is
// not installed at all.
func (l *leadershipManager) GateInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if strings.HasPrefix(info.FullMethod, "/grpc.health.v1.") {
			return handler(ctx, req)
		}
		if l.state.Load().IsLeader() {
			return handler(ctx, req)
		}
		if l.sharedBindingStore && standbyReadableMethods[info.FullMethod] {
			return handler(ctx, req)
		}
		return nil, status.Error(codes.Unavailable, "not the leader")
	}
}

// RegisterHealth registers the leader readiness service (NOT_SERVING until
// the lease is won) and keeps a handle for the acquire/loss/shutdown
// transitions.
func (l *leadershipManager) RegisterHealth(hs *health.Server) {
	l.health = hs
	hs.SetServingStatus(LeaderHealthService, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
}

// MarkNotServing flips the leader readiness service off; called on
// leadership loss (before process shutdown) and during graceful shutdown.
func (l *leadershipManager) MarkNotServing() {
	if l.health != nil {
		l.health.SetServingStatus(LeaderHealthService, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	}
}

// BindRuntime attaches the service-side dependencies needed by the
// sync-node-snapshots retrieval on acquire. Must be called after Service
// construction and before Run. The registry is the NodeRegistry interface:
// the manager only lists nodes through it, never the concrete type.
func (l *leadershipManager) BindRuntime(svc *Service, registry NodeRegistry) {
	l.svc = svc
	l.registry = registry
}

// Run drives leader election until ctx is done or the lease is lost; it
// blocks. onStop is the composition root's shutdown hook (e.g. canceling the
// process root context); it runs after the readiness flip so a partitioned
// ex-leader stops serving before exiting (fencing, #259).
func (l *leadershipManager) Run(ctx context.Context, onStop func()) error {
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("leader election requires in-cluster kubernetes access: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("leader election kubernetes client failed: %w", err)
	}
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("leader election identity failed: %w", err)
	}

	elector := newLeaderElector(l.logger, l.cfg.LeaderElection,
		clientset, hostname,
		l.onStartedLeading,
		func() {
			l.state.Store(newLeadershipSnapshot(l.cfg.ReportTTL))
			l.MarkNotServing()
			if onStop != nil {
				onStop()
			}
		},
	)
	return elector.Run(ctx)
}

// onStartedLeading is the elector's OnStartedLeading hook: readiness
// SERVING, then kick off sync-node-snapshots — refresh observations and
// bindings by pulling every node's admin snapshot instead of waiting for the
// next reporter backoff (#259).
func (l *leadershipManager) onStartedLeading(ctx context.Context) {
	// Capture the acquisition-time node set for recovery-pending semantics;
	// tolerate being called before BindRuntime (tests, construction-order
	// guard).
	known := make([]string, 0)
	if l.registry != nil {
		for _, n := range l.registry.Snapshot(false) {
			known = append(known, n.ID)
		}
	}
	l.state.Store(acquiredLeadershipSnapshot(l.cfg.ReportTTL, time.Now(), known...))
	if l.health != nil {
		l.health.SetServingStatus(LeaderHealthService, grpc_health_v1.HealthCheckResponse_SERVING)
	}
	if l.svc == nil || l.registry == nil {
		l.logger.Error("leader lifecycle run before BindRuntime; skipping sync-node-snapshots")
		return
	}
	ingestor := func(report *nodeReport, now time.Time) error {
		// Freshness is enforced atomically inside the registry by
		// HeartbeatUnlessStale (#341 review): a slow pull cannot overwrite a
		// heartbeat that landed while the request was in flight.
		_, err := l.svc.ingestNodeReport(report, now)
		return err
	}
	var refresher nodeSnapshotRefresher = NewConcurrentNodeSnapshotRefresher(l.logger, ingestor,
		NewAdminSnapshotFetcher(l.cfg.NodeAdminAPIKey),
		l.cfg.LeaderElection.SnapshotPullConcurrency)
	go refresher.Refresh(ctx, l.registry.Snapshot(false))
}

// ============================================================

// LeaderElector runs client-go leader election over a
// coordination.k8s.io/Lease (#259). Exactly one replica holds the lease at a
// time; the others wait. Election itself never touches Redis.
//
// The elector holds no leadership state; it forwards client-go callbacks to
// the caller (LeadershipManager), which owns the state transitions.
type leaderElector struct {
	logger    *zap.Logger
	cfg       config.SchedulerLeaderElectionConfig
	clientset kubernetes.Interface
	identity  string
	onStarted func(ctx context.Context)
	onStopped func()
}

// NewLeaderElector builds a runner. The clientset and identity are
// injected: production passes the in-cluster client and the pod hostname;
// tests pass a fake clientset and a stable identity.
func newLeaderElector(
	logger *zap.Logger,
	cfg config.SchedulerLeaderElectionConfig,
	clientset kubernetes.Interface,
	identity string,
	onStarted func(ctx context.Context),
	onStopped func(),
) *leaderElector {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &leaderElector{
		logger:    logger,
		cfg:       cfg,
		clientset: clientset,
		identity:  identity,
		onStarted: onStarted,
		onStopped: onStopped,
	}
}

func (r *leaderElector) Run(ctx context.Context) error {
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      r.cfg.LeaseName,
			Namespace: r.cfg.LeaseNamespace,
		},
		Client: r.clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: r.identity,
		},
	}

	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: r.cfg.LeaseDuration,
		RenewDeadline: r.cfg.RenewDeadline,
		RetryPeriod:   r.cfg.RetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				r.logger.Info("scheduler acquired leadership",
					zap.String("lease", r.cfg.LeaseNamespace+"/"+r.cfg.LeaseName),
				)
				if r.onStarted != nil {
					r.onStarted(leaderCtx)
				}
			},
			OnStoppedLeading: func() {
				r.logger.Warn("scheduler lost leadership; shutting down for fencing")
				if r.onStopped != nil {
					r.onStopped()
				}
			},
			OnNewLeader: func(identity string) {
				r.logger.Info("scheduler leader changed", zap.String("leader", identity))
			},
		},
		// Hold the lease until process exit: releasing on cancel would let
		// a standby acquire while this ex-leader is still draining in-flight
		// RPCs, opening a dual-primary window (#341 review). Failover after a
		// graceful stop is bounded by the remaining lease duration.
		ReleaseOnCancel: false,
	})
	if err != nil {
		return fmt.Errorf("leader election config: %w", err)
	}

	elector.Run(ctx)
	return nil
}
