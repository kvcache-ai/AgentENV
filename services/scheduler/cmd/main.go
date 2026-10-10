package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	scheduler "agentenv/services/scheduler/internal"
	"agentenv/services/shared/config"
	"agentenv/services/shared/logging"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"k8s.io/client-go/rest"
)

func main() {
	configPath := flag.String("config", "", "path to JSON config file")
	queryOnly := flag.Bool("query-only", false, "run a query-only scheduler that supports only LookupNode; requires scheduler.redis_addr")
	flag.Parse()

	cfg, err := config.LoadScheduler(*configPath, *queryOnly)
	if err != nil {
		log.Fatalf("load config failed: %v", err)
	}

	logger, err := logging.New(cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		log.Fatalf("init logger failed: %v", err)
	}
	defer logger.Sync()

	// rootCancel lets a leadership loss drive the shutdown path as a signal,
	// but with an important difference (#341 review): on leadership loss the
	// process force-stops instead of draining, so in-flight writes cannot
	// overlap the next leader (no dual-writer window). leadershipLost tells
	// the shutdown path which case it is in.
	var leadershipLost atomic.Bool
	rootCtx, rootCancel := context.WithCancel(context.Background())
	defer rootCancel()
	leadershipLossStop := func() {
		leadershipLost.Store(true)
		rootCancel()
	}
	sigCtx, stop := signal.NotifyContext(rootCtx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, closeStore := createBindingStore(logger, cfg)
	defer closeStore()

	// Leadership facade (#259): the factory picks the election manager (election
	// on) or a no-op (election off); all wiring below is unconditional.
	leadership := scheduler.NewLeadership(logger, cfg.Scheduler)

	interceptors := []grpc.UnaryServerInterceptor{
		scheduler.MetricsUnaryInterceptor(),
		leadership.GateInterceptor(),
	}
	g := grpc.NewServer(grpc.ChainUnaryInterceptor(interceptors...))
	var registry *scheduler.AtomicNodeRegistry
	var svc *scheduler.Service
	// Closed once discovery has produced its first sync. Closed immediately
	// unless leader election runs on kubernetes discovery — there, election
	// must not start before the first informer sync, or an early acquisition
	// would capture an empty registry and recover nothing (#341 review).
	waitForDiscoverySync := cfg.Scheduler.LeaderElection.Enabled && !*queryOnly &&
		strings.EqualFold(strings.TrimSpace(cfg.Scheduler.Discovery.Mode), "kubernetes")
	discoveryReady := make(chan struct{})
	if !waitForDiscoverySync {
		close(discoveryReady)
	}
	if *queryOnly {
		qo := scheduler.NewQueryOnlyService(logger, store)
		schedulerv1.RegisterSchedulerServer(g, qo)
		logger.Info("scheduler query-only service enabled", zap.String("redis_addr", cfg.Scheduler.RedisAddr))
	} else {
		registry = scheduler.NewAtomicNodeRegistry(nil, cfg.Scheduler.ReportTTL)
		switch strings.ToLower(strings.TrimSpace(cfg.Scheduler.Discovery.Mode)) {
		case "kubernetes":
			go runKubernetesDiscoveryWithRetry(sigCtx, logger, cfg.Scheduler.Discovery.Kubernetes, registry, discoveryReady)
		default:
			nodes := make([]scheduler.Node, 0, len(cfg.Scheduler.Nodes))
			for _, n := range cfg.Scheduler.Nodes {
				nodes = append(nodes, scheduler.Node{ID: n.ID, Endpoint: n.Endpoint})
			}
			registry.Set(nodes, nil)
		}

		svcOpts := []scheduler.ServiceOption{
			scheduler.WithArtifactStore(scheduler.NewInMemoryArtifactStore(
				cfg.Scheduler.ArtifactStoreCapacity,
				cfg.Scheduler.ArtifactLookupNodeLimit,
			)),
			scheduler.WithNodeResourceLimit(cfg.Scheduler.NodeResourceLimit),
		}
		svcOpts = append(svcOpts, leadership.ServiceOption())
		svc = scheduler.NewService(
			logger,
			registry,
			scheduler.NewStrategy(cfg.Scheduler.Strategy),
			store,
			svcOpts...,
		)
		go svc.RunObservedNodesMetrics(sigCtx, 15*time.Second)
		schedulerv1.RegisterSchedulerServer(g, svc)
	}

	hs := health.NewServer()
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	hs.SetServingStatus(schedulerv1.Scheduler_ServiceDesc.ServiceName, grpc_health_v1.HealthCheckResponse_SERVING)
	leadership.RegisterHealth(hs)
	grpc_health_v1.RegisterHealthServer(g, hs)

	leadership.BindRuntime(svc, registry)
	if waitForDiscoverySync {
		select {
		case <-discoveryReady:
		case <-time.After(30 * time.Second):
			logger.Fatal("kubernetes discovery initial sync timed out before leader election")
		case <-sigCtx.Done():
		}
	}
	go func() {
		if err := leadership.Run(sigCtx, leadershipLossStop); err != nil {
			logger.Fatal("leader election failed", zap.Error(err))
		}
	}()

	lis, err := net.Listen("tcp", cfg.Scheduler.GRPCListenAddr)
	if err != nil {
		logger.Fatal("listen failed", zap.Error(err), zap.String("addr", cfg.Scheduler.GRPCListenAddr))
	}
	logger.Info("scheduler gRPC server listening",
		zap.String("addr", cfg.Scheduler.GRPCListenAddr),
		zap.String("strategy", cfg.Scheduler.Strategy),
		zap.String("binding_store", bindingStoreName(cfg)),
		zap.Bool("query_only", *queryOnly),
	)

	metricsServer := &http.Server{
		Addr:    cfg.Scheduler.MetricsListenAddr,
		Handler: promhttp.Handler(),
	}
	go func() {
		logger.Info("scheduler metrics server listening", zap.String("addr", metricsServer.Addr))
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("scheduler metrics serve failed", zap.Error(err))
		}
	}()

	serveErrCh := make(chan error, 1)
	go func() {
		err := g.Serve(lis)
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serveErrCh <- err
			return
		}
		serveErrCh <- nil
	}()

	select {
	case err := <-serveErrCh:
		if err != nil {
			logger.Fatal("serve failed", zap.Error(err))
		}
		return
	case <-sigCtx.Done():
	}

	logger.Info("scheduler shutdown signal received")
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	hs.SetServingStatus(schedulerv1.Scheduler_ServiceDesc.ServiceName, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	leadership.MarkNotServing()

	if leadershipLost.Load() {
		// Leadership loss: stop immediately. In-flight RPCs fail and clients
		// retry onto the new leader — better than a dual-writer overlap
		// (#341 review).
		logger.Warn("leadership lost; forcing immediate stop to avoid dual-writer overlap")
		g.Stop()
	} else {
		gracefulStopDone := make(chan struct{})
		go func() {
			g.GracefulStop()
			close(gracefulStopDone)
		}()

		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()

		select {
		case <-gracefulStopDone:
			logger.Info("scheduler stopped gracefully")
		case <-timer.C:
			logger.Warn("scheduler graceful shutdown timed out; forcing stop")
			g.Stop()
			<-gracefulStopDone
		}
	}

	metricsShutdownCtx, cancelMetricsShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelMetricsShutdown()
	if err := metricsServer.Shutdown(metricsShutdownCtx); err != nil {
		logger.Warn("scheduler metrics graceful shutdown failed", zap.Error(err))
	}

	if err := <-serveErrCh; err != nil {
		logger.Fatal("serve failed", zap.Error(err))
	}
}

// effectiveBindingTTL floors the binding TTL under leader election (#259,
// failover safeguard): bindings must outlive the worst-case failover budget
// (lease detection + endpoints propagation + one reporter reconnect backoff),
// or live sandboxes would be misreported as NotFound mid-failover. The
// authoritative cleanup is ReconcileNode; the TTL only guards against a node
// that crashed for good, so raising it is harmless.
func effectiveBindingTTL(cfg config.Config) time.Duration {
	ttl := cfg.Scheduler.BindingTTL
	if cfg.Scheduler.LeaderElection.Enabled {
		floor := cfg.Scheduler.LeaderElection.LeaseDuration + 90*time.Second
		if floor > ttl {
			ttl = floor
		}
	}
	return ttl
}

func createBindingStore(logger *zap.Logger, cfg config.Config) (scheduler.BindingStore, func()) {
	ttl := effectiveBindingTTL(cfg)
	if ttl != cfg.Scheduler.BindingTTL {
		logger.Info("binding TTL raised to cover the failover budget",
			zap.Duration("configured", cfg.Scheduler.BindingTTL),
			zap.Duration("effective", ttl),
		)
	}
	if strings.TrimSpace(cfg.Scheduler.RedisAddr) == "" {
		return scheduler.NewInMemoryBindingStore(ttl), func() {}
	}

	store, err := scheduler.NewRedisBindingStore(cfg.Scheduler.RedisAddr, ttl)
	if err != nil {
		logger.Fatal("create redis binding store failed", zap.Error(err), zap.String("addr", cfg.Scheduler.RedisAddr))
	}
	return store, func() {
		if err := store.Close(); err != nil {
			logger.Warn("close redis binding store failed", zap.Error(err))
		}
	}
}

func bindingStoreName(cfg config.Config) string {
	if strings.TrimSpace(cfg.Scheduler.RedisAddr) != "" {
		return "redis"
	}
	return "memory"
}

func runKubernetesDiscoveryWithRetry(
	ctx context.Context,
	logger *zap.Logger,
	cfg config.SchedulerDiscoveryKubernetesConfig,
	registry *scheduler.AtomicNodeRegistry,
	ready ...chan<- struct{},
) {
	const (
		initialBackoff = 1 * time.Second
		maxBackoff     = 30 * time.Second
	)

	backoff := initialBackoff
	attempt := 0

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		attempt++
		discovery, err := scheduler.NewKubernetesDiscovery(logger, cfg, registry, ready...)
		if err != nil {
			if errors.Is(err, rest.ErrNotInCluster) {
				logger.Error("kubernetes discovery initialization failed with non-retryable error; stopping discovery loop",
					zap.Error(err),
					zap.Int("attempt", attempt),
				)
				return
			}

			logger.Warn("kubernetes discovery initialization failed; retrying",
				zap.Error(err),
				zap.Int("attempt", attempt),
				zap.Duration("retry_in", backoff),
			)
			if !sleepWithContext(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}

		err = discovery.Run(ctx)
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}

		logger.Warn("kubernetes discovery stopped unexpectedly; retrying",
			zap.Error(err),
			zap.Int("attempt", attempt),
			zap.Duration("retry_in", backoff),
		)
		if !sleepWithContext(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff, maxBackoff)
	}
}

func sleepWithContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextBackoff(current time.Duration, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		return max
	}
	return next
}
