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

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, closeStore := createBindingStore(logger, cfg)
	defer closeStore()

	g := grpc.NewServer(grpc.UnaryInterceptor(scheduler.MetricsUnaryInterceptor()))
	if *queryOnly {
		svc := scheduler.NewQueryOnlyService(logger, store)
		schedulerv1.RegisterSchedulerServer(g, svc)
		logger.Info("scheduler query-only service enabled", zap.String("redis_addr", cfg.Scheduler.RedisAddr))
	} else {
		discoveryMode := strings.ToLower(strings.TrimSpace(cfg.Scheduler.Discovery.Mode))
		var registry *scheduler.AtomicNodeRegistry
		if discoveryMode == "heartbeat" {
			registry = scheduler.NewHeartbeatNodeRegistry(cfg.Scheduler.ReportTTL)
		} else {
			registry = scheduler.NewAtomicNodeRegistry(nil, cfg.Scheduler.ReportTTL)
		}
		switch discoveryMode {
		case "kubernetes":
			go runKubernetesDiscoveryWithRetry(sigCtx, logger, cfg.Scheduler.Discovery.Kubernetes, registry)
		case "heartbeat":
			logger.Info("scheduler heartbeat discovery enabled")
		default:
			nodes := make([]scheduler.Node, 0, len(cfg.Scheduler.Nodes))
			for _, n := range cfg.Scheduler.Nodes {
				nodes = append(nodes, scheduler.Node{ID: n.ID, Endpoint: n.Endpoint})
			}
			registry.Set(nodes, nil)
		}

		options := []scheduler.ServiceOption{
			scheduler.WithArtifactStore(scheduler.NewInMemoryArtifactStore(
				cfg.Scheduler.ArtifactStoreCapacity,
				cfg.Scheduler.ArtifactLookupNodeLimit,
			)),
			scheduler.WithNodeResourceLimit(cfg.Scheduler.NodeResourceLimit),
			scheduler.WithHeartbeatRegistrationToken(cfg.Scheduler.HeartbeatRegistrationToken),
		}
		if cfg.Scheduler.Fleet.Enabled {
			fleetPlanner, planErr := scheduler.NewFleetPlanner(registry, scheduler.FleetPolicy{
				MinNodes: cfg.Scheduler.Fleet.MinNodes, MaxNodes: cfg.Scheduler.Fleet.MaxNodes,
				WarmNodes:            cfg.Scheduler.Fleet.WarmNodes,
				MaxSandboxesPerNode:  cfg.Scheduler.Fleet.MaxSandboxesPerNode,
				MaxMemoryUsedPercent: cfg.Scheduler.Fleet.MaxMemoryUsedPercent,
				EmptyGrace:           cfg.Scheduler.Fleet.EmptyGrace, DrainGrace: cfg.Scheduler.Fleet.DrainGrace,
				DemandTTL: cfg.Scheduler.Fleet.DemandTTL,
			})
			if planErr != nil {
				logger.Fatal("create scheduler fleet planner failed", zap.Error(planErr))
			}
			options = append(options, scheduler.WithFleetPlanner(fleetPlanner))
		}
		svc := scheduler.NewService(
			logger,
			registry,
			scheduler.NewStrategy(cfg.Scheduler.Strategy),
			store,
			options...,
		)
		go svc.RunObservedNodesMetrics(sigCtx, 15*time.Second)
		schedulerv1.RegisterSchedulerServer(g, svc)
	}

	hs := health.NewServer()
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	hs.SetServingStatus(schedulerv1.Scheduler_ServiceDesc.ServiceName, grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(g, hs)

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

	metricsShutdownCtx, cancelMetricsShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelMetricsShutdown()
	if err := metricsServer.Shutdown(metricsShutdownCtx); err != nil {
		logger.Warn("scheduler metrics graceful shutdown failed", zap.Error(err))
	}

	if err := <-serveErrCh; err != nil {
		logger.Fatal("serve failed", zap.Error(err))
	}
}

func createBindingStore(logger *zap.Logger, cfg config.Config) (scheduler.BindingStore, func()) {
	if strings.TrimSpace(cfg.Scheduler.RedisAddr) == "" {
		return scheduler.NewInMemoryBindingStore(cfg.Scheduler.BindingTTL), func() {}
	}

	store, err := scheduler.NewRedisBindingStore(cfg.Scheduler.RedisAddr, cfg.Scheduler.BindingTTL)
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
		discovery, err := scheduler.NewKubernetesDiscovery(logger, cfg, registry)
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
