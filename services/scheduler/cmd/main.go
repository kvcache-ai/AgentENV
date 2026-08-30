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
	"k8s.io/client-go/kubernetes"
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

	leaderElectionEnabled := !*queryOnly && cfg.Scheduler.LeaderElection.Enabled
	gate := newLeadershipGate(!leaderElectionEnabled)
	g := grpc.NewServer(grpc.ChainUnaryInterceptor(
		scheduler.MetricsUnaryInterceptor(),
		gate.unaryServerInterceptor(),
	))
	hs := health.NewServer()
	setServingStatus := func(serving bool) {
		status := grpc_health_v1.HealthCheckResponse_NOT_SERVING
		if serving {
			status = grpc_health_v1.HealthCheckResponse_SERVING
		}
		hs.SetServingStatus("", status)
		hs.SetServingStatus(schedulerv1.Scheduler_ServiceDesc.ServiceName, status)
	}
	setServingStatus(!leaderElectionEnabled)

	var runLeaderWork func(context.Context)
	if *queryOnly {
		svc := scheduler.NewQueryOnlyService(logger, store)
		schedulerv1.RegisterSchedulerServer(g, svc)
		logger.Info("scheduler query-only service enabled", zap.String("redis_addr", cfg.Scheduler.RedisAddr))
	} else {
		registry := scheduler.NewAtomicNodeRegistry(nil, cfg.Scheduler.ReportTTL)
		serviceOptions := []scheduler.ServiceOption{
			scheduler.WithArtifactStore(scheduler.NewInMemoryArtifactStore(
				cfg.Scheduler.ArtifactStoreCapacity,
				cfg.Scheduler.ArtifactLookupNodeLimit,
			)),
			scheduler.WithNodeResourceLimit(cfg.Scheduler.NodeResourceLimit),
		}
		if leaderElectionEnabled {
			serviceOptions = append(serviceOptions, scheduler.WithRequireFreshHeartbeat())
		}
		svc := scheduler.NewService(
			logger,
			registry,
			scheduler.NewStrategy(cfg.Scheduler.Strategy),
			store,
			serviceOptions...,
		)
		schedulerv1.RegisterSchedulerServer(g, svc)

		runLeaderWork = func(ctx context.Context) {
			switch strings.ToLower(strings.TrimSpace(cfg.Scheduler.Discovery.Mode)) {
			case "kubernetes":
				go runKubernetesDiscoveryWithRetry(ctx, logger, cfg.Scheduler.Discovery.Kubernetes, registry)
			default:
				nodes := make([]scheduler.Node, 0, len(cfg.Scheduler.Nodes))
				for _, n := range cfg.Scheduler.Nodes {
					nodes = append(nodes, scheduler.Node{ID: n.ID, Endpoint: n.Endpoint})
				}
				registry.Set(nodes, nil)
			}
			go svc.RunObservedNodesMetrics(ctx, 15*time.Second)
			<-ctx.Done()
		}
	}

	grpc_health_v1.RegisterHealthServer(g, hs)

	var leaderClient kubernetes.Interface
	if leaderElectionEnabled {
		restConfig, err := rest.InClusterConfig()
		if err != nil {
			logger.Fatal("load in-cluster config for scheduler leader election failed", zap.Error(err))
		}
		leaderClient, err = kubernetes.NewForConfig(restConfig)
		if err != nil {
			logger.Fatal("create Kubernetes client for scheduler leader election failed", zap.Error(err))
		}
	}

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

	leadershipLostCh := make(chan struct{}, 1)
	if leaderElectionEnabled {
		go runLeaderElection(
			sigCtx,
			logger,
			leaderClient,
			cfg.Scheduler.LeaderElection,
			func(leaderCtx context.Context) {
				gate.setLeader(true)
				setServingStatus(true)
				runLeaderWork(leaderCtx)
			},
			func() {
				gate.setLeader(false)
				setServingStatus(false)
				if sigCtx.Err() == nil {
					select {
					case leadershipLostCh <- struct{}{}:
					default:
					}
				}
			},
		)
	} else if !*queryOnly {
		go runLeaderWork(sigCtx)
	}

	select {
	case err := <-serveErrCh:
		if err != nil {
			logger.Fatal("serve failed", zap.Error(err))
		}
		return
	case <-leadershipLostCh:
		logger.Error("scheduler leadership lost; shutting down to discard stale observed state")
		stop()
	case <-sigCtx.Done():
	}

	logger.Info("scheduler shutdown signal received")
	setServingStatus(false)

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
