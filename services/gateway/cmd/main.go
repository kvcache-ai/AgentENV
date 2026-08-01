package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	gateway "agentenv/services/gateway/internal"
	"agentenv/services/shared/config"
	"agentenv/services/shared/logging"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func newSchedulerConn(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

func main() {
	configPath := flag.String("config", "", "path to JSON config file")
	flag.Parse()

	cfg, err := config.Load(*configPath, "gateway")
	if err != nil {
		log.Fatalf("load config failed: %v", err)
	}
	apiKey := strings.TrimSpace(os.Getenv("AENV_API_KEY"))
	if apiKey == "" {
		log.Fatal("AENV_API_KEY must be set when starting the gateway")
	}

	logger, err := logging.New(cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		log.Fatalf("init logger failed: %v", err)
	}
	defer logger.Sync()

	conn, err := newSchedulerConn(cfg.Gateway.SchedulerAddr)
	if err != nil {
		logger.Fatal("connect scheduler failed", zap.Error(err), zap.String("addr", cfg.Gateway.SchedulerAddr))
	}
	defer conn.Close()

	schedulerClient := schedulerv1.NewSchedulerClient(conn)
	queryOnlySchedulerClient := schedulerClient
	var queryOnlyConn *grpc.ClientConn
	if cfg.Gateway.QueryOnlySchedulerAddr != "" {
		queryOnlyConn, err = newSchedulerConn(cfg.Gateway.QueryOnlySchedulerAddr)
		if err != nil {
			logger.Fatal("connect query-only scheduler failed", zap.Error(err), zap.String("addr", cfg.Gateway.QueryOnlySchedulerAddr))
		}
		defer queryOnlyConn.Close()
		queryOnlySchedulerClient = schedulerv1.NewSchedulerClient(queryOnlyConn)
	}

	s, err := gateway.NewServer(logger, schedulerClient, gateway.ServerOptions{
		RequestTimeout:           cfg.Gateway.RequestTimeout,
		MaxResponseSize:          cfg.Gateway.ForwardResponseSize,
		APIKey:                   apiKey,
		DebugMode:                cfg.Gateway.DebugMode,
		SandboxProxyDomains:      cfg.Gateway.SandboxProxyDomains,
		QueryOnlySchedulerClient: queryOnlySchedulerClient,
	})
	if err != nil {
		logger.Fatal("init gateway server failed", zap.Error(err))
	}

	logger.Info("gateway listening",
		zap.String("addr", cfg.Gateway.HTTPListenAddr),
		zap.String("metrics_addr", cfg.Gateway.MetricsListenAddr),
		zap.String("scheduler", cfg.Gateway.SchedulerAddr),
		zap.String("query_only_scheduler", cfg.Gateway.QueryOnlySchedulerAddr),
		zap.Strings("sandbox_proxy_domains", s.SandboxProxyDomains()),
	)
	httpServer := &http.Server{
		Addr:    cfg.Gateway.HTTPListenAddr,
		Handler: s.Handler(),
	}
	metricsServer := &http.Server{
		Addr:    cfg.Gateway.MetricsListenAddr,
		Handler: promhttp.Handler(),
	}

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("gateway serve failed", zap.Error(err))
		}
	}()
	go func() {
		logger.Info("gateway metrics server listening", zap.String("addr", cfg.Gateway.MetricsListenAddr))
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("gateway metrics serve failed", zap.Error(err))
		}
	}()

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-sigCtx.Done()

	httpShutdownCtx, cancelHTTPShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelHTTPShutdown()
	if err := httpServer.Shutdown(httpShutdownCtx); err != nil {
		logger.Warn("gateway graceful shutdown failed", zap.Error(err))
	}

	metricsShutdownCtx, cancelMetricsShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelMetricsShutdown()
	if err := metricsServer.Shutdown(metricsShutdownCtx); err != nil {
		logger.Warn("gateway metrics graceful shutdown failed", zap.Error(err))
	}
}
