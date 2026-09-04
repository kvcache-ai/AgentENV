package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
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

const (
	apiKeyEnv         = "AENV_API_KEY"
	defaultAPIKeyPath = "/run/secrets/api-key"
	maxAPIKeyLen      = 256
	maxAPIKeyFileLen  = maxAPIKeyLen + 2
)

func newSchedulerConn(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

func loadAPIKey() (string, error) {
	return loadAPIKeyFrom(os.LookupEnv, defaultAPIKeyPath)
}

func loadAPIKeyFrom(lookupEnv func(string) (string, bool), secretPath string) (string, error) {
	value, source := "", apiKeyEnv
	if explicit, present := lookupEnv(apiKeyEnv); present {
		value = explicit
	} else {
		file, err := openSecretFile(secretPath)
		if err != nil {
			if os.IsNotExist(err) {
				return "", fmt.Errorf("%s must be set or %s must exist", apiKeyEnv, secretPath)
			}
			return "", fmt.Errorf("read secret %s: %w", secretPath, err)
		}
		defer file.Close()
		contents, err := io.ReadAll(io.LimitReader(file, maxAPIKeyFileLen+1))
		if err != nil {
			return "", fmt.Errorf("read secret %s: %w", secretPath, err)
		}
		value = strings.TrimSuffix(strings.TrimSuffix(string(contents), "\n"), "\r")
		source = secretPath
	}
	return validateAPIKey(value, source)
}

func openSecretFile(path string) (*os.File, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = syscall.Close(fd)
		return nil, fmt.Errorf("open returned an invalid file descriptor")
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, fmt.Errorf("must be a regular file")
	}
	return file, nil
}

func validateAPIKey(value, source string) (string, error) {
	if len(value) < 32 || len(value) > maxAPIKeyLen {
		return "", fmt.Errorf("API key from %s must contain between 32 and %d URL-safe characters", source, maxAPIKeyLen)
	}
	for _, char := range []byte(value) {
		if (char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '.' || char == '_' || char == '~' || char == '-' {
			continue
		}
		return "", fmt.Errorf("API key from %s must contain between 32 and %d URL-safe characters", source, maxAPIKeyLen)
	}
	return value, nil
}

func main() {
	configPath := flag.String("config", "", "path to JSON config file")
	flag.Parse()

	cfg, err := config.Load(*configPath, "gateway")
	if err != nil {
		log.Fatalf("load config failed: %v", err)
	}
	apiKey, err := loadAPIKey()
	if err != nil {
		log.Fatalf("load API key failed: %v", err)
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
