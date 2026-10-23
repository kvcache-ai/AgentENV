package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	gateway "agentenv/services/gateway/internal"
	"agentenv/services/shared/config"
	"agentenv/services/shared/logging"

	"github.com/caddyserver/certmagic"
	"github.com/libdns/alidns"
	"github.com/libdns/cloudflare"
	"github.com/libdns/route53"
	"github.com/libdns/tencentcloud"
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

	tlsConfig, startCertificates, closeTLS, err := prepareTLS(cfg.Gateway, logger)
	if err != nil {
		logger.Fatal("init gateway TLS failed", zap.Error(err))
	}
	defer closeTLS()
	certificateCtx, cancelCertificates := context.WithCancel(context.Background())
	defer cancelCertificates()
	listenAddr, scheme := cfg.Gateway.HTTPListenAddr, "http"
	if tlsConfig != nil {
		listenAddr, scheme = cfg.Gateway.HTTPSListenAddr, "https"
	}
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		logger.Fatal("bind gateway listener failed", zap.Error(err))
	}
	defer listener.Close()
	logger.Info("gateway listening",
		zap.String("addr", listenAddr),
		zap.String("scheme", scheme),
		zap.String("tls_mode", cfg.Gateway.TLS.Mode),
		zap.String("metrics_addr", cfg.Gateway.MetricsListenAddr),
		zap.String("scheduler", cfg.Gateway.SchedulerAddr),
		zap.String("query_only_scheduler", cfg.Gateway.QueryOnlySchedulerAddr),
		zap.Strings("sandbox_proxy_domains", s.SandboxProxyDomains()),
	)
	httpServer := &http.Server{
		Addr:      listenAddr,
		Handler:   s.Handler(),
		TLSConfig: tlsConfig,
	}
	metricsServer := &http.Server{
		Addr:    cfg.Gateway.MetricsListenAddr,
		Handler: promhttp.Handler(),
	}

	go func() {
		var serveErr error
		if tlsConfig == nil {
			serveErr = httpServer.Serve(listener)
		} else {
			serveErr = httpServer.ServeTLS(listener, "", "")
		}
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			logger.Fatal("gateway serve failed", zap.Error(serveErr))
		}
	}()
	if err := startCertificates(certificateCtx); err != nil {
		logger.Fatal("start certificate management failed", zap.Error(err))
	}
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

func prepareTLS(cfg config.GatewayConfig, logger *zap.Logger) (
	tlsConfig *tls.Config, start func(context.Context) error, closeTLS func(), err error,
) {
	start = func(context.Context) error { return nil }
	closeTLS = func() {}
	switch cfg.TLS.Mode {
	case "", "off":
		return nil, start, closeTLS, nil
	case "manual":
		cert, loadErr := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
		if loadErr != nil {
			return nil, start, closeTLS, fmt.Errorf("load gateway TLS certificate: %w", loadErr)
		}
		return &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{cert},
		}, start, closeTLS, nil
	case "acme":
	default:
		return nil, start, closeTLS, errors.New("unsupported gateway TLS mode")
	}

	host, port, err := net.SplitHostPort(cfg.HTTPSListenAddr)
	if err != nil {
		return nil, start, closeTLS, err
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil {
		return nil, start, closeTLS, err
	}
	var solver *certmagic.DNS01Solver
	if cfg.TLS.Challenge == "dns-01" {
		provider, providerErr := newDNSProvider(cfg.TLS.DNSProvider, cfg.TLS.DNSConfig)
		if providerErr != nil {
			return nil, start, closeTLS, providerErr
		}
		solver = &certmagic.DNS01Solver{DNSManager: certmagic.DNSManager{
			DNSProvider: provider,
		}}
	}
	if err := os.MkdirAll(cfg.TLS.StorageDir, 0o700); err != nil {
		return nil, start, closeTLS, fmt.Errorf("create TLS storage: %w", err)
	}
	var magic *certmagic.Config
	cache := certmagic.NewCache(certmagic.CacheOptions{
		GetConfigForCert: func(certmagic.Certificate) (*certmagic.Config, error) {
			return magic, nil
		},
		Logger: logger,
	})
	defaultName := ""
	for _, name := range cfg.TLS.Domains {
		if ip := net.ParseIP(name); ip != nil {
			defaultName = ip.String()
			break
		}
	}
	magic = certmagic.New(cache, certmagic.Config{
		// IP clients omit SNI; behind Docker/NAT the local address is not the public IP.
		DefaultServerName: defaultName,
		Storage:           &certmagic.FileStorage{Path: cfg.TLS.StorageDir},
		Logger:            logger,
	})
	issuer := certmagic.ACMEIssuer{
		CA:                      cfg.TLS.CA,
		Profile:                 certificateProfile(cfg.TLS.CA, cfg.TLS.Domains),
		Email:                   cfg.TLS.Email,
		Agreed:                  true,
		DisableHTTPChallenge:    true,
		DisableTLSALPNChallenge: solver != nil,
		ListenHost:              host,
		AltTLSALPNPort:          portNumber,
		Logger:                  logger,
	}
	if solver != nil {
		issuer.DNS01Solver = solver
	}
	magic.Issuers = []certmagic.Issuer{certmagic.NewACMEIssuer(magic, issuer)}
	tlsConfig = magic.TLSConfig()
	tlsConfig.NextProtos = append([]string{"h2", "http/1.1"}, tlsConfig.NextProtos...)
	start = func(ctx context.Context) error {
		return magic.ManageAsync(ctx, cfg.TLS.Domains)
	}
	return tlsConfig, start, cache.Stop, nil
}

// Let's Encrypt requires the shortlived profile for IP certificates. Apply it
// to this issuer's whole identifier set; custom CAs retain their own defaults.
func certificateProfile(ca string, identifiers []string) string {
	if ca != "" && ca != certmagic.LetsEncryptProductionCA && ca != certmagic.LetsEncryptStagingCA {
		return ""
	}
	for _, identifier := range identifiers {
		if net.ParseIP(identifier) != nil {
			return "shortlived"
		}
	}
	return ""
}

func newDNSProvider(name string, raw json.RawMessage) (certmagic.DNSProvider, error) {
	var provider certmagic.DNSProvider
	switch name {
	case "cloudflare":
		provider = &cloudflare.Provider{}
	case "alidns":
		provider = &alidns.Provider{}
	case "tencentcloud":
		provider = &tencentcloud.Provider{}
	case "route53":
		provider = &route53.Provider{}
	default:
		return nil, errors.New("unsupported gateway.tls.dns_provider")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return nil, errors.New("gateway.tls.dns_config must be a JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(provider); err != nil {
		// Decoder errors may contain user-supplied keys or values; do not log them.
		return nil, errors.New("invalid gateway.tls.dns_config: check provider field names and types")
	}
	missing := false
	switch p := provider.(type) {
	case *cloudflare.Provider:
		missing = strings.TrimSpace(p.APIToken) == ""
	case *alidns.Provider:
		missing = strings.TrimSpace(p.AccessKeyID) == "" || strings.TrimSpace(p.AccessKeySecret) == ""
	case *tencentcloud.Provider:
		missing = strings.TrimSpace(p.SecretId) == "" || strings.TrimSpace(p.SecretKey) == ""
	case *route53.Provider:
		// Empty credentials allow the AWS SDK credential chain (roles, env, profiles).
		missing = (strings.TrimSpace(p.AccessKeyId) == "") != (strings.TrimSpace(p.SecretAccessKey) == "")
	}
	if missing {
		return nil, errors.New("gateway.tls.dns_config is missing required provider credentials")
	}
	return provider, nil
}
