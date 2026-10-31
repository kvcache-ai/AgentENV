package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultSchedulerArtifactStoreCapacity = 1_000_000

// Connection limits for the Go HTTP listeners. There is deliberately no read
// or write timeout: proxied SSE, WebSocket and streaming responses may last
// indefinitely, so only the TLS handshake, HTTP/1.x request headers and idle
// keep-alive time are limited.
const (
	DefaultHTTPReadHeaderTimeout = 10 * time.Second
	DefaultHTTPIdleTimeout       = 120 * time.Second
)

type Node struct {
	ID       string `json:"id"`
	Endpoint string `json:"endpoint"`
}

type SchedulerDiscoveryKubernetesConfig struct {
	Namespace             string `json:"namespace"`
	ServiceName           string `json:"service_name"`
	Port                  int32  `json:"port"`
	Scheme                string `json:"scheme"`
	IgnorePodSelector     string `json:"ignore_pod_selector"`
	NoSchedulePodSelector string `json:"no_schedule_pod_selector"`
}

type SchedulerDiscoveryConfig struct {
	Mode       string                             `json:"mode"`
	Kubernetes SchedulerDiscoveryKubernetesConfig `json:"kubernetes"`
}

// NodeResourceLimit defines per-node resource thresholds for scheduling
// eligibility. A node exceeding any configured limit is excluded from
// scheduling candidates. Nil (absent) fields impose no limit.
//
// Allocated-percent limits (CPU and memory) can legitimately exceed 100%
// because allocated resources reflect the sum of all sandbox reservations,
// which may overcommit the physical capacity of the node.
type NodeResourceLimit struct {
	MaxSandboxCount           *uint32 `json:"max_sandbox_count"`
	MaxSandboxStartingCount   *uint32 `json:"max_sandbox_starting_count"`
	MaxCPUUsedPercent         *uint32 `json:"max_cpu_used_percent"`
	MaxCPUAllocatedPercent    *uint32 `json:"max_cpu_allocated_percent"` // can exceed 100 (overcommit)
	MaxMemoryUsedPercent      *uint32 `json:"max_memory_used_percent"`
	MaxMemoryAllocatedPercent *uint32 `json:"max_memory_allocated_percent"` // can exceed 100 (overcommit)

	// Limits that apply to the sum of the active running set plus paused
	// sandboxes. Paused sandboxes have released their VM-side CPU / memory
	// but still occupy persisted state on the node, so operators may want a
	// separate ceiling on total node footprint (including paused) on top of
	// the active-only ceilings above.
	MaxSandboxCountIncludingPaused         *uint32 `json:"max_sandbox_count_including_paused"`
	MaxAllocatedCPUIncludingPaused         *uint32 `json:"max_allocated_cpu_including_paused"`
	MaxAllocatedMemoryBytesIncludingPaused *uint64 `json:"max_allocated_memory_bytes_including_paused"`
}

// SchedulerLeaderElectionConfig configures Kubernetes Lease-based leader
// election for the scheduler (#259). Disabled by default; when disabled the
// scheduler behaves exactly as a single-writer process (today's behaviour).
// A Redis binding store is recommended but not required: without it, failover
// loses routing for pre-failover sandboxes (degraded mode).
type SchedulerLeaderElectionConfig struct {
	Enabled        bool          `json:"enabled"`
	LeaseName      string        `json:"lease_name"`
	LeaseNamespace string        `json:"lease_namespace"`
	LeaseDuration  time.Duration `json:"lease_duration"`
	RenewDeadline  time.Duration `json:"renew_deadline"`
	RetryPeriod    time.Duration `json:"retry_period"`
	// SnapshotPullConcurrency bounds how many nodes the post-acquisition
	// sync-node-snapshots pull fans out to at once. Zero means the default.
	SnapshotPullConcurrency int `json:"snapshot_pull_concurrency"`
}

type SchedulerConfig struct {
	GRPCListenAddr    string        `json:"grpc_listen_addr"`
	MetricsListenAddr string        `json:"metrics_listen_addr"`
	Strategy          string        `json:"strategy"`
	ReportTTL         time.Duration `json:"report_ttl"`
	BindingTTL        time.Duration `json:"binding_ttl"`
	RedisAddr         string        `json:"redis_addr"`
	// NodeAdminAPIKey authenticates the scheduler against each node's admin
	// API (X-Admin-Token header, OpenAPI AdminApiKeyAuth) for
	// sync-node-snapshots pulls (#259). It is a credential: prefer the
	// SCHEDULER_NODE_ADMIN_API_KEY env (mounted from the agentenv-auth
	// Secret) over this JSON field. Optional; without it, pulls fail auth
	// and nodes stay unobserved until heartbeats.
	NodeAdminAPIKey         string                        `json:"node_admin_api_key"`
	ArtifactStoreCapacity   int                           `json:"artifact_store_capacity"`
	ArtifactLookupNodeLimit int                           `json:"artifact_lookup_node_limit"`
	Nodes                   []Node                        `json:"nodes"`
	Discovery               SchedulerDiscoveryConfig      `json:"discovery"`
	NodeResourceLimit       *NodeResourceLimit            `json:"node_resource_limit"`
	LeaderElection          SchedulerLeaderElectionConfig `json:"leader_election"`
}

// EffectiveBindingTTL floors the binding TTL under leader election (#259,
// failover safeguard): bindings must outlive the worst-case failover budget
// (lease detection + endpoints propagation + one reporter reconnect
// backoff), or live sandboxes would be misreported as NotFound mid-failover.
// The authoritative cleanup is ReconcileNode; the TTL only guards against a
// node that crashed for good, so raising it is harmless.
func (s SchedulerConfig) EffectiveBindingTTL() time.Duration {
	ttl := s.BindingTTL
	if s.LeaderElection.Enabled {
		if floor := s.LeaderElection.LeaseDuration + 90*time.Second; floor > ttl {
			ttl = floor
		}
	}
	return ttl
}

func (s *SchedulerConfig) UnmarshalJSON(data []byte) error {
	type wire struct {
		GRPCListenAddr          *string                   `json:"grpc_listen_addr"`
		MetricsListenAddr       *string                   `json:"metrics_listen_addr"`
		Strategy                *string                   `json:"strategy"`
		ReportTTL               json.RawMessage           `json:"report_ttl"`
		BindingTTL              json.RawMessage           `json:"binding_ttl"`
		RedisAddr               *string                   `json:"redis_addr"`
		NodeAdminAPIKey         *string                   `json:"node_admin_api_key"`
		ArtifactStoreCapacity   *int                      `json:"artifact_store_capacity"`
		ArtifactLookupNodeLimit *int                      `json:"artifact_lookup_node_limit"`
		Nodes                   *[]Node                   `json:"nodes"`
		Discovery               *SchedulerDiscoveryConfig `json:"discovery"`
		NodeResourceLimit       *NodeResourceLimit        `json:"node_resource_limit"`
		LeaderElection          *leaderElectionWire       `json:"leader_election"`
	}

	parsed := wire{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}

	if parsed.GRPCListenAddr != nil {
		s.GRPCListenAddr = *parsed.GRPCListenAddr
	}
	if parsed.MetricsListenAddr != nil {
		s.MetricsListenAddr = *parsed.MetricsListenAddr
	}
	if parsed.Strategy != nil {
		s.Strategy = *parsed.Strategy
	}
	if parsed.Nodes != nil {
		s.Nodes = *parsed.Nodes
	}
	if parsed.Discovery != nil {
		s.Discovery = *parsed.Discovery
	}
	if parsed.NodeResourceLimit != nil {
		s.NodeResourceLimit = parsed.NodeResourceLimit
	}
	if parsed.RedisAddr != nil {
		s.RedisAddr = *parsed.RedisAddr
	}
	if parsed.NodeAdminAPIKey != nil {
		s.NodeAdminAPIKey = *parsed.NodeAdminAPIKey
	}
	if parsed.ArtifactStoreCapacity != nil {
		s.ArtifactStoreCapacity = *parsed.ArtifactStoreCapacity
	}
	if parsed.ArtifactLookupNodeLimit != nil {
		s.ArtifactLookupNodeLimit = *parsed.ArtifactLookupNodeLimit
	}

	if len(bytes.TrimSpace(parsed.ReportTTL)) > 0 {
		d, err := parseSchedulerDuration(parsed.ReportTTL, "scheduler.report_ttl")
		if err != nil {
			return err
		}
		s.ReportTTL = d
	}
	if len(bytes.TrimSpace(parsed.BindingTTL)) > 0 {
		d, err := parseSchedulerDuration(parsed.BindingTTL, "scheduler.binding_ttl")
		if err != nil {
			return err
		}
		s.BindingTTL = d
	}

	if parsed.LeaderElection != nil {
		le := parsed.LeaderElection
		if le.Enabled != nil {
			s.LeaderElection.Enabled = *le.Enabled
		}
		if le.LeaseName != nil {
			s.LeaderElection.LeaseName = *le.LeaseName
		}
		if le.LeaseNamespace != nil {
			s.LeaderElection.LeaseNamespace = *le.LeaseNamespace
		}
		if le.SnapshotPullConcurrency != nil {
			if *le.SnapshotPullConcurrency <= 0 {
				return fmt.Errorf("scheduler.leader_election.snapshot_pull_concurrency must be greater than zero, got %d", *le.SnapshotPullConcurrency)
			}
			s.LeaderElection.SnapshotPullConcurrency = *le.SnapshotPullConcurrency
		}
		durationFields := []struct {
			raw   json.RawMessage
			field string
			dst   *time.Duration
		}{
			{le.LeaseDuration, "scheduler.leader_election.lease_duration", &s.LeaderElection.LeaseDuration},
			{le.RenewDeadline, "scheduler.leader_election.renew_deadline", &s.LeaderElection.RenewDeadline},
			{le.RetryPeriod, "scheduler.leader_election.retry_period", &s.LeaderElection.RetryPeriod},
		}
		for _, f := range durationFields {
			if len(bytes.TrimSpace(f.raw)) == 0 {
				continue
			}
			d, err := parseSchedulerDuration(f.raw, f.field)
			if err != nil {
				return err
			}
			// Defaults apply to omitted fields only; an explicitly provided
			// non-positive duration is invalid, not a request for the default.
			if d <= 0 {
				return fmt.Errorf("%s must be greater than zero, got %q", f.field, d.String())
			}
			*f.dst = d
		}
	}

	return nil
}

// leaderElectionWire is the JSON wire form of SchedulerLeaderElectionConfig;
// durations arrive as strings and are parsed via parseSchedulerDuration.
type leaderElectionWire struct {
	Enabled                 *bool           `json:"enabled"`
	LeaseName               *string         `json:"lease_name"`
	LeaseNamespace          *string         `json:"lease_namespace"`
	LeaseDuration           json.RawMessage `json:"lease_duration"`
	RenewDeadline           json.RawMessage `json:"renew_deadline"`
	RetryPeriod             json.RawMessage `json:"retry_period"`
	SnapshotPullConcurrency *int            `json:"snapshot_pull_concurrency"`
}

func parseSchedulerDuration(raw json.RawMessage, field string) (time.Duration, error) {
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		d, parseErr := time.ParseDuration(strings.TrimSpace(asString))
		if parseErr != nil {
			return 0, fmt.Errorf("%s must be a duration string like \"30s\": %w", field, parseErr)
		}
		return d, nil
	}

	var asNumber json.Number
	if err := json.Unmarshal(raw, &asNumber); err == nil {
		return 0, fmt.Errorf("%s must be a duration string like \"30s\", got numeric value %s", field, asNumber.String())
	}

	return 0, fmt.Errorf("%s must be a duration string like \"30s\"", field)
}

type GatewayTLSConfig struct {
	Mode        string   `json:"mode"`
	CertFile    string   `json:"cert_file"`
	KeyFile     string   `json:"key_file"`
	Domains     []string `json:"domains"`
	StorageDir  string   `json:"storage_dir"`
	Email       string   `json:"email"`
	CA          string   `json:"ca"`
	Challenge   string   `json:"challenge"`
	DNSProvider string   `json:"dns_provider"`
	// Sensitive: never include this field in logs.
	DNSConfig json.RawMessage `json:"dns_config,omitempty"`
}

func (t GatewayTLSConfig) MarshalJSON() ([]byte, error) {
	type plain GatewayTLSConfig
	return json.Marshal(struct {
		plain
		DNSConfig *json.RawMessage `json:"dns_config,omitempty"`
	}{plain: plain(t)})
}

type GatewayConfig struct {
	HTTPSListenAddr        string           `json:"https_listen_addr"`
	TLS                    GatewayTLSConfig `json:"tls"`
	HTTPListenAddr         string           `json:"http_listen_addr"`
	MetricsListenAddr      string           `json:"metrics_listen_addr"`
	SchedulerAddr          string           `json:"scheduler_addr"`
	QueryOnlySchedulerAddr string           `json:"query_only_scheduler_addr"`
	RequestTimeout         time.Duration    `json:"request_timeout"`
	ReadHeaderTimeout      time.Duration    `json:"read_header_timeout"`
	IdleTimeout            time.Duration    `json:"idle_timeout"`
	ForwardResponseSize    int64            `json:"forward_response_size"`
	SandboxProxyDomains    []string         `json:"sandbox_proxy_domains"`
	// DebugMode enables debug-only behaviors in the gateway such as exposing
	// the backend node id on proxied responses. It is off by default.
	DebugMode bool `json:"debug_mode"`
}

func (g *GatewayConfig) UnmarshalJSON(data []byte) error {
	type wire struct {
		HTTPSListenAddr        *string           `json:"https_listen_addr"`
		TLS                    *GatewayTLSConfig `json:"tls"`
		HTTPListenAddr         *string           `json:"http_listen_addr"`
		MetricsListenAddr      *string           `json:"metrics_listen_addr"`
		SchedulerAddr          *string           `json:"scheduler_addr"`
		QueryOnlySchedulerAddr *string           `json:"query_only_scheduler_addr"`
		RequestTimeout         json.RawMessage   `json:"request_timeout"`
		ReadHeaderTimeout      json.RawMessage   `json:"read_header_timeout"`
		IdleTimeout            json.RawMessage   `json:"idle_timeout"`
		ForwardResponseSize    *int64            `json:"forward_response_size"`
		SandboxProxyDomains    *[]string         `json:"sandbox_proxy_domains"`
		DebugMode              *bool             `json:"debug_mode"`
	}

	parsed := wire{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}

	if parsed.HTTPSListenAddr != nil {
		g.HTTPSListenAddr = *parsed.HTTPSListenAddr
	}
	if parsed.TLS != nil {
		g.TLS = *parsed.TLS
	}
	if parsed.HTTPListenAddr != nil {
		g.HTTPListenAddr = *parsed.HTTPListenAddr
	}
	if parsed.MetricsListenAddr != nil {
		g.MetricsListenAddr = *parsed.MetricsListenAddr
	}
	if parsed.SchedulerAddr != nil {
		g.SchedulerAddr = *parsed.SchedulerAddr
	}
	if parsed.QueryOnlySchedulerAddr != nil {
		g.QueryOnlySchedulerAddr = *parsed.QueryOnlySchedulerAddr
	}
	if parsed.ForwardResponseSize != nil {
		g.ForwardResponseSize = *parsed.ForwardResponseSize
	}
	if parsed.SandboxProxyDomains != nil {
		g.SandboxProxyDomains = *parsed.SandboxProxyDomains
	}
	if parsed.DebugMode != nil {
		g.DebugMode = *parsed.DebugMode
	}

	if len(bytes.TrimSpace(parsed.RequestTimeout)) > 0 {
		d, err := parseGatewayRequestTimeout(parsed.RequestTimeout)
		if err != nil {
			return err
		}
		g.RequestTimeout = d
	}
	if len(bytes.TrimSpace(parsed.ReadHeaderTimeout)) > 0 {
		d, err := parseSchedulerDuration(parsed.ReadHeaderTimeout, "gateway.read_header_timeout")
		if err != nil {
			return err
		}
		g.ReadHeaderTimeout = d
	}
	if len(bytes.TrimSpace(parsed.IdleTimeout)) > 0 {
		d, err := parseSchedulerDuration(parsed.IdleTimeout, "gateway.idle_timeout")
		if err != nil {
			return err
		}
		g.IdleTimeout = d
	}

	return nil
}

func parseGatewayRequestTimeout(raw json.RawMessage) (time.Duration, error) {
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		d, parseErr := time.ParseDuration(strings.TrimSpace(asString))
		if parseErr != nil {
			return 0, fmt.Errorf("gateway.request_timeout must be a duration string like \"30s\": %w", parseErr)
		}
		return d, nil
	}

	var asNumber json.Number
	if err := json.Unmarshal(raw, &asNumber); err == nil {
		return 0, fmt.Errorf("gateway.request_timeout must be a duration string like \"30s\", got numeric value %s", asNumber.String())
	}

	return 0, errors.New("gateway.request_timeout must be a duration string like \"30s\"")
}

type Config struct {
	Service   string          `json:"service"`
	LogLevel  string          `json:"log_level"`
	LogFormat string          `json:"log_format"`
	Scheduler SchedulerConfig `json:"scheduler"`
	Gateway   GatewayConfig   `json:"gateway"`
}

func Load(path string, service string) (Config, error) {
	return load(path, service, false)
}

func LoadScheduler(path string, queryOnly bool) (Config, error) {
	return load(path, "scheduler", queryOnly)
}

func load(path string, service string, schedulerQueryOnly bool) (Config, error) {
	cfg := defaultConfig(service)
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config file: %w", err)
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
			return Config{}, fmt.Errorf("unmarshal config json: %w", err)
		}
	}
	if err := overrideWithEnv(&cfg); err != nil {
		return Config{}, err
	}
	cfg.Service = service
	cfg.applyDefaults()
	if err := cfg.validate(schedulerQueryOnly); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func defaultConfig(service string) Config {
	return Config{
		Service:   service,
		LogLevel:  "info",
		LogFormat: "auto",
		Scheduler: SchedulerConfig{
			GRPCListenAddr:          ":9090",
			MetricsListenAddr:       ":9101",
			Strategy:                "round_robin",
			ReportTTL:               30 * time.Second,
			BindingTTL:              30 * time.Second,
			ArtifactStoreCapacity:   defaultSchedulerArtifactStoreCapacity,
			ArtifactLookupNodeLimit: 0,
			Nodes: []Node{
				{ID: "local-node", Endpoint: "http://127.0.0.1:8000"},
			},
			Discovery: SchedulerDiscoveryConfig{
				Mode: "static",
				Kubernetes: SchedulerDiscoveryKubernetesConfig{
					Scheme: "http",
				},
			},
		},
		Gateway: GatewayConfig{
			HTTPSListenAddr:     ":8080",
			TLS:                 GatewayTLSConfig{Mode: "off"},
			HTTPListenAddr:      ":8080",
			MetricsListenAddr:   ":9102",
			SchedulerAddr:       "127.0.0.1:9090",
			RequestTimeout:      30 * time.Second,
			ReadHeaderTimeout:   DefaultHTTPReadHeaderTimeout,
			IdleTimeout:         DefaultHTTPIdleTimeout,
			ForwardResponseSize: 4 << 20,
			SandboxProxyDomains: []string{},
		},
	}
}

func overrideWithEnv(cfg *Config) error {
	set := func(key string, target *string) {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			*target = v
		}
	}
	set("LOG_LEVEL", &cfg.LogLevel)
	set("LOG_FORMAT", &cfg.LogFormat)
	set("SCHEDULER_GRPC_LISTEN_ADDR", &cfg.Scheduler.GRPCListenAddr)
	set("SCHEDULER_METRICS_LISTEN_ADDR", &cfg.Scheduler.MetricsListenAddr)
	set("SCHEDULER_STRATEGY", &cfg.Scheduler.Strategy)
	set("SCHEDULER_REDIS_ADDR", &cfg.Scheduler.RedisAddr)
	set("GATEWAY_HTTPS_LISTEN_ADDR", &cfg.Gateway.HTTPSListenAddr)
	// The node admin API key is a credential: pass it via env/Secret (same
	// agentenv-auth secret the nodes use), never via config files.
	set("SCHEDULER_NODE_ADMIN_API_KEY", &cfg.Scheduler.NodeAdminAPIKey)
	set("GATEWAY_HTTP_LISTEN_ADDR", &cfg.Gateway.HTTPListenAddr)
	set("GATEWAY_METRICS_LISTEN_ADDR", &cfg.Gateway.MetricsListenAddr)
	set("GATEWAY_SCHEDULER_ADDR", &cfg.Gateway.SchedulerAddr)
	set("GATEWAY_QUERY_ONLY_SCHEDULER_ADDR", &cfg.Gateway.QueryOnlySchedulerAddr)

	if v := strings.TrimSpace(os.Getenv("GATEWAY_SANDBOX_PROXY_DOMAINS")); v != "" {
		cfg.Gateway.SandboxProxyDomains = splitCommaSeparated(v)
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_BINDING_TTL")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_BINDING_TTL %q: %w", v, err)
		}
		cfg.Scheduler.BindingTTL = d
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_ARTIFACT_STORE_CAPACITY")); v != "" {
		capacity, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_ARTIFACT_STORE_CAPACITY %q: %w", v, err)
		}
		cfg.Scheduler.ArtifactStoreCapacity = capacity
	}

	if v := strings.TrimSpace(os.Getenv("SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT")); v != "" {
		limit, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT %q: %w", v, err)
		}
		cfg.Scheduler.ArtifactLookupNodeLimit = limit
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_REQUEST_TIMEOUT")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_REQUEST_TIMEOUT %q: %w", v, err)
		}
		cfg.Gateway.RequestTimeout = d
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_READ_HEADER_TIMEOUT")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_READ_HEADER_TIMEOUT %q: %w", v, err)
		}
		cfg.Gateway.ReadHeaderTimeout = d
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_IDLE_TIMEOUT")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_IDLE_TIMEOUT %q: %w", v, err)
		}
		cfg.Gateway.IdleTimeout = d
	}

	if v := strings.TrimSpace(os.Getenv("GATEWAY_DEBUG_MODE")); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid GATEWAY_DEBUG_MODE %q: %w", v, err)
		}
		cfg.Gateway.DebugMode = b
	}

	return nil
}

func splitCommaSeparated(raw string) []string {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			values = append(values, part)
		}
	}
	return values
}

func (c *Config) applyDefaults() {
	if c.Gateway.TLS.Mode == "" {
		c.Gateway.TLS.Mode = "off"
	}
	if c.Gateway.TLS.Challenge == "" {
		c.Gateway.TLS.Challenge = "tls-alpn-01"
	}

	if strings.TrimSpace(c.Scheduler.MetricsListenAddr) == "" {
		c.Scheduler.MetricsListenAddr = ":9101"
	}
	if c.Scheduler.ReportTTL <= 0 {
		c.Scheduler.ReportTTL = 30 * time.Second
	}
	if c.Scheduler.BindingTTL <= 0 {
		c.Scheduler.BindingTTL = 30 * time.Second
	}
	if strings.TrimSpace(c.Scheduler.Discovery.Mode) == "" {
		c.Scheduler.Discovery.Mode = "static"
	}
	if strings.TrimSpace(c.Scheduler.Discovery.Kubernetes.Scheme) == "" {
		c.Scheduler.Discovery.Kubernetes.Scheme = "http"
	}
	if strings.TrimSpace(c.Gateway.MetricsListenAddr) == "" {
		c.Gateway.MetricsListenAddr = ":9102"
	}
	if c.Scheduler.LeaderElection.Enabled {
		le := &c.Scheduler.LeaderElection
		if strings.TrimSpace(le.LeaseName) == "" {
			le.LeaseName = "agentenv-scheduler"
		}
		if strings.TrimSpace(le.LeaseNamespace) == "" {
			le.LeaseNamespace = "agentenv-system"
		}
		if le.LeaseDuration <= 0 {
			le.LeaseDuration = 15 * time.Second
		}
		if le.RenewDeadline <= 0 {
			le.RenewDeadline = 10 * time.Second
		}
		if le.RetryPeriod <= 0 {
			le.RetryPeriod = 2 * time.Second
		}
		if le.SnapshotPullConcurrency <= 0 {
			le.SnapshotPullConcurrency = 4
		}
	}
}

func (c Config) Validate() error {
	return c.validate(false)
}

func (c Config) validate(schedulerQueryOnly bool) error {
	if c.Service == "" {
		return errors.New("service is required")
	}
	if c.LogLevel == "" {
		return errors.New("log_level is required")
	}
	if c.LogFormat == "" {
		return errors.New("log_format is required")
	}
	switch strings.ToLower(c.LogFormat) {
	case "auto", "console", "json":
	default:
		return errors.New("log_format must be one of auto, console, json")
	}
	if c.Service == "scheduler" {
		if c.Scheduler.GRPCListenAddr == "" {
			return errors.New("scheduler.grpc_listen_addr is required")
		}
		if c.Scheduler.MetricsListenAddr == "" {
			return errors.New("scheduler.metrics_listen_addr is required")
		}
		if c.Scheduler.ReportTTL <= 0 {
			return errors.New("scheduler.report_ttl must be greater than zero")
		}
		if c.Scheduler.BindingTTL <= 0 {
			return errors.New("scheduler.binding_ttl must be greater than zero")
		}
		if c.Scheduler.LeaderElection.Enabled && schedulerQueryOnly {
			return errors.New("scheduler.leader_election is mutually exclusive with --query-only")
		}
		if schedulerQueryOnly {
			if strings.TrimSpace(c.Scheduler.RedisAddr) == "" {
				return errors.New("scheduler --query-only requires scheduler.redis_addr")
			}
			return nil
		}
		if c.Scheduler.ArtifactStoreCapacity <= 0 {
			return errors.New("scheduler.artifact_store_capacity must be greater than zero")
		}
		switch strings.ToLower(strings.TrimSpace(c.Scheduler.Discovery.Mode)) {
		case "static":
			if len(c.Scheduler.Nodes) == 0 {
				return errors.New("scheduler.nodes must not be empty")
			}
			for _, n := range c.Scheduler.Nodes {
				if n.ID == "" || n.Endpoint == "" {
					return errors.New("scheduler.nodes require id and endpoint")
				}
			}
		case "kubernetes":
			kube := c.Scheduler.Discovery.Kubernetes
			if strings.TrimSpace(kube.Namespace) == "" {
				return errors.New("scheduler.discovery.kubernetes.namespace is required")
			}
			if strings.TrimSpace(kube.ServiceName) == "" {
				return errors.New("scheduler.discovery.kubernetes.service_name is required")
			}
			if kube.Port <= 0 {
				return errors.New("scheduler.discovery.kubernetes.port must be greater than zero")
			}
			if strings.TrimSpace(kube.Scheme) == "" {
				return errors.New("scheduler.discovery.kubernetes.scheme is required")
			}
		default:
			return errors.New("scheduler.discovery.mode must be one of static, kubernetes")
		}
		if c.Scheduler.LeaderElection.Enabled {
			le := c.Scheduler.LeaderElection
			if strings.TrimSpace(le.LeaseName) == "" {
				return errors.New("scheduler.leader_election.lease_name is required")
			}
			if strings.TrimSpace(le.LeaseNamespace) == "" {
				return errors.New("scheduler.leader_election.lease_namespace is required")
			}
			// No-dual-leader timing invariant. client-go keeps an old
			// leader serving for up to retry_period + renew_deadline after
			// its last successful renew (wait.Until waits RetryPeriod, then
			// polls for up to RenewDeadline); a standby may acquire
			// lease_duration after observing that renew, and the Lease
			// record stores the duration in whole seconds. Expiry is
			// checked against each replica's own clock, so no synchronized
			// clocks are required (#341 maintainer review, yingdi-shan).
			if le.LeaseDuration.Truncate(time.Second) <= le.RenewDeadline+le.RetryPeriod {
				return errors.New("scheduler.leader_election requires lease_duration (whole seconds) > renew_deadline + retry_period")
			}
			if le.RenewDeadline <= 2*le.RetryPeriod {
				return errors.New("scheduler.leader_election requires renew_deadline > 2 * retry_period")
			}
		}
	}
	if c.Service == "gateway" {
		if err := c.Gateway.validateTLS(); err != nil {
			return err
		}
		if (c.Gateway.TLS.Mode == "" || c.Gateway.TLS.Mode == "off") && c.Gateway.HTTPListenAddr == "" {
			return errors.New("gateway.http_listen_addr is required")
		}
		if c.Gateway.MetricsListenAddr == "" {
			return errors.New("gateway.metrics_listen_addr is required")
		}
		if c.Gateway.SchedulerAddr == "" {
			return errors.New("gateway.scheduler_addr is required")
		}
		// Zero disables the limit in net/http, so it is rejected rather than
		// silently reopening slow-client connection exhaustion.
		if c.Gateway.ReadHeaderTimeout <= 0 {
			return errors.New("gateway.read_header_timeout must be greater than zero")
		}
		if c.Gateway.IdleTimeout <= 0 {
			return errors.New("gateway.idle_timeout must be greater than zero")
		}
	}
	return nil
}

func (g GatewayConfig) validateTLS() error {
	t := g.TLS
	switch t.Mode {
	case "", "off":
		return nil
	case "manual", "acme":
	default:
		return errors.New("gateway.tls.mode must be off, manual, or acme")
	}
	_, port, err := net.SplitHostPort(g.HTTPSListenAddr)
	n, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || n < 1 || n > 65535 {
		return errors.New("gateway.https_listen_addr must contain a valid TCP port")
	}
	if t.Mode == "manual" {
		if t.CertFile == "" || t.KeyFile == "" {
			return errors.New("gateway.tls manual mode requires cert_file and key_file")
		}
		return nil
	}
	if len(t.Domains) == 0 || strings.TrimSpace(t.StorageDir) == "" {
		return errors.New("gateway.tls acme mode requires domains and persistent storage_dir")
	}
	if t.CA != "" {
		u, err := url.Parse(t.CA)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
			return errors.New("gateway.tls.ca must be an HTTPS ACME directory URL")
		}
	}
	switch t.Challenge {
	case "", "tls-alpn-01":
		if t.DNSProvider != "" || len(t.DNSConfig) != 0 {
			return errors.New("gateway.tls.dns_provider and dns_config require challenge=dns-01")
		}
	case "dns-01":
		switch t.DNSProvider {
		case "cloudflare", "alidns", "tencentcloud", "route53":
		default:
			return errors.New("gateway.tls.dns_provider must be cloudflare, alidns, tencentcloud, or route53")
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(t.DNSConfig, &object); err != nil || object == nil {
			return errors.New("gateway.tls.dns_config must be a JSON object for dns-01")
		}
	default:
		return errors.New("gateway.tls.challenge must be tls-alpn-01 or dns-01")
	}
	ipCount := 0
	for _, name := range t.Domains {
		if ip, err := netip.ParseAddr(name); err == nil {
			ipCount++
			if ipCount > 1 {
				return errors.New("gateway.tls.domains supports one IP per listener (IP clients do not send SNI); DNS names may be combined with that IP")
			}
			if !publicCertificateIP(ip) {
				return fmt.Errorf("gateway.tls.domains contains non-public or special-purpose IP %q; use manual mode for local certificates", name)
			}
			if t.Challenge == "dns-01" {
				return errors.New("gateway.tls IP certificates require tls-alpn-01; DNS-01 cannot validate IP addresses")
			}
			continue
		}
		domain := strings.TrimPrefix(name, "*.")
		if !validCertificateDomain(domain) {
			return fmt.Errorf("gateway.tls.domains contains invalid DNS name %q (use manual mode for local IP certificates)", name)
		}
		if strings.HasPrefix(name, "*.") && t.Challenge != "dns-01" {
			return errors.New("gateway.tls wildcard domains require challenge=dns-01")
		}
	}
	return nil
}

func validCertificateDomain(name string) bool {
	if len(name) > 253 || !strings.Contains(name, ".") || net.ParseIP(name) != nil {
		return false
	}
	for _, label := range strings.Split(name, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
	}
	return true
}

// Conservative public-IP policy: exclude special-purpose ranges, including
// documentation, benchmarking and transition addresses.
// Reference: IANA IPv4/IPv6 Special-Purpose Address Registries (2025-10-09).
func publicCertificateIP(ip netip.Addr) bool {
	if ip.Zone() != "" || ip.Is4In6() || !ip.IsGlobalUnicast() || ip.IsPrivate() {
		return false
	}
	blocked := []string{
		"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24",
		"192.88.99.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4",
		"2001::/23", "2001:db8::/32", "2002::/16", "3fff::/20",
	}
	if ip.Is6() && !netip.MustParsePrefix("2000::/3").Contains(ip) {
		return false
	}
	for _, prefix := range blocked {
		if netip.MustParsePrefix(prefix).Contains(ip) {
			return false
		}
	}
	return true
}
