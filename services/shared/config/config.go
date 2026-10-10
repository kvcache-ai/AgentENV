package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultSchedulerArtifactStoreCapacity = 1_000_000

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
	// API (x-api-key header) for sync-node-snapshots pulls (#259). It is a
	// credential: prefer the SCHEDULER_NODE_ADMIN_API_KEY env (mounted from
	// the agentenv-auth Secret) over this JSON field. Optional; without it,
	// pulls fail auth and nodes stay unobserved until heartbeats.
	NodeAdminAPIKey         string                        `json:"node_admin_api_key"`
	ArtifactStoreCapacity   int                           `json:"artifact_store_capacity"`
	ArtifactLookupNodeLimit int                           `json:"artifact_lookup_node_limit"`
	Nodes                   []Node                        `json:"nodes"`
	Discovery               SchedulerDiscoveryConfig      `json:"discovery"`
	NodeResourceLimit       *NodeResourceLimit            `json:"node_resource_limit"`
	LeaderElection          SchedulerLeaderElectionConfig `json:"leader_election"`
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

type GatewayConfig struct {
	HTTPListenAddr         string        `json:"http_listen_addr"`
	MetricsListenAddr      string        `json:"metrics_listen_addr"`
	SchedulerAddr          string        `json:"scheduler_addr"`
	QueryOnlySchedulerAddr string        `json:"query_only_scheduler_addr"`
	RequestTimeout         time.Duration `json:"request_timeout"`
	ForwardResponseSize    int64         `json:"forward_response_size"`
	SandboxProxyDomains    []string      `json:"sandbox_proxy_domains"`
	// DebugMode enables debug-only behaviors in the gateway such as exposing
	// the backend node id on proxied responses. It is off by default.
	DebugMode bool `json:"debug_mode"`
}

func (g *GatewayConfig) UnmarshalJSON(data []byte) error {
	type wire struct {
		HTTPListenAddr         *string         `json:"http_listen_addr"`
		MetricsListenAddr      *string         `json:"metrics_listen_addr"`
		SchedulerAddr          *string         `json:"scheduler_addr"`
		QueryOnlySchedulerAddr *string         `json:"query_only_scheduler_addr"`
		RequestTimeout         json.RawMessage `json:"request_timeout"`
		ForwardResponseSize    *int64          `json:"forward_response_size"`
		SandboxProxyDomains    *[]string       `json:"sandbox_proxy_domains"`
		DebugMode              *bool           `json:"debug_mode"`
	}

	parsed := wire{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
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
			HTTPListenAddr:      ":8080",
			MetricsListenAddr:   ":9102",
			SchedulerAddr:       "127.0.0.1:9090",
			RequestTimeout:      30 * time.Second,
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
			// client-go leaderelection timing constraints.
			if le.LeaseDuration <= le.RenewDeadline {
				return errors.New("scheduler.leader_election requires lease_duration > renew_deadline")
			}
			if le.RenewDeadline <= 2*le.RetryPeriod {
				return errors.New("scheduler.leader_election requires renew_deadline > 2 * retry_period")
			}
		}
	}
	if c.Service == "gateway" {
		if c.Gateway.HTTPListenAddr == "" {
			return errors.New("gateway.http_listen_addr is required")
		}
		if c.Gateway.MetricsListenAddr == "" {
			return errors.New("gateway.metrics_listen_addr is required")
		}
		if c.Gateway.SchedulerAddr == "" {
			return errors.New("gateway.scheduler_addr is required")
		}
	}
	return nil
}
