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

type SchedulerFleetConfig struct {
	Enabled              bool          `json:"enabled"`
	MinNodes             uint32        `json:"min_nodes"`
	MaxNodes             uint32        `json:"max_nodes"`
	WarmNodes            uint32        `json:"warm_nodes"`
	MaxSandboxesPerNode  uint32        `json:"max_sandboxes_per_node"`
	MaxMemoryUsedPercent uint32        `json:"max_memory_used_percent"`
	EmptyGrace           time.Duration `json:"empty_grace"`
	DrainGrace           time.Duration `json:"drain_grace"`
	DemandTTL            time.Duration `json:"demand_ttl"`
}

func (f *SchedulerFleetConfig) UnmarshalJSON(data []byte) error {
	type wire struct {
		Enabled              *bool           `json:"enabled"`
		MinNodes             *uint32         `json:"min_nodes"`
		MaxNodes             *uint32         `json:"max_nodes"`
		WarmNodes            *uint32         `json:"warm_nodes"`
		MaxSandboxesPerNode  *uint32         `json:"max_sandboxes_per_node"`
		MaxMemoryUsedPercent *uint32         `json:"max_memory_used_percent"`
		EmptyGrace           json.RawMessage `json:"empty_grace"`
		DrainGrace           json.RawMessage `json:"drain_grace"`
		DemandTTL            json.RawMessage `json:"demand_ttl"`
	}
	parsed := wire{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}
	if parsed.Enabled != nil {
		f.Enabled = *parsed.Enabled
	}
	if parsed.MinNodes != nil {
		f.MinNodes = *parsed.MinNodes
	}
	if parsed.MaxNodes != nil {
		f.MaxNodes = *parsed.MaxNodes
	}
	if parsed.WarmNodes != nil {
		f.WarmNodes = *parsed.WarmNodes
	}
	if parsed.MaxSandboxesPerNode != nil {
		f.MaxSandboxesPerNode = *parsed.MaxSandboxesPerNode
	}
	if parsed.MaxMemoryUsedPercent != nil {
		f.MaxMemoryUsedPercent = *parsed.MaxMemoryUsedPercent
	}
	for field, rawTarget := range map[string]struct {
		raw    json.RawMessage
		target *time.Duration
	}{
		"scheduler.fleet.empty_grace": {parsed.EmptyGrace, &f.EmptyGrace},
		"scheduler.fleet.drain_grace": {parsed.DrainGrace, &f.DrainGrace},
		"scheduler.fleet.demand_ttl":  {parsed.DemandTTL, &f.DemandTTL},
	} {
		if len(bytes.TrimSpace(rawTarget.raw)) == 0 {
			continue
		}
		d, err := parseSchedulerDuration(rawTarget.raw, field)
		if err != nil {
			return err
		}
		*rawTarget.target = d
	}
	return nil
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

type SchedulerConfig struct {
	GRPCListenAddr             string                   `json:"grpc_listen_addr"`
	MetricsListenAddr          string                   `json:"metrics_listen_addr"`
	Strategy                   string                   `json:"strategy"`
	ReportTTL                  time.Duration            `json:"report_ttl"`
	BindingTTL                 time.Duration            `json:"binding_ttl"`
	RedisAddr                  string                   `json:"redis_addr"`
	ArtifactStoreCapacity      int                      `json:"artifact_store_capacity"`
	ArtifactLookupNodeLimit    int                      `json:"artifact_lookup_node_limit"`
	Nodes                      []Node                   `json:"nodes"`
	Discovery                  SchedulerDiscoveryConfig `json:"discovery"`
	NodeResourceLimit          *NodeResourceLimit       `json:"node_resource_limit"`
	Fleet                      SchedulerFleetConfig     `json:"fleet"`
	HeartbeatRegistrationToken string                   `json:"-"`
}

func (s *SchedulerConfig) UnmarshalJSON(data []byte) error {
	type wire struct {
		GRPCListenAddr          *string                   `json:"grpc_listen_addr"`
		MetricsListenAddr       *string                   `json:"metrics_listen_addr"`
		Strategy                *string                   `json:"strategy"`
		ReportTTL               json.RawMessage           `json:"report_ttl"`
		BindingTTL              json.RawMessage           `json:"binding_ttl"`
		RedisAddr               *string                   `json:"redis_addr"`
		ArtifactStoreCapacity   *int                      `json:"artifact_store_capacity"`
		ArtifactLookupNodeLimit *int                      `json:"artifact_lookup_node_limit"`
		Nodes                   *[]Node                   `json:"nodes"`
		Discovery               *SchedulerDiscoveryConfig `json:"discovery"`
		NodeResourceLimit       *NodeResourceLimit        `json:"node_resource_limit"`
		Fleet                   *SchedulerFleetConfig     `json:"fleet"`
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
	if parsed.Fleet != nil {
		s.Fleet = *parsed.Fleet
	}
	if parsed.RedisAddr != nil {
		s.RedisAddr = *parsed.RedisAddr
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

	return nil
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
			Fleet: SchedulerFleetConfig{
				MinNodes: 1, MaxNodes: 250, WarmNodes: 1,
				MaxSandboxesPerNode: 24, MaxMemoryUsedPercent: 85,
				EmptyGrace: 15 * time.Minute, DrainGrace: 15 * time.Minute,
				DemandTTL: 2 * time.Minute,
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
	set("SCHEDULER_HEARTBEAT_REGISTRATION_TOKEN", &cfg.Scheduler.HeartbeatRegistrationToken)
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
	if c.Scheduler.Fleet.MaxNodes == 0 {
		c.Scheduler.Fleet.MaxNodes = 250
	}
	if c.Scheduler.Fleet.MaxSandboxesPerNode == 0 {
		c.Scheduler.Fleet.MaxSandboxesPerNode = 24
	}
	if c.Scheduler.Fleet.MaxMemoryUsedPercent == 0 {
		c.Scheduler.Fleet.MaxMemoryUsedPercent = 85
	}
	if c.Scheduler.Fleet.EmptyGrace <= 0 {
		c.Scheduler.Fleet.EmptyGrace = 15 * time.Minute
	}
	if c.Scheduler.Fleet.DrainGrace <= 0 {
		c.Scheduler.Fleet.DrainGrace = 15 * time.Minute
	}
	if c.Scheduler.Fleet.DemandTTL <= 0 {
		c.Scheduler.Fleet.DemandTTL = 2 * time.Minute
	}
	if strings.TrimSpace(c.Gateway.MetricsListenAddr) == "" {
		c.Gateway.MetricsListenAddr = ":9102"
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
		case "heartbeat":
			if strings.TrimSpace(c.Scheduler.HeartbeatRegistrationToken) == "" {
				return errors.New("SCHEDULER_HEARTBEAT_REGISTRATION_TOKEN is required for heartbeat discovery")
			}
		default:
			return errors.New("scheduler.discovery.mode must be one of static, kubernetes, heartbeat")
		}
		if c.Scheduler.Fleet.Enabled {
			fleet := c.Scheduler.Fleet
			if fleet.MinNodes == 0 || fleet.MaxNodes == 0 || fleet.MinNodes > fleet.MaxNodes {
				return errors.New("scheduler.fleet requires positive min_nodes <= max_nodes")
			}
			if fleet.WarmNodes > fleet.MaxNodes {
				return errors.New("scheduler.fleet.warm_nodes must not exceed max_nodes")
			}
			if fleet.MaxSandboxesPerNode == 0 {
				return errors.New("scheduler.fleet.max_sandboxes_per_node must be greater than zero")
			}
			if fleet.MaxMemoryUsedPercent == 0 || fleet.MaxMemoryUsedPercent > 100 {
				return errors.New("scheduler.fleet.max_memory_used_percent must be between 1 and 100")
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
