package config

import (
	"strings"
	"testing"
	"time"
)

// Leader-election config surface tests (#259, test list group A).

func schedulerConfigWithLeaderElection(le SchedulerLeaderElectionConfig) Config {
	return Config{
		Service:   "scheduler",
		LogLevel:  "info",
		LogFormat: "json",
		Scheduler: SchedulerConfig{
			GRPCListenAddr:        ":9090",
			MetricsListenAddr:     ":9091",
			ReportTTL:             30 * time.Second,
			BindingTTL:            30 * time.Second,
			ArtifactStoreCapacity: 1,
			Nodes:                 []Node{{ID: "n1", Endpoint: "http://n1:8080"}},
			Discovery:             SchedulerDiscoveryConfig{Mode: "static"},
			LeaderElection:        le,
		},
	}
}

func defaultLeaderElection() SchedulerLeaderElectionConfig {
	return SchedulerLeaderElectionConfig{
		Enabled:        true,
		LeaseName:      "agentenv-scheduler",
		LeaseNamespace: "agentenv-system",
		LeaseDuration:  15 * time.Second,
		RenewDeadline:  10 * time.Second,
		RetryPeriod:    2 * time.Second,
	}
}

// Case A1: leader election on without redis_addr still passes validation.
// Redis is recommended for failover routing, but not a hard requirement;
// the degraded mode (lost routing for pre-failover sandboxes) is documented.
func TestLeaderElectionWithoutRedisPassesValidation(t *testing.T) {
	c := schedulerConfigWithLeaderElection(defaultLeaderElection())
	// Deliberately no RedisAddr.
	if err := c.validate(false); err != nil {
		t.Fatalf("leader election without redis_addr must pass validation, got %v", err)
	}
}

// Case A2: leader election is mutually exclusive with --query-only.
func TestLeaderElectionRejectsQueryOnly(t *testing.T) {
	c := schedulerConfigWithLeaderElection(defaultLeaderElection())
	err := c.validate(true)
	if err == nil {
		t.Fatal("leader election with --query-only must fail validation")
	}
	if !strings.Contains(err.Error(), "query-only") {
		t.Fatalf("error should mention the query-only conflict, got %v", err)
	}
}

// Case A3: invalid client-go timing triples are rejected.
// Rules: lease_duration > renew_deadline > 2 * retry_period.
func TestLeaderElectionRejectsInvalidTiming(t *testing.T) {
	cases := map[string]SchedulerLeaderElectionConfig{
		"renew_deadline_not_below_lease_duration": {
			Enabled: true, LeaseName: "l", LeaseNamespace: "ns",
			LeaseDuration: 10 * time.Second, RenewDeadline: 15 * time.Second, RetryPeriod: 2 * time.Second,
		},
		"retry_period_too_large": {
			Enabled: true, LeaseName: "l", LeaseNamespace: "ns",
			LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 6 * time.Second,
		},
		"missing_lease_name": {
			Enabled: true, LeaseNamespace: "ns",
			LeaseDuration: 15 * time.Second, RenewDeadline: 10 * time.Second, RetryPeriod: 2 * time.Second,
		},
	}
	for name, le := range cases {
		t.Run(name, func(t *testing.T) {
			if err := schedulerConfigWithLeaderElection(le).validate(false); err == nil {
				t.Fatalf("expected validation failure for %v", le)
			}
		})
	}
}

// Case A4: leader election off (default) behaves exactly like today —
// validation ignores leader-election settings entirely.
func TestLeaderElectionDisabledIsTransparent(t *testing.T) {
	c := schedulerConfigWithLeaderElection(SchedulerLeaderElectionConfig{
		Enabled:       false,
		LeaseDuration: 1 * time.Second, // invalid triple, must be ignored while disabled
		RenewDeadline: 10 * time.Second,
	})
	if err := c.validate(false); err != nil {
		t.Fatalf("disabled leader election must not affect validation, got %v", err)
	}
	if err := c.validate(true); err == nil || !strings.Contains(err.Error(), "redis_addr") {
		t.Fatalf("disabled leader election must keep existing query-only rules, got %v", err)
	}
}

// snapshot_pull_concurrency: defaults to 4 when enabled and unset;
// an explicit value is preserved.
func TestSnapshotPullConcurrencyDefault(t *testing.T) {
	c := schedulerConfigWithLeaderElection(defaultLeaderElection())
	c.applyDefaults()
	if got := c.Scheduler.LeaderElection.SnapshotPullConcurrency; got != 4 {
		t.Fatalf("unset snapshot_pull_concurrency must default to 4, got %d", got)
	}

	c = schedulerConfigWithLeaderElection(defaultLeaderElection())
	c.Scheduler.LeaderElection.SnapshotPullConcurrency = 16
	c.applyDefaults()
	if got := c.Scheduler.LeaderElection.SnapshotPullConcurrency; got != 16 {
		t.Fatalf("explicit snapshot_pull_concurrency must be preserved, got %d", got)
	}
}
