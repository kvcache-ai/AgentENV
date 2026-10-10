package main

import (
	"testing"
	"time"

	"agentenv/services/shared/config"
)

// Binding TTL floor tests (#259): under leader election the effective TTL
// must cover the failover budget (lease_duration + 90s); without election the
// configured value passes through unchanged.

func baseCfg(bindingTTL time.Duration) config.Config {
	return config.Config{
		Scheduler: config.SchedulerConfig{BindingTTL: bindingTTL},
	}
}

func TestEffectiveBindingTTLWithoutElection(t *testing.T) {
	cfg := baseCfg(30 * time.Second)
	if got := effectiveBindingTTL(cfg); got != 30*time.Second {
		t.Fatalf("election off: want configured TTL 30s, got %v", got)
	}
}

func TestEffectiveBindingTTLWithElectionFloors(t *testing.T) {
	cfg := baseCfg(30 * time.Second)
	cfg.Scheduler.LeaderElection = config.SchedulerLeaderElectionConfig{
		Enabled:       true,
		LeaseDuration: 15 * time.Second,
	}
	// floor = 15s + 90s = 105s > 30s
	if got := effectiveBindingTTL(cfg); got != 105*time.Second {
		t.Fatalf("election on: want floor 105s, got %v", got)
	}
}

func TestEffectiveBindingTTLWithElectionKeepsLargerConfigured(t *testing.T) {
	cfg := baseCfg(5 * time.Minute)
	cfg.Scheduler.LeaderElection = config.SchedulerLeaderElectionConfig{
		Enabled:       true,
		LeaseDuration: 15 * time.Second,
	}
	// floor = 105s < 5m: the larger configured value wins.
	if got := effectiveBindingTTL(cfg); got != 5*time.Minute {
		t.Fatalf("election on: want configured 5m (larger than floor), got %v", got)
	}
}
