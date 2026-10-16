package config

import (
	"testing"
	"time"
)

// Binding TTL floor tests (#259): under leader election the effective TTL
// must cover the failover budget (lease_duration + 90s); without election the
// configured value passes through unchanged.

func TestEffectiveBindingTTLWithoutElection(t *testing.T) {
	cfg := SchedulerConfig{BindingTTL: 30 * time.Second}
	if got := cfg.EffectiveBindingTTL(); got != 30*time.Second {
		t.Fatalf("election off: want configured TTL 30s, got %v", got)
	}
}

func TestEffectiveBindingTTLWithElectionFloors(t *testing.T) {
	cfg := SchedulerConfig{
		BindingTTL: 30 * time.Second,
		LeaderElection: SchedulerLeaderElectionConfig{
			Enabled:       true,
			LeaseDuration: 15 * time.Second,
		},
	}
	// floor = 15s + 90s = 105s > 30s
	if got := cfg.EffectiveBindingTTL(); got != 105*time.Second {
		t.Fatalf("election on: want floor 105s, got %v", got)
	}
}

func TestEffectiveBindingTTLWithElectionKeepsLargerConfigured(t *testing.T) {
	cfg := SchedulerConfig{
		BindingTTL: 5 * time.Minute,
		LeaderElection: SchedulerLeaderElectionConfig{
			Enabled:       true,
			LeaseDuration: 15 * time.Second,
		},
	}
	// floor = 105s < 5m: the larger configured value wins.
	if got := cfg.EffectiveBindingTTL(); got != 5*time.Minute {
		t.Fatalf("election on: want configured 5m (larger than floor), got %v", got)
	}
}
