package main

import (
	"context"
	"testing"
	"time"

	scheduler "agentenv/services/scheduler/internal"
	"agentenv/services/shared/config"

	"go.uber.org/zap"
)

func TestDiscoveryRetryMarksUnhealthyBeforeConstruction(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var statuses []bool
	runKubernetesDiscoveryWithRetry(
		ctx,
		zap.NewNop(),
		config.SchedulerDiscoveryKubernetesConfig{},
		scheduler.NewAtomicNodeRegistry(nil, time.Second),
		nil,
		func(serving bool) {
			statuses = append(statuses, serving)
		},
	)
	if len(statuses) == 0 {
		t.Fatal("constructor failure left the overall health status unchanged")
	}
	if statuses[0] {
		t.Fatal("constructor failure initially reported discovery as healthy")
	}
}
