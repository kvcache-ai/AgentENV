package main

import (
	"context"
	"sync/atomic"
	"testing"
)

// Issue ② (#341 maintainer review): client-go fires OnStoppedLeading on
// every Run exit — including normal SIGTERM shutdown. The loss callback
// must not mark leadershipLost on the normal path, or the graceful drain
// never runs.

func TestLeadershipLossStopMarksUnexpectedLoss(t *testing.T) {
	sigCtx, sigStop := context.WithCancel(context.Background())
	defer sigStop()
	var lost atomic.Bool
	cancelled := false

	stop := makeLeadershipLossStop(sigCtx, &lost, func() { cancelled = true })
	stop()

	if !lost.Load() {
		t.Fatal("an unexpected leadership loss (ctx still live) must mark leadershipLost")
	}
	if !cancelled {
		t.Fatal("an unexpected leadership loss must trigger process shutdown")
	}
}

func TestLeadershipLossStopIgnoresNormalShutdown(t *testing.T) {
	sigCtx, sigStop := context.WithCancel(context.Background())
	sigStop() // SIGTERM already arrived: this is the normal shutdown path
	var lost atomic.Bool
	cancelled := false

	stop := makeLeadershipLossStop(sigCtx, &lost, func() { cancelled = true })
	stop()

	if lost.Load() {
		t.Fatal("OnStoppedLeading during normal shutdown must NOT mark leadershipLost — the graceful drain must run")
	}
	if cancelled {
		t.Fatal("normal shutdown path must not re-cancel")
	}
}
