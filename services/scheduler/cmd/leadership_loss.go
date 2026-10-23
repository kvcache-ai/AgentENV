package main

import (
	"context"
	"sync/atomic"
)

// makeLeadershipLossStop builds the OnStoppedLeading hook. client-go fires
// OnStoppedLeading on every Run exit — both on an unexpected lease loss and
// on normal SIGTERM-driven shutdown. Only the former is a loss: when the
// process context is already cancelled, the graceful drain must run instead
// (#341 maintainer review, issue ②).
func makeLeadershipLossStop(sigCtx context.Context, leadershipLost *atomic.Bool, rootCancel context.CancelFunc) func() {
	return func() {
		if sigCtx.Err() != nil {
			return
		}
		leadershipLost.Store(true)
		rootCancel()
	}
}
