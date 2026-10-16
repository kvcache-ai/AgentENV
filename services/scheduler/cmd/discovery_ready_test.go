package main

import "testing"

// guozy18's review (#341): when leader election is disabled, main pre-closed
// discoveryReady and still handed it to kubernetes discovery, whose post-sync
// close then panicked (close of closed channel) — a crash on every non-HA
// single-scheduler deployment. The ready channel must exist only when
// startup actually waits for the first sync, and discovery alone closes it.
func TestDiscoveryReadyChannelsOnlyWhenWaiting(t *testing.T) {
	ready, chans := discoveryReadyFor(false)
	if ready != nil {
		t.Fatal("no wait, no channel: a caller-side channel invites the double-close panic")
	}
	if len(chans) != 0 {
		t.Fatal("discovery must receive no ready channel when startup does not wait")
	}

	ready, chans = discoveryReadyFor(true)
	if ready == nil || len(chans) != 1 {
		t.Fatal("election-gated startup must hand exactly one ready channel to discovery")
	}
}
