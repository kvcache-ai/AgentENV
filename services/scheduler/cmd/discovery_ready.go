package main

// discoveryReadyFor returns the ready-channel wiring for kubernetes
// discovery. A channel exists only when startup waits for the first sync
// (leader election on kubernetes discovery): discovery closes it once its
// initial sync completes. Every other mode must hand discovery no channel
// at all — discovery owns the close, and a caller-side close panics as
// "close of closed channel" once the post-sync close runs (#341 review,
// guozy18).
func discoveryReadyFor(waitForDiscoverySync bool) (chan struct{}, []chan<- struct{}) {
	if !waitForDiscoverySync {
		return nil, nil
	}
	ready := make(chan struct{})
	return ready, []chan<- struct{}{ready}
}
