package scheduler

import (
	"context"
	"testing"
	"time"

	"agentenv/services/shared/config"

	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// Issue ① wiring (#341, found by e2e T4 in run 38501272727): the holder
// re-verification only works if Run actually starts the watch loop — the
// method alone (and the unit test driving it by hand) leaves production
// with a gate that trusts the local flag forever. This test drives the
// loop the way Run must: once started, a holder flip closes the gate
// within the refresh cadence, without waiting for renew_deadline.
func TestHolderWatchClosesDualLeaderWindow(t *testing.T) {
	clientset := fake.NewSimpleClientset(newLeaseFixture("pod-a"))
	m := newLeadershipManager(nil, config.SchedulerConfig{
		ReportTTL: 30 * time.Second,
		LeaderElection: config.SchedulerLeaderElectionConfig{
			Enabled:        true,
			LeaseName:      "agentenv-scheduler",
			LeaseNamespace: "default",
		},
	})
	m.clientset = clientset
	m.identity = "pod-a"
	snap := acquiredLeadershipSnapshot(30*time.Second, time.Now())
	snap.holderID = "pod-a"
	m.state.Store(snap)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.watchHolder(ctx)

	// Self-holding stays serving across refresh ticks.
	time.Sleep(1200 * time.Millisecond)
	if !m.isServingLeader() {
		t.Fatal("sanity: while the Lease names this replica, the gate must stay open")
	}

	// Another replica takes the Lease; the local flag still says leader.
	lease := newLeaseFixture("pod-b")
	if err := clientset.Tracker().Update(coordinationv1.SchemeGroupVersion.WithResource("leases"), lease, "default"); err != nil {
		t.Fatalf("flip lease holder: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !m.isServingLeader() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("gate stayed open after the Lease holder flipped — the holder watch is not closing the dual-leader window")
}

// The watch must leave a healthy self-holding leader serving indefinitely.
func TestHolderWatchKeepsSelfLeaderServing(t *testing.T) {
	clientset := fake.NewSimpleClientset(newLeaseFixture("pod-a"))
	m := newLeadershipManager(nil, config.SchedulerConfig{
		ReportTTL: 30 * time.Second,
		LeaderElection: config.SchedulerLeaderElectionConfig{
			Enabled:        true,
			LeaseName:      "agentenv-scheduler",
			LeaseNamespace: "default",
		},
	})
	m.clientset = clientset
	m.identity = "pod-a"
	snap := acquiredLeadershipSnapshot(30*time.Second, time.Now())
	snap.holderID = "pod-a"
	m.state.Store(snap)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.watchHolder(ctx)

	deadline := time.Now().Add(2500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if !m.isServingLeader() {
			t.Fatal("gate closed while this replica still holds the Lease")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
