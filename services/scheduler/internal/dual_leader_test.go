package scheduler

import (
	"context"
	"testing"
	"time"

	"agentenv/services/shared/config"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// Issue ① (#341 maintainer review): the gate trusts only the local
// acquisition flag, which lags the real Lease holder by up to renew_deadline.
// After another replica takes the Lease, this replica must stop serving
// leader-gated RPCs as soon as the holder check notices — not only when
// OnStoppedLeading eventually fires.

func newLeaseFixture(holder string) *coordinationv1.Lease {
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{Name: "agentenv-scheduler", Namespace: "default"},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: &holder,
		},
	}
}

func TestGateStopsServingWhenLeaseHolderIsNotSelf(t *testing.T) {
	clientset := fake.NewSimpleClientset(newLeaseFixture("pod-a"))
	m := newLeadershipManager(nil, config.SchedulerConfig{
		ReportTTL: 30 * time.Second,
		LeaderElection: config.SchedulerLeaderElectionConfig{
			Enabled:        true,
			LeaseName:      "agentenv-scheduler",
			LeaseNamespace: "default",
		},
	})
	m.identity = "pod-a"
	m.clientset = clientset

	// This replica believes it just won; the first holder refresh confirms it
	// against the Lease (holder is pod-a).
	m.state.Store(acquiredLeadershipSnapshot(30*time.Second, time.Now()))
	m.refreshHolderOnce(context.Background())
	if !m.isServingLeader() {
		t.Fatal("sanity: after refresh confirms self-holding, the gate must serve")
	}

	// Another replica takes the Lease; the local flag still says leader for
	// up to renew_deadline (client-go has not noticed yet).
	lease := newLeaseFixture("pod-b")
	if err := clientset.Tracker().Update(coordinationv1.SchemeGroupVersion.WithResource("leases"), lease, "default"); err != nil {
		t.Fatalf("flip lease holder: %v", err)
	}
	m.refreshHolderOnce(context.Background())

	if m.isServingLeader() {
		t.Fatal("gate must stop serving once the Lease holder is another replica — local flag alone is not enough (dual-leader window)")
	}
}
