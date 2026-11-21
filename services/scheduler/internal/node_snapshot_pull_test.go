package scheduler

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
)

func TestPullNodeSnapshotsCancelsQueuedFetches(t *testing.T) {
	const concurrency = 2
	nodes := make([]Node, 12)
	started := make(chan struct{}, len(nodes))
	finished := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		defer close(finished)
		pullNodeSnapshots(ctx, zap.NewNop(), nodes, func(ctx context.Context, _ Node) (*schedulerv1.HeartbeatRequest, time.Time, error) {
			started <- struct{}{}
			<-ctx.Done()
			return nil, time.Time{}, ctx.Err()
		}, func(_ *schedulerv1.HeartbeatRequest, _, _ time.Time) error { return nil }, concurrency)
	}()

	deadline := time.After(3 * time.Second)
	for range concurrency {
		select {
		case <-started:
		case <-deadline:
			t.Fatal("snapshot fetches did not fill the available slots")
		}
	}
	cancel()
	select {
	case <-finished:
	case <-deadline:
		t.Fatal("snapshot pulls did not return after cancellation")
	}
	if got := concurrency + len(started); got != concurrency {
		t.Fatalf("fetch started for %d nodes; want only the %d already in flight", got, concurrency)
	}
}

func TestPullNodeSnapshotsDiscardsResponseAfterCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var ingested bool
	pullNodeSnapshots(ctx, zap.NewNop(), []Node{{ID: "node-a"}}, func(_ context.Context, node Node) (*schedulerv1.HeartbeatRequest, time.Time, error) {
		cancel()
		return &schedulerv1.HeartbeatRequest{NodeId: node.ID}, time.Now(), nil
	}, func(_ *schedulerv1.HeartbeatRequest, _, _ time.Time) error {
		ingested = true
		return nil
	}, 1)
	if ingested {
		t.Fatal("response from canceled recovery was ingested")
	}
}

func TestPullNodeSnapshotsContinuesAfterFetchError(t *testing.T) {
	nodes := []Node{{ID: "node-a"}, {ID: "node-b"}, {ID: "node-c"}, {ID: "node-d"}}
	var mu sync.Mutex
	var attempts, ingested []string
	pullNodeSnapshots(context.Background(), zap.NewNop(), nodes, func(_ context.Context, node Node) (*schedulerv1.HeartbeatRequest, time.Time, error) {
		mu.Lock()
		attempts = append(attempts, node.ID)
		mu.Unlock()
		if node.ID == "node-a" {
			return nil, time.Time{}, errors.New("node unavailable")
		}
		return &schedulerv1.HeartbeatRequest{NodeId: node.ID}, time.Now(), nil
	}, func(req *schedulerv1.HeartbeatRequest, _, _ time.Time) error {
		mu.Lock()
		defer mu.Unlock()
		ingested = append(ingested, req.GetNodeId())
		return nil
	}, 2)
	slices.Sort(attempts)
	slices.Sort(ingested)
	if !slices.Equal(attempts, []string{"node-a", "node-b", "node-c", "node-d"}) {
		t.Fatalf("attempts = %v; want one per node", attempts)
	}
	if !slices.Equal(ingested, []string{"node-b", "node-c", "node-d"}) {
		t.Fatalf("ingested = %v; want every successful node", ingested)
	}
}

func BenchmarkPullNodeSnapshotFanout(b *testing.B) {
	ctx := context.Background()
	logger := zap.NewNop()
	fetch := func(context.Context, Node) (*schedulerv1.HeartbeatRequest, time.Time, error) {
		return nil, time.Time{}, nil
	}
	ingest := func(*schedulerv1.HeartbeatRequest, time.Time, time.Time) error { return nil }
	for _, count := range []int{32, 256} {
		b.Run(fmt.Sprintf("nodes_%d", count), func(b *testing.B) {
			nodes := make([]Node, count)
			b.ReportAllocs()
			for b.Loop() {
				pullNodeSnapshots(ctx, logger, nodes, fetch, ingest, 4)
			}
		})
	}
}
