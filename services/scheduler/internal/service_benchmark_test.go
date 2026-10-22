package scheduler

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func benchmarkScheduleService(nodeCount int) *Service {
	nodes := make([]Node, nodeCount)
	for i := range nodes {
		nodes[i] = Node{
			ID:       fmt.Sprintf("node-%02d", i),
			Endpoint: fmt.Sprintf("http://node-%02d", i),
		}
	}
	return NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry(nodes, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)
}

type scheduleCall func(context.Context, *schedulerv1.ScheduleRequest) (*schedulerv1.ScheduleResponse, error)

// Keep worker setup outside the timer while counting exactly b.N calls.
func benchmarkScheduleCalls(b *testing.B, workers int, call scheduleCall) {
	b.Helper()
	ctx := context.Background()
	req := &schedulerv1.ScheduleRequest{}
	if resp, err := call(ctx, req); err != nil || resp.GetNode().GetNodeId() == "" {
		b.Fatalf("schedule warmup failed: response=%v, error=%v", resp, err)
	}

	workerCount := min(workers, b.N)
	start := make(chan struct{})
	errs := make(chan error, 1)
	var group sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		count := b.N / workerCount
		if i < b.N%workerCount {
			count++
		}
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			for range count {
				resp, err := call(ctx, req)
				if err == nil && resp.GetNode().GetNodeId() == "" {
					err = fmt.Errorf("schedule returned no node")
				}
				if err != nil {
					select {
					case errs <- err:
					default:
					}
					return
				}
			}
		}()
	}

	b.ReportAllocs()
	b.ResetTimer()
	close(start)
	group.Wait()
	b.StopTimer()
	select {
	case err := <-errs:
		b.Fatalf("schedule call failed: %v", err)
	default:
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "schedule/s")
}

func BenchmarkScheduleService(b *testing.B) {
	for _, nodes := range []int{1, 32} {
		b.Run(fmt.Sprintf("nodes=%d", nodes), func(b *testing.B) {
			service := benchmarkScheduleService(nodes)
			for _, workers := range []int{1, 2, 4, 8} {
				b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
					benchmarkScheduleCalls(b, workers, service.Schedule)
				})
			}
		})
	}
}

func BenchmarkScheduleGRPC(b *testing.B) {
	for _, nodes := range []int{1, 32} {
		b.Run(fmt.Sprintf("nodes=%d", nodes), func(b *testing.B) {
			service := benchmarkScheduleService(nodes)
			// bufconn exercises the real gRPC handler without a TCP listener.
			listener := bufconn.Listen(1 << 20)
			server := grpc.NewServer()
			schedulerv1.RegisterSchedulerServer(server, service)
			go func() {
				_ = server.Serve(listener)
			}()
			b.Cleanup(func() {
				server.Stop()
				_ = listener.Close()
			})

			conn, err := grpc.NewClient(
				"passthrough:///bufnet",
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					return listener.DialContext(ctx)
				}),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				b.Fatalf("create in-memory gRPC client: %v", err)
			}
			b.Cleanup(func() { _ = conn.Close() })
			client := schedulerv1.NewSchedulerClient(conn)

			for _, workers := range []int{1, 2, 4, 8} {
				b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
					benchmarkScheduleCalls(b, workers, func(ctx context.Context, req *schedulerv1.ScheduleRequest) (*schedulerv1.ScheduleResponse, error) {
						return client.Schedule(ctx, req)
					})
				})
			}
		})
	}
}
