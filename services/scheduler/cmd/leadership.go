package main

import (
	"context"
	"strings"
	"sync/atomic"

	"agentenv/services/shared/config"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type leadershipGate struct {
	leader atomic.Bool
}

func newLeadershipGate(initialLeader bool) *leadershipGate {
	gate := &leadershipGate{}
	gate.leader.Store(initialLeader)
	return gate
}

func (g *leadershipGate) setLeader(leader bool) {
	g.leader.Store(leader)
}

func (g *leadershipGate) unaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if strings.HasPrefix(info.FullMethod, "/grpc.health.v1.Health/") || g.leader.Load() {
			return handler(ctx, req)
		}
		return nil, status.Error(codes.Unavailable, "scheduler replica is not leader")
	}
}

func runLeaderElection(
	ctx context.Context,
	logger *zap.Logger,
	client kubernetes.Interface,
	cfg config.SchedulerLeaderElectionConfig,
	onStartedLeading func(context.Context),
	onStoppedLeading func(),
) {
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      cfg.LeaseName,
			Namespace: cfg.LeaseNamespace,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: cfg.Identity,
		},
	}

	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   cfg.LeaseDuration,
		RenewDeadline:   cfg.RenewDeadline,
		RetryPeriod:     cfg.RetryPeriod,
		ReleaseOnCancel: true,
		Name:            "agentenv-scheduler",
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: onStartedLeading,
			OnStoppedLeading: onStoppedLeading,
			OnNewLeader: func(identity string) {
				if identity == cfg.Identity {
					logger.Info("scheduler leadership acquired", zap.String("identity", identity))
					return
				}
				logger.Info("scheduler leader observed", zap.String("identity", identity))
			},
		},
	})
}
