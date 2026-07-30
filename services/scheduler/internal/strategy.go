package scheduler

import (
	"errors"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

var ErrNoNodes = errors.New("no nodes available")

type Strategy interface {
	Select(nodes []RichNode, hint *schedulerv1.ScheduleRequestHint) (RichNode, error)
	Name() string
}

type RoundRobinStrategy struct {
	next uint64
}

func (s *RoundRobinStrategy) Select(nodes []RichNode, _ *schedulerv1.ScheduleRequestHint) (RichNode, error) {
	if len(nodes) == 0 {
		return RichNode{}, ErrNoNodes
	}
	idx := atomic.AddUint64(&s.next, 1)
	return nodes[(idx-1)%uint64(len(nodes))], nil
}

func (s *RoundRobinStrategy) Name() string {
	return "round_robin"
}

type RandomStrategy struct{}

func NewRandomStrategy() *RandomStrategy {
	return &RandomStrategy{}
}

func (s *RandomStrategy) Select(nodes []RichNode, _ *schedulerv1.ScheduleRequestHint) (RichNode, error) {
	if len(nodes) == 0 {
		return RichNode{}, ErrNoNodes
	}
	return nodes[rand.Intn(len(nodes))], nil
}

func (s *RandomStrategy) Name() string {
	return "random"
}

type strategyConfig struct {
	placementReservationTTL time.Duration
}

type StrategyOption func(*strategyConfig)

func WithPlacementReservationTTL(ttl time.Duration) StrategyOption {
	return func(cfg *strategyConfig) {
		if ttl > 0 {
			cfg.placementReservationTTL = ttl
		}
	}
}

func NewStrategy(name string, opts ...StrategyOption) Strategy {
	cfg := strategyConfig{placementReservationTTL: defaultScheduleReservationTTL}
	for _, opt := range opts {
		opt(&cfg)
	}
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "random":
		return NewRandomStrategy()
	case "least_loaded":
		return &LeastLoadedStrategy{reservationTTL: cfg.placementReservationTTL}
	case "round_robin":
		fallthrough
	default:
		return &RoundRobinStrategy{}
	}
}
