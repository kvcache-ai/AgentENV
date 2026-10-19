package scheduler

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

var ErrNoNodes = errors.New("no nodes available")

type Strategy interface {
	Select(nodes []RichNode, hint *schedulerv1.ScheduleRequestHint) (RichNode, error)
	Name() string
}

const (
	// roundRobinEntryExpiry is how long a node's last-picked timestamp is
	// remembered. Entries idle longer than this are swept, so a node that
	// returns after a long absence is treated as never picked.
	roundRobinEntryExpiry = 24 * time.Hour
	// roundRobinExpirySweepInterval throttles expiry sweeps to at most one
	// per interval; steady-state Select calls only scan the candidates.
	roundRobinExpirySweepInterval = time.Minute
)

// RoundRobinStrategy picks the least-recently-picked candidate node. Because
// selection is keyed by node identity rather than slice index, it stays fair
// when the candidate set changes between calls (rate limits, filters): a
// stable set yields a classic round-robin cycle, and nodes that were
// temporarily filtered out resume instead of being skipped or double-picked.
type RoundRobinStrategy struct {
	mu        sync.Mutex
	entries   map[string]time.Time // nodeID -> last picked; absent = never picked
	lastSweep time.Time
	now       func() time.Time // injectable for tests
}

func (s *RoundRobinStrategy) Select(nodes []RichNode, _ *schedulerv1.ScheduleRequestHint) (RichNode, error) {
	if len(nodes) == 0 {
		return RichNode{}, ErrNoNodes
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.now == nil {
		s.now = time.Now
	}
	if s.entries == nil {
		s.entries = make(map[string]time.Time)
	}

	now := s.now()
	if now.Sub(s.lastSweep) >= roundRobinExpirySweepInterval {
		for id, lastPicked := range s.entries {
			if now.Sub(lastPicked) > roundRobinEntryExpiry {
				delete(s.entries, id)
			}
		}
		s.lastSweep = now
	}

	// Pick the candidate with the oldest last-picked timestamp. A node with
	// no entry reads as the zero time, so never-picked nodes win; ties break
	// on node ID, making the result deterministic regardless of input order.
	best := nodes[0]
	bestLastPicked := s.entries[best.ID]
	for _, n := range nodes[1:] {
		lastPicked := s.entries[n.ID]
		if lastPicked.Before(bestLastPicked) ||
			lastPicked.Equal(bestLastPicked) && n.ID < best.ID {
			best, bestLastPicked = n, lastPicked
		}
	}
	s.entries[best.ID] = now
	return best, nil
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

func NewStrategy(name string) Strategy {
	switch name {
	case "random":
		return NewRandomStrategy()
	case "round_robin":
		fallthrough
	default:
		return &RoundRobinStrategy{}
	}
}
