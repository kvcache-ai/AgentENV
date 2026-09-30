package scheduler

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"math/rand"
	"strings"
	"sync/atomic"

	schedulerv1 "agentenv/services/api/proto"
)

var ErrNoNodes = errors.New("no nodes available")

const reservedRuntimeMetadataKey = "yarl_reserved_runtime_id"

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

type WeightedImageAffinityStrategy struct {
	weights    map[string]float64
	fallbackRR RoundRobinStrategy
}

func NewWeightedImageAffinityStrategy(weights map[string]float64) *WeightedImageAffinityStrategy {
	owned := make(map[string]float64, len(weights))
	for id, weight := range weights {
		owned[strings.TrimSpace(id)] = weight
	}
	return &WeightedImageAffinityStrategy{weights: owned}
}

func (s *WeightedImageAffinityStrategy) Select(nodes []RichNode, hint *schedulerv1.ScheduleRequestHint) (RichNode, error) {
	if len(nodes) == 0 {
		return RichNode{}, ErrNoNodes
	}
	if runtimeID := reservedRuntimeID(hint); runtimeID != "" {
		for _, node := range nodes {
			if node.ID == runtimeID {
				return node, nil
			}
		}
		return RichNode{}, ErrNoNodes
	}
	key := primaryImageKey(hint)
	if key == "" {
		return s.fallbackRR.Select(nodes, hint)
	}

	bestIndex := -1
	bestScore := math.Inf(1)
	bestAffinityID := ""
	for i, node := range nodes {
		affinityID := node.AffinityID
		if affinityID == "" {
			affinityID = node.ID
		}
		weight, configured := s.weights[affinityID]
		if len(s.weights) > 0 && (!configured || weight <= 0) {
			continue
		}
		if weight <= 0 {
			weight = 1
		}
		score := rendezvousScore(key, affinityID, weight)
		if score < bestScore || (score == bestScore && affinityID < bestAffinityID) {
			bestIndex = i
			bestScore = score
			bestAffinityID = affinityID
		}
	}
	if bestIndex < 0 {
		return s.fallbackRR.Select(nodes, hint)
	}
	return nodes[bestIndex], nil
}

func reservedRuntimeID(hint *schedulerv1.ScheduleRequestHint) string {
	cold := hint.GetNewColdSandbox()
	if cold == nil {
		return ""
	}
	return strings.TrimSpace(cold.GetMetadata()[reservedRuntimeMetadataKey])
}

func (s *WeightedImageAffinityStrategy) Name() string {
	return "weighted_image_affinity"
}

func rendezvousScore(key string, affinityID string, weight float64) float64 {
	digest := sha256.Sum256([]byte(key + "\x00" + affinityID))
	// Keep the high 53 bits so the integer-to-float conversion is exact.
	mantissa := binary.BigEndian.Uint64(digest[:8]) >> 11
	u := float64(mantissa+1) / float64((uint64(1)<<53)+1)
	return -math.Log(u) / weight
}

func primaryImageKey(hint *schedulerv1.ScheduleRequestHint) string {
	cold := hint.GetNewColdSandbox()
	if cold == nil {
		return ""
	}
	for _, raw := range cold.GetImages() {
		image := strings.TrimSpace(raw)
		if image == "" {
			continue
		}
		if at := strings.LastIndex(image, "@sha256:"); at >= 0 {
			hexDigest := image[at+len("@sha256:"):]
			if len(hexDigest) == 64 && isHex(hexDigest) {
				return "sha256:" + strings.ToLower(hexDigest)
			}
		}
		return image
	}
	return ""
}

func isHex(value string) bool {
	for _, char := range value {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') || (char >= 'A' && char <= 'F')) {
			return false
		}
	}
	return true
}

func NewStrategy(name string, imageAffinityWeights ...map[string]float64) Strategy {
	switch name {
	case "random":
		return NewRandomStrategy()
	case "weighted_image_affinity":
		var weights map[string]float64
		if len(imageAffinityWeights) > 0 {
			weights = imageAffinityWeights[0]
		}
		return NewWeightedImageAffinityStrategy(weights)
	case "round_robin":
		fallthrough
	default:
		return &RoundRobinStrategy{}
	}
}
