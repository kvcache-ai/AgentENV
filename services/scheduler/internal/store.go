package scheduler

import (
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

const defaultBindingTTL = 30 * time.Second
const defaultArtifactStoreCapacity = 1_000_000

type BindingStore interface {
	Get(sandboxID string, now time.Time) (Node, bool, error)
	Record(sandboxID string, node Node, now time.Time) error
	ReconcileNode(node Node, sandboxIDs []string, now time.Time) error
}

type ArtifactStore interface {
	Record(clusterID string, backend string, key string, nodeID string)
	Forget(clusterID string, backend string, key string, nodeID string)
	Lookup(clusterID string, backend string, key string) []string
	LookupAll(clusterID string, backend string, key string) []string
	ForgetNode(nodeID string)
}

type bindingRecord struct {
	node      Node
	expiresAt time.Time
}

type InMemoryBindingStore struct {
	mu          sync.Mutex
	bindingTTL  time.Duration
	bindings    map[string]bindingRecord
	nodeBinding map[string]map[string]struct{}
}

func NewInMemoryBindingStore(bindingTTL time.Duration) *InMemoryBindingStore {
	if bindingTTL <= 0 {
		bindingTTL = defaultBindingTTL
	}
	return &InMemoryBindingStore{
		bindingTTL:  bindingTTL,
		bindings:    make(map[string]bindingRecord),
		nodeBinding: make(map[string]map[string]struct{}),
	}
}

func (s *InMemoryBindingStore) Get(sandboxID string, now time.Time) (Node, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.bindings[sandboxID]
	if !ok {
		return Node{}, false, nil
	}
	if !record.expiresAt.After(now) {
		s.deleteLocked(sandboxID)
		return Node{}, false, nil
	}
	return record.node, true, nil
}

func (s *InMemoryBindingStore) Record(sandboxID string, node Node, now time.Time) error {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.upsertLocked(sandboxID, node, now)
	return nil
}

func (s *InMemoryBindingStore) ReconcileNode(node Node, sandboxIDs []string, now time.Time) error {
	normalized := make(map[string]struct{}, len(sandboxIDs))
	for _, sandboxID := range sandboxIDs {
		sandboxID = strings.TrimSpace(sandboxID)
		if sandboxID == "" {
			continue
		}
		normalized[sandboxID] = struct{}{}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(normalized) == 0 {
		if bindings, ok := s.nodeBinding[node.ID]; ok {
			for sandboxID := range bindings {
				s.deleteLocked(sandboxID)
			}
		}
		return nil
	}

	expiresAt := now.Add(s.bindingTTL)
	for sandboxID := range normalized {
		s.upsertLockedWithExpiry(sandboxID, node, expiresAt)
	}

	current := s.nodeBinding[node.ID]
	if len(current) == 0 {
		return nil
	}

	for sandboxID := range current {
		if _, ok := normalized[sandboxID]; ok {
			continue
		}
		s.deleteLocked(sandboxID)
	}
	return nil
}

func (s *InMemoryBindingStore) upsertLocked(sandboxID string, node Node, now time.Time) {
	s.upsertLockedWithExpiry(sandboxID, node, now.Add(s.bindingTTL))
}

func (s *InMemoryBindingStore) upsertLockedWithExpiry(sandboxID string, node Node, expiresAt time.Time) {
	if existing, ok := s.bindings[sandboxID]; ok && existing.node.ID != node.ID {
		s.removeNodeBindingLocked(existing.node.ID, sandboxID)
	}

	if _, ok := s.nodeBinding[node.ID]; !ok {
		s.nodeBinding[node.ID] = make(map[string]struct{})
	}
	s.nodeBinding[node.ID][sandboxID] = struct{}{}
	s.bindings[sandboxID] = bindingRecord{
		node:      node,
		expiresAt: expiresAt,
	}
}

func (s *InMemoryBindingStore) deleteLocked(sandboxID string) {
	record, ok := s.bindings[sandboxID]
	if !ok {
		return
	}
	delete(s.bindings, sandboxID)
	s.removeNodeBindingLocked(record.node.ID, sandboxID)
}

func (s *InMemoryBindingStore) removeNodeBindingLocked(nodeID string, sandboxID string) {
	bindings, ok := s.nodeBinding[nodeID]
	if !ok {
		return
	}
	delete(bindings, sandboxID)
	if len(bindings) == 0 {
		delete(s.nodeBinding, nodeID)
	}
}

type artifactIndexKey struct {
	clusterID string
	backend   string
	key       string
}

type InMemoryArtifactStore struct {
	mu              sync.RWMutex
	entries         map[artifactIndexKey]map[string]struct{}
	nodeKeys        map[string]map[artifactIndexKey]struct{}
	lru             *lru.Cache[artifactIndexKey, struct{}]
	lookupNodeLimit int
}

func NewInMemoryArtifactStore(capacity int, lookupNodeLimit int) *InMemoryArtifactStore {
	if capacity <= 0 {
		capacity = defaultArtifactStoreCapacity
	}

	store := &InMemoryArtifactStore{
		entries:         make(map[artifactIndexKey]map[string]struct{}),
		nodeKeys:        make(map[string]map[artifactIndexKey]struct{}),
		lookupNodeLimit: lookupNodeLimit,
	}
	cache, err := lru.NewWithEvict(capacity, store.evictLocked)
	if err != nil {
		panic(err)
	}
	store.lru = cache
	return store
}

func (s *InMemoryArtifactStore) Record(clusterID string, backend string, key string, nodeID string) {
	indexKey, ok := normalizeArtifactIndexKey(clusterID, backend, key)
	nodeID = strings.TrimSpace(nodeID)
	if !ok || nodeID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.entries[indexKey]; !ok {
		s.entries[indexKey] = make(map[string]struct{})
	}
	s.entries[indexKey][nodeID] = struct{}{}

	if _, ok := s.nodeKeys[nodeID]; !ok {
		s.nodeKeys[nodeID] = make(map[artifactIndexKey]struct{})
	}
	s.nodeKeys[nodeID][indexKey] = struct{}{}
	s.lru.Add(indexKey, struct{}{})
}

func (s *InMemoryArtifactStore) Forget(clusterID string, backend string, key string, nodeID string) {
	indexKey, ok := normalizeArtifactIndexKey(clusterID, backend, key)
	nodeID = strings.TrimSpace(nodeID)
	if !ok || nodeID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.forgetLocked(indexKey, nodeID)
}

func (s *InMemoryArtifactStore) Lookup(clusterID string, backend string, key string) []string {
	return s.lookup(clusterID, backend, key, s.lookupNodeLimit)
}

func (s *InMemoryArtifactStore) LookupAll(clusterID string, backend string, key string) []string {
	return s.lookup(clusterID, backend, key, 0)
}

func (s *InMemoryArtifactStore) lookup(clusterID string, backend string, key string, limit int) []string {
	indexKey, ok := normalizeArtifactIndexKey(clusterID, backend, key)
	if !ok {
		return nil
	}

	s.mu.RLock()
	nodes := s.entries[indexKey]
	if len(nodes) == 0 {
		s.mu.RUnlock()
		return nil
	}
	s.lru.Get(indexKey)
	resultCapacity := len(nodes)
	if limit > 0 && limit < resultCapacity {
		resultCapacity = limit
	}
	result := make([]string, 0, resultCapacity)
	for nodeID := range nodes {
		result = append(result, nodeID)
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	s.mu.RUnlock()

	return result
}

func (s *InMemoryArtifactStore) ForgetNode(nodeID string) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	keys := s.nodeKeys[nodeID]
	for indexKey := range keys {
		if nodes := s.entries[indexKey]; len(nodes) > 0 {
			delete(nodes, nodeID)
			if len(nodes) == 0 {
				s.removeArtifactKeyLocked(indexKey)
			}
		}
	}
	delete(s.nodeKeys, nodeID)
}

func (s *InMemoryArtifactStore) forgetLocked(indexKey artifactIndexKey, nodeID string) {
	if nodes := s.entries[indexKey]; len(nodes) > 0 {
		delete(nodes, nodeID)
		if len(nodes) == 0 {
			s.removeArtifactKeyLocked(indexKey)
		}
	}

	if keys := s.nodeKeys[nodeID]; len(keys) > 0 {
		delete(keys, indexKey)
		if len(keys) == 0 {
			delete(s.nodeKeys, nodeID)
		}
	}
}

func (s *InMemoryArtifactStore) removeArtifactKeyLocked(indexKey artifactIndexKey) {
	delete(s.entries, indexKey)
	s.lru.Remove(indexKey)
}

// evictLocked runs from LRU callbacks while callers hold s.mu's write lock.
func (s *InMemoryArtifactStore) evictLocked(indexKey artifactIndexKey, _ struct{}) {
	nodes := s.entries[indexKey]
	delete(s.entries, indexKey)
	for nodeID := range nodes {
		if keys := s.nodeKeys[nodeID]; len(keys) > 0 {
			delete(keys, indexKey)
			if len(keys) == 0 {
				delete(s.nodeKeys, nodeID)
			}
		}
	}
}

func normalizeArtifactIndexKey(clusterID string, backend string, key string) (artifactIndexKey, bool) {
	indexKey := artifactIndexKey{
		clusterID: strings.TrimSpace(clusterID),
		backend:   strings.TrimSpace(backend),
		key:       strings.TrimSpace(key),
	}
	return indexKey, indexKey.clusterID != "" && indexKey.backend != "" && indexKey.key != ""
}
