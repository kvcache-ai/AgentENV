package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
)

type snapshot struct {
	info   snapshots.Info
	rootfs string
}

// Snapshotter is a minimal containerd snapshotter that bind-mounts existing
// directories as container rootfs.
type Snapshotter struct {
	root  string
	mu    sync.RWMutex
	snaps map[string]*snapshot
}

func NewSnapshotter(root string) (*Snapshotter, error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, fmt.Errorf("create root dir: %w", err)
	}
	s := &Snapshotter{
		root:  root,
		snaps: make(map[string]*snapshot),
	}
	config := s.getConfig()
	log.Printf("[DEBUG] loaded config with %d entries", len(config))
	for k, v := range config {
		log.Printf("[DEBUG]   %s -> %s", k, v)
	}
	return s, nil
}

func (s *Snapshotter) configPath() string {
	return filepath.Join(s.root, "config.json")
}

// getConfig reads the config file from disk every time (no caching).
func (s *Snapshotter) getConfig() map[string]string {
	config := make(map[string]string)
	data, err := os.ReadFile(s.configPath())
	if err != nil {
		return config
	}
	json.Unmarshal(data, &config)
	return config
}

// RegisterRootfs adds a chain ID → rootfs mapping and persists it atomically.
func (s *Snapshotter) RegisterRootfs(chainID, rootfsPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	config := s.getConfig()
	config[chainID] = rootfsPath
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.root, "config-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, s.configPath())
}

// configuredRootfs returns the rootfs path if the key is registered in config.
// Handles namespaced keys like "moby/2/sha256:abc..." by also checking the
// bare chain ID ("sha256:abc...").
func (s *Snapshotter) configuredRootfs(key string) (string, bool) {
	config := s.getConfig()
	if p, ok := config[key]; ok {
		return p, true
	}
	// Strip namespace prefix (e.g. "moby/2/") and retry
	if i := strings.LastIndex(key, "/sha256:"); i >= 0 {
		bare := key[i+1:]
		if p, ok := config[bare]; ok {
			return p, true
		}
	}
	return "", false
}

func (s *Snapshotter) Stat(ctx context.Context, key string) (snapshots.Info, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if sn, ok := s.snaps[key]; ok {
		log.Printf("[DEBUG] Stat(%q) -> found in memory, kind=%v", key, sn.info.Kind)
		return sn.info, nil
	}

	// Check persistent config (for surviving restarts)
	if _, ok := s.configuredRootfs(key); ok {
		log.Printf("[DEBUG] Stat(%q) -> found in config", key)
		return snapshots.Info{
			Name:    key,
			Kind:    snapshots.KindCommitted,
			Created: time.Now(),
			Updated: time.Now(),
		}, nil
	}

	log.Printf("[DEBUG] Stat(%q) -> not found", key)
	return snapshots.Info{}, fmt.Errorf("snapshot %q: %w", key, errdefs.ErrNotFound)
}

func (s *Snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[DEBUG] Update(%q, fieldpaths=%v)", info.Name, fieldpaths)

	sn, ok := s.snaps[info.Name]
	if !ok {
		return snapshots.Info{}, fmt.Errorf("snapshot %q: %w", info.Name, errdefs.ErrNotFound)
	}

	if len(fieldpaths) == 0 {
		sn.info.Labels = info.Labels
	} else {
		for _, fp := range fieldpaths {
			if fp == "labels" {
				sn.info.Labels = info.Labels
			}
		}
	}
	sn.info.Updated = time.Now()
	return sn.info, nil
}

func (s *Snapshotter) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	log.Printf("[DEBUG] Usage(%q)", key)
	return snapshots.Usage{}, nil
}

func (s *Snapshotter) Mounts(ctx context.Context, key string) ([]mount.Mount, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if sn, ok := s.snaps[key]; ok {
		log.Printf("[DEBUG] Mounts(%q) -> memory rootfs=%s", key, sn.rootfs)
		return bindMount(sn.rootfs), nil
	}
	if rootfs, ok := s.configuredRootfs(key); ok {
		log.Printf("[DEBUG] Mounts(%q) -> config rootfs=%s", key, rootfs)
		return bindMount(rootfs), nil
	}
	log.Printf("[DEBUG] Mounts(%q) -> not found", key)
	return nil, fmt.Errorf("snapshot %q: %w", key, errdefs.ErrNotFound)
}

func (s *Snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	log.Printf("[DEBUG] Prepare(key=%q, parent=%q)", key, parent)
	return s.createSnapshot(ctx, snapshots.KindActive, key, parent, opts...)
}

func (s *Snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	log.Printf("[DEBUG] View(key=%q, parent=%q)", key, parent)
	return s.createSnapshot(ctx, snapshots.KindView, key, parent, opts...)
}

func (s *Snapshotter) createSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.snaps[key]; ok {
		log.Printf("[DEBUG] createSnapshot(%q) -> already exists", key)
		return nil, fmt.Errorf("snapshot %q: %w", key, errdefs.ErrAlreadyExists)
	}

	var base snapshots.Info
	for _, opt := range opts {
		opt(&base)
	}
	// Determine rootfs: config(parent) > parent's stored rootfs > temp dir
	rootfs := ""
	source := "none"
	if parent != "" {
		if r, ok := s.configuredRootfs(parent); ok {
			rootfs = r
			source = "config"
		}
	}
	if rootfs == "" && parent != "" {
		if p, ok := s.snaps[parent]; ok {
			rootfs = p.rootfs
			source = "parent"
		}
	}
	if rootfs == "" {
		dir, err := os.MkdirTemp(s.root, "snap-")
		if err != nil {
			return nil, fmt.Errorf("create temp dir: %w", err)
		}
		rootfs = dir
		source = "tempdir"
	}

	log.Printf("[DEBUG] createSnapshot(%q) -> rootfs=%s (source=%s)", key, rootfs, source)

	now := time.Now()
	s.snaps[key] = &snapshot{
		info: snapshots.Info{
			Name:    key,
			Parent:  parent,
			Kind:    kind,
			Labels:  base.Labels,
			Created: now,
			Updated: now,
		},
		rootfs: rootfs,
	}
	return bindMount(rootfs), nil
}

func (s *Snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	sn, ok := s.snaps[key]
	if !ok {
		log.Printf("[DEBUG] Commit(%q, %q) -> key not found", name, key)
		return fmt.Errorf("snapshot %q: %w", key, errdefs.ErrNotFound)
	}

	var base snapshots.Info
	for _, opt := range opts {
		opt(&base)
	}

	log.Printf("[DEBUG] Commit(%q, %q) rootfs=%s labels=%v", name, key, sn.rootfs, base.Labels)

	delete(s.snaps, key)
	sn.info.Name = name
	sn.info.Kind = snapshots.KindCommitted
	sn.info.Updated = time.Now()
	if base.Labels != nil {
		sn.info.Labels = base.Labels
	}
	s.snaps[name] = sn
	return nil
}

func (s *Snapshotter) Remove(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Printf("[DEBUG] Remove(%q)", key)
	delete(s.snaps, key)
	return nil
}

func (s *Snapshotter) Walk(ctx context.Context, fn snapshots.WalkFunc, filters ...string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	config := s.getConfig()
	log.Printf("[DEBUG] Walk(filters=%v) snaps=%d config=%d", filters, len(s.snaps), len(config))

	for _, sn := range s.snaps {
		if err := fn(ctx, sn.info); err != nil {
			return err
		}
	}
	// Also walk config entries
	for key := range config {
		info := snapshots.Info{
			Name:    key,
			Kind:    snapshots.KindCommitted,
			Created: time.Now(),
			Updated: time.Now(),
		}
		if err := fn(ctx, info); err != nil {
			return err
		}
	}
	return nil
}

func (s *Snapshotter) Close() error {
	log.Printf("[DEBUG] Close()")
	return nil
}

func bindMount(path string) []mount.Mount {
	return []mount.Mount{
		{
			Type:    "bind",
			Source:  path,
			Options: []string{"rbind", "rw"},
		},
	}
}
