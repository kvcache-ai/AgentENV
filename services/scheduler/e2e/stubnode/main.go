// stubnode is a minimal agentenv-node double for scheduler HA e2e tests.
// It serves the admin HTTP API the leader pulls (with heartbeat-parity
// fields), sends periodic gRPC heartbeats, and exposes a /control plane
// the test driver uses to inject metrics, sandbox rosters, and heartbeat
// pauses.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type stubState struct {
	mu              sync.Mutex
	sandboxIDs      []string
	sandboxCount    uint32
	cpuPercent      uint32
	pauseHeartbeats bool
	pauseAdmin      bool
}

type stub struct {
	id        string
	clusterID string
	apiKey    string
	state     *stubState
	client    schedulerv1.SchedulerClient
}

func main() {
	id := flag.String("id", mustEnv("NODE_ID", "stub-1"), "node id")
	listen := flag.String("listen", ":8000", "admin HTTP listen address")
	schedulerAddr := flag.String("scheduler", mustEnv("SCHEDULER_ADDR", "agentenv-scheduler:9090"), "scheduler gRPC address")
	clusterID := flag.String("cluster-id", "e2e", "cluster id")
	apiKey := flag.String("api-key", os.Getenv("AENV_API_KEY"), "admin API key for /nodes and /sandboxes")
	interval := flag.Duration("interval", 5*time.Second, "heartbeat interval")
	flag.Parse()

	conn, err := grpc.NewClient(*schedulerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial scheduler: %v", err)
	}
	defer conn.Close()

	s := &stub{
		id:        *id,
		clusterID: *clusterID,
		apiKey:    *apiKey,
		state:     &stubState{sandboxIDs: []string{}, cpuPercent: 5},
		client:    schedulerv1.NewSchedulerClient(conn),
	}

	go s.heartbeatLoop(*interval)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /nodes", s.withAuth(s.handleNodes))
	mux.HandleFunc("GET /sandboxes", s.withAuth(s.handleSandboxes))
	mux.HandleFunc("POST /control/metrics", s.handleSetMetrics)
	mux.HandleFunc("POST /control/sandboxes", s.handleSetSandboxes)
	mux.HandleFunc("POST /control/pause", s.handlePause)
	mux.HandleFunc("POST /control/resume", s.handleResume)
	mux.HandleFunc("POST /control/admin-pause", s.handleAdminPause)
	mux.HandleFunc("POST /control/admin-resume", s.handleAdminResume)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	log.Printf("stubnode %s listening on %s, heartbeating to %s", *id, *listen, *schedulerAddr)
	log.Fatal(http.ListenAndServe(*listen, mux))
}

func mustEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func (s *stub) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.apiKey != "" && r.Header.Get("x-api-key") != s.apiKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		s.state.mu.Lock()
		paused := s.state.pauseAdmin
		s.state.mu.Unlock()
		if paused {
			http.Error(w, "admin paused", http.StatusServiceUnavailable)
			return
		}
		next(w, r)
	}
}

func (s *stub) heartbeatLoop(interval time.Duration) {
	for {
		s.state.mu.Lock()
		paused := s.state.pauseHeartbeats
		s.state.mu.Unlock()
		if !paused {
			if _, err := s.client.Heartbeat(context.Background(), s.heartbeatRequest()); err != nil {
				log.Printf("heartbeat failed: %v", err)
			}
		}
		time.Sleep(interval)
	}
}

func (s *stub) heartbeatRequest() *schedulerv1.HeartbeatRequest {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	return &schedulerv1.HeartbeatRequest{
		NodeId:            s.id,
		ClusterId:         s.clusterID,
		ServiceInstanceId: s.id + "-instance",
		Version:           "e2e",
		Commit:            "e2e",
		MachineInfo: &schedulerv1.MachineInfo{
			CpuFamily:       "e2e",
			CpuModel:        "e2e",
			CpuModelName:    "stub-cpu",
			CpuArchitecture: "x86_64",
			CpuConfigJson:   `{"e2e":true}`,
		},
		Snapshot: &schedulerv1.NodeSnapshot{
			Status:           schedulerv1.NodeStatus_NODE_STATUS_READY,
			AllocatedCpu:     1,
			CpuPercent:       s.state.cpuPercent,
			CpuCount:         8,
			MemoryUsedBytes:  1 << 30,
			MemoryTotalBytes: 8 << 30,
			SandboxCount:     s.state.sandboxCount,
		},
		SandboxIds:  append([]string(nil), s.state.sandboxIDs...),
		P2PEndpoint: &schedulerv1.P2PEndpoint{Backend: "iroh", Address: s.id + ".p2p.e2e"},
	}
}

func (s *stub) handleNodes(w http.ResponseWriter, _ *http.Request) {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	writeJSON(w, []map[string]any{{
		"version":              "e2e",
		"commit":               "e2e",
		"id":                   s.id,
		"serviceInstanceID":    s.id + "-instance",
		"clusterID":            s.clusterID,
		"status":               "ready",
		"sandboxCount":         s.state.sandboxCount,
		"sandboxIDs":           s.state.sandboxIDs,
		"createSuccesses":      0,
		"createFails":          0,
		"sandboxStartingCount": 0,
		"sandboxPausedCount":   0,
		"p2pEndpoint":          map[string]string{"backend": "iroh", "address": s.id + ".p2p.e2e"},
		"machineInfo": map[string]string{
			"cpuFamily":       "e2e",
			"cpuModel":        "e2e",
			"cpuModelName":    "stub-cpu",
			"cpuArchitecture": "x86_64",
			"cpuConfigJSON":   `{"e2e":true}`,
		},
		"metrics": map[string]any{
			"allocatedCPU":               1,
			"allocatedMemoryBytes":       1 << 30,
			"cpuPercent":                 s.state.cpuPercent,
			"cpuCount":                   8,
			"memoryUsedBytes":            1 << 30,
			"memoryTotalBytes":           8 << 30,
			"pausedAllocatedCPU":         0,
			"pausedAllocatedMemoryBytes": 0,
			"disks":                      []any{},
		},
	}})
}

func (s *stub) handleSandboxes(w http.ResponseWriter, _ *http.Request) {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()
	out := make([]map[string]string, 0, len(s.state.sandboxIDs))
	for _, id := range s.state.sandboxIDs {
		out = append(out, map[string]string{"sandboxID": id})
	}
	writeJSON(w, out)
}

func (s *stub) handleSetMetrics(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SandboxCount uint32 `json:"sandboxCount"`
		CpuPercent   uint32 `json:"cpuPercent"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.state.mu.Lock()
	s.state.sandboxCount = req.SandboxCount
	s.state.cpuPercent = req.CpuPercent
	s.state.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *stub) handleSetSandboxes(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SandboxIDs []string `json:"sandboxIDs"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.state.mu.Lock()
	s.state.sandboxIDs = req.SandboxIDs
	s.state.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *stub) handlePause(w http.ResponseWriter, _ *http.Request) {
	s.state.mu.Lock()
	s.state.pauseHeartbeats = true
	s.state.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *stub) handleResume(w http.ResponseWriter, _ *http.Request) {
	s.state.mu.Lock()
	s.state.pauseHeartbeats = false
	s.state.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *stub) handleAdminPause(w http.ResponseWriter, _ *http.Request) {
	s.state.mu.Lock()
	s.state.pauseAdmin = true
	s.state.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (s *stub) handleAdminResume(w http.ResponseWriter, _ *http.Request) {
	s.state.mu.Lock()
	s.state.pauseAdmin = false
	s.state.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		fmt.Fprintf(w, `{"error":%q}`, err.Error())
	}
}
