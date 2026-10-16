package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
)

// ============================================================

// nodeSnapshotRefresher is this file's contract: refresh the leader's
// observations and bindings for a set of nodes by pulling their admin
// snapshots (sync-node-snapshots, #259). The only production implementation
// is ConcurrentNodeSnapshotRefresher; callers (the leadership manager) depend on this
// interface, not on the concrete type.
type nodeSnapshotRefresher interface {
	Refresh(ctx context.Context, nodes []Node)
}

// nodeReport is the neutral currency of node-state ingestion (#259): what a
// fetcher returns after successfully pulling a node, and what the shared
// ingest path consumes. Both the Heartbeat RPC handler and
// sync-node-snapshots convert into it; a fetcher never fabricates an RPC
// request.
type nodeReport struct {
	nodeID            string
	serviceInstanceID string
	clusterID         string
	version           string
	commit            string
	machineInfo       *schedulerv1.MachineInfo
	snapshot          *schedulerv1.NodeSnapshot
	// sandboxIDs carries the node's authoritative sandbox roster. Three
	// states matter:
	//   non-nil (including empty) — authoritative (heartbeat): reconcile
	//     bindings to exactly this set;
	//   nil — unknown (snapshot pull): bindings are left untouched. A pull
	//     cannot see paused sandboxes or in-flight template builds, so
	//     reconciling from it would delete live bindings on failover
	//     (#341 review). Binding refresh stays with heartbeats, and the
	//     binding TTL floor covers the gap.
	sandboxIDs  []string
	p2pEndpoint *schedulerv1.P2PEndpoint
	// fetchedAt records when a pulled report's data was captured. A report
	// built from a heartbeat leaves it zero (always fresh). The ingest guard
	// uses it to keep a slow pull from overwriting a newer heartbeat
	// (#341 review): skip the pull when the node has already reported
	// something captured after this data.
	fetchedAt time.Time
}

// toHeartbeatRequest converts the report to the registry's proto shape; the
// conversion is contained here so the registry interface stays unchanged.
func (r *nodeReport) toHeartbeatRequest() *schedulerv1.HeartbeatRequest {
	return &schedulerv1.HeartbeatRequest{
		NodeId:            r.nodeID,
		ClusterId:         r.clusterID,
		ServiceInstanceId: r.serviceInstanceID,
		Version:           r.version,
		Commit:            r.commit,
		MachineInfo:       r.machineInfo,
		Snapshot:          r.snapshot,
		SandboxIds:        r.sandboxIDs,
		P2PEndpoint:       r.p2pEndpoint,
	}
}

// NodeSnapshotFetcher retrieves a single node's current snapshot from its
// admin endpoints for sync-node-snapshots on leadership acquisition (#259).
// It returns the fetched data (a nodeReport), not an RPC request. It is an
// injected seam so the retrieval logic is testable without HTTP or node
// credentials.
type NodeSnapshotFetcher func(ctx context.Context, node Node) (*nodeReport, error)

// ConcurrentNodeSnapshotRefresher drives the post-acquisition state rebuild: fan out a
// retrieval of every registry node's snapshot with bounded concurrency and
// feed each response through the shared ingest path (ingestNodeReport — the
// same code a heartbeat takes). A failed retrieval leaves that node
// unobserved; the recovery window's fresh-observations-only rule keeps it out of
// scheduling until its next report.
// nodeReportIngester is the retriever's only dependency on the Service: the
// shared ingest path. The retriever never holds the Service itself.
type nodeReportIngester func(report *nodeReport, now time.Time) error

type ConcurrentNodeSnapshotRefresher struct {
	logger      *zap.Logger
	ingest      nodeReportIngester
	fetch       NodeSnapshotFetcher
	concurrency int
}

func NewConcurrentNodeSnapshotRefresher(logger *zap.Logger, ingest nodeReportIngester, fetch NodeSnapshotFetcher, concurrency int) *ConcurrentNodeSnapshotRefresher {
	if logger == nil {
		logger = zap.NewNop()
	}
	if concurrency <= 0 {
		concurrency = 4
	}
	return &ConcurrentNodeSnapshotRefresher{logger: logger, ingest: ingest, fetch: fetch, concurrency: concurrency}
}

// Refresh pulls a snapshot from every node in nodes and ingests the results.
// Every node is fetched exactly once, successes are ingested, failures are
// logged and leave the node unobserved (the recovery window's
// fresh-observations-only rule keeps it out of scheduling until its next
// report).
func (r *ConcurrentNodeSnapshotRefresher) Refresh(ctx context.Context, nodes []Node) {
	sem := make(chan struct{}, r.concurrency)
	var wg sync.WaitGroup
	for _, node := range nodes {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			report, err := r.fetch(ctx, node)
			if err != nil {
				r.logger.Warn("sync-node-snapshots: node snapshot pull failed",
					zap.String("node_id", node.ID), zap.Error(err))
				return
			}
			if report == nil {
				return
			}
			if err := r.ingest(report, time.Now()); err != nil {
				r.logger.Warn("sync-node-snapshots: ingesting pulled snapshot failed",
					zap.String("node_id", node.ID), zap.Error(err))
			}
		}()
	}
	wg.Wait()
}

// ============================================================

// adminNodeResponse mirrors the agentenv server admin GET /nodes entry
// (openapi: Node). Field names follow the API's camelCase JSON.
type adminNodeResponse struct {
	Version            string   `json:"version"`
	Commit             string   `json:"commit"`
	ID                 string   `json:"id"`
	ServiceInstanceID  string   `json:"serviceInstanceID"`
	ClusterID          string   `json:"clusterID"`
	SandboxCount       uint32   `json:"sandboxCount"`
	CreateSuccesses    uint64   `json:"createSuccesses"`
	CreateFails        uint64   `json:"createFails"`
	SandboxStartingCnt uint32   `json:"sandboxStartingCount"`
	SandboxPausedCount uint32   `json:"sandboxPausedCount"`
	Status             string   `json:"status"`
	SandboxIDs         []string `json:"sandboxIDs"`
	P2pEndpoint        *struct {
		Backend string `json:"backend"`
		Address string `json:"address"`
	} `json:"p2pEndpoint"`
	MachineInfo struct {
		CPUFamily       string `json:"cpuFamily"`
		CPUModel        string `json:"cpuModel"`
		CPUModelName    string `json:"cpuModelName"`
		CPUArchitecture string `json:"cpuArchitecture"`
		CPUConfigJSON   string `json:"cpuConfigJSON"`
	} `json:"machineInfo"`
	Metrics struct {
		AllocatedCPU               uint32 `json:"allocatedCPU"`
		AllocatedMemoryBytes       uint64 `json:"allocatedMemoryBytes"`
		CPUPercent                 uint32 `json:"cpuPercent"`
		CPUCount                   uint32 `json:"cpuCount"`
		MemoryUsedBytes            uint64 `json:"memoryUsedBytes"`
		MemoryTotalBytes           uint64 `json:"memoryTotalBytes"`
		PausedAllocatedCPU         uint32 `json:"pausedAllocatedCPU"`
		PausedAllocatedMemoryBytes uint64 `json:"pausedAllocatedMemoryBytes"`
		Disks                      []struct {
			MountPoint     string `json:"mountPoint"`
			Device         string `json:"device"`
			FilesystemType string `json:"filesystemType"`
			UsedBytes      uint64 `json:"usedBytes"`
			TotalBytes     uint64 `json:"totalBytes"`
		} `json:"disks"`
	} `json:"metrics"`
}

// adminStatusToProto maps the admin /nodes status string to the proto
// NodeStatus. Unknown values stay UNSPECIFIED (the registry then derives
// CONNECTING, same as a first heartbeat).
func adminStatusToProto(status string) schedulerv1.NodeStatus {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "ready":
		return schedulerv1.NodeStatus_NODE_STATUS_READY
	case "connecting":
		return schedulerv1.NodeStatus_NODE_STATUS_CONNECTING
	case "unhealthy":
		return schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY
	case "lingering":
		return schedulerv1.NodeStatus_NODE_STATUS_LINGERING
	default:
		return schedulerv1.NodeStatus_NODE_STATUS_UNSPECIFIED
	}
}

// NewAdminSnapshotFetcher builds the production NodeSnapshotFetcher (#259,
// sync-node-snapshots): it pulls GET {endpoint}/nodes for observations and
// returns the fetched data as a nodeReport for the shared ingest path.
// Bindings are deliberately not pulled (see nodeReport.sandboxIDs).
// The node's admin API requires the x-api-key header. The HTTP client is an
// implementation detail with a bounded per-request timeout; tests inject
// through the NodeSnapshotFetcher seam, not this constructor.
func NewAdminSnapshotFetcher(apiKey string) NodeSnapshotFetcher {
	client := &http.Client{
		Timeout: 5 * time.Second,
		// Never follow redirects: Go re-sends headers (including x-api-key)
		// to the redirect target, which would leak the admin key (#341
		// review). A redirect from a node admin API is an error, not a hint.
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return func(ctx context.Context, node Node) (*nodeReport, error) {
		base := strings.TrimRight(node.Endpoint, "/")
		// Stamp the capture time before the request: a heartbeat landing
		// while this request is in flight must count as newer than whatever
		// the response contains (#341 review).
		fetchedAt := time.Now()

		var nodes []adminNodeResponse
		if err := adminGetJSON(ctx, client, apiKey, base+"/nodes", &nodes); err != nil {
			return nil, fmt.Errorf("pull %s /nodes: %w", node.ID, err)
		}
		var entry *adminNodeResponse
		for i := range nodes {
			if nodes[i].ID == node.ID {
				entry = &nodes[i]
				break
			}
		}
		// Never accept a response for a different node ID: a stale or
		// misconfigured endpoint must not overwrite another node's
		// observation (#341 review).
		if entry == nil {
			return nil, fmt.Errorf("pull %s /nodes: node not in admin response", node.ID)
		}

		// Only /nodes is pulled: observations, not the sandbox roster. See
		// nodeReport.sandboxIDs for why the roster is never reconciled from
		// a pull.
		return reportFromAdmin(entry, fetchedAt), nil
	}
}

func adminGetJSON(ctx context.Context, client *http.Client, apiKey, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if strings.TrimSpace(apiKey) != "" {
		req.Header.Set("x-api-key", apiKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}
	// Bound the decoded body: pulls fan out across nodes during acquisition,
	// and a faulty or compromised node must not make the leader allocate
	// from an arbitrarily large response (#341 review).
	return json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(out)
}

func reportFromAdmin(n *adminNodeResponse, fetchedAt time.Time) *nodeReport {
	// The admin roster is authoritative when present (same source as
	// heartbeat sandbox_ids: orchestrator.list_sandbox_ids), so a parity
	// pull may reconcile bindings; an absent roster stays nil and skips
	// reconciliation (#341 review).
	var sandboxIDs []string
	if n.SandboxIDs != nil {
		sandboxIDs = []string{}
		for _, id := range n.SandboxIDs {
			if strings.TrimSpace(id) != "" {
				sandboxIDs = append(sandboxIDs, id)
			}
		}
	}
	var p2p *schedulerv1.P2PEndpoint
	if n.P2pEndpoint != nil {
		p2p = &schedulerv1.P2PEndpoint{Backend: n.P2pEndpoint.Backend, Address: n.P2pEndpoint.Address}
	}
	disks := make([]*schedulerv1.DiskMetric, 0, len(n.Metrics.Disks))
	for _, d := range n.Metrics.Disks {
		disks = append(disks, &schedulerv1.DiskMetric{
			MountPoint:     d.MountPoint,
			Device:         d.Device,
			FilesystemType: d.FilesystemType,
			UsedBytes:      d.UsedBytes,
			TotalBytes:     d.TotalBytes,
		})
	}
	return &nodeReport{
		nodeID:            n.ID,
		sandboxIDs:        sandboxIDs,
		p2pEndpoint:       p2p,
		clusterID:         n.ClusterID,
		serviceInstanceID: n.ServiceInstanceID,
		version:           n.Version,
		commit:            n.Commit,
		fetchedAt:         fetchedAt,
		machineInfo: &schedulerv1.MachineInfo{
			CpuFamily:       n.MachineInfo.CPUFamily,
			CpuModel:        n.MachineInfo.CPUModel,
			CpuModelName:    n.MachineInfo.CPUModelName,
			CpuArchitecture: n.MachineInfo.CPUArchitecture,
			CpuConfigJson:   n.MachineInfo.CPUConfigJSON,
		},
		snapshot: &schedulerv1.NodeSnapshot{
			Status:                     adminStatusToProto(n.Status),
			AllocatedCpu:               n.Metrics.AllocatedCPU,
			AllocatedMemoryBytes:       n.Metrics.AllocatedMemoryBytes,
			CpuPercent:                 n.Metrics.CPUPercent,
			CpuCount:                   n.Metrics.CPUCount,
			MemoryUsedBytes:            n.Metrics.MemoryUsedBytes,
			MemoryTotalBytes:           n.Metrics.MemoryTotalBytes,
			Disks:                      disks,
			SandboxCount:               n.SandboxCount,
			SandboxStartingCount:       n.SandboxStartingCnt,
			CreateSuccesses:            n.CreateSuccesses,
			CreateFails:                n.CreateFails,
			ReportedAtUnixMs:           time.Now().UnixMilli(),
			PausedSandboxCount:         n.SandboxPausedCount,
			PausedAllocatedCpu:         n.Metrics.PausedAllocatedCPU,
			PausedAllocatedMemoryBytes: n.Metrics.PausedAllocatedMemoryBytes,
		},
	}
}
