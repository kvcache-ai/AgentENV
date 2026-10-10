package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The Service sees leadership state only through the narrow leadershipView
// seam (defined in leadership.go); LeadershipManager is the only production
// implementation, and the internal snapshot object is never handed out.

type Service struct {
	schedulerv1.UnimplementedSchedulerServer
	logger        *zap.Logger
	nodes         NodeRegistry
	strategy      Strategy
	store         BindingStore
	artifacts     ArtifactStore
	resourceLimit *config.NodeResourceLimit
	leadership    leadershipView
	// for test cases, inject for time-related function checking on `inRecoveryWindow`
	now func() time.Time
}

func NewService(logger *zap.Logger, nodes NodeRegistry, strategy Strategy, store BindingStore, opts ...ServiceOption) *Service {
	if logger == nil {
		logger = zap.NewNop()
	}
	if nodes == nil {
		nodes = NewAtomicNodeRegistry(nil, defaultObservedReportTTL)
	}
	s := &Service{
		logger:    logger,
		nodes:     nodes,
		strategy:  strategy,
		store:     store,
		artifacts: NewInMemoryArtifactStore(defaultArtifactStoreCapacity, 0),
		now:       time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// ServiceOption configures optional Service behaviour.
type ServiceOption func(*Service)

// WithNodeResourceLimit sets per-node resource thresholds for scheduling.
func WithNodeResourceLimit(limit *config.NodeResourceLimit) ServiceOption {
	return func(s *Service) {
		s.resourceLimit = limit
	}
}

func WithArtifactStore(store ArtifactStore) ServiceOption {
	return func(s *Service) {
		s.artifacts = store
	}
}

// WithLeadership attaches the read-only leadership view so scheduling and
// lookup can apply recovery-window rules right after a leadership
// transition (#259).
func WithLeadership(leadership leadershipView) ServiceOption {
	return func(s *Service) {
		s.leadership = leadership
	}
}

// WithClock overrides the time source (tests only).
func WithClock(now func() time.Time) ServiceOption {
	return func(s *Service) {
		s.now = now
	}
}

type QueryOnlyService struct {
	schedulerv1.UnimplementedSchedulerServer
	logger *zap.Logger
	store  BindingStore
}

func NewQueryOnlyService(logger *zap.Logger, store BindingStore) *QueryOnlyService {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &QueryOnlyService{logger: logger, store: store}
}

func (s *QueryOnlyService) LookupNode(_ context.Context, req *schedulerv1.LookupNodeRequest) (*schedulerv1.LookupNodeResponse, error) {
	return lookupNode(s.logger, s.store, req)
}

func (s *Service) Schedule(_ context.Context, req *schedulerv1.ScheduleRequest) (resp *schedulerv1.ScheduleResponse, err error) {
	start := time.Now()
	defer func() {
		recordSchedulerSchedule(s.strategy.Name(), start, err)
	}()

	discovered := s.nodes.Snapshot( /* allowLingering */ false)
	rich := make([]RichNode, 0, len(discovered))
	for _, n := range discovered {
		rich = append(rich, RichNode{
			Node:     n,
			Snapshot: s.nodes.PeekObserved(n.ID),
		})
	}

	rich = s.filterFreshObservations(rich)

	eligible := FilterByResourceLimit(rich, s.resourceLimit)

	node, selectErr := s.strategy.Select(eligible, req.GetHint())
	if selectErr != nil {
		s.logger.Debug("scheduler selection failed",
			zap.String("strategy", s.strategy.Name()),
			zap.String("hint", summarizeScheduleHint(req.GetHint())),
			zap.Int("candidate_nodes", len(rich)),
			zap.Int("eligible_nodes", len(eligible)),
			zap.Error(selectErr),
		)
		if errors.Is(selectErr, ErrNoNodes) {
			err = status.Error(codes.Unavailable, "no nodes available")
			return nil, err
		}
		err = status.Error(codes.Internal, selectErr.Error())
		return nil, err
	}
	s.logger.Debug("scheduler selected node",
		zap.String("strategy", s.strategy.Name()),
		zap.String("hint", summarizeScheduleHint(req.GetHint())),
		zap.String("node_id", node.ID),
		zap.String("endpoint", node.Endpoint),
		zap.Int("candidate_nodes", len(rich)),
		zap.Int("eligible_nodes", len(eligible)),
	)
	return &schedulerv1.ScheduleResponse{Node: node.Node.ToProto()}, nil
}

// filterFreshObservations applies the fresh-observations-only rule (#259):
// a node that was known at leadership acquisition is a scheduling candidate
// only once it has reported at or after the acquisition — a timer alone
// must not readmit a node whose reporter is still backing off (#341
// review). Nodes that joined after the acquisition are not in the
// recovery-pending set and keep the steady-state fail-open behaviour; with
// leader election disabled everything passes.
func (s *Service) filterFreshObservations(rich []RichNode) []RichNode {
	if s.leadership == nil {
		return rich
	}
	since, ok := s.leadership.LeaderSince()
	if !ok {
		return rich
	}
	// Compare at millisecond precision: LastReportAt is reconstructed from a
	// millisecond timestamp while the acquisition time has nanoseconds, so a
	// report in the same millisecond as the acquisition must count as fresh
	// (#341 review).
	sinceMs := since.UnixMilli()
	fresh := make([]RichNode, 0, len(rich))
	for _, n := range rich {
		at, reported := s.nodes.LastReportAt(n.Node.ID)
		if reported && at.UnixMilli() >= sinceMs {
			fresh = append(fresh, n)
			continue
		}
		var lastReport *time.Time
		if reported {
			lastReport = &at
		}
		if !s.leadership.RecoveryPending(n.Node.ID, lastReport) {
			fresh = append(fresh, n)
		}
	}
	return fresh
}

// inRecoveryWindow reports whether the recovery window following leadership
// acquisition is open right now. Without leader election it is always closed.
func (s *Service) inRecoveryWindow() bool {
	return s.leadership != nil && s.leadership.InRecoveryWindow(s.now())
}

// summarizeScheduleHint renders a compact, log-friendly description of a
// scheduling hint.
func summarizeScheduleHint(hint *schedulerv1.ScheduleRequestHint) string {
	switch k := hint.GetKind().(type) {
	case *schedulerv1.ScheduleRequestHint_NewColdSandbox:
		c := k.NewColdSandbox
		return fmt.Sprintf("new_cold_sandbox cpu=%d memory_mb=%d images=%v", c.GetCpuCount(), c.GetMemoryMb(), c.GetImages())
	case *schedulerv1.ScheduleRequestHint_NewSandbox:
		return "new_sandbox"
	default:
		return "none"
	}
}

func (s *Service) ListNodes(_ context.Context, _ *schedulerv1.ListNodesRequest) (*schedulerv1.ListNodesResponse, error) {
	snapshot := s.nodes.Snapshot( /* allowLingering */ true)
	nodes := make([]*schedulerv1.Node, 0, len(snapshot))
	for _, node := range snapshot {
		nodes = append(nodes, node.ToProto())
	}

	s.logger.Debug("scheduler listed nodes", zap.Int("node_count", len(nodes)))

	return &schedulerv1.ListNodesResponse{Nodes: nodes}, nil
}

func (s *Service) LookupNode(_ context.Context, req *schedulerv1.LookupNodeRequest) (*schedulerv1.LookupNodeResponse, error) {
	resp, err := lookupNode(s.logger, s.store, req)
	if err != nil && status.Code(err) == codes.NotFound && s.inRecoveryWindow() {
		// Recovery window open: a missing binding means "not yet rebuilt",
		// not "does not exist" (#259).
		return nil, status.Error(codes.Unavailable, "sandbox assignment not rebuilt yet")
	}
	return resp, err
}

func lookupNode(logger *zap.Logger, store BindingStore, req *schedulerv1.LookupNodeRequest) (*schedulerv1.LookupNodeResponse, error) {
	if strings.TrimSpace(req.GetSandboxId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}
	node, ok, getErr := store.Get(req.GetSandboxId(), time.Now())
	if getErr != nil {
		logger.Warn("scheduler lookup binding store failed", zap.String("sandbox_id", req.GetSandboxId()), zap.Error(getErr))
		return nil, status.Error(codes.Unavailable, "binding store unavailable")
	}
	if !ok {
		logger.Debug("scheduler lookup missed sandbox assignment", zap.String("sandbox_id", req.GetSandboxId()))
		return nil, status.Error(codes.NotFound, "sandbox assignment not found")
	}
	logger.Debug("scheduler lookup resolved sandbox assignment",
		zap.String("sandbox_id", req.GetSandboxId()),
		zap.String("node_id", node.ID),
		zap.String("endpoint", node.Endpoint),
	)
	return &schedulerv1.LookupNodeResponse{Node: node.ToProto()}, nil
}

func (s *Service) RecordAssignment(_ context.Context, req *schedulerv1.RecordAssignmentRequest) (*schedulerv1.RecordAssignmentResponse, error) {
	if strings.TrimSpace(req.GetSandboxId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}
	node := NodeFromProto(req.GetNode())
	if strings.TrimSpace(node.ID) == "" || strings.TrimSpace(node.Endpoint) == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and endpoint are required")
	}
	if !s.isKnownNode(node) {
		s.logger.Warn("scheduler rejected assignment for unknown node",
			zap.String("sandbox_id", req.GetSandboxId()),
			zap.String("node_id", node.ID),
			zap.String("endpoint", node.Endpoint),
		)
		return nil, status.Error(codes.InvalidArgument, "node is not in scheduler node list")
	}
	if err := s.store.Record(req.GetSandboxId(), node, time.Now()); err != nil {
		s.logger.Warn("scheduler record assignment binding store failed",
			zap.String("sandbox_id", req.GetSandboxId()),
			zap.String("node_id", node.ID),
			zap.Error(err),
		)
		return nil, status.Error(codes.Unavailable, "binding store unavailable")
	}
	s.logger.Debug("scheduler recorded sandbox assignment",
		zap.String("sandbox_id", req.GetSandboxId()),
		zap.String("node_id", node.ID),
		zap.String("endpoint", node.Endpoint),
	)
	return &schedulerv1.RecordAssignmentResponse{}, nil
}

func (s *Service) Heartbeat(_ context.Context, req *schedulerv1.HeartbeatRequest) (*schedulerv1.HeartbeatResponse, error) {
	nodeID := strings.TrimSpace(req.GetNodeId())
	serviceInstanceID := strings.TrimSpace(req.GetServiceInstanceId())
	if nodeID == "" || serviceInstanceID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and service_instance_id are required")
	}

	return s.ingestNodeReport(nodeReportFromProto(req), s.now())
}

// ingestNodeError maps registry ingest failures to gRPC statuses.
func ingestNodeError(logger *zap.Logger, nodeID string, err error) error {
	if errors.Is(err, ErrNodeNotInRegistry) {
		logger.Warn("scheduler rejected observed registration for unknown node",
			zap.String("node_id", nodeID),
		)
		return status.Error(codes.InvalidArgument, "node is not in scheduler node list")
	}
	return status.Error(codes.Internal, "node registry heartbeat failed")
}

// nodeReportFromProto adapts a Heartbeat RPC request to the neutral ingest
// currency. Identity validation already happened in Heartbeat.
func nodeReportFromProto(req *schedulerv1.HeartbeatRequest) *nodeReport {
	// A heartbeat roster is always authoritative, including when empty:
	// proto3 decodes absent and empty repeated fields identically, so force
	// a non-nil slice to keep heartbeats reconciling (see nodeReport).
	sandboxIDs := req.GetSandboxIds()
	if sandboxIDs == nil {
		sandboxIDs = []string{}
	}
	return &nodeReport{
		nodeID:            req.GetNodeId(),
		clusterID:         req.GetClusterId(),
		serviceInstanceID: req.GetServiceInstanceId(),
		version:           req.GetVersion(),
		commit:            req.GetCommit(),
		machineInfo:       req.GetMachineInfo(),
		snapshot:          req.GetSnapshot(),
		sandboxIDs:        sandboxIDs,
		p2pEndpoint:       req.GetP2PEndpoint(),
	}
}

// ingestNodeReport is the shared ingest path for heartbeat RPCs and pulled
// node snapshots (sync-node-snapshots on leadership acquisition, #259):
// update registry observations, then reconcile sandbox bindings.
func (s *Service) ingestNodeReport(report *nodeReport, now time.Time) (*schedulerv1.HeartbeatResponse, error) {
	req := report.toHeartbeatRequest()
	if !report.fetchedAt.IsZero() {
		// Pulled report: skip when the node already reported something
		// newer (#341 review). The residual check-then-act window is
		// microseconds and self-heals on the next heartbeat.
		if at, ok := s.nodes.LastReportAt(report.nodeID); ok && at.After(report.fetchedAt) {
			return &schedulerv1.HeartbeatResponse{}, nil
		}
		// The admin API does not serve the P2P endpoint; carry over the
		// previously known one so a pull does not erase it (#341 review).
		if req.P2PEndpoint == nil {
			req.P2PEndpoint = s.nodes.P2PEndpointFor(report.nodeID)
		}
	}
	node, cpuConfigJSON, err := s.nodes.Heartbeat(req, now)
	if err != nil {
		return nil, ingestNodeError(s.logger, report.nodeID, err)
	}
	// Reconcile only from an authoritative roster (heartbeat). A snapshot
	// pull leaves sandboxIDs nil — bindings are refreshed by heartbeats and
	// guarded meanwhile by the binding TTL floor (#341 review).
	if report.sandboxIDs != nil {
		if err := s.store.ReconcileNode(node, report.sandboxIDs, now); err != nil {
			s.logger.Warn("scheduler heartbeat binding reconcile failed",
				zap.String("node_id", report.nodeID),
				zap.Error(err),
			)
			return nil, status.Error(codes.Unavailable, "binding store unavailable")
		}
	}
	return &schedulerv1.HeartbeatResponse{CpuConfigJson: cpuConfigJSON}, nil
}

func (s *Service) ReportSandboxEvent(_ context.Context, req *schedulerv1.ReportSandboxEventRequest) (*schedulerv1.ReportSandboxEventResponse, error) {
	s.logger.Debug("scheduler ignored sandbox event batch",
		zap.String("node_id", req.GetNodeId()),
		zap.String("service_instance_id", req.GetServiceInstanceId()),
		zap.Int("event_count", len(req.GetEvents())),
	)
	return &schedulerv1.ReportSandboxEventResponse{}, nil
}

func (s *Service) RunObservedNodesMetrics(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	s.refreshObservedNodesMetrics(time.Now())

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.refreshObservedNodesMetrics(now)
		}
	}
}

func (s *Service) refreshObservedNodesMetrics(now time.Time) {
	recordObservedNodes(s.nodes.ListObserved("", now))
}

func (s *Service) ListObservedNodes(_ context.Context, req *schedulerv1.ListObservedNodesRequest) (*schedulerv1.ListObservedNodesResponse, error) {
	nodes := s.nodes.ListObserved(req.GetClusterId(), time.Now())
	return &schedulerv1.ListObservedNodesResponse{
		Nodes: nodes,
	}, nil
}

func (s *Service) ListP2PPeers(_ context.Context, req *schedulerv1.ListP2PPeersRequest) (*schedulerv1.ListP2PPeersResponse, error) {
	peers := s.nodes.ListP2pPeers(
		req.GetClusterId(),
		req.GetBackend(),
		req.GetExcludeNodeId(),
		time.Now(),
	)
	return &schedulerv1.ListP2PPeersResponse{Peers: peers}, nil
}

func (s *Service) RecordP2PArtifact(_ context.Context, req *schedulerv1.RecordP2PArtifactRequest) (*schedulerv1.RecordP2PArtifactResponse, error) {
	if strings.TrimSpace(req.GetClusterId()) == "" || strings.TrimSpace(req.GetBackend()) == "" || strings.TrimSpace(req.GetKey()) == "" || strings.TrimSpace(req.GetNodeId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "cluster_id, backend, key, and node_id are required")
	}
	if _, ok := s.nodes.Resolve(req.GetNodeId()); !ok {
		return nil, status.Error(codes.InvalidArgument, "node is not in scheduler node list")
	}

	s.artifacts.Record(req.GetClusterId(), req.GetBackend(), req.GetKey(), req.GetNodeId())
	s.logger.Debug("scheduler recorded P2P artifact",
		zap.String("cluster_id", req.GetClusterId()),
		zap.String("backend", req.GetBackend()),
		zap.String("key", req.GetKey()),
		zap.String("node_id", req.GetNodeId()),
	)
	return &schedulerv1.RecordP2PArtifactResponse{}, nil
}

func (s *Service) ForgetP2PArtifact(_ context.Context, req *schedulerv1.ForgetP2PArtifactRequest) (*schedulerv1.ForgetP2PArtifactResponse, error) {
	if strings.TrimSpace(req.GetClusterId()) == "" || strings.TrimSpace(req.GetBackend()) == "" || strings.TrimSpace(req.GetKey()) == "" || strings.TrimSpace(req.GetNodeId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "cluster_id, backend, key, and node_id are required")
	}

	s.artifacts.Forget(req.GetClusterId(), req.GetBackend(), req.GetKey(), req.GetNodeId())
	s.logger.Debug("scheduler forgot P2P artifact",
		zap.String("cluster_id", req.GetClusterId()),
		zap.String("backend", req.GetBackend()),
		zap.String("key", req.GetKey()),
		zap.String("node_id", req.GetNodeId()),
	)
	return &schedulerv1.ForgetP2PArtifactResponse{}, nil
}

func (s *Service) LookupP2PArtifact(_ context.Context, req *schedulerv1.LookupP2PArtifactRequest) (*schedulerv1.LookupP2PArtifactResponse, error) {
	if strings.TrimSpace(req.GetClusterId()) == "" || strings.TrimSpace(req.GetBackend()) == "" || strings.TrimSpace(req.GetKey()) == "" {
		return nil, status.Error(codes.InvalidArgument, "cluster_id, backend, and key are required")
	}

	nodeIDs := s.artifacts.Lookup(req.GetClusterId(), req.GetBackend(), req.GetKey())
	peers := s.nodes.FilterP2pPeers(
		req.GetClusterId(),
		req.GetBackend(),
		nodeIDs,
		req.GetExcludeNodeId(),
		time.Now(),
	)
	return &schedulerv1.LookupP2PArtifactResponse{Peers: peers}, nil
}

func (s *Service) GetNode(_ context.Context, req *schedulerv1.GetNodeRequest) (*schedulerv1.GetNodeResponse, error) {
	nodeID := strings.TrimSpace(req.GetNodeId())
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	node, ok := s.nodes.GetObserved(nodeID, req.GetClusterId(), s.now())
	if !ok {
		if s.inRecoveryWindow() {
			// Same recovery-window rule as LookupNode (#259).
			return nil, status.Error(codes.Unavailable, "observed node not rebuilt yet")
		}
		return nil, status.Error(codes.NotFound, "observed node not found")
	}

	return &schedulerv1.GetNodeResponse{Node: node}, nil
}

func (s *Service) UnregisterNode(_ context.Context, req *schedulerv1.UnregisterNodeRequest) (*schedulerv1.UnregisterNodeResponse, error) {
	nodeID := strings.TrimSpace(req.GetNodeId())
	serviceInstanceID := strings.TrimSpace(req.GetServiceInstanceId())
	if nodeID == "" || serviceInstanceID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and service_instance_id are required")
	}

	unregisterErr := s.nodes.UnregisterObserved(nodeID, serviceInstanceID)
	if unregisterErr != nil {
		if errors.Is(unregisterErr, ErrServiceInstanceMismatch) {
			return nil, status.Error(codes.FailedPrecondition, "service instance mismatch")
		}
		return nil, status.Error(codes.Internal, unregisterErr.Error())
	}

	now := time.Now()
	if err := s.store.ReconcileNode(Node{ID: nodeID}, nil, now); err != nil {
		s.logger.Warn("scheduler unregister binding reconcile failed",
			zap.String("node_id", nodeID),
			zap.Error(err),
		)
		return nil, status.Error(codes.Unavailable, "binding store unavailable")
	}
	s.artifacts.ForgetNode(nodeID)

	return &schedulerv1.UnregisterNodeResponse{}, nil
}

func (s *Service) isKnownNode(node Node) bool {
	return s.nodes.Contains(node)
}
