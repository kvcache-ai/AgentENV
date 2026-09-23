package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

const (
	snapshotPlacementPath = "/internal/snapshots/placement"
)

type snapshotOperation int

const (
	snapshotOperationNone snapshotOperation = iota
	snapshotOperationPromote
	snapshotOperationDelete
	snapshotOperationMetadata
)

type snapshotPlacement struct {
	SnapshotType string `json:"snapshotType"`
	OwnerNodeID  string `json:"ownerNodeID,omitempty"`
}

// Classify explicit snapshot control-plane routes before considering sandbox
// routing headers. Decode after splitting so escaped slashes stay in the reference.
func snapshotRequestOperation(r *http.Request) (snapshotOperation, string) {
	parts := strings.Split(strings.Trim(requestEscapedPath(r), "/"), "/")
	for index, raw := range parts {
		segment, err := url.PathUnescape(raw)
		if err != nil {
			return snapshotOperationNone, ""
		}
		parts[index] = segment
	}
	switch {
	case r.Method == http.MethodGet && parts[0] == "snapshots" && len(parts) <= 2:
		return snapshotOperationMetadata, ""
	case r.Method == http.MethodDelete && len(parts) == 2 && (parts[0] == "templates" || parts[0] == "snapshots"):
		if strings.TrimSpace(parts[1]) != "" {
			return snapshotOperationDelete, parts[1]
		}
	case r.Method == http.MethodPost && len(parts) == 3 && parts[0] == "snapshots" && parts[2] == "promote":
		if strings.TrimSpace(parts[1]) != "" {
			return snapshotOperationPromote, parts[1]
		}
	}
	return snapshotOperationNone, ""
}

func (s *Server) routeSnapshotRequest(
	ctx context.Context,
	incoming *http.Request,
	operation snapshotOperation,
	snapshotRef string,
) (*schedulerv1.Node, bool, *proxyResponseError) {
	// Snapshot node selection carries no sandbox placement signal: ask the
	// scheduler for a random active node directly (skipping placement
	// filters) so metadata discovery does not spend a sandbox launch's
	// admission quota.
	rpcStart := time.Now()
	resp, err := s.scheduler.Schedule(ctx, &schedulerv1.ScheduleRequest{SandboxAgnostic: true})
	recordGatewaySchedulerRPC("Schedule", rpcStart, err)
	if err != nil {
		return nil, false, schedulerResponseError(err)
	}
	metadataNode := resp.GetNode()
	if operation == snapshotOperationMetadata {
		return metadataNode, false, nil
	}
	placement, placementErr := s.lookupSnapshotPlacement(ctx, incoming, metadataNode, snapshotRef)
	if placementErr != nil {
		if operation == snapshotOperationDelete && placementErr.statusCode == http.StatusNotFound {
			// Missing placement preserves idempotent deletion on the metadata node.
			return metadataNode, false, nil
		}
		if operation == snapshotOperationPromote && placementErr.statusCode == http.StatusBadRequest {
			placementErr.statusCode = http.StatusConflict
		}
		return nil, false, placementErr
	}

	ownerNodeID := ""
	switch placement.SnapshotType {
	case "distributed":
		if strings.TrimSpace(placement.OwnerNodeID) != "" {
			return nil, false, unavailableSnapshotPlacement(errors.New("distributed snapshot must not include ownerNodeID"))
		}
	case "local":
		ownerNodeID = strings.TrimSpace(placement.OwnerNodeID)
		if ownerNodeID == "" {
			return nil, false, unavailableSnapshotPlacement(errors.New("local snapshot is missing ownerNodeID"))
		}
	default:
		return nil, false, unavailableSnapshotPlacement(errors.New("snapshotType must be local or distributed"))
	}

	if ownerNodeID != "" {
		node, err := s.resolveReadySnapshotOwner(ctx, ownerNodeID)
		return node, err == nil, err
	}
	return metadataNode, false, nil
}

func unavailableSnapshotPlacement(cause error) *proxyResponseError {
	return &proxyResponseError{
		statusCode: http.StatusServiceUnavailable,
		message:    "snapshot placement unavailable",
		cause:      cause,
	}
}

func (s *Server) lookupSnapshotPlacement(
	ctx context.Context,
	incoming *http.Request,
	metadataNode *schedulerv1.Node,
	snapshotRef string,
) (*snapshotPlacement, *proxyResponseError) {
	query := url.Values{}
	query.Set("snapshotID", snapshotRef)
	target, err := joinUpstream(
		metadataNode.GetEndpoint(),
		snapshotPlacementPath,
		snapshotPlacementPath,
		query.Encode(),
	)
	if err != nil {
		return nil, unavailableSnapshotPlacement(fmt.Errorf("build snapshot placement URL: %w", err))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, unavailableSnapshotPlacement(fmt.Errorf("build snapshot placement request: %w", err))
	}
	for _, name := range []string{headerAPIKey, "Authorization", "X-Supabase-Token", "X-Supabase-Team", "X-Admin-Token"} {
		for _, value := range incoming.Header.Values(name) {
			req.Header.Add(name, value)
		}
	}
	req.Header.Set("Accept", "application/json")

	client := *s.httpClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, unavailableSnapshotPlacement(fmt.Errorf("request snapshot placement: %w", err))
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusUnauthorized:
		return nil, &proxyResponseError{statusCode: http.StatusUnauthorized, message: "authentication required"}
	case http.StatusBadRequest:
		return nil, &proxyResponseError{statusCode: http.StatusBadRequest, message: "invalid snapshot reference"}
	case http.StatusNotFound:
		return nil, &proxyResponseError{statusCode: http.StatusNotFound, message: fmt.Sprintf("snapshot %s not found", snapshotRef)}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, unavailableSnapshotPlacement(fmt.Errorf("snapshot placement returned status %d", resp.StatusCode))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, unavailableSnapshotPlacement(fmt.Errorf("read snapshot placement response: %w", err))
	}
	var placement snapshotPlacement
	if err := json.Unmarshal(body, &placement); err != nil {
		return nil, unavailableSnapshotPlacement(fmt.Errorf("decode snapshot placement response: %w", err))
	}
	return &placement, nil
}

func (s *Server) resolveReadySnapshotOwner(
	ctx context.Context,
	ownerNodeID string,
) (*schedulerv1.Node, *proxyResponseError) {
	rpcStart := time.Now()
	resp, err := s.scheduler.GetNode(ctx, &schedulerv1.GetNodeRequest{NodeId: ownerNodeID})
	recordGatewaySchedulerRPC("GetNode", rpcStart, err)
	if err != nil {
		return nil, &proxyResponseError{
			statusCode: http.StatusServiceUnavailable,
			message:    "snapshot owner unavailable",
			cause:      err,
		}
	}

	observed := resp.GetNode()
	if observed.GetNodeId() != ownerNodeID {
		return nil, unavailableSnapshotOwner("scheduler returned a different snapshot owner")
	}
	if observed.GetSnapshot().GetStatus() != schedulerv1.NodeStatus_NODE_STATUS_READY {
		return nil, unavailableSnapshotOwner("snapshot owner is not ready")
	}

	return &schedulerv1.Node{
		NodeId:   observed.GetNodeId(),
		Endpoint: strings.TrimSpace(observed.GetEndpoint()),
	}, nil
}

func unavailableSnapshotOwner(message string) *proxyResponseError {
	return &proxyResponseError{
		statusCode: http.StatusServiceUnavailable,
		message:    "snapshot owner unavailable",
		cause:      errors.New(message),
	}
}
