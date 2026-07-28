package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

const (
	defaultTemplateAliasHTTPTimeout       = 2 * time.Second
	defaultTemplateAliasLookupTimeout     = 2 * time.Second
	defaultTemplateAliasScheduleReserve   = 5 * time.Second
	defaultTemplateAliasLookupConcurrency = 8
	// Alias resolution is advisory, so cap concurrent cluster fan-outs globally
	// and skip locality when the gateway is already at capacity.
	defaultTemplateAliasResolutionConcurrency = 8
)

type templateAliasResponse struct {
	TemplateID string `json:"templateID"`
}

// resolveSnapshotLocalityHint replaces the request's template alias with the
// canonical snapshot ID before deriving fixed snapshot artifact keys. Alias
// resolution is best-effort: locality is only a placement preference, so a
// resolver failure must not prevent the normal sandbox request from being
// scheduled.
func (s *Server) resolveSnapshotLocalityHint(
	ctx context.Context,
	incoming *http.Request,
	hint *schedulerv1.ScheduleRequestHint,
) *schedulerv1.ScheduleRequestHint {
	if hint == nil || hint.GetNewSandbox() == nil {
		return hint
	}

	templateID := strings.TrimSpace(hint.GetNewSandbox().GetTemplateId())
	canonicalID, ok := canonicalSnapshotID(templateID)
	if !ok {
		if !isSnapshotAlias(templateID) {
			hint.LocalityRequirements = nil
			return hint
		}

		release, ok := s.tryAcquireTemplateAliasResolution()
		if !ok {
			hint.LocalityRequirements = nil
			return hint
		}
		defer release()

		var err error
		lookupCtx, cancel, ok := s.templateAliasLookupContext(ctx)
		if !ok {
			hint.LocalityRequirements = nil
			return hint
		}
		canonicalID, err = s.resolveTemplateAlias(lookupCtx, incoming, templateID)
		cancel()
		if err != nil {
			s.logger.Debug("snapshot alias locality resolution unavailable",
				zap.String("template_id", templateID),
				zap.Error(err),
			)
			hint.LocalityRequirements = nil
			return hint
		}
	}

	hint.LocalityRequirements = snapshotLocalityRequirements(canonicalID)
	return hint
}

func (s *Server) tryAcquireTemplateAliasResolution() (func(), bool) {
	slots := s.templateAliasResolutionSlots
	if slots == nil {
		return func() {}, true
	}
	select {
	case slots <- struct{}{}:
		return func() { <-slots }, true
	default:
		return func() {}, false
	}
}

func (s *Server) templateAliasLookupContext(parent context.Context) (context.Context, context.CancelFunc, bool) {
	lookupTimeout := s.templateAliasLookupTimeout
	if lookupTimeout <= 0 {
		lookupTimeout = defaultTemplateAliasLookupTimeout
	}

	reserve := s.templateAliasScheduleReserve
	if reserve < 0 {
		reserve = 0
	}
	if deadline, ok := parent.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= reserve {
			return nil, func() {}, false
		}
		if available := remaining - reserve; lookupTimeout > available {
			lookupTimeout = available
		}
	}
	if lookupTimeout <= 0 {
		return nil, func() {}, false
	}

	lookupCtx, cancel := context.WithTimeout(parent, lookupTimeout)
	return lookupCtx, cancel, true
}

func canonicalSnapshotID(value string) (string, bool) {
	parsed, err := uuid.Parse(strings.TrimSpace(value))
	if err != nil {
		return "", false
	}
	return parsed.String(), true
}

func isSnapshotAlias(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if !(char == '-' ||
			char == '_' ||
			(char >= '0' && char <= '9') ||
			(char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z')) {
			return false
		}
	}
	return true
}

func (s *Server) resolveTemplateAlias(
	ctx context.Context,
	incoming *http.Request,
	alias string,
) (string, error) {
	rpcStart := time.Now()
	resp, err := s.scheduler.ListNodes(ctx, &schedulerv1.ListNodesRequest{})
	recordGatewaySchedulerRPC("ListNodes", rpcStart, err)
	if err != nil {
		return "", fmt.Errorf("list nodes for template alias resolution: %w", err)
	}

	nodes := make([]*schedulerv1.Node, 0, len(resp.GetNodes()))
	seenEndpoints := make(map[string]struct{}, len(resp.GetNodes()))
	for _, node := range resp.GetNodes() {
		if node == nil || strings.TrimSpace(node.GetEndpoint()) == "" {
			continue
		}
		endpoint := strings.TrimSpace(node.GetEndpoint())
		if _, seen := seenEndpoints[endpoint]; seen {
			continue
		}
		seenEndpoints[endpoint] = struct{}{}
		nodes = append(nodes, node)
	}
	if len(nodes) == 0 {
		return "", fmt.Errorf("no nodes available for template alias resolution")
	}

	type result struct {
		canonicalID string
		err         error
	}
	lookupCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	concurrency := s.templateAliasLookupConcurrency
	if concurrency <= 0 {
		concurrency = defaultTemplateAliasLookupConcurrency
	}
	if concurrency > len(nodes) {
		concurrency = len(nodes)
	}
	results := make(chan result, concurrency)
	nextNode := make(chan *schedulerv1.Node)
	var workers sync.WaitGroup
	workers.Add(concurrency)
	for range concurrency {
		go func() {
			defer workers.Done()
			for {
				var node *schedulerv1.Node
				var ok bool
				select {
				case node, ok = <-nextNode:
					if !ok {
						return
					}
				case <-lookupCtx.Done():
					return
				}

				canonicalID, err := s.resolveTemplateAliasOnNode(lookupCtx, incoming, node, alias)
				select {
				case results <- result{canonicalID: canonicalID, err: err}:
				case <-lookupCtx.Done():
					return
				}
			}
		}()
	}
	go func() {
		defer close(nextNode)
		for _, node := range nodes {
			select {
			case nextNode <- node:
			case <-lookupCtx.Done():
				return
			}
		}
	}()
	go func() {
		workers.Wait()
		close(results)
	}()

	var firstErr error
	for resolved := range results {
		if resolved.err == nil {
			cancel()
			return resolved.canonicalID, nil
		}
		if firstErr == nil {
			firstErr = resolved.err
		}
	}
	if firstErr == nil {
		firstErr = lookupCtx.Err()
	}
	if firstErr == nil {
		firstErr = fmt.Errorf("template alias resolution produced no result")
	}
	return "", firstErr
}

func (s *Server) resolveTemplateAliasOnNode(
	ctx context.Context,
	incoming *http.Request,
	node *schedulerv1.Node,
	alias string,
) (string, error) {
	if !isSnapshotAlias(alias) {
		return "", fmt.Errorf("invalid template alias %q", alias)
	}
	path := "/templates/aliases/" + alias
	target, err := joinUpstream(strings.TrimSpace(node.GetEndpoint()), path, path, "")
	if err != nil {
		return "", fmt.Errorf("build template alias lookup URL: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return "", fmt.Errorf("build template alias lookup request: %w", err)
	}
	if incoming != nil {
		req.Header = incoming.Header.Clone()
		req.Host = incoming.Host
		injectForwardedHeaders(req.Header, incoming)
	}
	req.Header.Del("Content-Length")

	client := s.templateAliasHTTPClient
	if client == nil {
		client = s.httpClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("query template alias on node %s: %w", node.GetNodeId(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("node %s returned status %d for template alias", node.GetNodeId(), resp.StatusCode)
	}

	var body templateAliasResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("decode template alias response from node %s: %w", node.GetNodeId(), err)
	}
	canonicalID, ok := canonicalSnapshotID(body.TemplateID)
	if !ok {
		return "", fmt.Errorf("node %s returned invalid canonical template ID %q", node.GetNodeId(), body.TemplateID)
	}
	return canonicalID, nil
}
