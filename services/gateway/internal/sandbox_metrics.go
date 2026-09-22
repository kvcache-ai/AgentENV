package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	schedulerv1 "agentenv/services/api/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func isSandboxMetricsListRequest(r *http.Request) bool {
	return r.Method == http.MethodGet && r.URL.Path == "/sandboxes/metrics"
}

func parseSandboxMetricsIDs(r *http.Request) ([]string, error) {
	values := r.URL.Query()["sandbox_ids"]
	if len(values) != 1 {
		return nil, fmt.Errorf("sandbox_ids must be a single comma-separated list")
	}
	ids := strings.Split(values[0], ",")
	if len(ids) > 100 {
		return nil, fmt.Errorf("sandbox_ids must contain at most 100 IDs")
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if strings.TrimSpace(id) == "" || strings.TrimSpace(id) != id || seen[id] {
			return nil, fmt.Errorf("sandbox_ids must contain distinct, non-empty IDs without surrounding whitespace")
		}
		if !isValidSandboxID(id) {
			return nil, fmt.Errorf("sandbox_ids must contain valid sandbox IDs")
		}
		seen[id] = true
	}
	return ids, nil
}

type sandboxMetricsResponse struct {
	Sandboxes map[string]json.RawMessage `json:"sandboxes"`
}

type metricsNodeGroup struct {
	node *schedulerv1.Node
	ids  []string
}

func (s *Server) handleSandboxMetricsList(w http.ResponseWriter, r *http.Request, ctx context.Context) {
	ids, err := parseSandboxMetricsIDs(r)
	if err != nil {
		s.writeJSON(w, http.StatusBadRequest, map[string]any{"code": 400, "message": err.Error()})
		return
	}
	// Bound scheduler lookups and HTTP fanout independently. Metrics queries
	// never allocate a node or establish new sandbox assignments.
	groups := make(map[string]*metricsNodeGroup)
	var mu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup
	slots := make(chan struct{}, 16)
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				mu.Lock()
				if firstErr == nil {
					firstErr = ctx.Err()
				}
				mu.Unlock()
				return
			}
			defer func() { <-slots }()
			resp, err := s.queryOnlyScheduler.LookupNode(ctx, &schedulerv1.LookupNodeRequest{SandboxId: id})
			if status.Code(err) == codes.NotFound {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			node := resp.GetNode()
			if node == nil || node.GetEndpoint() == "" {
				if firstErr == nil {
					firstErr = fmt.Errorf("missing metrics node")
				}
				return
			}
			group := groups[node.GetNodeId()]
			if group == nil {
				group = &metricsNodeGroup{node: node}
				groups[node.GetNodeId()] = group
			}
			group.ids = append(group.ids, id)
		}(id)
	}
	wg.Wait()
	if firstErr != nil {
		s.writeSchedulerError(w, firstErr)
		return
	}

	merged := sandboxMetricsResponse{Sandboxes: make(map[string]json.RawMessage)}
	for _, group := range groups {
		wg.Add(1)
		go func(group *metricsNodeGroup) {
			defer wg.Done()
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				mu.Lock()
				if firstErr == nil {
					firstErr = ctx.Err()
				}
				mu.Unlock()
				return
			}
			defer func() { <-slots }()
			result, err := s.fetchNodeSandboxMetrics(ctx, r, group)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			// Only merge IDs assigned to this node and requested by the caller.
			for _, id := range group.ids {
				if metric, ok := result.Sandboxes[id]; ok {
					merged.Sandboxes[id] = metric
				}
			}
		}(group)
	}
	wg.Wait()
	if firstErr != nil {
		// Do not disguise an unavailable node as missing guest samples.
		s.writeJSON(w, http.StatusInternalServerError, map[string]any{"code": 500, "message": "sandbox metrics unavailable"})
		return
	}
	s.writeJSON(w, http.StatusOK, merged)
}

func (s *Server) fetchNodeSandboxMetrics(ctx context.Context, incoming *http.Request, group *metricsNodeGroup) (sandboxMetricsResponse, error) {
	var result sandboxMetricsResponse
	query := url.Values{"sandbox_ids": {strings.Join(group.ids, ",")}}
	target, err := joinUpstream(group.node.GetEndpoint(), "/sandboxes/metrics", "", query.Encode())
	if err != nil {
		return result, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return result, err
	}
	req.Header = incoming.Header.Clone()
	req.Host = incoming.Host
	injectForwardedHeaders(req.Header, incoming)
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return result, fmt.Errorf("metrics upstream returned %d", resp.StatusCode)
	}
	err = json.NewDecoder(io.LimitReader(resp.Body, 1024*1024)).Decode(&result)
	if err == nil && result.Sandboxes == nil {
		err = fmt.Errorf("missing sandboxes in metrics response")
	}
	return result, err
}
