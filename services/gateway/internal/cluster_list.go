package gateway

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
)

const maxCursorSandboxID = "ffffffff-ffff-ffff-ffff-ffffffffffff"

type listedSandbox struct {
	TemplateID  string            `json:"templateID"`
	Alias       *string           `json:"alias,omitempty"`
	SandboxID   string            `json:"sandboxID"`
	ClientID    string            `json:"clientID"`
	StartedAt   time.Time         `json:"startedAt"`
	EndAt       time.Time         `json:"endAt"`
	CPUCount    uint32            `json:"cpuCount"`
	MemoryMB    uint32            `json:"memoryMB"`
	DiskSizeMB  uint32            `json:"diskSizeMB"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	State       string            `json:"state"`
	EnvdVersion string            `json:"envdVersion"`
}

type clusterListResult struct {
	items []listedSandbox
	err   error
}

type clusterListStatusError struct {
	statusCode int
	message    string
}

func (e *clusterListStatusError) Error() string {
	return e.message
}

func isClusterListRequest(r *http.Request) bool {
	if r.Method != http.MethodGet {
		return false
	}
	switch canonicalClusterListPath(r.URL.Path) {
	case "/sandboxes", "/v2/sandboxes":
		return true
	default:
		return false
	}
}

func canonicalClusterListPath(path string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(path), "/")
	if trimmed == "" {
		return "/"
	}
	return trimmed
}

func (s *Server) handleClusterList(w http.ResponseWriter, r *http.Request, routingCtx context.Context) {
	descending := true
	if canonicalClusterListPath(r.URL.Path) == "/v2/sandboxes" {
		var err error
		descending, err = parseClusterListOrder(r)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid order: %v", err), http.StatusBadRequest)
			return
		}
	}

	rpcStart := time.Now()
	resp, err := s.scheduler.ListNodes(routingCtx, &schedulerv1.ListNodesRequest{})
	recordGatewaySchedulerRPC("ListNodes", rpcStart, err)
	if err != nil {
		s.writeSchedulerError(w, err)
		return
	}

	items, err := s.fetchClusterList(routingCtx, r, resp.GetNodes(), descending)
	if err != nil {
		var statusErr *clusterListStatusError
		if errors.As(err, &statusErr) && statusErr.statusCode >= 400 && statusErr.statusCode < 500 {
			http.Error(w, statusErr.message, statusErr.statusCode)
			return
		}

		s.logger.Warn("cluster sandbox list failed",
			zap.Error(err),
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
		)
		http.Error(w, "cluster list unavailable", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if canonicalClusterListPath(r.URL.Path) != "/v2/sandboxes" {
		s.writeJSON(w, http.StatusOK, items)
		return
	}

	limit, err := parseClusterListLimit(r)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid limit: %v", err), http.StatusBadRequest)
		return
	}

	page, nextToken, err := paginateListedSandboxes(items, r.URL.Query().Get("nextToken"), limit, descending)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid next token: %v", err), http.StatusBadRequest)
		return
	}
	if nextToken != "" {
		w.Header().Set("x-next-token", nextToken)
	}
	if clusterListIncludesRunning(r) {
		w.Header().Set("x-total-running", strconv.Itoa(runningSandboxCount(items)))
	}
	s.writeJSON(w, http.StatusOK, page)
}

func (s *Server) fetchClusterList(ctx context.Context, incoming *http.Request, nodes []*schedulerv1.Node, descending bool) ([]listedSandbox, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no scheduler nodes available")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan clusterListResult, len(nodes))
	var wg sync.WaitGroup

	for _, node := range nodes {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			items, err := s.fetchNodeClusterList(ctx, incoming, node)
			if err != nil {
				cancel()
				results <- clusterListResult{
					err: fmt.Errorf("node %s list failed: %w", node.GetNodeId(), err),
				}
				return
			}
			results <- clusterListResult{items: items}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	merged := make([]listedSandbox, 0)
	var firstErr error
	var errOnce sync.Once
	for result := range results {
		if result.err != nil {
			errOnce.Do(func() { firstErr = result.err })
			continue
		}
		merged = append(merged, result.items...)
	}
	if firstErr != nil {
		return nil, firstErr
	}

	sortListedSandboxes(merged, descending)
	return dedupListedSandboxes(merged), nil
}

func (s *Server) fetchNodeClusterList(ctx context.Context, incoming *http.Request, node *schedulerv1.Node) ([]listedSandbox, error) {
	target, err := joinUpstream(
		node.GetEndpoint(),
		incoming.URL.Path,
		requestEscapedPath(incoming),
		clusterListRawQuery(incoming),
	)
	if err != nil {
		return nil, fmt.Errorf("build upstream url: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, fmt.Errorf("build upstream request: %w", err)
	}
	req.Header = incoming.Header.Clone()
	req.Host = incoming.Host
	injectForwardedHeaders(req.Header, incoming)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("perform upstream request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &clusterListStatusError{
			statusCode: resp.StatusCode,
			message:    http.StatusText(resp.StatusCode),
		}
	}

	items := make([]listedSandbox, 0)
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		return nil, fmt.Errorf("decode upstream response: %w", err)
	}
	return items, nil
}

func clusterListRawQuery(r *http.Request) string {
	if canonicalClusterListPath(r.URL.Path) != "/v2/sandboxes" {
		return r.URL.RawQuery
	}

	query := r.URL.Query()
	query.Del("nextToken")
	query.Del("limit")
	return query.Encode()
}

func parseClusterListLimit(r *http.Request) (*int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("limit"))
	if raw == "" {
		return nil, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil {
		return nil, err
	}
	return &limit, nil
}

func parseClusterListOrder(r *http.Request) (bool, error) {
	switch r.URL.Query().Get("order") {
	case "", "desc":
		return true, nil
	case "asc":
		return false, nil
	default:
		return false, fmt.Errorf("must be asc or desc")
	}
}

func clusterListIncludesRunning(r *http.Request) bool {
	states := strings.Split(r.URL.Query().Get("state"), ",")
	return len(states) != 1 || states[0] != "paused"
}

func runningSandboxCount(items []listedSandbox) int {
	count := 0
	for _, item := range items {
		if item.State == "running" {
			count++
		}
	}
	return count
}

func sortListedSandboxes(items []listedSandbox, descending bool) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].StartedAt.Equal(items[j].StartedAt) {
			if descending {
				return items[i].SandboxID < items[j].SandboxID
			}
			return items[i].SandboxID > items[j].SandboxID
		}
		if descending {
			return items[i].StartedAt.After(items[j].StartedAt)
		}
		return items[i].StartedAt.Before(items[j].StartedAt)
	})
}

func dedupListedSandboxes(items []listedSandbox) []listedSandbox {
	if len(items) < 2 {
		return items
	}

	// TODO: When sandbox migration is supported, replace this "keep first" fallback
	// with a deterministic winner based on authoritative ownership or versioning.
	seen := make(map[string]struct{}, len(items))
	deduped := make([]listedSandbox, 0, len(items))
	for _, item := range items {
		if _, ok := seen[item.SandboxID]; ok {
			continue
		}
		seen[item.SandboxID] = struct{}{}
		deduped = append(deduped, item)
	}
	return deduped
}

func paginateListedSandboxes(items []listedSandbox, nextToken string, limit *int, descending bool) ([]listedSandbox, string, error) {
	cursorTime, cursorID, err := parseClusterListNextToken(nextToken, descending)
	if err != nil {
		return nil, "", err
	}

	page := make([]listedSandbox, 0, len(items))
	for _, item := range items {
		pastCursor := item.StartedAt.Before(cursorTime)
		pastID := item.SandboxID > cursorID
		if !descending {
			pastCursor = item.StartedAt.After(cursorTime)
			pastID = item.SandboxID < cursorID
		}
		if pastCursor || (item.StartedAt.Equal(cursorTime) && pastID) {
			page = append(page, item)
		}
	}

	if limit != nil && *limit < len(page) {
		if *limit <= 0 {
			page = page[:0]
		} else {
			page = page[:*limit]
		}
	}

	return page, nextClusterListToken(page, limit, descending), nil
}

func parseClusterListNextToken(token string, descending bool) (time.Time, string, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		if descending {
			return time.Now(), maxCursorSandboxID, nil
		}
		return time.Unix(0, 0).UTC(), "00000000-0000-0000-0000-000000000000", nil
	}

	decoded, err := base64.URLEncoding.DecodeString(token)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("error decoding cursor: %w", err)
	}

	parts := strings.Split(string(decoded), "__")
	if len(parts) != 2 && len(parts) != 3 {
		return time.Time{}, "", fmt.Errorf("invalid cursor format")
	}
	tokenDescending := true
	if len(parts) == 3 {
		if parts[2] != "asc" {
			return time.Time{}, "", fmt.Errorf("invalid cursor direction: %s", parts[2])
		}
		tokenDescending = false
	}
	if tokenDescending != descending {
		return time.Time{}, "", fmt.Errorf("cursor order does not match request")
	}

	cursorTime, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, "", fmt.Errorf("invalid timestamp format in cursor: %w", err)
	}
	if !isValidSandboxID(parts[1]) {
		return time.Time{}, "", fmt.Errorf("invalid sandbox id in cursor: %s", parts[1])
	}

	return cursorTime.UTC(), parts[1], nil
}

func nextClusterListToken(items []listedSandbox, limit *int, descending bool) string {
	if limit == nil || *limit <= 0 || len(items) != *limit {
		return ""
	}
	last := items[len(items)-1]
	raw := fmt.Sprintf("%s__%s", last.StartedAt.UTC().Format(time.RFC3339Nano), last.SandboxID)
	if !descending {
		raw += "__asc"
	}
	return base64.URLEncoding.EncodeToString([]byte(raw))
}

func isValidSandboxID(id string) bool {
	if len(id) != 36 {
		return false
	}
	for i, ch := range id {
		switch i {
		case 8, 13, 18, 23:
			if ch != '-' {
				return false
			}
		default:
			if !isHexDigit(ch) {
				return false
			}
		}
	}
	return true
}

func isHexDigit(ch rune) bool {
	return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
}
