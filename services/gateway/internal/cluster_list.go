package gateway

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	payload   json.RawMessage
	sandboxID string
	startedAt time.Time
	state     string
}

type listedSandboxIndex struct {
	SandboxID string    `json:"sandboxID"`
	StartedAt time.Time `json:"startedAt"`
	State     string    `json:"state"`
}

func (s *listedSandbox) UnmarshalJSON(data []byte) error {
	var index listedSandboxIndex
	if err := json.Unmarshal(data, &index); err != nil {
		return err
	}

	// Keep the node response opaque so additive API fields survive aggregation.
	s.payload = append(s.payload[:0], data...)
	s.sandboxID = index.SandboxID
	s.startedAt = index.StartedAt
	s.state = index.State
	return nil
}

func (s listedSandbox) MarshalJSON() ([]byte, error) {
	if len(s.payload) == 0 {
		return nil, errors.New("listed sandbox payload is empty")
	}
	return s.payload, nil
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
		if item.state == "running" {
			count++
		}
	}
	return count
}

func sortListedSandboxes(items []listedSandbox, descending bool) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].startedAt.Equal(items[j].startedAt) {
			if descending {
				return items[i].sandboxID < items[j].sandboxID
			}
			return items[i].sandboxID > items[j].sandboxID
		}
		if descending {
			return items[i].startedAt.After(items[j].startedAt)
		}
		return items[i].startedAt.Before(items[j].startedAt)
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
		if _, ok := seen[item.sandboxID]; ok {
			continue
		}
		seen[item.sandboxID] = struct{}{}
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
		pastCursor := item.startedAt.Before(cursorTime)
		pastID := item.sandboxID > cursorID
		if !descending {
			pastCursor = item.startedAt.After(cursorTime)
			pastID = item.sandboxID < cursorID
		}
		if pastCursor || (item.startedAt.Equal(cursorTime) && pastID) {
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
	raw := fmt.Sprintf("%s__%s", last.startedAt.UTC().Format(time.RFC3339Nano), last.sandboxID)
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

const (
	webhookDeliveriesKind      = "deliveries"
	webhookStatsKind           = "stats"
	defaultWebhookDeliveryPage = 25
	maxWebhookDeliveryPage     = 100
	defaultWebhookStatsRange   = 24 * time.Hour
)

// webhookAggregateRequest reports whether r is
// GET /events/webhooks/{webhookID}/{deliveries|stats}.
func webhookAggregateRequest(r *http.Request) (string, bool) {
	if r.Method != http.MethodGet {
		return "", false
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) != 4 || parts[0] != "events" || parts[1] != "webhooks" || strings.TrimSpace(parts[2]) == "" {
		return "", false
	}
	switch parts[3] {
	case webhookDeliveriesKind, webhookStatsKind:
		return parts[3], true
	default:
		return "", false
	}
}

// webhookNodeStatusError carries a node's non-2xx response so client errors
// (unknown webhook, invalid cursor or range) reach the caller unchanged.
type webhookNodeStatusError struct {
	statusCode int
	body       []byte
}

func (e *webhookNodeStatusError) Error() string {
	return fmt.Sprintf("node returned %d", e.statusCode)
}

func (s *Server) handleWebhookAggregate(w http.ResponseWriter, r *http.Request, ctx context.Context, kind string) {
	var (
		rawQuery string
		limit    int
	)
	switch kind {
	case webhookDeliveriesKind:
		var err error
		if limit, err = parseWebhookDeliveryLimit(r); err != nil {
			s.writeJSON(w, http.StatusBadRequest, map[string]any{"code": 400, "message": err.Error()})
			return
		}
		// Every node applies the same cursor, filters, and limit, so the
		// first `limit` merged groups are the global page.
		rawQuery = r.URL.RawQuery
	case webhookStatsKind:
		query, err := webhookStatsQuery(r, time.Now().UTC())
		if err != nil {
			s.writeJSON(w, http.StatusBadRequest, map[string]any{"code": 400, "message": err.Error()})
			return
		}
		rawQuery = query
	}

	rpcStart := time.Now()
	resp, err := s.scheduler.ListNodes(ctx, &schedulerv1.ListNodesRequest{})
	recordGatewaySchedulerRPC("ListNodes", rpcStart, err)
	if err != nil {
		s.writeSchedulerError(w, err)
		return
	}

	bodies, err := s.fetchWebhookAggregate(ctx, r, resp.GetNodes(), rawQuery)
	var merged any
	if err == nil {
		switch kind {
		case webhookDeliveriesKind:
			merged, err = mergeWebhookDeliveries(bodies, limit, r.URL.Query().Get("orderAsc") == "true")
		case webhookStatsKind:
			merged, err = mergeWebhookStats(bodies)
		}
	}
	if err != nil {
		var statusErr *webhookNodeStatusError
		if errors.As(err, &statusErr) && statusErr.statusCode >= 400 && statusErr.statusCode < 500 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(statusErr.statusCode)
			_, _ = w.Write(statusErr.body)
			return
		}
		s.logger.Warn("webhook "+kind+" aggregation failed",
			zap.Error(err),
			zap.String("path", r.URL.Path),
		)
		s.writeJSON(w, http.StatusBadGateway, map[string]any{"code": 502, "message": "webhook " + kind + " unavailable"})
		return
	}
	s.writeJSON(w, http.StatusOK, merged)
}

func parseWebhookDeliveryLimit(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return defaultWebhookDeliveryPage, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 1 || limit > maxWebhookDeliveryPage {
		return 0, fmt.Errorf("limit must be an integer between 1 and %d", maxWebhookDeliveryPage)
	}
	return limit, nil
}

// webhookStatsQuery pins the stats range so every node reports identical
// hourly buckets, using the node defaults (the last 24 hours) when absent.
func webhookStatsQuery(r *http.Request, now time.Time) (string, error) {
	query := r.URL.Query()
	parse := func(name string, fallback time.Time) (time.Time, error) {
		raw := query.Get(name)
		if raw == "" {
			return fallback, nil
		}
		value, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return time.Time{}, fmt.Errorf("%s must be an RFC 3339 date-time", name)
		}
		return value.UTC(), nil
	}
	end, err := parse("end", now)
	if err != nil {
		return "", err
	}
	start, err := parse("start", end.Add(-defaultWebhookStatsRange))
	if err != nil {
		return "", err
	}
	return url.Values{
		"start": {start.Format(time.RFC3339Nano)},
		"end":   {end.Format(time.RFC3339Nano)},
	}.Encode(), nil
}

// fetchWebhookAggregate returns every node's response body, failing when any
// node fails: partial history would silently under-report deliveries.
func (s *Server) fetchWebhookAggregate(ctx context.Context, incoming *http.Request, nodes []*schedulerv1.Node, rawQuery string) ([][]byte, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no scheduler nodes available")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	bodies := make([][]byte, len(nodes))
	errs := make([]error, len(nodes))
	var wg sync.WaitGroup
	for i, node := range nodes {
		wg.Add(1)
		go func(i int, node *schedulerv1.Node) {
			defer wg.Done()
			body, err := s.fetchNodeWebhookAggregate(ctx, incoming, node, rawQuery)
			if err != nil {
				errs[i] = fmt.Errorf("node %s: %w", node.GetNodeId(), err)
				return
			}
			bodies[i] = body
		}(i, node)
	}
	wg.Wait()
	// Prefer a client error (e.g. unknown webhook) over transport failures.
	for _, err := range errs {
		var statusErr *webhookNodeStatusError
		if errors.As(err, &statusErr) && statusErr.statusCode >= 400 && statusErr.statusCode < 500 {
			return nil, err
		}
	}
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return bodies, nil
}

func (s *Server) fetchNodeWebhookAggregate(ctx context.Context, incoming *http.Request, node *schedulerv1.Node, rawQuery string) ([]byte, error) {
	target, err := joinUpstream(node.GetEndpoint(), incoming.URL.Path, requestEscapedPath(incoming), rawQuery)
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
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read upstream response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &webhookNodeStatusError{statusCode: resp.StatusCode, body: body}
	}
	return body, nil
}

// webhookDeliveryGroupKey mirrors the node's page order: the first attempt's
// timestamp in milliseconds, then the event ID.
type webhookDeliveryGroupKey struct {
	millis  int64
	eventID string
}

func (k webhookDeliveryGroupKey) less(other webhookDeliveryGroupKey) bool {
	if k.millis != other.millis {
		return k.millis < other.millis
	}
	return k.eventID < other.eventID
}

type webhookDeliveryGroup struct {
	payload json.RawMessage
	key     webhookDeliveryGroupKey
}

type webhookDeliveriesPage struct {
	Data       []json.RawMessage `json:"data"`
	NextCursor *string           `json:"nextCursor"`
}

func mergeWebhookDeliveries(bodies [][]byte, limit int, ascending bool) (webhookDeliveriesPage, error) {
	var groups []webhookDeliveryGroup
	hasMore := false
	for _, body := range bodies {
		var page struct {
			Data       []json.RawMessage `json:"data"`
			NextCursor *string           `json:"nextCursor"`
		}
		if err := json.Unmarshal(body, &page); err != nil {
			return webhookDeliveriesPage{}, fmt.Errorf("decode deliveries page: %w", err)
		}
		hasMore = hasMore || page.NextCursor != nil
		for _, payload := range page.Data {
			// Keep the group opaque so additive API fields survive merging.
			var index struct {
				EventID  string `json:"eventId"`
				Attempts []struct {
					Timestamp time.Time `json:"timestamp"`
				} `json:"attempts"`
			}
			if err := json.Unmarshal(payload, &index); err != nil {
				return webhookDeliveriesPage{}, fmt.Errorf("decode delivery group: %w", err)
			}
			if len(index.Attempts) == 0 {
				return webhookDeliveriesPage{}, fmt.Errorf("delivery group %s has no attempts", index.EventID)
			}
			first := index.Attempts[0].Timestamp
			for _, attempt := range index.Attempts[1:] {
				if attempt.Timestamp.Before(first) {
					first = attempt.Timestamp
				}
			}
			groups = append(groups, webhookDeliveryGroup{
				payload: payload,
				key:     webhookDeliveryGroupKey{millis: first.UnixMilli(), eventID: index.EventID},
			})
		}
	}

	sort.Slice(groups, func(i, j int) bool {
		if ascending {
			return groups[i].key.less(groups[j].key)
		}
		return groups[j].key.less(groups[i].key)
	})
	if len(groups) > limit {
		hasMore = true
		groups = groups[:limit]
	}

	page := webhookDeliveriesPage{Data: make([]json.RawMessage, 0, len(groups))}
	for _, group := range groups {
		page.Data = append(page.Data, group.payload)
	}
	if hasMore && len(groups) > 0 {
		last := groups[len(groups)-1].key
		cursor := fmt.Sprintf("%d_%s", last.millis, last.eventID)
		page.NextCursor = &cursor
	}
	return page, nil
}

type webhookDurationStats struct {
	Minimum float64 `json:"minimum"`
	Average float64 `json:"average"`
	Maximum float64 `json:"maximum"`
}

type webhookStatsBucket struct {
	Timestamp  time.Time            `json:"timestamp"`
	Total      int64                `json:"total"`
	Failed     int64                `json:"failed"`
	DurationMs webhookDurationStats `json:"durationMs"`
}

type webhookDeliveryStats struct {
	Buckets    []webhookStatsBucket `json:"buckets"`
	Total      int64                `json:"total"`
	Failed     int64                `json:"failed"`
	DurationMs webhookDurationStats `json:"durationMs"`
}

// combineDurations merges two duration summaries over `total` attempts each.
// Summaries over zero attempts are all-zero placeholders and are ignored.
func combineDurations(a webhookDurationStats, aTotal int64, b webhookDurationStats, bTotal int64) webhookDurationStats {
	switch {
	case bTotal == 0:
		return a
	case aTotal == 0:
		return b
	}
	sum := a.Average*float64(aTotal) + b.Average*float64(bTotal)
	return webhookDurationStats{
		Minimum: min(a.Minimum, b.Minimum),
		Average: sum / float64(aTotal+bTotal),
		Maximum: max(a.Maximum, b.Maximum),
	}
}

func mergeWebhookStats(bodies [][]byte) (webhookDeliveryStats, error) {
	var merged webhookDeliveryStats
	buckets := make(map[int64]*webhookStatsBucket)
	for _, body := range bodies {
		var stats webhookDeliveryStats
		if err := json.Unmarshal(body, &stats); err != nil {
			return webhookDeliveryStats{}, fmt.Errorf("decode delivery stats: %w", err)
		}
		merged.DurationMs = combineDurations(merged.DurationMs, merged.Total, stats.DurationMs, stats.Total)
		merged.Total += stats.Total
		merged.Failed += stats.Failed
		for _, bucket := range stats.Buckets {
			key := bucket.Timestamp.UnixNano()
			existing, ok := buckets[key]
			if !ok {
				bucket.Timestamp = bucket.Timestamp.UTC()
				copied := bucket
				buckets[key] = &copied
				continue
			}
			existing.DurationMs = combineDurations(existing.DurationMs, existing.Total, bucket.DurationMs, bucket.Total)
			existing.Total += bucket.Total
			existing.Failed += bucket.Failed
		}
	}
	merged.Buckets = make([]webhookStatsBucket, 0, len(buckets))
	for _, bucket := range buckets {
		merged.Buckets = append(merged.Buckets, *bucket)
	}
	sort.Slice(merged.Buckets, func(i, j int) bool {
		return merged.Buckets[i].Timestamp.Before(merged.Buckets[j].Timestamp)
	})
	return merged, nil
}
