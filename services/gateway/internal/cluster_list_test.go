package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
)

func TestParseClusterListOrder(t *testing.T) {
	for query, wantDescending := range map[string]bool{
		"":           true,
		"order=desc": true,
		"order=asc":  false,
	} {
		r := httptest.NewRequest("GET", "/v2/sandboxes?"+query, nil)
		got, err := parseClusterListOrder(r)
		if err != nil {
			t.Fatalf("parse order %q failed: %v", query, err)
		}
		if got != wantDescending {
			t.Errorf("parse order %q = %t, want %t", query, got, wantDescending)
		}
	}

	r := httptest.NewRequest("GET", "/v2/sandboxes?order=sideways", nil)
	if _, err := parseClusterListOrder(r); err == nil {
		t.Fatal("expected invalid order to fail")
	}
}

func TestClusterListIncludesRunning(t *testing.T) {
	for query, want := range map[string]bool{
		"":                       true,
		"state=running":          true,
		"state=paused":           false,
		"state=running%2Cpaused": true,
	} {
		r := httptest.NewRequest("GET", "/v2/sandboxes?"+query, nil)
		if got := clusterListIncludesRunning(r); got != want {
			t.Errorf("query %q includes running = %t, want %t", query, got, want)
		}
	}
}

func TestParseClusterListNextTokenRejectsOrderMismatch(t *testing.T) {
	items := []listedSandbox{{
		sandboxID: "00000000-0000-0000-0000-000000000001",
		startedAt: time.Unix(1, 0).UTC(),
	}}
	limit := 1

	ascToken := nextClusterListToken(items, &limit, false)
	if _, _, err := parseClusterListNextToken(ascToken, true); err == nil {
		t.Fatal("expected ascending cursor to be rejected for descending request")
	}
	if _, _, err := parseClusterListNextToken(ascToken, false); err != nil {
		t.Fatalf("matching ascending cursor rejected: %v", err)
	}

	descToken := nextClusterListToken(items, &limit, true)
	if _, _, err := parseClusterListNextToken(descToken, false); err == nil {
		t.Fatal("expected descending cursor to be rejected for ascending request")
	}
}

const testWebhookID = "0192f3a1-7c2e-7b44-9a51-3f2a8b7c6d5e"

// fakeDeliveryNode serves one node's delivery history with the node API's
// ordering and cursor semantics (first-attempt millis, then event id).
type fakeDeliveryNode struct {
	groups []fakeDeliveryGroup
	mu     sync.Mutex
	query  []url.Values
}

type fakeDeliveryGroup struct {
	eventID string
	at      time.Time
}

func (n *fakeDeliveryNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n.mu.Lock()
	n.query = append(n.query, r.URL.Query())
	n.mu.Unlock()
	if r.Header.Get(headerAPIKey) != testAPIKey {
		http.Error(w, "missing auth", http.StatusUnauthorized)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/stats") {
		http.Error(w, "unexpected", http.StatusInternalServerError)
		return
	}
	query := r.URL.Query()
	ascending := query.Get("orderAsc") == "true"
	limit := 25
	fmt.Sscan(query.Get("limit"), &limit)
	groups := append([]fakeDeliveryGroup(nil), n.groups...)
	key := func(g fakeDeliveryGroup) webhookDeliveryGroupKey {
		return webhookDeliveryGroupKey{millis: g.at.UnixMilli(), eventID: g.eventID}
	}
	sort.Slice(groups, func(i, j int) bool {
		if ascending {
			return key(groups[i]).less(key(groups[j]))
		}
		return key(groups[j]).less(key(groups[i]))
	})
	if cursor := query.Get("cursor"); cursor != "" {
		var millis int64
		var eventID string
		parts := strings.SplitN(cursor, "_", 2)
		fmt.Sscan(parts[0], &millis)
		eventID = parts[1]
		bound := webhookDeliveryGroupKey{millis: millis, eventID: eventID}
		filtered := groups[:0]
		for _, g := range groups {
			if (ascending && bound.less(key(g))) || (!ascending && key(g).less(bound)) {
				filtered = append(filtered, g)
			}
		}
		groups = filtered
	}
	var next *string
	if len(groups) > limit {
		last := key(groups[limit-1])
		cursor := fmt.Sprintf("%d_%s", last.millis, last.eventID)
		next = &cursor
		groups = groups[:limit]
	}
	data := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		data = append(data, map[string]any{
			"eventId":   g.eventID,
			"eventType": "sandbox.lifecycle.created",
			"sandboxId": "sbx-" + g.eventID[len(g.eventID)-2:],
			"attempts": []map[string]any{
				{"id": g.eventID, "timestamp": g.at.Add(time.Second).Format(time.RFC3339Nano), "nodeExtra": true},
				{"id": g.eventID, "timestamp": g.at.Format(time.RFC3339Nano)},
			},
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"data": data, "nextCursor": next})
}

func webhookTestServer(t *testing.T, endpoints ...string) http.Handler {
	t.Helper()
	nodes := make([]*schedulerv1.Node, 0, len(endpoints))
	for i, endpoint := range endpoints {
		nodes = append(nodes, &schedulerv1.Node{NodeId: fmt.Sprintf("node-%d", i), Endpoint: endpoint})
	}
	server := newTestServer(t, stubSchedulerClient{
		listNodesFunc: func(context.Context, *schedulerv1.ListNodesRequest, ...grpc.CallOption) (*schedulerv1.ListNodesResponse, error) {
			return &schedulerv1.ListNodesResponse{Nodes: nodes}, nil
		},
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			t.Fatal("webhook aggregation must not schedule a single node")
			return nil, nil
		},
	}, time.Second, 1<<20)
	return authenticatedTestHandler(server)
}

func eventID(n int) string {
	return fmt.Sprintf("00000000-0000-0000-0000-0000000000%02d", n)
}

func getWebhookPage(t *testing.T, handler http.Handler, query string) (int, map[string]json.RawMessage) {
	t.Helper()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/events/webhooks/"+testWebhookID+"/deliveries?"+query, nil)
	handler.ServeHTTP(recorder, request)
	var body map[string]json.RawMessage
	_ = json.Unmarshal(recorder.Body.Bytes(), &body)
	return recorder.Code, body
}

func pageEventIDs(t *testing.T, body map[string]json.RawMessage) ([]string, *string) {
	t.Helper()
	var data []struct {
		EventID  string           `json:"eventId"`
		Attempts []map[string]any `json:"attempts"`
	}
	if err := json.Unmarshal(body["data"], &data); err != nil {
		t.Fatalf("decode data: %v", err)
	}
	ids := make([]string, 0, len(data))
	for _, group := range data {
		if len(group.Attempts) != 2 || group.Attempts[0]["nodeExtra"] != true {
			t.Fatalf("group %s was not passed through verbatim: %v", group.EventID, group.Attempts)
		}
		ids = append(ids, group.EventID)
	}
	var next *string
	_ = json.Unmarshal(body["nextCursor"], &next)
	return ids, next
}

func TestWebhookAggregateRequest(t *testing.T) {
	for _, tc := range []struct {
		method, path, kind string
		ok                 bool
	}{
		{http.MethodGet, "/events/webhooks/abc/deliveries", "deliveries", true},
		{http.MethodGet, "/events/webhooks/abc/stats/", "stats", true},
		{http.MethodPost, "/events/webhooks/abc/stats", "", false},
		{http.MethodGet, "/events/webhooks/abc", "", false},
		{http.MethodGet, "/events/webhooks", "", false},
		{http.MethodGet, "/events/webhooks//stats", "", false},
		{http.MethodGet, "/events/sandboxes/abc/stats", "", false},
	} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		kind, ok := webhookAggregateRequest(r)
		if kind != tc.kind || ok != tc.ok {
			t.Errorf("%s %s = (%q, %t), want (%q, %t)", tc.method, tc.path, kind, ok, tc.kind, tc.ok)
		}
	}
}

func TestWebhookDeliveriesMergeAndPaginateAcrossNodes(t *testing.T) {
	base := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	nodeA := &fakeDeliveryNode{}
	nodeB := &fakeDeliveryNode{}
	// Interleave events across nodes; events 3 and 4 share a millisecond.
	for i := 1; i <= 7; i++ {
		at := base.Add(time.Duration(i) * time.Minute)
		if i == 4 {
			at = base.Add(3 * time.Minute)
		}
		group := fakeDeliveryGroup{eventID: eventID(i), at: at}
		if i%2 == 0 {
			nodeA.groups = append(nodeA.groups, group)
		} else {
			nodeB.groups = append(nodeB.groups, group)
		}
	}
	a := httptest.NewServer(nodeA)
	defer a.Close()
	b := httptest.NewServer(nodeB)
	defer b.Close()
	handler := webhookTestServer(t, a.URL, b.URL)

	for _, tc := range []struct {
		name  string
		query string
		want  []string
	}{
		{"descending", "limit=3", []string{eventID(7), eventID(6), eventID(5), eventID(4), eventID(3), eventID(2), eventID(1)}},
		{"ascending", "limit=2&orderAsc=true", []string{eventID(1), eventID(2), eventID(3), eventID(4), eventID(5), eventID(6), eventID(7)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var seen []string
			query := tc.query
			for pages := 0; ; pages++ {
				if pages > 10 {
					t.Fatal("pagination did not terminate")
				}
				status, body := getWebhookPage(t, handler, query)
				if status != http.StatusOK {
					t.Fatalf("status = %d, body = %v", status, body)
				}
				ids, next := pageEventIDs(t, body)
				seen = append(seen, ids...)
				if next == nil {
					break
				}
				query = tc.query + "&cursor=" + url.QueryEscape(*next)
			}
			if !equalStrings(seen, tc.want) {
				t.Fatalf("events = %v, want %v", seen, tc.want)
			}
		})
	}

	// Filters reach every node unchanged.
	getWebhookPage(t, handler, "deliveryStatus=failed&eventType=sandbox.lifecycle.killed")
	for _, node := range []*fakeDeliveryNode{nodeA, nodeB} {
		last := node.query[len(node.query)-1]
		if last.Get("deliveryStatus") != "failed" || last.Get("eventType") != "sandbox.lifecycle.killed" {
			t.Errorf("node query = %v, want filters forwarded", last)
		}
	}
}

func TestWebhookDeliveriesRejectsInvalidLimit(t *testing.T) {
	handler := webhookTestServer(t, "http://127.0.0.1:1")
	for _, limit := range []string{"0", "101", "abc"} {
		if status, _ := getWebhookPage(t, handler, "limit="+limit); status != http.StatusBadRequest {
			t.Errorf("limit=%s status = %d, want 400", limit, status)
		}
	}
}

func TestWebhookAggregatePropagatesNodeClientErrors(t *testing.T) {
	healthy := httptest.NewServer(&fakeDeliveryNode{})
	defer healthy.Close()
	missing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":404,"message":"webhook not found"}`))
	}))
	defer missing.Close()
	status, body := getWebhookPage(t, webhookTestServer(t, healthy.URL, missing.URL), "")
	if status != http.StatusNotFound || string(body["message"]) != `"webhook not found"` {
		t.Fatalf("status = %d body = %v, want node 404 passed through", status, body)
	}

	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer failing.Close()
	if status, _ := getWebhookPage(t, webhookTestServer(t, healthy.URL, failing.URL), ""); status != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 when a node fails", status)
	}
}

func TestWebhookStatsMergeAcrossNodes(t *testing.T) {
	hour := time.Date(2026, 10, 8, 10, 0, 0, 0, time.UTC)
	node := func(total, failed int64, minimum, average, maximum float64, buckets []webhookStatsBucket) (*httptest.Server, *[]url.Values) {
		var queries []url.Values
		var mu sync.Mutex
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			queries = append(queries, r.URL.Query())
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(webhookDeliveryStats{
				Buckets:    buckets,
				Total:      total,
				Failed:     failed,
				DurationMs: webhookDurationStats{Minimum: minimum, Average: average, Maximum: maximum},
			})
		}))
		return server, &queries
	}
	empty := webhookDurationStats{}
	a, aQueries := node(3, 1, 10, 20, 40, []webhookStatsBucket{
		{Timestamp: hour, Total: 2, Failed: 1, DurationMs: webhookDurationStats{Minimum: 10, Average: 15, Maximum: 20}},
		{Timestamp: hour.Add(time.Hour), Total: 1, DurationMs: webhookDurationStats{Minimum: 40, Average: 40, Maximum: 40}},
	})
	defer a.Close()
	b, bQueries := node(1, 1, 100, 100, 100, []webhookStatsBucket{
		{Timestamp: hour, Total: 1, Failed: 1, DurationMs: webhookDurationStats{Minimum: 100, Average: 100, Maximum: 100}},
		{Timestamp: hour.Add(time.Hour), DurationMs: empty},
	})
	defer b.Close()
	handler := webhookTestServer(t, a.URL, b.URL)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/events/webhooks/"+testWebhookID+"/stats", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body)
	}
	var stats webhookDeliveryStats
	if err := json.Unmarshal(recorder.Body.Bytes(), &stats); err != nil {
		t.Fatal(err)
	}
	if stats.Total != 4 || stats.Failed != 2 {
		t.Fatalf("totals = %d/%d, want 4/2", stats.Total, stats.Failed)
	}
	if stats.DurationMs != (webhookDurationStats{Minimum: 10, Average: 40, Maximum: 100}) {
		t.Fatalf("duration = %+v", stats.DurationMs)
	}
	if len(stats.Buckets) != 2 || stats.Buckets[0].Total != 3 || stats.Buckets[0].Failed != 2 {
		t.Fatalf("buckets = %+v", stats.Buckets)
	}
	if got := stats.Buckets[0].DurationMs; got != (webhookDurationStats{Minimum: 10, Average: 130.0 / 3, Maximum: 100}) {
		t.Fatalf("first bucket duration = %+v", got)
	}
	// An empty bucket's zero placeholder must not drag the minimum down.
	if got := stats.Buckets[1].DurationMs; got != (webhookDurationStats{Minimum: 40, Average: 40, Maximum: 40}) {
		t.Fatalf("second bucket duration = %+v", got)
	}

	// Both nodes received the same pinned range, so their buckets line up.
	qa, qb := (*aQueries)[0], (*bQueries)[0]
	if qa.Get("start") == "" || qa.Get("start") != qb.Get("start") || qa.Get("end") != qb.Get("end") {
		t.Fatalf("node ranges differ: %v vs %v", qa, qb)
	}
	start, _ := time.Parse(time.RFC3339Nano, qa.Get("start"))
	end, _ := time.Parse(time.RFC3339Nano, qa.Get("end"))
	if end.Sub(start) != 24*time.Hour {
		t.Fatalf("default range = %v, want 24h", end.Sub(start))
	}
}

func TestWebhookStatsQueryPinsRange(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	r := httptest.NewRequest(http.MethodGet, "/x?end=2026-10-01T00:00:00%2B02:00", nil)
	query, err := webhookStatsQuery(r, now)
	if err != nil {
		t.Fatal(err)
	}
	values, _ := url.ParseQuery(query)
	if values.Get("end") != "2026-09-30T22:00:00Z" || values.Get("start") != "2026-09-29T22:00:00Z" {
		t.Fatalf("query = %v", values)
	}
	if _, err := webhookStatsQuery(httptest.NewRequest(http.MethodGet, "/x?start=yesterday", nil), now); err == nil {
		t.Fatal("expected invalid start to fail")
	}
}

func TestGatewayRouteLabelWebhooks(t *testing.T) {
	for path, want := range map[string]string{
		"/events/webhooks":                "/events/webhooks",
		"/events/webhooks/abc":            "/events/webhooks/{webhook_id}",
		"/events/webhooks/abc/deliveries": "/events/webhooks/{webhook_id}/deliveries",
		"/events/webhooks/abc/stats":      "/events/webhooks/{webhook_id}/stats",
		"/events/webhooks/abc/other":      "unmatched",
	} {
		if got := gatewayRouteLabel(path); got != want {
			t.Errorf("gatewayRouteLabel(%q) = %q, want %q", path, got, want)
		}
	}
}
