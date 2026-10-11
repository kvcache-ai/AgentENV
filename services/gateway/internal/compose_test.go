package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/compose"

	"google.golang.org/grpc"
)

func TestComposePlacementUsesImagesAndPreservesBody(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	for _, source := range []string{
		`{"services":{"app":{"image":"` + digest + `"}}}`,
		"services: {app: {image: '" + digest + "'}}",
		"services: {app: {image: '${IMAGE}'}, ignored: {image: 'sha256:" + strings.Repeat("b", 64) + "', profiles: [extra]}}",
		"services: {app: {image: '" + digest + "'}}\nx-note: " + strings.Repeat("x", maxHintBodyBytes),
	} {
		body, _ := json.Marshal(map[string]any{"compose": source, "composeEnv": map[string]string{"IMAGE": digest}, "metadata": map[string]string{"team": "alpha"}})
		r := newHintRequest(t, http.MethodPost, "/sandboxes-compose", string(body))
		r.Header.Set("x-agentenv-required-node", "ignored-node")
		hint, err := buildScheduleHint(r)
		if err != nil {
			t.Fatal(err)
		}
		if len(hint.GetNewColdSandbox().GetImages()) != 1 || hint.GetNewColdSandbox().GetImages()[0] != digest {
			t.Fatalf("dependencies: %v", hint)
		}
		if hint.GetNewColdSandbox().GetMetadata()["team"] != "alpha" {
			t.Fatal("metadata lost")
		}
		restored, err := io.ReadAll(r.Body)
		if err != nil || string(restored) != string(body) {
			t.Fatal("request body changed")
		}
	}
	for _, body := range []string{`{"compose":"services: {app: {build: .}}"}`, strings.Repeat("x", compose.MaxRequestBytes+1)} {
		if _, err := buildScheduleHint(newHintRequest(t, "POST", "/sandboxes-compose", body)); err == nil {
			t.Fatal("invalid request scheduled")
		}
	}
}

func TestInvalidImageSchedulingRequestsKeepAPIErrorContract(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			t.Fatal("invalid request must not allocate capacity")
			return nil, nil
		},
	}, time.Second, 1024)
	for _, request := range []struct{ method, path, body string }{
		{"POST", "/sandboxes-compose", `{"compose":"services: {app: {image: busybox, volumes: ['/etc:/host']}}"}`},
	} {
		response := httptest.NewRecorder()
		authenticatedTestHandler(server).ServeHTTP(response, httptest.NewRequest(request.method, request.path, strings.NewReader(request.body)))
		var body struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}
		if response.Code != http.StatusBadRequest || response.Header().Get("Content-Type") != "application/json" || json.Unmarshal(response.Body.Bytes(), &body) != nil || body.Code != http.StatusBadRequest || body.Message == "" {
			t.Fatalf("invalid API error: %d %s", response.Code, response.Body.String())
		}
	}
}

func TestComposeCreateRouting(t *testing.T) {
	body := `{"compose":"services: {app: {image: busybox}}","cpuCount":4,"memoryMB":2048}`
	r := httptest.NewRequest(http.MethodPost, "/sandboxes-compose", strings.NewReader(body))
	hint, err := buildScheduleHint(r)
	if err != nil || hint.GetNewColdSandbox().GetCpuCount() != 4 || hint.GetNewColdSandbox().GetMemoryMb() != 2048 {
		t.Fatalf("compose resource hint: %v, %v", hint, err)
	}
	restored, _ := io.ReadAll(r.Body)
	if string(restored) != body {
		t.Fatal("body not preserved")
	}
	if !shouldRecordAssignment(r, routeSourceSchedule, false) {
		t.Fatal("Compose creation must record a sandbox assignment")
	}
	if got := requestTimeoutFor(r, 30*time.Second); got != 330*time.Second {
		t.Fatalf("Compose deadline: %v", got)
	}
	if got := requestTimeoutFor(r, 10*time.Minute); got != 10*time.Minute {
		t.Fatalf("configured deadline shortened: %v", got)
	}
	r.URL.Path = "/sandboxes-compose/plan"
	if got := requestTimeoutFor(r, 30*time.Second); got != 60*time.Second {
		t.Fatalf("Compose planning deadline: %v", got)
	}
	if shouldRecordAssignment(r, routeSourceSchedule, false) {
		t.Fatal("planning must not record a sandbox assignment")
	}
	if hint, err := buildScheduleHint(r); err != nil || hint != nil {
		t.Fatalf("planning must not request sandbox resources: %v, %v", hint, err)
	}
	r.Method = http.MethodGet
	if got := requestTimeoutFor(r, time.Second); got != time.Second {
		t.Fatalf("non-create deadline changed: %v", got)
	}
}
