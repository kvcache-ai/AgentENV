package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestComposeCreateRouting(t *testing.T) {
	body := `{"compose":"services: {}","cpuCount":4,"memoryMB":2048}`
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
	r.Method = http.MethodGet
	if got := requestTimeoutFor(r, time.Second); got != time.Second {
		t.Fatalf("non-create deadline changed: %v", got)
	}
}
