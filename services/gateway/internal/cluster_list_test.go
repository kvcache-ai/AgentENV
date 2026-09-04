package gateway

import (
	"net/http/httptest"
	"testing"
	"time"
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
		SandboxID: "00000000-0000-0000-0000-000000000001",
		StartedAt: time.Unix(1, 0).UTC(),
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
