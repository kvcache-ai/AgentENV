package gateway

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"slices"
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
		sandboxID: "00000000-0000-0000-0000-000000000001",
		startedAt: time.Unix(1, 0).UTC(),
	}}

	ascToken := encodeClusterListToken(items[0], false)
	if _, _, err := parseClusterListNextToken(ascToken, true); err == nil {
		t.Fatal("expected ascending cursor to be rejected for descending request")
	}
	if _, _, err := parseClusterListNextToken(ascToken, false); err != nil {
		t.Fatalf("matching ascending cursor rejected: %v", err)
	}

	descToken := encodeClusterListToken(items[0], true)
	if _, _, err := parseClusterListNextToken(descToken, false); err == nil {
		t.Fatal("expected descending cursor to be rejected for ascending request")
	}
}

type paginationVectors struct {
	Tokens []struct {
		Name      string `json:"name"`
		Token     string `json:"token"`
		Canonical bool   `json:"canonical"`
		Cursor    *struct {
			Time  time.Time `json:"time"`
			ID    string    `json:"id"`
			Order string    `json:"order"`
		} `json:"cursor"`
	} `json:"tokens"`
	Items []struct {
		ID        string    `json:"id"`
		StartedAt time.Time `json:"startedAt"`
	} `json:"items"`
	Pages []struct {
		Name  string   `json:"name"`
		Order string   `json:"order"`
		Limit *int     `json:"limit"`
		After *string  `json:"after"`
		Want  []string `json:"want"`
		Next  *string  `json:"next"`
	} `json:"pages"`
}

// The node API runs the same vectors in src/api/impls/pagination.rs.
func TestClusterListMatchesSharedPaginationVectors(t *testing.T) {
	raw, err := os.ReadFile("../../../src/api/testdata/pagination.json")
	if err != nil {
		t.Fatalf("read pagination vectors: %v", err)
	}
	var vectors paginationVectors
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatalf("parse pagination vectors: %v", err)
	}

	for _, tc := range vectors.Tokens {
		t.Run("token/"+tc.Name, func(t *testing.T) {
			if tc.Cursor == nil {
				for _, descending := range []bool{true, false} {
					if _, _, err := parseClusterListNextToken(tc.Token, descending); err == nil {
						t.Fatalf("token accepted with descending=%v, want rejection", descending)
					}
				}
				return
			}
			descending := tc.Cursor.Order == "desc"
			gotTime, gotID, err := parseClusterListNextToken(tc.Token, descending)
			if err != nil {
				t.Fatalf("parse token: %v", err)
			}
			if !gotTime.Equal(tc.Cursor.Time) || gotID != tc.Cursor.ID {
				t.Fatalf("parsed (%s, %s), want (%s, %s)", gotTime, gotID, tc.Cursor.Time, tc.Cursor.ID)
			}
			if _, _, err := parseClusterListNextToken(tc.Token, !descending); err == nil {
				t.Fatal("token accepted for the opposite order")
			}
			if tc.Canonical {
				got := encodeClusterListToken(listedSandbox{sandboxID: tc.Cursor.ID, startedAt: tc.Cursor.Time}, descending)
				if got != tc.Token {
					t.Fatalf("encoded %q, want %q", got, tc.Token)
				}
			}
		})
	}

	for _, tc := range vectors.Pages {
		t.Run("page/"+tc.Name, func(t *testing.T) {
			descending := tc.Order == "desc"
			items := make([]listedSandbox, 0, len(vectors.Items))
			for _, item := range vectors.Items {
				items = append(items, listedSandbox{sandboxID: item.ID, startedAt: item.StartedAt})
			}
			sortListedSandboxes(items, descending)
			after := ""
			if tc.After != nil {
				after = *tc.After
			}
			page, next, err := paginateListedSandboxes(items, after, tc.Limit, descending)
			if err != nil {
				t.Fatalf("paginate: %v", err)
			}
			got := make([]string, 0, len(page))
			for _, item := range page {
				got = append(got, item.sandboxID)
			}
			if !slices.Equal(got, tc.Want) {
				t.Fatalf("page = %v, want %v", got, tc.Want)
			}
			wantNext := ""
			if tc.Next != nil {
				wantNext = *tc.Next
			}
			if next != wantNext {
				t.Fatalf("next token = %q, want %q", next, wantNext)
			}
		})
	}
}
