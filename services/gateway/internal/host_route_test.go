package gateway

import (
	"strings"
	"testing"
)

func TestParseHostRoute(t *testing.T) {
	const domain = "sandbox-proxy.example.invalid"
	const altDomain = "sandbox-proxy-alt.example.invalid"
	const sandboxID = "018ff4b7-8c0d-7f7c-9a99-123456789abc"

	tests := []struct {
		name        string
		host        string
		wantRoute   *hostRoute
		wantMessage string
	}{
		{
			name:      "success",
			host:      "40988-" + sandboxID + "." + domain,
			wantRoute: &hostRoute{sandboxID: sandboxID, targetPort: 40988},
		},
		{
			name:      "success with second configured domain",
			host:      "40988-" + sandboxID + "." + altDomain,
			wantRoute: &hostRoute{sandboxID: sandboxID, targetPort: 40988},
		},
		{
			name:      "success with request host port and trailing dot",
			host:      "40988-" + sandboxID + "." + domain + ".:443",
			wantRoute: &hostRoute{sandboxID: sandboxID, targetPort: 40988},
		},
		{
			name:        "port not numeric",
			host:        "abc-" + sandboxID + "." + domain,
			wantMessage: "invalid sandbox data-plane host: port is not numeric",
		},
		{
			name:        "port zero out of range",
			host:        "0-" + sandboxID + "." + domain,
			wantMessage: "invalid sandbox data-plane host: port out of range",
		},
		{
			name:        "port too large out of range",
			host:        "70000-" + sandboxID + "." + domain,
			wantMessage: "invalid sandbox data-plane host: port out of range",
		},
		{
			name:        "empty sandbox id",
			host:        "40988-." + domain,
			wantMessage: "invalid sandbox data-plane host: sandbox id is empty",
		},
		{
			name:        "invalid sandbox id character",
			host:        "40988-sbx_bad." + domain,
			wantMessage: "invalid sandbox data-plane host: sandbox id is invalid",
		},
		{
			name:        "label too long",
			host:        "40988-" + strings.Repeat("a", 58) + "." + domain,
			wantMessage: "invalid sandbox data-plane host: label too long",
		},
		{
			name: "data-plane shaped host under unconfigured domain is ignored",
			host: "40988-" + sandboxID + ".evil.test",
		},
		{
			name: "bare proxy domain is control plane",
			host: domain,
		},
		{
			name: "non data-plane wildcard label is ignored",
			host: "asdfasdfasdfasdf." + domain,
		},
	}

	domains, err := normalizeProxyDomains([]string{domain, altDomain})
	if err != nil {
		t.Fatalf("normalize proxy domains failed: %v", err)
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, gotErr := parseHostRoute(tc.host, domains)
			if tc.wantMessage != "" {
				if gotErr == nil {
					t.Fatalf("expected host routing error %q", tc.wantMessage)
				}
				if gotErr.Error() != tc.wantMessage {
					t.Fatalf("error = %q, want %q", gotErr.Error(), tc.wantMessage)
				}
				return
			}
			if gotErr != nil {
				t.Fatalf("unexpected host routing error: %s", gotErr.Error())
			}
			if tc.wantRoute == nil {
				if got != nil {
					t.Fatalf("expected no route, got %#v", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("expected route %#v, got nil", tc.wantRoute)
			}
			if *got != *tc.wantRoute {
				t.Fatalf("route = %#v, want %#v", got, tc.wantRoute)
			}
		})
	}
}

func TestParseConnectAuthorityRoute(t *testing.T) {
	const domain = "sandbox-proxy.example.invalid"
	const sandboxID = "018ff4b7-8c0d-7f7c-9a99-123456789abc"

	tests := []struct {
		name        string
		authority   string
		wantRoute   *hostRoute
		wantMessage string
	}{
		{
			name:      "matching authority port",
			authority: "8080-" + sandboxID + "." + domain + ":8080",
			wantRoute: &hostRoute{sandboxID: sandboxID, targetPort: 8080},
		},
		{
			name:      "authority without port",
			authority: "8080-" + sandboxID + "." + domain,
			wantRoute: &hostRoute{sandboxID: sandboxID, targetPort: 8080},
		},
		{
			name:      "authority with trailing dot",
			authority: "8080-" + sandboxID + "." + domain + ".:8080",
			wantRoute: &hostRoute{sandboxID: sandboxID, targetPort: 8080},
		},
		{
			name:      "mismatched authority port is ignored",
			authority: "8080-" + sandboxID + "." + domain + ":9090",
			wantRoute: &hostRoute{sandboxID: sandboxID, targetPort: 8080},
		},
		{
			name:      "non-numeric authority port is ignored",
			authority: "8080-" + sandboxID + "." + domain + ":http",
			wantRoute: &hostRoute{sandboxID: sandboxID, targetPort: 8080},
		},
		{
			name:        "invalid data-plane host still rejected",
			authority:   "abc-" + sandboxID + "." + domain + ":8080",
			wantMessage: "invalid sandbox data-plane host: port is not numeric",
		},
		{
			name:      "non data-plane authority is ignored, port untouched",
			authority: "other.example.test:not-a-port",
		},
		{
			name:      "bare proxy domain is control plane",
			authority: domain + ":443",
		},
		{
			name:      "empty authority",
			authority: "",
		},
	}

	domains, err := normalizeProxyDomains([]string{domain})
	if err != nil {
		t.Fatalf("normalize proxy domains failed: %v", err)
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, gotErr := parseConnectAuthorityRoute(tc.authority, domains)
			if tc.wantMessage != "" {
				if gotErr == nil {
					t.Fatalf("expected authority routing error %q", tc.wantMessage)
				}
				if gotErr.Error() != tc.wantMessage {
					t.Fatalf("error = %q, want %q", gotErr.Error(), tc.wantMessage)
				}
				return
			}
			if gotErr != nil {
				t.Fatalf("unexpected authority routing error: %s", gotErr.Error())
			}
			if tc.wantRoute == nil {
				if got != nil {
					t.Fatalf("expected no route, got %#v", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("expected route %#v, got nil", tc.wantRoute)
			}
			if *got != *tc.wantRoute {
				t.Fatalf("route = %#v, want %#v", got, tc.wantRoute)
			}
		})
	}
}

func TestNormalizeProxyDomains(t *testing.T) {
	got, err := normalizeProxyDomains([]string{
		" Sandbox-Proxy.Example.Invalid. ",
		"sandbox-proxy.example.invalid",
		"localHost",
	})
	if err != nil {
		t.Fatalf("normalize proxy domains failed: %v", err)
	}

	want := []string{"sandbox-proxy.example.invalid", "localhost"}
	if !equalStrings(got, want) {
		t.Fatalf("domains = %#v, want %#v", got, want)
	}
}

func TestParseHostRouteWithOverlappingDomains(t *testing.T) {
	domains, err := normalizeProxyDomains([]string{"example.invalid", "sandbox-proxy.example.invalid"})
	if err != nil {
		t.Fatalf("normalize proxy domains failed: %v", err)
	}

	got, gotErr := parseHostRoute("40988-sbx123.sandbox-proxy.example.invalid", domains)
	if gotErr != nil || got == nil || got.sandboxID != "sbx123" || got.targetPort != 40988 {
		t.Fatalf("route = %#v, error = %v; want sandbox sbx123 port 40988", got, gotErr)
	}

	got, gotErr = parseHostRoute("sandbox-proxy.example.invalid", domains)
	if gotErr != nil || got != nil {
		t.Fatalf("route = %#v, error = %v; want no route and no error", got, gotErr)
	}
}

func TestNormalizeProxyDomainsRejectsInvalidDomain(t *testing.T) {
	_, err := normalizeProxyDomains([]string{"not a domain!"})
	if err == nil {
		t.Fatal("expected invalid proxy domain to be rejected")
	}
}
