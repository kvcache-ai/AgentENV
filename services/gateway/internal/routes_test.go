package gateway

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestMatchPathRoute(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
		owner  routeOwner
		id     string
	}{
		{name: "sandbox detail", method: http.MethodGet, path: "/sandboxes/sbx-123", owner: ownerSandbox, id: "sbx-123"},
		{name: "sandbox detail with trailing slash", method: http.MethodGet, path: "/sandboxes/sbx-123/", owner: ownerSandbox, id: "sbx-123"},
		{name: "sandbox delete", method: http.MethodDelete, path: "/sandboxes/sbx-123", owner: ownerSandbox, id: "sbx-123"},
		{name: "v2 sandbox logs", method: http.MethodGet, path: "/v2/sandboxes/sbx-123/logs", owner: ownerSandbox, id: "sbx-123"},
		{name: "v2 logs wrong method", method: http.MethodPost, path: "/v2/sandboxes/sbx-123/logs"},
		{name: "v2 logs missing ID", method: http.MethodGet, path: "/v2/sandboxes//logs"},
		{name: "v2 sandbox connect", method: http.MethodPost, path: "/v2/sandboxes/sbx-123/connect", owner: ownerSandbox, id: "sbx-123"},
		{name: "v2 connect wrong method", method: http.MethodGet, path: "/v2/sandboxes/sbx-123/connect"},
		{name: "sandbox pause", method: http.MethodPost, path: "/sandboxes/sbx-123/pause", owner: ownerSandbox, id: "sbx-123"},
		{name: "sandbox pause blank ID", method: http.MethodPost, path: "/sandboxes/ /pause"},
		{name: "sandbox pause empty segment", method: http.MethodPost, path: "/sandboxes//pause"},
		{name: "sandbox fork", method: http.MethodPost, path: "/sandboxes/sbx-123/fork", owner: ownerSandboxFork, id: "sbx-123"},
		{name: "sandbox network update", method: http.MethodPut, path: "/sandboxes/sbx-123/network", owner: ownerSandbox, id: "sbx-123"},
		{name: "network update wrong method", method: http.MethodPost, path: "/sandboxes/sbx-123/network"},
		{name: "custom extension params get", method: http.MethodGet, path: "/sandboxes/sbx-123/custom-extension-params", owner: ownerSandbox, id: "sbx-123"},
		{name: "custom extension params patch", method: http.MethodPatch, path: "/sandboxes/sbx-123/custom-extension-params", owner: ownerSandbox, id: "sbx-123"},
		{name: "custom extension params post", method: http.MethodPost, path: "/sandboxes/sbx-123/custom-extension-params"},
		{name: "data-plane shaped path", method: http.MethodGet, path: "/sandboxes/sbx-123/files"},
		{name: "sandbox list", method: http.MethodGet, path: "/sandboxes"},
		{name: "cold sandbox create", method: http.MethodPost, path: "/sandboxes-cold"},
		{name: "v2 build allocation", method: http.MethodPost, path: "/v2/templates/tmpl/builds/build-1", owner: ownerNewBuild, id: "build-1"},
		{name: "v2 build wrong method", method: http.MethodGet, path: "/v2/templates/tmpl/builds/build-1"},
		{name: "builder allocation", method: http.MethodPut, path: "/templates/tmpl/builds/build-1/builder", owner: ownerNewBuildConfirmed, id: "build-1"},
		{name: "builder release", method: http.MethodDelete, path: "/templates/tmpl/builds/build-1/builder", owner: ownerBuild, id: "build-1"},
		{name: "build status read", method: http.MethodGet, path: "/templates/tmpl/builds/build-1/status", owner: ownerBuildRead, id: "build-1"},
		{name: "build logs read", method: http.MethodGet, path: "/templates/tmpl/builds/build-1/logs", owner: ownerBuildRead, id: "build-1"},
		{name: "build status other method", method: http.MethodPost, path: "/templates/tmpl/builds/build-1/status", owner: ownerBuild, id: "build-1"},
		{name: "build missing template", method: http.MethodGet, path: "/templates//builds/build-1/status"},
		{name: "unknown build child", method: http.MethodGet, path: "/templates/tmpl/builds/build-1/other"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			route, ok := matchPathRoute(tc.method, tc.path)
			if wantOK := tc.owner != 0; ok != wantOK || route.owner != tc.owner || route.id != tc.id {
				t.Fatalf("matchPathRoute(%s %s) = (%+v, %v), want owner %v id %q", tc.method, tc.path, route, ok, tc.owner, tc.id)
			}
		})
	}
}

// Every API operation addressed by a sandbox or build ID must be routed by
// its path, and every path route must name an API operation.
func TestPathRoutesCoverOpenAPI(t *testing.T) {
	raw, err := os.ReadFile("../../../src/api/openapi.yml")
	if err != nil {
		t.Fatalf("read OpenAPI spec: %v", err)
	}
	var spec struct {
		Paths map[string]map[string]any `yaml:"paths"`
	}
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("parse OpenAPI spec: %v", err)
	}

	used := make([]bool, len(pathRoutes))
	for path, operations := range spec.Paths {
		for method := range operations {
			method = strings.ToUpper(method)
			switch method {
			case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
			default:
				continue
			}
			if isSandboxMetricsListRequest(httptest.NewRequest(method, path, nil)) {
				// Answered by the gateway before path routing.
				continue
			}
			ownedByID := strings.Contains(path, "{sandboxID}") || strings.Contains(path, "{buildID}")
			if _, ok := matchPathRoute(method, path); ok != ownedByID {
				t.Errorf("%s %s: path routed = %v, want %v; update pathRoutes", method, path, ok, ownedByID)
			}
			for i, candidate := range pathRoutes {
				if candidate.method != "" && candidate.method != method {
					continue
				}
				if _, matched := matchPathPattern(candidate.pattern, strings.Split(strings.Trim(path, "/"), "/")); matched {
					used[i] = true
				}
			}
		}
	}
	for i, route := range pathRoutes {
		if !used[i] {
			t.Errorf("pathRoutes entry %q %s matches no OpenAPI operation", route.method, route.pattern)
		}
	}
}
