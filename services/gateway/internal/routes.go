package gateway

import (
	"net/http"
	"strings"
)

// routeOwner says how the gateway picks the node for a request whose path
// names the sandbox or build that owns it.
type routeOwner int

const (
	// ownerSandbox routes to the node that owns the {id} sandbox.
	ownerSandbox routeOwner = iota + 1
	// ownerSandboxFork routes to the source sandbox's node and records the
	// forked children from the response.
	ownerSandboxFork
	// ownerBuild routes to the node that runs the {id} template build.
	ownerBuild
	// ownerBuildRead routes like ownerBuild, but a build without a binding is
	// finished and is read from the shared template repository instead.
	ownerBuildRead
	// ownerNewBuild schedules a builder and binds the {id} build to it.
	ownerNewBuild
	// ownerNewBuildConfirmed schedules a builder and binds the build ID the
	// upstream returns in x-agentenv-build-id, which must equal {id}.
	ownerNewBuildConfirmed
)

type pathRoute struct {
	method  string // empty matches any method
	pattern string // "{id}" captures the owner ID; other "{...}" segments match any value
	owner   routeOwner
}

// pathRoutes lists every API route whose owning node follows from its path.
// The first match wins. TestPathRoutesCoverOpenAPI keeps it in step with
// src/api/openapi.yml.
var pathRoutes = []pathRoute{
	{http.MethodGet, "/sandboxes/{id}", ownerSandbox},
	{http.MethodDelete, "/sandboxes/{id}", ownerSandbox},
	{http.MethodGet, "/sandboxes/{id}/metrics", ownerSandbox},
	{http.MethodPost, "/sandboxes/{id}/pause", ownerSandbox},
	{http.MethodPost, "/sandboxes/{id}/resume", ownerSandbox},
	{http.MethodPost, "/sandboxes/{id}/fork", ownerSandboxFork},
	{http.MethodPost, "/sandboxes/{id}/connect", ownerSandbox},
	{http.MethodPost, "/sandboxes/{id}/timeout", ownerSandbox},
	{http.MethodPost, "/sandboxes/{id}/refreshes", ownerSandbox},
	{http.MethodPost, "/sandboxes/{id}/snapshots", ownerSandbox},
	{http.MethodPut, "/sandboxes/{id}/network", ownerSandbox},
	{http.MethodGet, "/sandboxes/{id}/custom-extension-params", ownerSandbox},
	{http.MethodPatch, "/sandboxes/{id}/custom-extension-params", ownerSandbox},
	{http.MethodGet, "/v2/sandboxes/{id}/logs", ownerSandbox},
	{http.MethodPost, "/v2/sandboxes/{id}/connect", ownerSandbox},

	{http.MethodPost, "/v2/templates/{templateID}/builds/{id}", ownerNewBuild},
	{http.MethodPut, "/templates/{templateID}/builds/{id}/builder", ownerNewBuildConfirmed},
	{http.MethodGet, "/templates/{templateID}/builds/{id}/status", ownerBuildRead},
	{http.MethodGet, "/templates/{templateID}/builds/{id}/logs", ownerBuildRead},
	{"", "/templates/{templateID}/builds/{id}/builder", ownerBuild},
	{"", "/templates/{templateID}/builds/{id}/status", ownerBuild},
	{"", "/templates/{templateID}/builds/{id}/logs", ownerBuild},
}

type pathRouteMatch struct {
	owner routeOwner
	id    string
}

func (m pathRouteMatch) allocatesBuild() bool {
	return m.owner == ownerNewBuild || m.owner == ownerNewBuildConfirmed
}

// matchPathRoute returns the first route in pathRoutes that matches the
// request, with the owner ID taken from its path.
func matchPathRoute(method, path string) (pathRouteMatch, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for _, route := range pathRoutes {
		if route.method != "" && route.method != method {
			continue
		}
		if id, ok := matchPathPattern(route.pattern, parts); ok {
			return pathRouteMatch{owner: route.owner, id: id}, true
		}
	}
	return pathRouteMatch{}, false
}

func matchPathPattern(pattern string, parts []string) (string, bool) {
	segments := strings.Split(strings.Trim(pattern, "/"), "/")
	if len(segments) != len(parts) {
		return "", false
	}
	id := ""
	for i, segment := range segments {
		part := parts[i]
		if !strings.HasPrefix(segment, "{") {
			if part != segment {
				return "", false
			}
			continue
		}
		if strings.TrimSpace(part) == "" {
			return "", false
		}
		if segment == "{id}" {
			id = strings.TrimSpace(part)
		}
	}
	return id, true
}
