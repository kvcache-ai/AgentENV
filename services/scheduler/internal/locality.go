package scheduler

import (
	"strings"

	schedulerv1 "agentenv/services/api/proto"
)

type ArtifactProviderLookup interface {
	LookupAll(clusterID string, backend string, key string) []string
}

type LocalityPreferenceStats struct {
	RequirementCount  int
	ProviderNodeCount int
	MaxCoverage       int
}

type localityNamespace struct {
	clusterID string
	backend   string
}

// PreferLocalNodes keeps candidates tied for the highest positive artifact
// coverage. With no usable locality information it preserves the input set.
func PreferLocalNodes(
	nodes []RichNode,
	requirements []*schedulerv1.LocalityRequirement,
	providers ArtifactProviderLookup,
) ([]RichNode, LocalityPreferenceStats) {
	stats := LocalityPreferenceStats{}
	if len(nodes) == 0 || providers == nil {
		return nodes, stats
	}

	keys := deduplicateLocalityRequirementKeys(requirements)
	stats.RequirementCount = len(keys)
	if len(keys) == 0 {
		return nodes, stats
	}

	candidates := make(map[localityNamespace]map[string]struct{})
	for _, node := range nodes {
		namespace := localityNamespace{
			clusterID: strings.TrimSpace(node.ClusterID),
			backend:   strings.TrimSpace(node.P2pBackend),
		}
		if namespace.clusterID == "" || namespace.backend == "" {
			continue
		}
		if candidates[namespace] == nil {
			candidates[namespace] = make(map[string]struct{})
		}
		candidates[namespace][node.ID] = struct{}{}
	}

	scores := make(map[string]int)
	providerNodes := make(map[string]struct{})
	for namespace, namespaceCandidates := range candidates {
		for _, key := range keys {
			seenProviders := make(map[string]struct{})
			for _, nodeID := range providers.LookupAll(namespace.clusterID, namespace.backend, key) {
				if _, seen := seenProviders[nodeID]; seen {
					continue
				}
				seenProviders[nodeID] = struct{}{}
				if _, eligible := namespaceCandidates[nodeID]; !eligible {
					continue
				}
				scores[nodeID]++
				providerNodes[nodeID] = struct{}{}
				if scores[nodeID] > stats.MaxCoverage {
					stats.MaxCoverage = scores[nodeID]
				}
			}
		}
	}
	stats.ProviderNodeCount = len(providerNodes)

	if stats.MaxCoverage == 0 {
		return nodes, stats
	}
	preferred := make([]RichNode, 0, len(nodes))
	for _, node := range nodes {
		if scores[node.ID] == stats.MaxCoverage {
			preferred = append(preferred, node)
		}
	}
	return preferred, stats
}

func deduplicateLocalityRequirementKeys(requirements []*schedulerv1.LocalityRequirement) []string {
	seen := make(map[string]struct{}, len(requirements))
	keys := make([]string, 0, len(requirements))
	for _, requirement := range requirements {
		key := strings.TrimSpace(requirement.GetKey())
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return keys
}
