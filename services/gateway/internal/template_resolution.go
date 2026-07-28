package gateway

import (
	"strings"

	schedulerv1 "agentenv/services/api/proto"

	"github.com/google/uuid"
)

// resolveSnapshotLocalityHint derives fixed snapshot artifact requirements only
// from canonical snapshot IDs. Template aliases intentionally fall back to
// normal scheduling: the Gateway has no authoritative pre-scheduling alias
// resolver, while querying every node would expand credential exposure and
// could observe inconsistent alias mappings.
func resolveSnapshotLocalityHint(
	hint *schedulerv1.ScheduleRequestHint,
) *schedulerv1.ScheduleRequestHint {
	if hint == nil || hint.GetNewSandbox() == nil {
		return hint
	}

	canonicalID, ok := canonicalSnapshotID(hint.GetNewSandbox().GetTemplateId())
	if !ok {
		hint.LocalityRequirements = nil
		return hint
	}

	hint.LocalityRequirements = snapshotLocalityRequirements(canonicalID)
	return hint
}

func canonicalSnapshotID(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if len(value) != 36 ||
		value[8] != '-' ||
		value[13] != '-' ||
		value[18] != '-' ||
		value[23] != '-' {
		return "", false
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return "", false
	}
	return parsed.String(), true
}
