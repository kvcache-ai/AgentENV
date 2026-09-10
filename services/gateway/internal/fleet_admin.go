package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	schedulerv1 "agentenv/services/api/proto"
)

const maxFleetAdminBody = 1 << 20

type fleetPlanHTTPRequest struct {
	FleetNodeIDs []string `json:"fleetNodeIds"`
}

type fleetNodeHTTPReference struct {
	NodeID            string `json:"nodeId"`
	ServiceInstanceID string `json:"serviceInstanceId"`
}

type fleetPlanHTTPResponse struct {
	DesiredNodes       uint32                   `json:"desiredNodes"`
	ReadyNodes         uint32                   `json:"readyNodes"`
	ProvisioningNodes  uint32                   `json:"provisioningNodes"`
	CordonCandidates   []fleetNodeHTTPReference `json:"cordonCandidates"`
	DeleteCandidates   []fleetNodeHTTPReference `json:"deleteCandidates"`
	UncordonCandidates []fleetNodeHTTPReference `json:"uncordonCandidates"`
	Reason             string                   `json:"reason"`
}

type fleetNodeActionHTTPRequest struct {
	ServiceInstanceID string `json:"serviceInstanceId"`
}

func (s *Server) handleFleetAdmin(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
	defer cancel()

	if r.URL.Path == "/fleet/plan" {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleFleetPlan(w, r, ctx)
		return
	}

	nodeID, action, ok := parseFleetNodeActionPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var request fleetNodeActionHTTPRequest
	if err := decodeFleetAdminJSON(r, &request); err != nil || strings.TrimSpace(request.ServiceInstanceID) == "" {
		http.Error(w, "valid serviceInstanceId is required", http.StatusBadRequest)
		return
	}

	var err error
	switch action {
	case "cordon":
		_, err = s.scheduler.CordonNode(ctx, &schedulerv1.CordonNodeRequest{
			NodeId: nodeID, ServiceInstanceId: request.ServiceInstanceID,
		})
	case "uncordon":
		_, err = s.scheduler.UncordonNode(ctx, &schedulerv1.UncordonNodeRequest{
			NodeId: nodeID, ServiceInstanceId: request.ServiceInstanceID,
		})
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		s.writeSchedulerError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleFleetPlan(w http.ResponseWriter, r *http.Request, ctx context.Context) {
	var request fleetPlanHTTPRequest
	if err := decodeFleetAdminJSON(r, &request); err != nil {
		http.Error(w, "invalid fleet plan request", http.StatusBadRequest)
		return
	}
	response, err := s.scheduler.GetFleetPlan(ctx, &schedulerv1.GetFleetPlanRequest{FleetNodeIds: request.FleetNodeIDs})
	if err != nil {
		s.writeSchedulerError(w, err)
		return
	}
	s.writeJSON(w, http.StatusOK, fleetPlanHTTPResponse{
		DesiredNodes: response.GetDesiredNodes(), ReadyNodes: response.GetReadyNodes(),
		ProvisioningNodes:  response.GetProvisioningNodes(),
		CordonCandidates:   fleetNodeReferencesFromProto(response.GetCordonCandidates()),
		DeleteCandidates:   fleetNodeReferencesFromProto(response.GetDeleteCandidates()),
		UncordonCandidates: fleetNodeReferencesFromProto(response.GetUncordonCandidates()),
		Reason:             response.GetReason(),
	})
}

func parseFleetNodeActionPath(path string) (nodeID, action string, ok bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 4 || parts[0] != "fleet" || parts[1] != "nodes" || strings.TrimSpace(parts[2]) == "" {
		return "", "", false
	}
	if parts[3] != "cordon" && parts[3] != "uncordon" {
		return "", "", false
	}
	if strings.ContainsAny(parts[2], "?#") {
		return "", "", false
	}
	return parts[2], parts[3], true
}

func decodeFleetAdminJSON(r *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxFleetAdminBody+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func fleetNodeReferencesFromProto(nodes []*schedulerv1.FleetNodeReference) []fleetNodeHTTPReference {
	result := make([]fleetNodeHTTPReference, 0, len(nodes))
	for _, node := range nodes {
		if node == nil {
			continue
		}
		result = append(result, fleetNodeHTTPReference{
			NodeID: node.GetNodeId(), ServiceInstanceID: node.GetServiceInstanceId(),
		})
	}
	return result
}
