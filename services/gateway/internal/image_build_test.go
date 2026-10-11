package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
)

func TestImageBuildRouting(t *testing.T) {
	r := newHintRequest(t, "POST", "/images/builds", `{"timeout":3600}`)
	hint, err := buildScheduleHint(r)
	if err != nil || hint != nil {
		t.Fatalf("hint=%v error=%v", hint, err)
	}
	if !shouldRecordAssignment(r, routeSourceSchedule, false) {
		t.Fatal("image builds require a route binding")
	}
	for _, path := range []string{"/images/builds/job", "/images/builds/job/logs", "/images/builds/job/builder"} {
		if id, ok := imageBuildIDFromPath(path); !ok || id != "job" {
			t.Fatalf("invalid image route: %s", path)
		}
	}
	for _, path := range []string{"/images/builds", "/images/builds/job/unknown", "/images/builds/job/logs/extra"} {
		if _, ok := imageBuildIDFromPath(path); ok {
			t.Fatalf("accepted invalid image route: %s", path)
		}
	}
}

func TestCompletedImageBuildUsesRetainedBuildBinding(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer upstream.Close()
	server := newTestServer(t, stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			if req.GetSandboxId() != "build-1" {
				t.Fatal("wrong build identity")
			}
			return &schedulerv1.LookupNodeResponse{Node: &schedulerv1.Node{NodeId: "internal-owner", Endpoint: upstream.URL}}, nil
		},
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			t.Fatal("result reads must not allocate capacity")
			return nil, nil
		},
	}, time.Second, 1024)
	for _, operation := range []struct{ method, path string }{{"GET", ""}, {"GET", "/logs"}, {"DELETE", ""}} {
		response := httptest.NewRecorder()
		authenticatedTestHandler(server).ServeHTTP(response, httptest.NewRequest(operation.method, "/images/builds/build-1"+operation.path, nil))
		if response.Code != http.StatusNoContent {
			t.Fatalf("status=%d %s", response.Code, response.Body.String())
		}
	}
}

func TestImageBuildAllocationRecordsAssignment(t *testing.T) {
	for _, buildID := range []string{"build-123", ""} {
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("x-agentenv-build-id", buildID)
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"buildID":"build-123","imageName":"aenv-build:build-123"}`))
		}))
		recorded := false
		server := newTestServer(t, stubSchedulerClient{
			scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
				return &schedulerv1.ScheduleResponse{Node: &schedulerv1.Node{NodeId: "builder", Endpoint: upstream.URL}}, nil
			},
			recordAssignmentFunc: func(_ context.Context, req *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
				if buildID == "" || req.GetSandboxId() != buildID {
					t.Error("incorrect image build binding")
				}
				recorded = true
				return &schedulerv1.RecordAssignmentResponse{}, nil
			},
		}, time.Second, 1024)
		request := httptest.NewRequest(http.MethodPost, "/images/builds", strings.NewReader(`{}`))
		request.Header.Set(headerSandboxID, "unrelated")
		request.Header.Set(headerTargetPort, "1234")
		if server.isSandboxDataPlaneRequest(request) {
			t.Fatal("image builds must require API authentication")
		}
		response := httptest.NewRecorder()
		authenticatedTestHandler(server).ServeHTTP(response, request)
		upstream.Close()
		if buildID == "" {
			if response.Code != http.StatusBadGateway || recorded {
				t.Fatal("accepted missing binding")
			}
		} else if response.Code != http.StatusAccepted || !recorded {
			t.Fatalf("failed image build binding: %d %s", response.Code, response.Body.String())
		}
	}
}

func TestImageCatalogRoutesWithoutBuildOrSandboxBinding(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()
	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(_ context.Context, req *schedulerv1.ScheduleRequest, _ ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			if req.GetHint() != nil {
				t.Error("catalog access must not request sandbox resources")
			}
			return &schedulerv1.ScheduleResponse{Node: &schedulerv1.Node{NodeId: "reader", Endpoint: upstream.URL}}, nil
		},
		lookupNodeFunc: func(context.Context, *schedulerv1.LookupNodeRequest, ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			t.Fatal("catalog access must not look up a worker")
			return nil, nil
		},
		recordAssignmentFunc: func(context.Context, *schedulerv1.RecordAssignmentRequest, ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			t.Fatal("catalog access must not create a binding")
			return nil, nil
		},
	}, time.Second, 1024)
	for _, operation := range []struct{ method, path, label string }{
		{"GET", "/images?limit=1", "/images"},
		{"GET", "/images/" + digest, "/images/{image_digest}"},
		{"DELETE", "/images/" + digest, "/images/{image_digest}"},
	} {
		request := httptest.NewRequest(operation.method, operation.path, nil)
		request.Header.Set(headerSandboxID, "unrelated")
		request.Header.Set(headerTargetPort, "1234")
		if server.isSandboxDataPlaneRequest(request) {
			t.Fatal("catalog access must require API authentication")
		}
		if label := gatewayRouteLabel(request.URL.Path); label != operation.label {
			t.Fatalf("unexpected metric label %q", label)
		}
		response := httptest.NewRecorder()
		authenticatedTestHandler(server).ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("status=%d %s", response.Code, response.Body.String())
		}
	}
}
