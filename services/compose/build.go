package compose

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type BuildService struct {
	Name       string            `json:"name"`
	Context    string            `json:"context"`
	Dockerfile string            `json:"dockerfile"`
	Args       map[string]string `json:"args"`
	Target     string            `json:"target,omitempty"`
	NoCache    bool              `json:"noCache,omitempty"`
}

type BuildPlan struct {
	Compose  json.RawMessage `json:"compose"`
	Services []BuildService  `json:"services"`
}

// PrepareBuild shares the runtime validation/interpolation rules, but permits
// local Dockerfile builds. Paths are resolved by the CLI, never by the server.
func PrepareBuild(ctx context.Context, request Request) (*BuildPlan, error) {
	project, err := loadProject(ctx, request, true)
	if err != nil {
		return nil, err
	}
	plan := &BuildPlan{}
	names := project.ServiceNames()
	sort.Strings(names)
	images := map[string]string{}
	for _, name := range names {
		service := project.Services[name]
		if build := service.Build; build != nil {
			context := build.Context
			if context == "" {
				context = "."
			}
			if strings.Contains(context, "://") || strings.HasPrefix(context, "git@") || strings.Contains(context, "#") || context == "-" {
				return nil, fmt.Errorf("services.%s.build.context must be a local directory", name)
			}
			dockerfile := build.Dockerfile
			if dockerfile == "" {
				dockerfile = "Dockerfile"
			}
			args := map[string]string{}
			for key, value := range build.Args {
				if value != nil {
					args[key] = *value
				} else if resolved, ok := request.Environment[key]; ok {
					args[key] = resolved
				}
			}
			plan.Services = append(plan.Services, BuildService{
				Name: name, Context: context, Dockerfile: dockerfile,
				Args: args, Target: build.Target, NoCache: build.NoCache,
			})
			// This placeholder is replaced only after a successful registry export.
			service.Image = "aenv-build/" + name + ":pending"
		}
		service.Build = nil
		service.PullPolicy = ""
		images[name] = service.Image
		project.Services[name] = service
	}
	// Validate the exact runtime contract (ports, platform, image-only services,
	// etc.) before the CLI starts any remote build. Reuse its dollar escaping.
	runtime, err := runtimePlan(project)
	if err != nil {
		return nil, err
	}
	var normalized map[string]any
	if err := json.Unmarshal(runtime.Compose, &normalized); err != nil {
		return nil, err
	}
	services := normalized["services"].(map[string]any)
	for name, image := range images {
		services[name].(map[string]any)["image"] = escapeDollars(image)
	}
	// compose-go emits an empty IPAM object even when none was requested.
	// The sandbox API deliberately rejects user-provided IPAM configuration.
	if networks, ok := normalized["networks"].(map[string]any); ok {
		for _, value := range networks {
			delete(value.(map[string]any), "ipam")
		}
	}
	plan.Compose, err = json.Marshal(normalized)
	return plan, err
}

// Harbor's docker-compose-build.yaml supplies these defaults before merging a
// task's environment/docker-compose.yaml. Enable explicitly so generic Compose
// projects keep their own main service behavior.
func applyHarborDefaults(raw map[string]any) error {
	services := raw["services"].(map[string]any)
	main, ok := services["main"].(map[string]any)
	if !ok {
		return fmt.Errorf("--harbor requires a main service")
	}
	if _, hasBuild := main["build"]; !hasBuild {
		if _, hasImage := main["image"]; !hasImage {
			main["build"] = map[string]any{"context": "."}
		}
	}
	if _, exists := main["command"]; !exists {
		main["command"] = []any{"sh", "-c", "sleep infinity"}
	}
	return nil
}
