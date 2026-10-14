// Package compose prepares an image-only Compose project for an AgentENV sandbox.
package compose

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/netip"
	"sort"
	"strconv"
	"strings"

	"github.com/compose-spec/compose-go/v2/loader"
	"github.com/compose-spec/compose-go/v2/template"
	"github.com/compose-spec/compose-go/v2/types"
	"gopkg.in/yaml.v3"
)

const MaxServices = 24

// MaxPlanBytes matches the guest's complete, newline-terminated startup frame.
const MaxPlanBytes = 4 * 1024 * 1024

// Allow the host to add empty composeEnv/profiles to a 2 MiB HTTP request.
const MaxRequestBytes = 2*1024*1024 + 64

type Request struct {
	Mode        string            `json:"mode,omitempty"`
	Harbor      bool              `json:"harbor,omitempty"`
	Compose     string            `json:"compose"`
	Environment map[string]string `json:"composeEnv,omitempty"`
	Profiles    []string          `json:"profiles,omitempty"`
}

type Service struct {
	Name       string `json:"name"`
	Image      string `json:"image"`
	LocalImage string `json:"localImage"`
	DriveID    string `json:"driveID"`
	MountPath  string `json:"mountPath"`
}

type Plan struct {
	Compose  json.RawMessage `json:"compose"`
	Services []Service       `json:"services"`
}

// Prepare never imports the server's environment, files, or remote resources.
// Validate the raw tree before invoking the loader, which can otherwise read
// env_file/include/extends/configs during normalization.
func Prepare(ctx context.Context, request Request) (*Plan, error) {
	project, err := loadProject(ctx, request, false)
	if err != nil {
		return nil, err
	}
	return runtimePlan(project)
}

func parseDocument(source string) (map[string]any, error) {
	if len(source) == 0 || len(source) > 1024*1024 {
		return nil, fmt.Errorf("compose must contain between 1 byte and 1 MiB")
	}
	var document yaml.Node
	decoder := yaml.NewDecoder(strings.NewReader(source))
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, fmt.Errorf("compose must contain exactly one YAML document")
	}
	if err := validateExpansion(&document); err != nil {
		return nil, err
	}
	var raw map[string]any
	if err := document.Decode(&raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func loadProject(ctx context.Context, request Request, allowBuild bool) (*types.Project, error) {
	raw, err := parseDocument(request.Compose)
	if err != nil {
		return nil, err
	}
	if err := validateRaw(raw, allowBuild); err != nil {
		return nil, err
	}
	if request.Harbor {
		if !allowBuild {
			return nil, fmt.Errorf("harbor defaults are only supported in build mode")
		}
		if err := applyHarborDefaults(raw); err != nil {
			return nil, err
		}
	}
	budget := expansionBudget{MaxPlanBytes}
	project, err := loader.LoadWithContext(ctx, types.ConfigDetails{
		WorkingDir: "/var/lib/agentenv-compose",
		// Pass the exact validated tree; do not parse the source twice with
		// potentially different YAML merge/document handling.
		ConfigFiles: []types.ConfigFile{{Filename: "compose.yaml", Config: raw}},
		Environment: types.Mapping(request.Environment),
	}, func(o *loader.Options) {
		o.SetProjectName("aenv", true)
		o.ResolvePaths = false
		o.SkipInclude = true
		o.SkipExtends = true
		o.SkipResolveEnvironment = true
		o.Profiles = request.Profiles
		o.Interpolate.Substitute = budget.substitute
	})
	if err != nil {
		return nil, err
	}
	// env_file is rejected above, so this only resolves environment entries
	// against the explicitly supplied environment map.
	// The loader may already have filled null entries during normalization;
	// count resolved values as well before copying/serializing the project.
	environmentBudget := expansionBudget{MaxPlanBytes}
	for _, service := range project.Services {
		for key, value := range service.Environment {
			resolved := request.Environment[key]
			if value == nil {
				value = &resolved
			}
			if err := environmentBudget.take(len(*value)); err != nil {
				return nil, err
			}
		}
	}
	project, err = project.WithServicesEnvironmentResolved(true)
	if err != nil {
		return nil, err
	}
	if len(project.Services) == 0 || len(project.Services) > MaxServices {
		return nil, fmt.Errorf("compose must select 1..%d services", MaxServices)
	}
	return project, validateProject(project, allowBuild)
}

func validateProject(project *types.Project, allowBuild bool) error {
	bindings := portBindings{}
	for _, name := range project.ServiceNames() {
		service := project.Services[name]
		if service.Image == "" && (!allowBuild || service.Build == nil) {
			return fmt.Errorf("services.%s.image is required", name)
		}
		if service.Platform != "" && service.Platform != "linux/amd64" {
			return fmt.Errorf("services.%s.platform: only linux/amd64 is supported", name)
		}
		if mode := service.NetworkMode; mode != "" {
			target := strings.TrimPrefix(mode, "service:")
			if target == mode || target == name {
				return fmt.Errorf("services.%s.network_mode only supports another service in this project", name)
			}
			if _, exists := project.Services[target]; !exists {
				return fmt.Errorf("services.%s.network_mode references inactive service %s", name, target)
			}
		}
		for _, capability := range service.CapAdd {
			if capability != "SYS_PTRACE" {
				return fmt.Errorf("services.%s.cap_add only supports SYS_PTRACE", name)
			}
		}
		for _, port := range service.Ports {
			if err := bindings.add(name, port); err != nil {
				return err
			}
		}
	}
	return nil
}

func runtimePlan(project *types.Project) (*Plan, error) {
	plan := &Plan{}
	names := project.ServiceNames()
	sort.Strings(names)
	for i, name := range names {
		service := project.Services[name]
		id := fmt.Sprintf("compose_%d", i)
		alias := fmt.Sprintf("aenv-compose/service-%d:local", i)
		plan.Services = append(plan.Services, Service{
			Name: name, Image: service.Image, LocalImage: alias,
			DriveID: id, MountPath: "/mnt/" + id,
		})
		service.Image = alias
		project.Services[name] = service
	}
	var err error
	plan.Compose, err = renderProject(project)
	if err != nil {
		return nil, err
	}
	return plan, validatePlanSize(plan)
}

// Both outputs contain active services only and survive the next Compose load
// without interpolating literal dollars again.
func renderProject(project *types.Project) (json.RawMessage, error) {
	for name, service := range project.Services {
		service.Profiles = nil
		project.Services[name] = service
	}
	encoded, err := project.MarshalJSON()
	if err != nil {
		return nil, err
	}
	var normalized map[string]any
	if err := json.Unmarshal(encoded, &normalized); err != nil {
		return nil, err
	}
	// compose-go emits empty IPAM even though custom IPAM is unsupported.
	if networks, ok := normalized["networks"].(map[string]any); ok {
		for _, value := range networks {
			delete(value.(map[string]any), "ipam")
		}
	}
	return json.Marshal(escapeDollars(normalized))
}

func validatePlanSize(plan any) error {
	encoded, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	if len(encoded)+1 > MaxPlanBytes {
		return fmt.Errorf("Compose plan exceeds 4 MiB")
	}
	return nil
}

func escapeDollars(value any) any {
	switch v := value.(type) {
	case string:
		return strings.ReplaceAll(v, "$", "$$")
	case []any:
		for i := range v {
			v[i] = escapeDollars(v[i])
		}
	case map[string]any:
		for key := range v {
			v[key] = escapeDollars(v[key])
		}
	}
	return value
}

func allowedKeys(value map[string]any, path, allowed string) error {
	set := " " + allowed + " "
	for key := range value {
		if strings.HasPrefix(key, "x-") {
			continue
		}
		if !strings.Contains(set, " "+key+" ") {
			return fmt.Errorf("%s%s is not supported by sandbox Compose", path, key)
		}
	}
	return nil
}

func validateRaw(raw map[string]any, allowBuild bool) error {
	if err := allowedKeys(raw, "", "name version services networks volumes"); err != nil {
		return err
	}
	services, ok := raw["services"].(map[string]any)
	if !ok {
		return fmt.Errorf("services must be a mapping")
	}
	for name, value := range services {
		s, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("services.%s must be a mapping", name)
		}
		allowed := "image platform command entrypoint environment user working_dir depends_on healthcheck restart ports expose networks network_mode cap_add volumes tmpfs profiles labels hostname init stop_signal stop_grace_period read_only shm_size mem_limit cpus"
		if allowBuild {
			allowed += " build pull_policy"
			if policy, ok := s["pull_policy"]; ok && policy != "build" {
				return fmt.Errorf("services.%s.pull_policy only supports build", name)
			}
			if build, ok := s["build"].(map[string]any); ok {
				if err := allowedKeys(build, "services."+name+".build.", "context dockerfile args target no_cache"); err != nil {
					return err
				}
			}
		}
		if err := allowedKeys(s, "services."+name+".", allowed); err != nil {
			return err
		}
		if volumes, ok := s["volumes"].([]any); ok {
			for _, volume := range volumes {
				switch v := volume.(type) {
				case string:
					parts := strings.Split(v, ":")
					if len(parts) < 2 || strings.ContainsAny(parts[0], "/\\$~") || strings.HasPrefix(parts[0], ".") || parts[0] == "" {
						return fmt.Errorf("services.%s.volumes only supports named volumes", name)
					}
				case map[string]any:
					if v["type"] != "volume" && v["type"] != "tmpfs" {
						return fmt.Errorf("services.%s.volumes only supports volume or tmpfs mounts", name)
					}
				}
			}
		}
	}
	for _, section := range []string{"volumes", "networks"} {
		entries, _ := raw[section].(map[string]any)
		for name, value := range entries {
			if value == nil {
				continue
			}
			entry, ok := value.(map[string]any)
			if !ok {
				return fmt.Errorf("%s.%s must be a mapping", section, name)
			}
			allowed := "name labels driver"
			if section == "networks" {
				allowed += " internal attachable"
			}
			if err := allowedKeys(entry, section+"."+name+".", allowed); err != nil {
				return err
			}
			if driver, ok := entry["driver"]; ok {
				want := "local"
				if section == "networks" {
					want = "bridge"
				}
				if driver != want {
					return fmt.Errorf("%s.%s.driver must be %s", section, name, want)
				}
			}
		}
	}
	return nil
}

type expansionBudget struct{ remaining int }

func (b *expansionBudget) take(size int) error {
	if size > b.remaining {
		return fmt.Errorf("Compose expansion exceeds the 4 MiB budget")
	}
	b.remaining -= size
	return nil
}

// Count aliases before decoding them into maps/slices. Charging for each node
// also bounds collections of tiny values, not just large scalar strings.
func validateExpansion(node *yaml.Node) error {
	budget := expansionBudget{MaxPlanBytes}
	active := map[*yaml.Node]bool{}
	var visit func(*yaml.Node, int) error
	visit = func(node *yaml.Node, depth int) error {
		if depth > 128 || active[node] {
			return fmt.Errorf("Compose YAML nesting or recursive alias exceeds the supported limit")
		}
		if err := budget.take(64 + len(node.Value)); err != nil {
			return err
		}
		active[node] = true
		defer delete(active, node)
		if node.Kind == yaml.AliasNode {
			return visit(node.Alias, depth+1)
		}
		for _, child := range node.Content {
			if err := visit(child, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(node, 0)
}

func (b *expansionBudget) substitute(value string, mapping template.Mapping) (string, error) {
	if err := b.take(len(value)); err != nil {
		return "", err
	}
	var budgetErr error
	result, err := template.Substitute(value, func(key string) (string, bool) {
		if budgetErr != nil {
			return "", true
		}
		resolved, ok := mapping(key)
		// Refuse large replacements before the template package concatenates
		// them, including repeated references within a single scalar.
		budgetErr = b.take(len(resolved))
		if budgetErr != nil {
			return "", true
		}
		return resolved, ok
	})
	if budgetErr != nil {
		return "", budgetErr
	}
	return result, err
}

type publishedPort struct {
	service string
	host    netip.Addr // Invalid means Docker's default binding on all addresses.
	target  uint32
}

type portBindings map[uint16][]publishedPort

func (bindings portBindings) add(service string, port types.ServicePortConfig) error {
	published, err := strconv.ParseUint(port.Published, 10, 16)
	if err != nil || published == 0 {
		return fmt.Errorf("services.%s.ports requires fixed published ports", service)
	}
	if published == 49983 {
		return fmt.Errorf("services.%s.ports: port 49983 is reserved for envd", service)
	}
	if port.Protocol != "tcp" && port.Protocol != "" {
		return fmt.Errorf("services.%s.ports: only TCP publishing is supported", service)
	}
	var host netip.Addr
	if port.HostIP != "" {
		host, err = netip.ParseAddr(strings.Trim(port.HostIP, "[]"))
		if err != nil {
			return fmt.Errorf("services.%s.ports: invalid host IP %q", service, port.HostIP)
		}
		host = host.Unmap()
	}
	for _, previous := range bindings[uint16(published)] {
		if previous.service == service && previous.host == host && previous.target == port.Target {
			return nil // Identical mappings within one service are redundant.
		}
		overlaps := !host.IsValid() || !previous.host.IsValid() || host == previous.host ||
			(host.Is4() == previous.host.Is4() && (host.IsUnspecified() || previous.host.IsUnspecified()))
		if overlaps {
			return fmt.Errorf("services.%s.ports: published TCP port %d conflicts with services.%s", service, published, previous.service)
		}
	}
	bindings[uint16(published)] = append(bindings[uint16(published)], publishedPort{service, host, port.Target})
	return nil
}

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
			service.Image = "aenv-build/" + name + ":pending"
		}
		service.Build = nil
		service.PullPolicy = ""
		project.Services[name] = service
	}
	plan.Compose, err = renderProject(project)
	if err != nil {
		return nil, err
	}
	return plan, validatePlanSize(plan)
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

// ReadRequest accepts one bounded JSON request from the standalone planner.
func ReadRequest(reader io.Reader) (Request, error) {
	var request Request
	input, err := io.ReadAll(io.LimitReader(reader, MaxRequestBytes+1))
	if err != nil {
		return request, err
	}
	if len(input) > MaxRequestBytes {
		return request, fmt.Errorf("Compose planner request exceeds %d bytes", MaxRequestBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return request, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return request, fmt.Errorf("expected one JSON request")
	}
	return request, nil
}
