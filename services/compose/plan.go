// Package compose prepares an image-only Compose project for an AgentENV sandbox.
package compose

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/compose-spec/compose-go/v2/loader"
	"github.com/compose-spec/compose-go/v2/template"
	"github.com/compose-spec/compose-go/v2/types"
	"go.yaml.in/yaml/v3"
)

// MaxServices matches MAX_EXTRA_DRIVES in src/sandbox/firecracker/config.rs:
// every service runs from its own image drive.
const MaxServices = 24

// MaxSourceBytes bounds the submitted Compose source.
const MaxSourceBytes = 1024 * 1024

// MaxPlanBytes separately bounds YAML expansion, interpolation, resolved
// environment values and the rendered project. The server applies the same
// limit after adding image configurations.
const MaxPlanBytes = 4 * 1024 * 1024

// defaultEnvdPort is the server's default tools.control_plane_port.
const defaultEnvdPort = 49983

// projectName must match the --project-name used by src/orchestrator/compose.rs.
const projectName = "aenv"

type Request struct {
	Compose     string            `json:"compose"`
	Environment map[string]string `json:"composeEnv,omitempty"`
	Profiles    []string          `json:"profiles,omitempty"`
	// EnvdPort is the guest control-plane port, which services must not
	// publish. Zero selects the server default.
	EnvdPort uint16 `json:"envdPort,omitempty"`
}

type Service struct {
	Name       string `json:"name"`
	Image      string `json:"image"`
	LocalImage string `json:"localImage"`
}

type Plan struct {
	Compose  json.RawMessage `json:"compose"`
	Services []Service       `json:"services"`
}

// InvalidError reports a Compose project that the sandbox cannot run.
type InvalidError struct{ Err error }

func (e *InvalidError) Error() string { return e.Err.Error() }

func (e *InvalidError) Unwrap() error { return e.Err }

// Prepare never imports the server's environment, files, or remote resources.
// Validate the raw tree before invoking the loader, which can otherwise read
// env_file/label_file/include/extends during normalization.
func Prepare(ctx context.Context, request Request) (*Plan, error) {
	project, err := loadProject(ctx, request)
	if err != nil {
		return nil, &InvalidError{err}
	}
	plan, err := runtimePlan(project)
	if err != nil {
		return nil, err
	}
	if len(plan.Compose) > MaxPlanBytes {
		return nil, &InvalidError{fmt.Errorf("compose plan exceeds the 4 MiB limit")}
	}
	return plan, nil
}

func parseDocument(source string) (map[string]any, error) {
	if len(source) == 0 || len(source) > MaxSourceBytes {
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

func loadProject(ctx context.Context, request Request) (*types.Project, error) {
	raw, err := parseDocument(request.Compose)
	if err != nil {
		return nil, err
	}
	if err := validateRaw(raw); err != nil {
		return nil, err
	}
	budget := expansionBudget{MaxPlanBytes}
	project, err := loader.LoadWithContext(ctx, types.ConfigDetails{
		WorkingDir: "/var/lib/agentenv-compose",
		// Pass the exact validated tree; do not parse the source twice with
		// potentially different YAML merge/document handling.
		ConfigFiles: []types.ConfigFile{{Filename: "compose.yaml", Config: raw}},
		Environment: types.Mapping(request.Environment),
	}, func(o *loader.Options) {
		o.SetProjectName(projectName, true)
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
	for _, service := range project.Services {
		service.Environment.RemoveEmpty()
	}
	if len(project.Services) == 0 || len(project.Services) > MaxServices {
		return nil, fmt.Errorf("compose must select 1..%d services", MaxServices)
	}
	envdPort := request.EnvdPort
	if envdPort == 0 {
		envdPort = defaultEnvdPort
	}
	return project, validateProject(project, envdPort)
}

// validateProject checks interpolated values against what the sandbox runtime
// supports. Options that only affect the guest pass through: the VM, not the
// guest Docker daemon, is the isolation boundary.
func validateProject(project *types.Project, envdPort uint16) error {
	for name, volume := range project.Volumes {
		if volume.Driver != "" && volume.Driver != "local" {
			return fmt.Errorf("volumes.%s.driver must be local", name)
		}
	}
	for name, network := range project.Networks {
		if network.Driver != "" && network.Driver != "bridge" {
			return fmt.Errorf("networks.%s.driver must be bridge", name)
		}
	}
	published := map[uint64]string{}
	for _, name := range project.ServiceNames() {
		service := project.Services[name]
		if service.Image == "" {
			return fmt.Errorf("services.%s.image is required", name)
		}
		if service.Platform != "" && service.Platform != "linux/amd64" {
			return fmt.Errorf("services.%s.platform: only linux/amd64 is supported", name)
		}
		// A service's container runs directly on its own writable image drive.
		if service.GetScale() > 1 {
			return fmt.Errorf("services.%s: scale and deploy.replicas above 1 are not supported", name)
		}
		// The loader already rejects service: targets that are missing, inactive
		// or cyclic. Containers outside the project do not exist in the guest.
		if mode := service.NetworkMode; mode != "" && mode != "none" && mode != "host" && mode != "bridge" && !strings.HasPrefix(mode, "service:") {
			return fmt.Errorf("services.%s.network_mode only supports none, host, bridge or service:<name>", name)
		}
		for _, volume := range service.Volumes {
			switch volume.Type {
			case types.VolumeTypeVolume, types.VolumeTypeTmpfs:
			case types.VolumeTypeBind:
				// The sandbox has no project directory; only guest paths can be bound.
				if !strings.HasPrefix(volume.Source, "/") {
					return fmt.Errorf("services.%s.volumes: bind source %q must be an absolute guest path", name, volume.Source)
				}
			default:
				return fmt.Errorf("services.%s.volumes: %s mounts are not supported", name, volume.Type)
			}
		}
		for _, port := range service.Ports {
			number, err := strconv.ParseUint(port.Published, 10, 16)
			if err != nil || number == 0 {
				return fmt.Errorf("services.%s.ports requires fixed published ports", name)
			}
			if port.Protocol != "tcp" && port.Protocol != "" {
				return fmt.Errorf("services.%s.ports: only TCP publishing is supported", name)
			}
			if number == uint64(envdPort) {
				return fmt.Errorf("services.%s.ports: port %d is reserved for envd", name, number)
			}
			// The sandbox proxy connects to the guest address, so a published
			// port must listen on every guest address.
			if port.HostIP != "" && port.HostIP != "0.0.0.0" && port.HostIP != "::" {
				return fmt.Errorf("services.%s.ports: host_ip must be empty, 0.0.0.0 or :: to be reachable through the sandbox proxy", name)
			}
			if other, exists := published[number]; exists {
				return fmt.Errorf("services.%s.ports: port %d is already published by service %s", name, number, other)
			}
			published[number] = name
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
		alias := fmt.Sprintf("aenv-compose/service-%d:local", i)
		plan.Services = append(plan.Services, Service{
			Name: name, Image: service.Image, LocalImage: alias,
		})
		service.Image = alias
		project.Services[name] = service
	}
	var err error
	plan.Compose, err = renderProject(project)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

// The output contains active services only and survives the next Compose load
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
			if network, ok := value.(map[string]any); ok {
				delete(network, "ipam")
			}
		}
	}
	return json.Marshal(escapeDollars(normalized))
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

// Keys the loader resolves against the planner host (env_file, label_file,
// extends, include) must never be allowed. The other omissions cannot work in
// a sandbox: the server resolves images, there is no project directory or
// swarm, and the guest has no external resources, plugins or GPUs.
var (
	topLevelKeys = keySet("name version services networks volumes")
	serviceKeys  = keySet(`annotations attach cap_add cap_drop command container_name
		cpu_shares cpus cpuset depends_on deploy devices dns dns_opt dns_search domainname
		entrypoint environment expose extra_hosts group_add healthcheck hostname image init
		ipc labels links logging mem_limit mem_reservation memswap_limit network_mode
		networks oom_score_adj pid pids_limit platform ports post_start pre_stop privileged
		profiles read_only restart scale security_opt shm_size stdin_open stop_grace_period
		stop_signal sysctls tmpfs tty ulimits user volumes volumes_from working_dir`)
	volumeKeys = keySet("name labels driver driver_opts")
	// Custom IPAM could overlap the sandbox's own guest network.
	networkKeys = keySet("name labels driver internal attachable")
)

func keySet(keys string) map[string]bool {
	set := map[string]bool{}
	for _, key := range strings.Fields(keys) {
		set[key] = true
	}
	return set
}

func allowedKeys(value map[string]any, path string, allowed map[string]bool) error {
	for key := range value {
		if !allowed[key] && !strings.HasPrefix(key, "x-") {
			return fmt.Errorf("%s%s is not supported by sandbox Compose", path, key)
		}
	}
	return nil
}

// stringMap fails closed: a YAML mapping with any non-string key, such as a
// !!binary key, decodes as map[any]any and must not skip validation.
func stringMap(value any, path string) (map[string]any, error) {
	if value == nil {
		return nil, nil
	}
	entries, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s must be a mapping with string keys", path)
	}
	return entries, nil
}

func validateRaw(raw map[string]any) error {
	if err := allowedKeys(raw, "", topLevelKeys); err != nil {
		return err
	}
	services, err := stringMap(raw["services"], "services")
	if err != nil {
		return err
	}
	if services == nil {
		return fmt.Errorf("services must be a mapping")
	}
	for name, value := range services {
		service, err := stringMap(value, "services."+name)
		if err != nil {
			return err
		}
		if err := allowedKeys(service, "services."+name+".", serviceKeys); err != nil {
			return err
		}
	}
	for _, section := range []struct {
		name string
		keys map[string]bool
	}{{"volumes", volumeKeys}, {"networks", networkKeys}} {
		entries, err := stringMap(raw[section.name], section.name)
		if err != nil {
			return err
		}
		for name, value := range entries {
			entry, err := stringMap(value, section.name+"."+name)
			if err != nil {
				return err
			}
			if err := allowedKeys(entry, section.name+"."+name+".", section.keys); err != nil {
				return err
			}
		}
	}
	return nil
}

type expansionBudget struct{ remaining int }

func (b *expansionBudget) take(size int) error {
	if size > b.remaining {
		return fmt.Errorf("compose expansion exceeds the 4 MiB limit")
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
			return fmt.Errorf("compose YAML nesting or recursive alias exceeds the supported limit")
		}
		// Docker Compose gives tags such as !reset and !override their own
		// semantics; plain decoding would silently differ.
		if tag := node.ShortTag(); strings.HasPrefix(tag, "!") && !strings.HasPrefix(tag, "!!") {
			return fmt.Errorf("YAML tag %s is not supported by sandbox Compose", tag)
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
