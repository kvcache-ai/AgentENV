// Package compose loads a Compose project the way Docker Compose does and
// plans it for an AgentENV sandbox.
package compose

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/compose-spec/compose-go/v2/cli"
	"github.com/compose-spec/compose-go/v2/types"
	"github.com/sirupsen/logrus"
)

// MaxServices matches MAX_EXTRA_DRIVES in src/sandbox/firecracker/config.rs:
// every service runs from its own image drive.
const MaxServices = 24

// MaxPlanBytes bounds the rendered project, which the CLI sends to the guest
// on stdin.
const MaxPlanBytes = 4 * 1024 * 1024

// DefaultEnvdPort is the server's default tools.control_plane_port.
const DefaultEnvdPort = 49983

// Options mirrors the Docker Compose project flags.
type Options struct {
	ConfigPaths []string
	ProjectDir  string
	EnvFiles    []string
	ProjectName string
	Profiles    []string
	// EnvdPort is the guest control-plane port, which services must not
	// publish.
	EnvdPort uint16
	// Build plans only the service image builds. Like docker compose build,
	// it checks only what building needs; aenv compose up checks the rest.
	Build bool
}

// Build has an empty Dockerfile when DockerfileInline is set.
type Build struct {
	Context          string            `json:"context"`
	Dockerfile       string            `json:"dockerfile"`
	DockerfileInline string            `json:"dockerfileInline,omitempty"`
	Args             map[string]string `json:"args"`
	Target           string            `json:"target,omitempty"`
	NoCache          bool              `json:"noCache,omitempty"`
}

// Service is an active service. Image is empty for a service that only
// declares build. As in Docker Compose, a service without build whose image
// another service builds runs that build's image: BuiltBy names the builder.
type Service struct {
	Name    string `json:"name"`
	Image   string `json:"image,omitempty"`
	Build   *Build `json:"build,omitempty"`
	BuiltBy string `json:"builtBy,omitempty"`
}

// Plan contains the rendered project, without build sections, and its
// services sorted by name.
type Plan struct {
	ProjectName      string `json:"projectName"`
	ProjectDirectory string `json:"projectDirectory"`
	// ComposeFiles are the loaded files, "-" for stdin, after discovery.
	ComposeFiles []string        `json:"composeFiles"`
	Compose      json.RawMessage `json:"compose"`
	Services     []Service       `json:"services"`
}

// InvalidError reports a Compose project that the sandbox cannot run.
type InvalidError struct{ Err error }

func (e *InvalidError) Error() string { return e.Err.Error() }

func (e *InvalidError) Unwrap() error { return e.Err }

// Prepare loads the project like Docker Compose, from the process
// environment, working directory and stdin, checks that the sandbox can run
// it, or with Build only that its services can be built, and writes warnings
// to stderr.
func Prepare(ctx context.Context, options Options) (*Plan, error) {
	stdin, err := readStdin(options.ConfigPaths)
	if err != nil {
		return nil, err
	}
	var includes []string
	project, err := load(ctx, options, stdin, true, &includes)
	if err != nil {
		return nil, &InvalidError{err}
	}
	if options.Build {
		if err := validateBuilds(project); err != nil {
			return nil, &InvalidError{err}
		}
		return plan(project)
	}
	// The same project with relative paths unresolved tells which paths the
	// files spelled relative to the project.
	unresolved, err := load(ctx, options, stdin, false, nil)
	if err != nil {
		return nil, &InvalidError{err}
	}
	envdPort := options.EnvdPort
	if envdPort == 0 {
		envdPort = DefaultEnvdPort
	}
	roots := append([]string{project.WorkingDir}, includes...)
	if err := validate(project, unresolved, roots, envdPort); err != nil {
		return nil, &InvalidError{err}
	}
	return plan(project)
}

// readStdin buffers a "-" Compose file, which both loads read from os.Stdin.
func readStdin(paths []string) ([]byte, error) {
	for _, path := range paths {
		if path == "-" {
			return io.ReadAll(os.Stdin)
		}
	}
	return nil, nil
}

// load follows buildProjectOptions and postProcessProject in Docker Compose's
// internal/projectloader/loader.go and the environment resolution of
// WithServices in cmd/compose/compose.go. Remote resources are not loaded.
func load(ctx context.Context, options Options, stdin []byte, resolvePaths bool, includes *[]string) (*types.Project, error) {
	envFiles := options.EnvFiles
	if len(envFiles) == 0 {
		// Docker Compose's --env-file defaults to COMPOSE_ENV_FILES.
		envFiles = strings.FieldsFunc(os.Getenv("COMPOSE_ENV_FILES"), func(r rune) bool { return r == ',' })
	}
	fns := []cli.ProjectOptionsFn{
		cli.WithWorkingDirectory(options.ProjectDir),
		cli.WithOsEnv,
	}
	if _, present := os.LookupEnv("PWD"); !present {
		if pwd, err := os.Getwd(); err == nil {
			fns = append(fns, cli.WithEnv([]string{"PWD=" + pwd}))
		}
	}
	fns = append(fns,
		cli.WithEnvFiles(envFiles...),
		cli.WithDotEnv,
		cli.WithConfigFileEnv,
		cli.WithDefaultConfigPath,
		cli.WithEnvFiles(envFiles...),
		cli.WithDotEnv,
		cli.WithDefaultProfiles(options.Profiles...),
		cli.WithName(options.ProjectName),
		cli.WithoutEnvironmentResolution,
		cli.WithResolvedPaths(resolvePaths),
	)
	projectOptions, err := cli.NewProjectOptions(options.ConfigPaths, fns...)
	if err != nil {
		return nil, err
	}
	if includes != nil {
		// Only includes declared by the loaded files report an event.
		projectOptions.WithListeners(func(event string, metadata map[string]any) {
			paths, _ := metadata["path"].(types.StringList)
			if event != "include" || len(paths) == 0 {
				return
			}
			path := paths[0]
			if dir, _ := metadata["workingdir"].(string); !filepath.IsAbs(path) {
				path = filepath.Join(dir, path)
			}
			*includes = append(*includes, filepath.Dir(path))
		})
	}
	if !resolvePaths {
		// The resolving load reports the warnings.
		logrus.SetOutput(io.Discard)
		defer logrus.SetOutput(os.Stderr)
	}
	var project *types.Project
	err = withStdin(stdin, func() (err error) {
		project, err = projectOptions.LoadProject(ctx)
		return err
	})
	if err != nil {
		return nil, err
	}
	if project.Name == "" {
		return nil, fmt.Errorf("project name can't be empty. Use ProjectName option to set a valid name")
	}
	if project, err = project.WithServicesEnabled(); err != nil {
		return nil, err
	}
	if project, err = project.WithSelectedServices(nil); err != nil {
		return nil, err
	}
	return project.WithoutUnnecessaryResources().WithServicesEnvironmentResolved(true)
}

func withStdin(content []byte, fn func() error) error {
	if content == nil {
		return fn()
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		return err
	}
	defer reader.Close()
	go func() {
		_, _ = writer.Write(content)
		writer.Close()
	}()
	saved := os.Stdin
	os.Stdin = reader
	defer func() { os.Stdin = saved }()
	return fn()
}

// validate checks the loaded project against what the sandbox runtime
// supports. Options that only affect the guest pass through: the VM, not the
// guest Docker daemon, is the isolation boundary.
func validate(project, unresolved *types.Project, roots []string, envdPort uint16) error {
	if len(project.Services) == 0 || len(project.Services) > MaxServices {
		return fmt.Errorf("compose must select 1..%d services", MaxServices)
	}
	if len(project.Jobs) > 0 {
		return fmt.Errorf("jobs are not supported by sandbox Compose")
	}
	if len(project.Models) > 0 {
		return fmt.Errorf("models are not supported by sandbox Compose")
	}
	for name, volume := range project.Volumes {
		if volume.External {
			return fmt.Errorf("volumes.%s: external volumes do not exist in the sandbox", name)
		}
		if volume.Driver != "" && volume.Driver != "local" {
			return fmt.Errorf("volumes.%s.driver must be local", name)
		}
	}
	for name, network := range project.Networks {
		if network.External {
			return fmt.Errorf("networks.%s: external networks do not exist in the sandbox", name)
		}
		if network.Driver != "" && network.Driver != "bridge" {
			return fmt.Errorf("networks.%s.driver must be bridge", name)
		}
	}
	published := map[uint64]string{}
	for _, name := range project.ServiceNames() {
		service := project.Services[name]
		if service.Provider != nil {
			return fmt.Errorf("services.%s.provider is not supported by sandbox Compose", name)
		}
		if len(service.Models) > 0 {
			return fmt.Errorf("services.%s.models is not supported by sandbox Compose", name)
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
		raw := unresolved.Services[name]
		if err := validateVolumes(name, service.Volumes, raw.Volumes, roots); err != nil {
			return err
		}
		for i, hook := range service.PreStart {
			// The hook runs from its service's image drive.
			if hook.Image != "" && hook.Image != service.Image {
				return fmt.Errorf("services.%s.pre_start[%d].image must be the service image", name, i)
			}
			var rawHook []types.ServiceVolumeConfig
			if i < len(raw.PreStart) {
				rawHook = raw.PreStart[i].Volumes
			}
			if err := validateVolumes(name, hook.Volumes, rawHook, roots); err != nil {
				return err
			}
		}
		if service.Build != nil {
			if err := validateBuild(name, service.Build); err != nil {
				return err
			}
		}
		if service.Develop != nil {
			warn("services.%s.develop is ignored by sandbox Compose", name)
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
	objects := map[string]types.FileObjectConfig{}
	for name, config := range project.Configs {
		objects["configs."+name] = types.FileObjectConfig(config)
	}
	for name, secret := range project.Secrets {
		objects["secrets."+name] = types.FileObjectConfig(secret)
	}
	for path, object := range objects {
		switch {
		case bool(object.External):
			return fmt.Errorf("%s: external objects do not exist in the sandbox", path)
		case object.File != "":
			return fmt.Errorf("%s: file sources are not supported: the project runs in a remote sandbox (copy the file into the image; configs can use inline content)", path)
		case object.Environment != "":
			// The guest Compose runs with a clean environment.
			return fmt.Errorf("%s: environment sources are not supported by sandbox Compose", path)
		}
	}
	return nil
}

// validateVolumes rejects mounts the guest cannot provide. The project runs in
// a new sandbox, a non-local platform, which the Compose Specification
// requires to reject relative host paths. An absolute source in the project
// or an included project directory, such as ${PWD}/data, is equally a file on
// this machine (docker/compose#7125), so it is rejected too. Other absolute
// bind sources are guest paths.
func validateVolumes(service string, volumes, unresolved []types.ServiceVolumeConfig, roots []string) error {
	for i, volume := range volumes {
		switch volume.Type {
		case types.VolumeTypeVolume, types.VolumeTypeTmpfs:
		case types.VolumeTypeBind:
			relative := len(unresolved) == len(volumes) && unresolved[i].Source != volume.Source
			if relative || within(volume.Source, roots) {
				return fmt.Errorf("services.%s.volumes: bind mount of local path %s is not supported: the project runs in a remote sandbox (copy the files into the image, or use a named volume or a guest path)", service, volume.Source)
			}
		default:
			return fmt.Errorf("services.%s.volumes: %s mounts are not supported", service, volume.Type)
		}
	}
	return nil
}

func within(path string, roots []string) bool {
	for _, root := range roots {
		if relative, err := filepath.Rel(root, path); err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// validateBuilds checks the services that declare build, as docker compose
// build checks only what building needs. The builder produces x86-64 images,
// which the sandbox runtime requires.
func validateBuilds(project *types.Project) error {
	for _, name := range project.ServiceNames() {
		service := project.Services[name]
		if service.Build == nil {
			continue
		}
		if service.Platform != "" && service.Platform != "linux/amd64" {
			return fmt.Errorf("services.%s.platform: only linux/amd64 is supported", name)
		}
		if err := validateBuild(name, service.Build); err != nil {
			return err
		}
	}
	return nil
}

// validateBuild accepts what the server's builder can reproduce: a local
// context, a Dockerfile or inline Dockerfile, arguments, target and no_cache.
// Options that change the built image are rejected; the others are ignored
// with a warning.
func validateBuild(service string, build *types.BuildConfig) error {
	context := build.Context
	if strings.Contains(context, "://") || strings.HasPrefix(context, "git@") || strings.Contains(context, "#") {
		return fmt.Errorf("services.%s.build.context must be a local directory", service)
	}
	for _, unsupported := range []struct {
		key string
		set bool
	}{
		{"secrets", len(build.Secrets) > 0},
		{"ssh", len(build.SSH) > 0},
		{"additional_contexts", len(build.AdditionalContexts) > 0},
		{"entitlements", len(build.Entitlements) > 0},
		{"privileged", build.Privileged},
	} {
		if unsupported.set {
			return fmt.Errorf("services.%s.build.%s is not supported by sandbox Compose", service, unsupported.key)
		}
	}
	for _, ignored := range []struct {
		key string
		set bool
	}{
		{"cache_from", len(build.CacheFrom) > 0},
		{"cache_to", len(build.CacheTo) > 0},
		{"extra_hosts", len(build.ExtraHosts) > 0},
		{"isolation", build.Isolation != ""},
		{"labels", len(build.Labels) > 0},
		{"network", build.Network != ""},
		{"no_cache_filter", len(build.NoCacheFilter) > 0},
		{"platforms", len(build.Platforms) > 0},
		{"provenance", build.Provenance != ""},
		{"pull", build.Pull},
		{"sbom", build.SBOM != ""},
		{"shm_size", build.ShmSize != 0},
		{"tags", len(build.Tags) > 0},
		{"ulimits", len(build.Ulimits) > 0},
	} {
		if ignored.set {
			warn("services.%s.build.%s is ignored by sandbox Compose", service, ignored.key)
		}
	}
	return nil
}

func warn(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "warning: "+format+"\n", args...)
}

func plan(project *types.Project) (*Plan, error) {
	result := &Plan{
		ProjectName: project.Name, ProjectDirectory: project.WorkingDir,
		ComposeFiles: project.ComposeFiles, Services: []Service{},
	}
	names := project.ServiceNames()
	sort.Strings(names)
	// Docker Compose's isServiceImageToBuild: a service whose image a build
	// produces does not pull it.
	builders := map[string]string{}
	for _, name := range names {
		service := project.Services[name]
		if _, exists := builders[service.Image]; service.Build != nil && service.Image != "" && !exists {
			builders[service.Image] = name
		}
	}
	for _, name := range names {
		service := project.Services[name]
		planned := Service{Name: name, Image: service.Image}
		if service.Build == nil {
			planned.BuiltBy = builders[service.Image]
		}
		if build := service.Build; build != nil {
			context := build.Context
			// Normalization adds the default "." after paths are resolved.
			if !filepath.IsAbs(context) {
				context = filepath.Join(project.WorkingDir, context)
			}
			// An argument without a value keeps its Dockerfile default.
			args := map[string]string{}
			for key, value := range build.Args {
				if value != nil {
					args[key] = *value
				}
			}
			planned.Build = &Build{
				Context: context, Dockerfile: build.Dockerfile, DockerfileInline: build.DockerfileInline,
				Args: args, Target: build.Target, NoCache: build.NoCache,
			}
		}
		result.Services = append(result.Services, planned)
	}
	var err error
	result.Compose, err = render(project)
	if err != nil {
		return nil, err
	}
	if len(result.Compose) > MaxPlanBytes {
		return nil, &InvalidError{fmt.Errorf("compose plan exceeds the 4 MiB limit")}
	}
	return result, nil
}

// render returns the active services as the guest runs them: the CLI
// provides every image, the guest has none of this machine's files, and
// the guest Compose loads the document once more.
func render(project *types.Project) (json.RawMessage, error) {
	// Labels are resolved during load; the label files stay on this machine.
	project, err := project.WithServicesLabelsResolved(true)
	if err != nil {
		return nil, err
	}
	for name, service := range project.Services {
		service.Profiles = nil
		service.Build = nil
		service.PullPolicy = ""
		service.PullRefreshAfter = ""
		service.Develop = nil
		// Unset values must not be resolved again from the guest environment.
		service.Environment.RemoveEmpty()
		for i := range service.PreStart {
			service.PreStart[i].Environment.RemoveEmpty()
			// Inherit the service image again once the CLI replaces it.
			if service.PreStart[i].Image == service.Image {
				service.PreStart[i].Image = ""
			}
		}
		project.Services[name] = service
	}
	encoded, err := project.MarshalJSON()
	if err != nil {
		return nil, err
	}
	// As docker compose config does, escape the interpolated values so the
	// next load does not interpolate them again.
	return bytes.ReplaceAll(encoded, []byte{'$'}, []byte{'$', '$'}), nil
}
