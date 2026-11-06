package compose

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/compose-spec/compose-go/v2/loader"
	"github.com/compose-spec/compose-go/v2/types"
)

// clearComposeEnv keeps the developer's COMPOSE_* settings out of the tests.
func clearComposeEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"COMPOSE_FILE", "COMPOSE_PATH_SEPARATOR", "COMPOSE_PROFILES",
		"COMPOSE_PROJECT_NAME", "COMPOSE_ENV_FILES", "COMPOSE_DISABLE_ENV_FILE"} {
		t.Setenv(key, "")
		os.Unsetenv(key)
	}
}

// project writes files into a new working directory named "app".
func project(t *testing.T, files map[string]string) string {
	t.Helper()
	clearComposeEnv(t)
	dir := filepath.Join(t.TempDir(), "app")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range files {
		path := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(dir)
	return dir
}

func prepare(t *testing.T, compose string) *Plan {
	t.Helper()
	project(t, map[string]string{"compose.yaml": compose})
	plan, err := Prepare(context.Background(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

// rejects requires an InvalidError, which the planner command reports with its
// dedicated exit status, and checks that the intended rule caused it.
func rejects(t *testing.T, options Options, message string) {
	t.Helper()
	_, err := Prepare(context.Background(), options)
	var invalid *InvalidError
	if !errors.As(err, &invalid) || !strings.Contains(err.Error(), message) {
		t.Fatalf("expected invalid Compose error containing %q, got %v", message, err)
	}
}

func lines(t *testing.T, path string) []string {
	t.Helper()
	content, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var result []string
	for _, line := range strings.Split(string(content), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			result = append(result, line)
		}
	}
	return result
}

// options parses the Docker Compose project flags used by the parity cases.
func options(t *testing.T, args []string) Options {
	t.Helper()
	var result Options
	for i := 0; i < len(args); i += 2 {
		if i+1 == len(args) {
			t.Fatalf("flag %s has no value", args[i])
		}
		value := args[i+1]
		switch args[i] {
		case "-f", "--file":
			result.ConfigPaths = append(result.ConfigPaths, value)
		case "-p", "--project-name":
			result.ProjectName = value
		case "--profile":
			result.Profiles = append(result.Profiles, value)
		case "--env-file":
			result.EnvFiles = append(result.EnvFiles, value)
		case "--project-directory":
			result.ProjectDir = value
		default:
			t.Fatalf("unsupported flag %s", args[i])
		}
	}
	return result
}

func copyCase(t *testing.T, source, target string) {
	t.Helper()
	err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		if entry.Name() == "dotenv" {
			relative = filepath.Join(filepath.Dir(relative), ".env")
		}
		destination := filepath.Join(target, relative)
		if entry.IsDir() {
			return os.MkdirAll(destination, 0o755)
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(destination, content, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
}

// assertMatchesDockerCompose loads the project in the current directory like
// `docker compose ARGS config --format json` and compares both documents.
func assertMatchesDockerCompose(t *testing.T, args []string) {
	t.Helper()
	project, err := load(context.Background(), options(t, args), nil, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := project.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	encoded = bytes.ReplaceAll(encoded, []byte{'$'}, []byte{'$', '$'})
	command := exec.Command("docker", append(append([]string{"compose"}, args...), "config", "--format", "json")...)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	expected, err := command.Output()
	if err != nil {
		t.Fatalf("docker compose config: %v: %s", err, stderr.String())
	}
	var got, want any
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(expected, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("planner:\n%s\ndocker compose config:\n%s", encoded, expected)
	}
}

// The planner must load every project exactly like the installed Docker
// Compose, which is skipped when it is unavailable.
func TestLoadMatchesDockerCompose(t *testing.T) {
	version, err := exec.Command("docker", "compose", "version", "--short").Output()
	if err != nil {
		t.Skip("docker compose is not installed")
	}
	t.Logf("docker compose %s", bytes.TrimSpace(version))
	cases, err := filepath.Glob(filepath.Join("testdata", "parity", "*"))
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range cases {
		if info, err := os.Stat(source); err != nil || !info.IsDir() {
			continue
		}
		source, err := filepath.Abs(source)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(filepath.Base(source), func(t *testing.T) {
			clearComposeEnv(t)
			for _, entry := range lines(t, filepath.Join(source, "env")) {
				key, value, _ := strings.Cut(entry, "=")
				t.Setenv(key, value)
			}
			dir := filepath.Join(t.TempDir(), filepath.Base(source))
			copyCase(t, source, dir)
			t.Chdir(dir)
			assertMatchesDockerCompose(t, lines(t, filepath.Join(source, "args")))
		})
	}
	t.Run("deploy", func(t *testing.T) {
		clearComposeEnv(t)
		t.Setenv("AENV_API_KEY", "e2b_parity")
		t.Setenv("SANDBOX_PROXY_DOMAINS", "sandbox.example.com")
		assertMatchesDockerCompose(t, []string{"-f", filepath.Join("..", "..", "deploy", "docker-compose.yml")})
	})
}

// Interpolation runs once on this machine, as Docker Compose does; the guest
// Compose must not interpolate or resolve the rendered values again.
func TestRenderedProjectIsInterpolatedOnce(t *testing.T) {
	t.Setenv("VALUE", "$literal")
	t.Setenv("FROM_OS", "os")
	os.Unsetenv("UNSET")
	plan := prepare(t, `services:
  a:
    image: busybox:1.37
    command: ["sh", "-c", "echo $$HOME ${VALUE}"]
    environment:
      PASS: ${VALUE}
      FROM_OS:
      UNSET:
`)
	guest, err := loader.LoadWithContext(context.Background(), types.ConfigDetails{
		ConfigFiles: []types.ConfigFile{{Filename: "-", Content: plan.Compose}},
		Environment: types.Mapping{"HOME": "wrong", "literal": "wrong", "UNSET": "guest"},
	}, func(o *loader.Options) { o.SetProjectName(plan.ProjectName, true) })
	if err != nil {
		t.Fatal(err)
	}
	service := guest.Services["a"]
	if got := service.Command[2]; got != "echo $HOME $literal" {
		t.Fatalf("command changed: %q", got)
	}
	if got := *service.Environment["PASS"]; got != "$literal" {
		t.Fatal(got)
	}
	if got := *service.Environment["FROM_OS"]; got != "os" {
		t.Fatal(got)
	}
	if _, ok := service.Environment["UNSET"]; ok {
		t.Fatal("an unset variable was resolved again in the guest")
	}
}

// The guest has no build contexts, label files or env files, and runs
// pre_start hooks from the service image the CLI provides.
func TestRenderedProjectIsSelfContained(t *testing.T) {
	project(t, map[string]string{
		"compose.yaml": `services:
  app:
    build: ./app
    image: example/app:dev
    pull_policy: build
    label_file: app.labels
    env_file: app.env
    pre_start:
      - command: ["true"]
    develop:
      watch: [{path: ., action: rebuild}]
`,
		"app.labels": "team=sandbox\n",
		"app.env":    "FROM_FILE=yes\n",
	})
	plan, err := Prepare(context.Background(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	var rendered struct {
		Services map[string]map[string]any `json:"services"`
	}
	if err := json.Unmarshal(plan.Compose, &rendered); err != nil {
		t.Fatal(err)
	}
	app := rendered.Services["app"]
	for _, key := range []string{"build", "pull_policy", "label_file", "env_file", "develop", "profiles"} {
		if _, ok := app[key]; ok {
			t.Fatalf("rendered service keeps %s: %s", key, plan.Compose)
		}
	}
	if app["labels"].(map[string]any)["team"] != "sandbox" || app["environment"].(map[string]any)["FROM_FILE"] != "yes" {
		t.Fatalf("label or env file not resolved: %s", plan.Compose)
	}
	if hook := app["pre_start"].([]any)[0].(map[string]any); hook["image"] != nil {
		t.Fatalf("pre_start hook must inherit the replaced service image: %s", plan.Compose)
	}
	// The guest Compose runs the hook from whatever image the CLI assigns.
	var document map[string]any
	if err := json.Unmarshal(plan.Compose, &document); err != nil {
		t.Fatal(err)
	}
	document["services"].(map[string]any)["app"].(map[string]any)["image"] = "aenv-compose/service-0:local"
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	guest, err := loader.LoadWithContext(context.Background(), types.ConfigDetails{
		ConfigFiles: []types.ConfigFile{{Filename: "-", Content: encoded}},
	}, func(o *loader.Options) { o.SetProjectName(plan.ProjectName, true) })
	if err != nil {
		t.Fatal(err)
	}
	if got := guest.Services["app"].PreStart[0].Image; got != "aenv-compose/service-0:local" {
		t.Fatalf("pre_start image: %q", got)
	}
}

func TestBuildServices(t *testing.T) {
	dir := project(t, map[string]string{"compose.yaml": `services:
  app:
    build:
      context: ./app
      dockerfile: docker/Custom
      args: {VALUE: "${VALUE}", FROM_ENV: null, USE_DEFAULT: null}
      target: final
      no_cache: true
  worker:
    build: {dockerfile: Worker.Dockerfile}
    image: example/worker:dev
  sidecar:
    image: example/worker:dev
  inline:
    build:
      dockerfile_inline: |
        FROM busybox
        ENV VALUE=${VALUE}
  db:
    image: redis:7
  optional:
    build: ./optional
    profiles: [extra]
`})
	t.Setenv("VALUE", "literal$VALUE")
	t.Setenv("FROM_ENV", "a=b")
	os.Unsetenv("USE_DEFAULT")
	plan, err := Prepare(context.Background(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	want := []Service{
		{Name: "app", Build: &Build{
			Context: filepath.Join(dir, "app"), Dockerfile: "docker/Custom",
			Args:   map[string]string{"VALUE": "literal$VALUE", "FROM_ENV": "a=b"},
			Target: "final", NoCache: true,
		}},
		{Name: "db", Image: "redis:7"},
		{Name: "inline", Build: &Build{
			Context: dir, DockerfileInline: "FROM busybox\nENV VALUE=literal$VALUE\n", Args: map[string]string{},
		}},
		// The sidecar runs the image the worker builds, as in Docker Compose.
		{Name: "sidecar", Image: "example/worker:dev", BuiltBy: "worker"},
		{Name: "worker", Image: "example/worker:dev", Build: &Build{
			Context: dir, Dockerfile: "Worker.Dockerfile", Args: map[string]string{},
		}},
	}
	if !reflect.DeepEqual(plan.Services, want) {
		got, _ := json.Marshal(plan.Services)
		t.Fatalf("services: %s", got)
	}
	if plan.ProjectDirectory != dir {
		t.Fatalf("project directory: %s", plan.ProjectDirectory)
	}
	if want := []string{filepath.Join(dir, "compose.yaml")}; !reflect.DeepEqual(plan.ComposeFiles, want) {
		t.Fatalf("compose files: %v", plan.ComposeFiles)
	}
}

func TestProjectName(t *testing.T) {
	compose := "services: {app: {image: busybox}}\n"
	if got := prepare(t, compose).ProjectName; got != "app" {
		t.Fatalf("directory name: %q", got)
	}
	if got := prepare(t, "name: named\n"+compose).ProjectName; got != "named" {
		t.Fatalf("name attribute: %q", got)
	}
	project(t, map[string]string{"compose.yaml": "name: named\n" + compose})
	t.Setenv("COMPOSE_PROJECT_NAME", "from-env")
	plan, err := Prepare(context.Background(), Options{})
	if err != nil || plan.ProjectName != "from-env" {
		t.Fatalf("COMPOSE_PROJECT_NAME: %v %v", plan, err)
	}
	plan, err = Prepare(context.Background(), Options{ProjectName: "flag"})
	if err != nil || plan.ProjectName != "flag" {
		t.Fatalf("-p: %v %v", plan, err)
	}
	rejects(t, Options{ProjectName: "Not Valid"}, "invalid project name")
}

// Both loads must read a project from stdin, which resolves against the
// working directory.
func TestStdinProject(t *testing.T) {
	dir := project(t, nil)
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stdin
	os.Stdin = reader
	t.Cleanup(func() { os.Stdin = saved })
	go func() {
		writer.WriteString("services: {app: {image: busybox, volumes: ['./data:/data']}}\n")
		writer.Close()
	}()
	plan, err := Prepare(context.Background(), Options{ConfigPaths: []string{"-"}})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{filepath.Join(dir, "data")}; !reflect.DeepEqual(plan.Files, want) {
		t.Fatalf("files: %v", plan.Files)
	}
}

func TestRejectUnsupportedProjects(t *testing.T) {
	var many strings.Builder
	many.WriteString("services:\n")
	for i := 0; i <= MaxServices; i++ {
		fmt.Fprintf(&many, "  app%d: {image: busybox}\n", i)
	}
	for _, test := range []struct{ name, compose, message string }{
		{"too many services", many.String(), "compose must select 1..24 services"},
		{"profile-less project", "services: {app: {image: busybox, profiles: [debug]}}", "compose must select 1..24 services"},
		{"scale", "services: {app: {image: busybox, scale: 2}}", "deploy.replicas above 1"},
		{"replicas", "services: {app: {image: busybox, deploy: {replicas: 2}}}", "deploy.replicas above 1"},
		{"platform", "services: {app: {image: busybox, platform: linux/arm64}}", "only linux/amd64"},
		{"container network", "services: {app: {image: busybox, network_mode: 'container:other'}}", "network_mode only supports"},
		{"provider", "services: {app: {provider: {type: model}}}", "provider is not supported"},
		{"jobs", "services: {app: {image: busybox}}\njobs: {migrate: {image: busybox, triggers: {manual: true}}}", "jobs are not supported"},
		{"external volume", "services: {app: {image: busybox, volumes: ['data:/data']}}\nvolumes: {data: {external: true}}", "external volumes"},
		{"volume driver", "services: {app: {image: busybox, volumes: ['data:/data']}}\nvolumes: {data: {driver: nfs}}", "volumes.data.driver must be local"},
		{"external network", "services: {app: {image: busybox, networks: [ext]}}\nnetworks: {ext: {external: true}}", "external networks"},
		{"network driver", "services: {app: {image: busybox, networks: [o]}}\nnetworks: {o: {driver: overlay}}", "networks.o.driver must be bridge"},
		{"config environment", "services: {app: {image: busybox, configs: [cfg]}}\nconfigs: {cfg: {environment: CFG}}", "configs.cfg: environment sources"},
		{"secret environment", "services: {app: {image: busybox, secrets: [token]}}\nsecrets: {token: {environment: TOKEN}}", "secrets.token: environment sources"},
		{"external secret", "services: {app: {image: busybox, secrets: [token]}}\nsecrets: {token: {external: true}}", "secrets.token: external objects"},
		{"image mount", "services: {app: {image: busybox, volumes: [{type: image, source: busybox, target: /image}]}}", "image mounts are not supported"},
		{"pre_start image", "services: {app: {image: busybox, pre_start: [{image: alpine, command: ['true']}]}}", "pre_start[0].image must be the service image"},
		{"remote context", "services: {app: {build: {context: 'https://example.com/repo.git'}}}", "must be a local directory"},
		{"build secrets", "services: {app: {build: {context: ., secrets: [token]}}}\nsecrets: {token: {environment: TOKEN}}", "build.secrets is not supported"},
		{"additional contexts", "services: {app: {build: {context: ., additional_contexts: {base: /srv}}}}", "build.additional_contexts is not supported"},
		{"build ssh", "services: {app: {build: {context: ., ssh: [default]}}}", "build.ssh is not supported"},
		{"published range", "services: {app: {image: busybox, ports: ['80']}}", "requires fixed published ports"},
		{"udp", "services: {app: {image: busybox, ports: ['8080:80/udp']}}", "only TCP publishing"},
		{"loopback", "services: {app: {image: busybox, ports: ['127.0.0.1:8080:80']}}", "host_ip must be"},
		{"envd port", "services: {app: {image: busybox, ports: ['49983:80']}}", "port 49983 is reserved for envd"},
		{"duplicate port", "services: {a: {image: busybox, ports: ['8080:80']}, b: {image: busybox, ports: ['8080:80']}}", "port 8080 is already published by service a"},
	} {
		t.Run(test.name, func(t *testing.T) {
			project(t, map[string]string{"compose.yaml": test.compose})
			rejects(t, Options{}, test.message)
		})
	}
}

// Absolute bind sources outside the project are guest paths. Relative ones,
// and those inside the project or an included project, are files on this
// machine, as are config and secret files; the outermost of them are listed.
func TestLocalFiles(t *testing.T) {
	plan := prepare(t, `services:
  app:
    image: busybox
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - type: bind
        source: /dev
        target: /dev
`)
	if len(plan.Files) != 0 {
		t.Fatalf("guest paths listed as local files: %v", plan.Files)
	}

	dir := project(t, nil)
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	inside, err := json.Marshal(filepath.Join(dir, "absolute"))
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"compose.yaml": fmt.Sprintf(`include: [lib/compose.yaml]
services:
  app:
    image: busybox
    volumes:
      - ./data:/data
      - ./data/nested:/nested
      - ../outside:/outside
      - ~/cache:/cache
      - {type: bind, source: %s, target: /absolute}
    configs: [settings]
    secrets: [token]
configs:
  settings: {file: ./settings.conf}
secrets:
  token: {file: ./token}
`, inside),
		"lib/compose.yaml": "services: {db: {image: redis, volumes: ['./pgdata:/data']}}\n",
	}
	for name, content := range files {
		if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	plan, err = Prepare(context.Background(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		filepath.Join(dir, "absolute"), filepath.Join(dir, "data"), filepath.Join(dir, "lib", "pgdata"),
		filepath.Join(dir, "settings.conf"), filepath.Join(dir, "token"),
		filepath.Join(filepath.Dir(dir), "outside"), filepath.Join(home, "cache"),
	}
	sort.Strings(want)
	if !reflect.DeepEqual(plan.Files, want) {
		t.Fatalf("files:\n%v\nwant:\n%v", plan.Files, want)
	}
}

// The VM is the isolation boundary, so options that only affect the guest pass through.
func TestGuestOnlyOptions(t *testing.T) {
	plan := prepare(t, `services:
  app:
    image: busybox
    privileged: true
    cap_add: [NET_ADMIN, CAP_SYS_PTRACE]
    network_mode: host
    deploy: {resources: {limits: {memory: 64M}}}
    devices: [/dev/kvm:/dev/kvm]
    volumes: ['/app/node_modules', 'data:/data', 'cache.v1:/cache']
    ports: ['0.0.0.0:8080:80', {target: 81, published: '8081', host_ip: '::'}]
  isolated:
    image: busybox
    network_mode: none
    configs: [settings]
volumes:
  data: {driver_opts: {type: none, o: bind, device: /srv}}
  cache.v1: {}
networks:
  custom: {ipam: {config: [{subnet: 10.10.0.0/24}]}}
configs:
  settings: {content: "key=value"}
`)
	if len(plan.Services) != 2 {
		t.Fatal(plan.Services)
	}
	project(t, map[string]string{"compose.yaml": "services: {app: {image: busybox, ports: ['49983:80']}}"})
	if _, err := Prepare(context.Background(), Options{EnvdPort: 5000}); err != nil {
		t.Fatal(err)
	}
}

func TestRepositoryImageReferencesPassThroughUnchanged(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	plan := prepare(t, "services:\n  web: {image: aenv/web:v1}\n  worker: {image: "+digest+"}\n")
	images := map[string]string{}
	for _, service := range plan.Services {
		images[service.Name] = service.Image
	}
	if images["web"] != "aenv/web:v1" || images["worker"] != digest {
		t.Fatalf("image references changed: %v", images)
	}
}

func TestRejectOversizedPlans(t *testing.T) {
	t.Setenv("DATA", strings.Repeat("$", 3*1024*1024))
	project(t, map[string]string{"compose.yaml": "services: {app: {image: busybox, command: ['${DATA}']}}"})
	// Escaping doubles every dollar of the interpolated value.
	rejects(t, Options{}, "plan exceeds")
}
