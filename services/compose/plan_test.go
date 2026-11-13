package compose

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
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
	for _, test := range []struct {
		name  string
		files map[string]string
		args  []string
		env   map[string]string
	}{
		{
			name: "envfiles",
			files: map[string]string{
				"compose.yaml": `services:
  app:
    image: "busybox:${TAG:-latest}"
    env_file:
      - app.env
      - path: optional.env
        required: false
    environment:
      FROM_OS: ${FROM_OS}
      FROM_DOTENV: ${FROM_DOTENV}
      FROM_EXTRA: ${FROM_EXTRA:-unset}
      PRECEDENCE: ${PRECEDENCE}
      ALTERNATE: ${FROM_OS:+set}
      NESTED: ${MISSING:-${FROM_DOTENV}}
      LITERAL: "$$NOT_INTERPOLATED"
      PASSTHROUGH:
      UNSET:
`,
				".env":      "FROM_DOTENV=dotenv\nPRECEDENCE=dotenv\nTAG=1.37\n",
				"extra.env": "FROM_EXTRA=extra\n",
				"app.env":   "APP_FILE=yes\nPRECEDENCE=from-app-env\n",
			},
			args: []string{"-p", "parity", "--env-file", ".env", "--env-file", "extra.env"},
			env:  map[string]string{"FROM_OS": "os", "PRECEDENCE": "os", "PASSTHROUGH": "os"},
		},
		{
			name: "extends",
			files: map[string]string{
				"compose.yaml": `services:
  base:
    image: busybox:1.37
    environment: {LEVEL: base}
    volumes: ["./data:/data"]
  child:
    extends: base
    environment: {EXTRA: "1"}
  remote:
    extends:
      file: common/services.yaml
      service: shared
`,
				"common/services.yaml": `services:
  shared:
    image: redis:7
    volumes: ["./cache:/cache"]
    env_file: shared.env
`,
				"common/shared.env": "SHARED=yes\n",
			},
			args: []string{"-p", "parity"},
		},
		{
			name: "files",
			files: map[string]string{
				"compose.yaml": `services:
  web:
    image: nginx:1.27-alpine
    ports: ["8080:80"]
    volumes: ["/srv/www:/usr/share/nginx/html:ro"]
`,
				"extra.yaml": `services:
  web:
    ports: ["8081:81"]
    volumes: ["/srv/logs:/var/log/nginx"]
  cache:
    image: redis:7
`,
			},
			args: []string{"-f", "compose.yaml", "-f", "extra.yaml", "-p", "parity"},
		},
		{
			name: "include",
			files: map[string]string{
				"compose.yaml": `include:
  - sub/compose.yaml
services:
  app:
    image: busybox:1.37
    depends_on: [db]
`,
				"sub/compose.yaml": `services:
  db:
    image: postgres:${PG_VERSION}
    volumes: ["./pgdata:/var/lib/postgresql/data"]
`,
				"sub/.env": "PG_VERSION=17\n",
			},
			args: []string{"-p", "parity"},
		},
		{
			name: "merge",
			files: map[string]string{"compose.yaml": `x-common: &common
  image: busybox:1.37
  environment: &env
    A: "1"
    B: "2"
services:
  web:
    <<: *common
    environment:
      <<: *env
      B: overridden
    ports: ["8080:80", "8081:81"]
    labels: {keep: "yes", drop: "yes"}
  worker:
    <<: *common
    command: ["sh", "-c", "echo $$HOME"]
---
services:
  web:
    ports: !override ["9090:90"]
    labels:
      drop: !reset null
`},
			args: []string{"-p", "parity"},
		},
		{
			name: "override",
			files: map[string]string{
				"compose.yaml": `name: parity
services:
  web:
    image: nginx:${NGINX_TAG}
    ports: ["8080:80"]
    environment: {MODE: base}
`,
				"compose.override.yaml": `services:
  web:
    environment: {MODE: override}
  debug:
    image: busybox:1.37
`,
				".env": "NGINX_TAG=1.27-alpine\n",
			},
		},
		{
			name: "profiles",
			files: map[string]string{"compose.yaml": `name: parity
services:
  app:
    image: busybox:1.37
  debug:
    image: busybox:1.37
    profiles: [debug]
  metrics:
    image: busybox:1.37
    profiles: [metrics]
`},
			env: map[string]string{"COMPOSE_PROFILES": "debug"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			project(t, test.files)
			for key, value := range test.env {
				t.Setenv(key, value)
			}
			assertMatchesDockerCompose(t, test.args)
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

// Like docker compose build, planning builds checks only what building needs;
// the sandbox runtime checks run when the project starts.
func TestBuildChecksOnlyBuilds(t *testing.T) {
	project(t, map[string]string{"compose.yaml": `services:
  app:
    build: .
    volumes: ['./data:/data']
    secrets: [token]
    ports: ['49983:80']
  db:
    image: redis
    scale: 2
secrets:
  token: {file: ./token}
`})
	rejects(t, Options{}, "bind mount of local path")
	plan, err := Prepare(context.Background(), Options{Build: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Services) != 2 || plan.Services[0].Build == nil {
		t.Fatalf("services: %+v", plan.Services)
	}
	for _, test := range []struct{ name, compose, message string }{
		{"remote context", "services: {app: {build: {context: 'https://example.com/repo.git'}}}", "must be a local directory"},
		{"platform", "services: {app: {build: ., platform: linux/arm64}}", "only linux/amd64"},
	} {
		t.Run(test.name, func(t *testing.T) {
			project(t, map[string]string{"compose.yaml": test.compose})
			rejects(t, Options{Build: true}, test.message)
		})
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
	rejects(t, Options{ConfigPaths: []string{"-"}}, "bind mount of local path "+filepath.Join(dir, "data"))
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
		{"secret file", "services: {app: {image: busybox, secrets: [token]}}\nsecrets: {token: {file: ./token}}", "secrets.token: file sources"},
		{"relative bind", "services: {app: {image: busybox, volumes: ['../data:/data']}}", "bind mount of local path"},
		{"home bind", "services: {app: {image: busybox, volumes: ['~/data:/data']}}", "bind mount of local path"},
		{"pre_start relative bind", "services: {app: {image: busybox, pre_start: [{command: ['true'], volumes: ['../data:/data']}]}}", "bind mount of local path"},
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

// Absolute bind sources outside the project are guest paths; those inside the
// project or an included project, however spelled, are files on this machine.
func TestBindSources(t *testing.T) {
	project(t, map[string]string{"compose.yaml": `services:
  app:
    image: busybox
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - type: bind
        source: /dev
        target: /dev
`})
	if _, err := Prepare(context.Background(), Options{}); err != nil {
		t.Fatal(err)
	}
	dir := project(t, nil)
	source, err := json.Marshal(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("compose.yaml", []byte(fmt.Sprintf(
		"services: {app: {image: busybox, volumes: [{type: bind, source: %s, target: /data}]}}", source)), 0o644); err != nil {
		t.Fatal(err)
	}
	rejects(t, Options{}, "bind mount of local path")
	project(t, map[string]string{
		"compose.yaml":     "include: [lib/compose.yaml]\nservices: {app: {image: busybox}}\n",
		"lib/compose.yaml": "services: {db: {image: redis, volumes: ['./data:/data']}}\n",
	})
	rejects(t, Options{}, "bind mount of local path")
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
