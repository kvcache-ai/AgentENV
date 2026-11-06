package compose

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/compose-spec/compose-go/v2/loader"
	"github.com/compose-spec/compose-go/v2/types"
)

func TestIndependentServicesAndSingleInterpolation(t *testing.T) {
	t.Setenv("HOST_ONLY_SECRET", "must-not-leak")
	plan, err := Prepare(context.Background(), Request{
		Compose: `services:
  a:
    image: ${IMAGE}
    command: ["sh", "-c", "echo $$HOME ${VALUE}"]
    environment:
      PASS: ${VALUE}
      HOST_ONLY_SECRET:
  b:
    image: ${IMAGE}
`, Environment: map[string]string{"IMAGE": "busybox:1.37", "VALUE": "$literal"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Services) != 2 || plan.Services[0].Image != plan.Services[1].Image || plan.Services[0].LocalImage == plan.Services[1].LocalImage {
		t.Fatalf("missing isolation: %+v", plan.Services)
	}
	guest, err := loader.LoadWithContext(context.Background(), types.ConfigDetails{
		ConfigFiles: []types.ConfigFile{{Filename: "-", Content: plan.Compose}},
		Environment: types.Mapping{"HOME": "wrong", "literal": "wrong", "HOST_ONLY_SECRET": "runtime"},
	}, func(o *loader.Options) { o.SetProjectName("aenv", true) })
	if err != nil {
		t.Fatal(err)
	}
	if got := guest.Services["a"].Command[2]; got != "echo $HOME $literal" {
		t.Fatalf("command changed: %q", got)
	}
	if got := *guest.Services["a"].Environment["PASS"]; got != "$literal" {
		t.Fatal(got)
	}
	if _, ok := guest.Services["a"].Environment["HOST_ONLY_SECRET"]; ok {
		t.Fatal("unresolved environment was resolved again at runtime")
	}
	if strings.Contains(string(plan.Compose), "must-not-leak") {
		t.Fatal("host environment leaked")
	}
}

func TestRejectExpandedPlans(t *testing.T) {
	var aliases strings.Builder
	aliases.WriteString("x-value: &value " + strings.Repeat("x", 32*1024) + "\nservices:\n  app:\n    image: busybox\n    labels:\n")
	for i := 0; i < 160; i++ {
		fmt.Fprintf(&aliases, "      key%d: *value\n", i)
	}
	var inherited strings.Builder
	inherited.WriteString("services:\n")
	for i := 0; i < MaxServices; i++ {
		fmt.Fprintf(&inherited, "  app%d:\n    image: busybox\n    environment:\n      DATA:\n", i)
	}
	for _, test := range []struct {
		name    string
		request Request
		message string
	}{
		{"aliases", Request{Compose: aliases.String()}, "expansion exceeds"},
		{"single scalar interpolation", Request{
			Compose:     "services: {app: {image: busybox, command: ['" + strings.Repeat("${DATA}", 24) + "']}}",
			Environment: map[string]string{"DATA": strings.Repeat("x", 256*1024)},
		}, "expansion exceeds"},
		{"inherited environment", Request{
			Compose: inherited.String(), Environment: map[string]string{"DATA": strings.Repeat("x", 256*1024)},
		}, "expansion exceeds"},
		// Within the interpolation budget, but escaping doubles every dollar.
		{"rendered plan", Request{
			Compose:     "services: {app: {image: busybox, command: ['${DATA}']}}",
			Environment: map[string]string{"DATA": strings.Repeat("$", 3*1024*1024)},
		}, "plan exceeds"},
	} {
		t.Run(test.name, func(t *testing.T) { rejects(t, test.request, test.message) })
	}
}

// rejects requires an InvalidError, which the planner command reports with its
// dedicated exit status, and checks that the intended rule caused it.
func rejects(t *testing.T, request Request, message string) {
	t.Helper()
	_, err := Prepare(context.Background(), request)
	var invalid *InvalidError
	if !errors.As(err, &invalid) || !strings.Contains(err.Error(), message) {
		t.Fatalf("expected invalid Compose error containing %q, got %v", message, err)
	}
}

func withService(field string) Request {
	return Request{Compose: "services:\n  app:\n    image: busybox\n    " + field + "\n"}
}

func TestRejectExternalInputs(t *testing.T) {
	rejects(t, Request{Compose: "services: {app: {image: busybox}}\n---\nservices: {app: {privileged: true}}"}, "exactly one YAML document")
	for _, test := range []struct{ field, message string }{
		{"env_file: /etc/passwd", "services.app.env_file is not supported"},
		{"label_file: /etc/passwd", "services.app.label_file is not supported"},
		{"extends: {file: /etc/passwd, service: foo}", "services.app.extends is not supported"},
		{"build: .", "services.app.build is not supported"},
		{"deploy: {replicas: 2}", "deploy.replicas above 1"},
		{"scale: 2", "deploy.replicas above 1"},
		{"network_mode: container:other", "network_mode only supports"},
		{"network_mode: service:app", "dependency cycle detected"},
		{"platform: linux/arm64", "only linux/amd64"},
		{"volumes: ['./data:/data']", "must be an absolute guest path"},
		{"volumes: [{type: image, source: busybox, target: /image}]", "image mounts are not supported"},
	} {
		t.Run(test.field, func(t *testing.T) { rejects(t, withService(test.field), test.message) })
	}
	for _, test := range []struct{ extra, message string }{
		{"include: /etc/passwd", "include is not supported"},
		{"configs: {foo: {file: /etc/passwd}}", "configs is not supported"},
		{"volumes: {data: {external: true}}", "volumes.data.external is not supported"},
		{"volumes: {data: {driver: nfs}}", "volumes.data.driver must be local"},
		{"networks: {ext: {external: true}}", "networks.ext.external is not supported"},
		{"networks: {custom: {ipam: {config: [{subnet: 10.0.0.0/8}]}}}", "networks.custom.ipam is not supported"},
	} {
		t.Run(test.extra, func(t *testing.T) {
			rejects(t, Request{Compose: "services: {app: {image: busybox}}\n" + test.extra}, test.message)
		})
	}
	rejects(t, Request{Compose: `services:
  app: {image: busybox, network_mode: "service:debug"}
  debug: {image: busybox, profiles: [debug]}
`}, `depends on undefined service "debug"`)
	var many strings.Builder
	many.WriteString("services:\n")
	for i := 0; i <= MaxServices; i++ {
		fmt.Fprintf(&many, "  app%d: {image: busybox}\n", i)
	}
	rejects(t, Request{Compose: many.String()}, "compose must select 1..24 services")
}

// Mappings with a non-string key decode as map[any]any and must not bypass
// validation; compose-go would still accept the decoded !!binary strings.
func TestRejectNonStringKeysAndCustomTags(t *testing.T) {
	for _, test := range []struct{ compose, message string }{
		{"services: {app: {image: busybox, !!binary cHJpdmlsZWdlZA==: true}}", "services.app must be a mapping with string keys"},
		{"services: {app: {image: busybox}}\nvolumes: {!!binary ZGF0YQ==: {external: true}}", "volumes must be a mapping with string keys"},
		{"services: {app: {image: busybox}}\nnetworks: {ext: {external: true}, !!binary eA==: {}}", "networks must be a mapping with string keys"},
		{"services: {app: {image: busybox, ports: !reset []}}", "YAML tag !reset is not supported"},
	} {
		t.Run(test.compose, func(t *testing.T) { rejects(t, Request{Compose: test.compose}, test.message) })
	}
}

func TestPublishedPorts(t *testing.T) {
	for _, test := range []struct{ ports, message string }{
		{"['127.0.0.1:8080:80']", "host_ip must be"},
		{"[{target: 80, published: '8080', host_ip: '::1'}]", "host_ip must be"},
		{"['10.0.0.5:8080:80']", "host_ip must be"},
		{"['80']", "requires fixed published ports"},
		{"['8080:80/udp']", "only TCP publishing"},
		{"['49983:80']", "port 49983 is reserved for envd"},
		{"['8080:80', '8080:81']", "port 8080 is already published by service app"},
	} {
		t.Run(test.ports, func(t *testing.T) { rejects(t, withService("ports: "+test.ports), test.message) })
	}
	rejects(t, Request{Compose: `services:
  a: {image: busybox, ports: ['8080:80']}
  b: {image: busybox, ports: ['8080:80']}
`}, "port 8080 is already published by service a")
	custom := withService("ports: ['5000:80']")
	custom.EnvdPort = 5000
	rejects(t, custom, "port 5000 is reserved for envd")
	accepted := withService("ports: ['49983:80', '0.0.0.0:8080:80', {target: 81, published: '8081', host_ip: '::'}]")
	accepted.EnvdPort = 5000
	if _, err := Prepare(context.Background(), accepted); err != nil {
		t.Fatal(err)
	}
}

// The VM is the isolation boundary, so options that only affect the guest pass through.
func TestGuestOnlyOptions(t *testing.T) {
	_, err := Prepare(context.Background(), Request{Compose: `services:
  app:
    image: busybox
    privileged: true
    cap_add: [NET_ADMIN, CAP_SYS_PTRACE]
    network_mode: host
    deploy: {resources: {limits: {memory: 64M}}}
    volumes: ['/var/run/docker.sock:/var/run/docker.sock', '/app/node_modules', 'data:/data']
  isolated:
    image: busybox
    network_mode: none
volumes:
  data: {driver_opts: {type: none, o: bind, device: /srv}}
`})
	if err != nil {
		t.Fatal(err)
	}
}

func TestProfilesAndVolumes(t *testing.T) {
	plan, err := Prepare(context.Background(), Request{Compose: `services:
  app:
    image: redis:7
    volumes: [data:/data]
    ports: ['8080:80']
  debug:
    image: busybox
    profiles: [debug]
volumes:
  data: {}
`})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Services) != 1 {
		t.Fatal(plan.Services)
	}
	var doc map[string]any
	if err := json.Unmarshal(plan.Compose, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc["services"].(map[string]any)) != 1 {
		t.Fatal(string(plan.Compose))
	}
}

func TestRepositoryImageReferencesPassThroughUnchanged(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	plan, err := Prepare(context.Background(), Request{Compose: `services:
  web:
    image: aenv/web:v1
  worker:
    image: ` + digest + `
`})
	if err != nil {
		t.Fatal(err)
	}
	images := map[string]string{}
	for _, service := range plan.Services {
		images[service.Name] = service.Image
	}
	if images["web"] != "aenv/web:v1" || images["worker"] != digest {
		t.Fatalf("image references changed: %v", images)
	}
}

func TestBuildPlanPreservesRuntimeAndBuildSemantics(t *testing.T) {
	plan, err := PrepareBuild(context.Background(), Request{
		Compose: `services:
  app:
    build:
      context: ./app
      dockerfile: docker/Custom
      args: {VALUE: "${VALUE}", FROM_ENV: null, USE_DEFAULT: null}
      target: final
      no_cache: true
    pull_policy: build
    command: [sh, -c, 'echo $$VALUE']
    environment: {VALUE: "${VALUE}"}
    depends_on: {db: {condition: service_healthy}}
  db:
    image: redis:7
    healthcheck: {test: [CMD, redis-cli, ping]}
  optional:
    build: ./optional
    profiles: [extra]
`, Environment: map[string]string{"VALUE": "literal$VALUE", "FROM_ENV": "a=b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Services) != 1 {
		t.Fatalf("unexpected build services: %+v", plan.Services)
	}
	build := plan.Services[0]
	if build.Context != "./app" || build.Dockerfile != "docker/Custom" || build.Target != "final" || !build.NoCache || build.Args["VALUE"] != "literal$VALUE" || build.Args["FROM_ENV"] != "a=b" {
		t.Fatalf("build settings changed: %+v", build)
	}
	if _, exists := build.Args["USE_DEFAULT"]; exists {
		t.Fatal("unset build argument must retain its Dockerfile default")
	}
	var output map[string]any
	if err := json.Unmarshal(plan.Compose, &output); err != nil {
		t.Fatal(err)
	}
	services := output["services"].(map[string]any)
	app := services["app"].(map[string]any)
	if app["build"] != nil || app["pull_policy"] != nil || services["optional"] != nil {
		t.Fatalf("output must contain only active image services: %s", plan.Compose)
	}
	if services["db"].(map[string]any)["image"] != "redis:7" {
		t.Fatal("image-only service changed")
	}
	// Feed the built file back through the runtime planner, then interpolate it
	// once as guest Compose does. Literal dollars must survive both boundaries.
	runtime, err := Prepare(context.Background(), Request{Compose: string(plan.Compose)})
	if err != nil {
		t.Fatal(err)
	}
	project, err := loader.LoadWithContext(context.Background(), types.ConfigDetails{
		WorkingDir:  "/var/lib/agentenv-compose",
		ConfigFiles: []types.ConfigFile{{Content: runtime.Compose}},
	}, func(o *loader.Options) { o.SetProjectName("aenv", true) })
	if err != nil {
		t.Fatal(err)
	}
	if got := *project.Services["app"].Environment["VALUE"]; got != "literal$VALUE" {
		t.Fatalf("literal dollar lost: %q", got)
	}
	if got := project.Services["app"].Command[2]; got != "echo $VALUE" {
		t.Fatalf("shell dollar lost: %q", got)
	}
}

func TestBuildPlanWithoutBuildsListsNoServices(t *testing.T) {
	plan, err := PrepareBuild(context.Background(), Request{Compose: "services: {app: {image: busybox:1.37}}"})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"services":[]`) {
		t.Fatalf("the CLI requires a services list: %s", encoded)
	}
}

// rejectsBuild is rejects for build mode.
func rejectsBuild(t *testing.T, request Request, message string) {
	t.Helper()
	_, err := PrepareBuild(context.Background(), request)
	var invalid *InvalidError
	if !errors.As(err, &invalid) || !strings.Contains(err.Error(), message) {
		t.Fatalf("expected invalid Compose error containing %q, got %v", message, err)
	}
}

func TestBuildRejectsUnsupportedFeaturesBeforeBuilding(t *testing.T) {
	for _, test := range []struct{ settings, message string }{
		{"command: ['true']", "neither an image nor a build context"},
		{"build: {context: 'https://example.com/repo.git'}", "must be a local directory"},
		{"build: {context: ., secrets: [token]}", "services.app.build.secrets is not supported"},
		{"build: {context: ., additional_contexts: {api: 'service:api'}}", "services.app.build.additional_contexts is not supported"},
		{"build: {dockerfile_inline: 'FROM scratch'}", "services.app.build.dockerfile_inline is not supported"},
		{"build: {context: ., !!binary c3No: default}", "services.app.build must be a mapping with string keys"},
		{"build: .\n    pull_policy: always", "pull_policy only supports build"},
		{"build: .\n    env_file: /etc/passwd", "services.app.env_file is not supported"},
		{"build: .\n    network_mode: container:other", "network_mode only supports"},
		{"build: .\n    platform: linux/arm64", "only linux/amd64"},
	} {
		t.Run(test.settings, func(t *testing.T) {
			rejectsBuild(t, Request{Compose: "services:\n  app:\n    " + test.settings + "\n"}, test.message)
		})
	}
}

func TestNamedVolumeShortSyntaxPreservesDots(t *testing.T) {
	for _, volume := range []any{
		"cache.v1:/cache",
		map[string]any{"type": "volume", "source": "cache.v1", "target": "/cache"},
	} {
		source, err := json.Marshal(map[string]any{
			"services": map[string]any{"app": map[string]any{"image": "busybox", "volumes": []any{volume}}},
			"volumes":  map[string]any{"cache.v1": map[string]any{}},
		})
		if err != nil {
			t.Fatal(err)
		}
		request := Request{Compose: string(source)}
		plan, err := Prepare(context.Background(), request)
		if err != nil {
			t.Fatal(err)
		}
		document := plan.Compose
		var rendered struct {
			Services map[string]struct{ Volumes []types.ServiceVolumeConfig }
		}
		if err := json.Unmarshal(document, &rendered); err != nil {
			t.Fatal(err)
		}
		volumes := rendered.Services["app"].Volumes
		if len(volumes) != 1 || volumes[0].Type != "volume" || volumes[0].Source != "cache.v1" || volumes[0].Target != "/cache" {
			t.Fatalf("named volume changed: %+v", volumes)
		}
	}
	for _, source := range []string{".", "..", ".cache", "./cache", "../cache", "~/cache"} {
		document, err := json.Marshal(map[string]any{
			"services": map[string]any{"app": map[string]any{"image": "busybox", "volumes": []string{source + ":/cache"}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		rejects(t, Request{Compose: string(document)}, "must be an absolute guest path")
	}
}
