package compose

import (
	"context"
	"encoding/json"
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
	if len(plan.Services) != 2 || plan.Services[0].Image != plan.Services[1].Image || plan.Services[0].LocalImage == plan.Services[1].LocalImage || plan.Services[0].MountPath == plan.Services[1].MountPath {
		t.Fatalf("missing isolation: %+v", plan.Services)
	}
	guest, err := loader.LoadWithContext(context.Background(), types.ConfigDetails{
		ConfigFiles: []types.ConfigFile{{Filename: "-", Content: plan.Compose}},
		Environment: types.Mapping{"HOME": "wrong", "literal": "wrong"},
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
		{"JSON escaping", Request{
			Compose:     "services: {app: {image: busybox, command: ['${DATA}${DATA}${DATA}']}}",
			Environment: map[string]string{"DATA": strings.Repeat("\x01", 256*1024)},
		}, "plan exceeds"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := Prepare(context.Background(), test.request)
			if err == nil || !strings.Contains(err.Error(), test.message) {
				t.Fatalf("expected %q, got %v", test.message, err)
			}
		})
	}
}

func TestPublishedPortConflicts(t *testing.T) {
	for _, test := range []struct {
		first, second string
		conflict      bool
	}{
		{"8080:80", "8080:81", true},
		{"0.0.0.0:8080:80", "127.0.0.1:8080:81", true},
		{"127.0.0.1:8080:80", "0.0.0.0:8080:81", true},
		{"8080:80", "[::1]:8080:81", true},
		{"[::]:8080:80", "[::1]:8080:81", true},
		{"[::1]:8080:80", "[::1]:8080:81", true},
		{"127.0.0.1:8080:80", "127.0.0.2:8080:81", false},
		{"127.0.0.1:8080:80", "[::1]:8080:81", false},
		{"8080:80", "8081:80", false},
	} {
		t.Run(test.first+"/"+test.second, func(t *testing.T) {
			_, err := Prepare(context.Background(), Request{Compose: fmt.Sprintf(
				"services:\n  a:\n    image: busybox\n    ports: [%q]\n  b:\n    image: busybox\n    ports: [%q]\n",
				test.first, test.second,
			)})
			if test.conflict {
				if err == nil || !strings.Contains(err.Error(), "conflicts") {
					t.Fatalf("expected port conflict, got %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPortsWithinServiceAndInactiveProfiles(t *testing.T) {
	for _, test := range []struct {
		ports   string
		wantErr bool
	}{
		{"['8080:80', '8080:81']", true},
		{"['8080:80', '8080:80']", false},
		{"['8080:80']", false},
	} {
		_, err := Prepare(context.Background(), Request{Compose: "services:\n  a:\n    image: busybox\n    ports: " + test.ports + "\n  disabled:\n    image: busybox\n    profiles: [debug]\n    ports: ['8080:80']\n"})
		if (err != nil) != test.wantErr {
			t.Fatalf("ports %s: %v", test.ports, err)
		}
	}
}

func TestRejectExternalInputs(t *testing.T) {
	if _, err := Prepare(context.Background(), Request{Compose: "services: {app: {image: busybox}}\n---\nservices: {app: {privileged: true}}"}); err == nil {
		t.Fatal("second YAML document bypassed validation")
	}
	for _, field := range []string{"env_file: /etc/passwd", "extends: {file: /etc/passwd, service: foo}", "build: .", "deploy: {replicas: 2}", "network_mode: host", "privileged: true", "volumes: ['/etc:/host']", "volumes: [{type: bind, source: /etc, target: /host}]", "label_file: /etc/passwd"} {
		t.Run(field, func(t *testing.T) {
			_, err := Prepare(context.Background(), Request{Compose: "services:\n  app:\n    image: busybox\n    " + field + "\n"})
			if err == nil {
				t.Fatal("unsupported field accepted")
			}
		})
	}
	for _, extra := range []string{"include: /etc/passwd", "configs: {foo: {file: /etc/passwd}}", "volumes: {data: {driver_opts: {device: /etc}}}", "networks: {ext: {external: true}}"} {
		if _, err := Prepare(context.Background(), Request{Compose: "services: {app: {image: busybox}}\n" + extra}); err == nil {
			t.Fatal(extra)
		}
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
	// Feed the built file back through the server planner, then interpolate it
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

func TestHarborDefaultsAreExplicitAndPreserveOverrides(t *testing.T) {
	const source = `services:
  main:
    network_mode: service:api
    cap_add: [SYS_PTRACE]
    depends_on: {api: {condition: service_healthy}}
  api:
    build: ./api
    healthcheck: {test: [CMD, "true"]}
`
	if _, err := PrepareBuild(context.Background(), Request{Compose: source}); err == nil {
		t.Fatal("generic Compose must reject a missing image/build")
	}
	plan, err := PrepareBuild(context.Background(), Request{Compose: source, Harbor: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Services) != 2 || plan.Services[1].Context != "." || plan.Services[1].Dockerfile != "Dockerfile" {
		t.Fatalf("missing main build: %+v", plan.Services)
	}
	if !strings.Contains(string(plan.Compose), `"sleep infinity"`) {
		t.Fatalf("missing keepalive: %s", plan.Compose)
	}
	if _, err := Prepare(context.Background(), Request{Compose: string(plan.Compose)}); err != nil {
		t.Fatal(err)
	}
	override := "services:\n  main:\n    build: {context: ./custom, dockerfile: Agent}\n    command: [my-command]\n"
	plan, err = PrepareBuild(context.Background(), Request{Compose: override, Harbor: true})
	if err != nil || plan.Services[0].Dockerfile != "Agent" || !strings.Contains(string(plan.Compose), "my-command") {
		t.Fatalf("task overrides changed: %+v, %v", plan, err)
	}
}

func TestBuildRejectsUnsupportedFeaturesBeforeBuilding(t *testing.T) {
	for _, settings := range []string{
		"build: {context: 'https://example.com/repo.git'}",
		"build: {context: ., secrets: [token]}",
		"build: {context: ., additional_contexts: {api: 'service:api'}}",
		"build: {dockerfile_inline: 'FROM scratch'}",
		"build: .\n    volumes: ['/etc:/host']",
		"build: .\n    network_mode: host",
		"build: .\n    network_mode: container:other",
		"build: .\n    cap_add: [SYS_ADMIN]",
		"build: .\n    platform: linux/arm64",
	} {
		if _, err := PrepareBuild(context.Background(), Request{Compose: "services:\n  app:\n    " + settings + "\n"}); err == nil {
			t.Errorf("accepted unsupported settings: %s", settings)
		}
	}
}

func TestRequestFrameBoundaries(t *testing.T) {
	valid := `{"compose":"services: {app: {image: busybox}}"}`
	for _, test := range []struct {
		name, input string
		wantErr     bool
	}{
		{"valid", valid, false},
		{"at limit", valid + strings.Repeat(" ", MaxRequestBytes-len(valid)), false},
		{"over limit", valid + strings.Repeat(" ", MaxRequestBytes-len(valid)+1), true},
		{"hidden second value", valid + strings.Repeat(" ", MaxRequestBytes-len(valid)) + "{}", true},
		{"second value", valid + "{}", true},
		{"truncated", valid[:len(valid)-1], true},
		{"unknown field", `{"compose":"services: {}","unknown":true}`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ReadRequest(strings.NewReader(test.input))
			if (err != nil) != test.wantErr {
				t.Fatalf("error = %v, wantErr = %v", err, test.wantErr)
			}
		})
	}
}

func TestNamedVolumeShortSyntaxPreservesDots(t *testing.T) {
	for _, build := range []bool{false, true} {
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
			var document json.RawMessage
			if build {
				plan, err := PrepareBuild(context.Background(), request)
				if err != nil {
					t.Fatal(err)
				}
				document = plan.Compose
			} else {
				plan, err := Prepare(context.Background(), request)
				if err != nil {
					t.Fatal(err)
				}
				document = plan.Compose
			}
			var rendered struct {
				Services map[string]struct{ Volumes []types.ServiceVolumeConfig }
			}
			if err := json.Unmarshal(document, &rendered); err != nil {
				t.Fatal(err)
			}
			volumes := rendered.Services["app"].Volumes
			if len(volumes) != 1 || volumes[0].Type != "volume" || volumes[0].Source != "cache.v1" || volumes[0].Target != "/cache" {
				t.Fatalf("build=%v: named volume changed: %+v", build, volumes)
			}
		}
	}
	for _, source := range []string{".", "..", ".cache", "./cache", "../cache", "/cache", "~/cache"} {
		document, err := json.Marshal(map[string]any{
			"services": map[string]any{"app": map[string]any{"image": "busybox", "volumes": []string{source + ":/cache"}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Prepare(context.Background(), Request{Compose: string(document)}); err == nil {
			t.Fatalf("host path %q accepted", source)
		}
	}
}

func TestHostDefaultsFitAfterMaximumHTTPRequest(t *testing.T) {
	request := map[string]any{
		"compose":    "services: {app: {image: busybox}}",
		"composeEnv": map[string]string{"P": ""},
	}
	empty, _ := json.Marshal(request)
	request["composeEnv"].(map[string]string)["P"] = strings.Repeat("x", 2*1024*1024-len(empty))
	request["profiles"] = []string{}
	input, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if len(input) <= 2*1024*1024 {
		t.Fatal("fixture must exceed the old planner limit")
	}
	if _, err := ReadRequest(strings.NewReader(string(input))); err != nil {
		t.Fatal(err)
	}
}
