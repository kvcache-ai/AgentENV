package compose

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/compose-spec/compose-go/v2/loader"
	"github.com/compose-spec/compose-go/v2/types"
)

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
