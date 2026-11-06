#!/usr/bin/env bash
# Live Compose checks through `aenv compose up` against a node or gateway. Run it
# from run_e2e.sh, or directly with AENV_URL and AENV_API_KEY set. Each group is
# opt-in:
#   AENV_COMPOSE_BASE_IMAGE=IMAGE       lifecycle, project loading, rejection and
#                                       image defaults, with a guest image built
#                                       from images/compose and pullable by the nodes
#   AENV_COMPOSE_BUILD_TEST=1           service builds with `aenv compose build`
#                                       and `aenv compose up`; the builder must
#                                       reach Docker Hub
#   AENV_COMPOSE_TERMINAL_BENCH_TEST=1  a Terminal-Bench 4.0 task started the way
#                                       Harbor merges its Compose files; needs
#                                       GitHub, and the builder must reach Docker
#                                       Hub and Debian
# Without AENV_COMPOSE_BASE_IMAGE, the first `compose up` builds the bundled guest
# image with the server's builder. AENV_BIN selects the CLI (make build-aenv) and
# AENV_COMPOSE_PLANNER the planner, which is otherwise built from services/.
# Deletes only this suite's sandboxes, snapshots and build records; built images
# stay in the repository.
set -euo pipefail

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "${SUITE_DIR}/../lib/helpers.sh"
init_suite "19_compose"

if [[ -z "${AENV_COMPOSE_BASE_IMAGE:-}" && "${AENV_COMPOSE_BUILD_TEST:-}" != "1" &&
  "${AENV_COMPOSE_TERMINAL_BENCH_TEST:-}" != "1" ]]; then
  echo "AENV_COMPOSE_BASE_IMAGE not set and no Compose build test enabled; skipping Compose checks"
  exit 0
fi
log "Suite: Compose"

repo_root="$(cd "${SUITE_DIR}/../../../.." && pwd)"
aenv="${AENV_BIN:-${CARGO_TARGET_DIR:-${repo_root}/target}/debug/aenv}"
[[ -x "$aenv" ]] || die "Build the test client with make build-aenv"
work=$(mktemp -d)
cleanup() {
  _cleanup_e2e
  rm -rf "$work"
}
trap cleanup EXIT
export XDG_CONFIG_HOME="$work"
mkdir -p "$work/aenv"
printf 'url = "%s"\napi_key = "%s"\n' "$AENV_URL" "$AENV_API_KEY" >"$work/aenv/credentials"
chmod 600 "$work/aenv/credentials"
planner="${AENV_COMPOSE_PLANNER:-$work/aenv-compose-plan}"
[[ -n "${AENV_COMPOSE_PLANNER:-}" ]] || go -C "$repo_root/services" build -o "$planner" ./compose/cmd

COMPOSE=$(
  cat <<'EOF'
services:
  a:
    image: busybox:1.37
    command: [sh, -c, "mkdir -p /www; echo a >/marker; echo a >/www/index.html; exec httpd -f -p 8080 -h /www"]
    environment:
      VALUE: ${VALUE}
      HOME:
    ports: ["8080:8080"]
    volumes: [data:/data]
    healthcheck:
      test: [CMD, wget, -q, -O, /dev/null, http://localhost:8080]
      interval: 1s
      timeout: 1s
      retries: 10
  b:
    image: busybox:1.37
    command: [sh, -c, "mkdir -p /www; echo b >/marker; echo b >/www/index.html; exec httpd -f -p 8080 -h /www"]
    ports: ["8081:8080"]
    depends_on:
      a:
        condition: service_healthy
volumes:
  data: {}
EOF
)

# A Terminal-Bench 4.0 task with a sidecar, and Harbor's base Compose file,
# under which Harbor merges each task's environment/docker-compose.yaml.
TERMINAL_BENCH=https://github.com/harbor-framework/terminal-bench.git
TERMINAL_BENCH_COMMIT=452bf305c6daa62fc59061d22133a7cbc7c1572e
TERMINAL_BENCH_TASK=tasks/freight-dispatch-shift/environment
HARBOR_BUILD_COMPOSE=https://raw.githubusercontent.com/harbor-framework/harbor/v0.25.0/src/harbor/environments/docker/docker-compose-build.yaml

rand_hex() {
  od -An -N6 -tx1 /dev/urandom | tr -d ' \n'
}

check() {
  local msg="$1"
  shift
  if "$@"; then _pass "$msg"; else _fail "$msg"; fi
}

refute_contains() {
  local haystack="$1" needle="$2" msg="$3"
  if [[ "$haystack" != *"$needle"* ]]; then _pass "$msg"; else _fail "$msg" "no ${needle}" "$haystack"; fi
}

# Starts a test in a new project directory: the planner loads .env from it.
begin_test() {
  log "Test: $1"
  dir=$(mktemp -d "$work/$1.XXXXXX")
}

# Deletes what the finished test created, newest first, so that its sandboxes
# stop holding resources.
end_test() {
  local i
  for ((i = ${#_TRACKED_SANDBOX_IDS[@]} - 1; i >= 0; i--)); do
    delete_sandbox "${_TRACKED_SANDBOX_IDS[i]}"
  done
  for ((i = ${#_TRACKED_TEMPLATE_IDS[@]} - 1; i >= 0; i--)); do
    api_delete "/templates/${_TRACKED_TEMPLATE_IDS[i]}"
  done
  _TRACKED_SANDBOX_IDS=()
  _TRACKED_TEMPLATE_IDS=()
}

# compose_up DIR PROJECT ARGS...: runs `aenv compose up` from DIR, with the
# project on stdin for `-f -`. Sets up_status, up_out (the sandbox ID) and up_err.
compose_up() {
  local cwd="$1" project="$2"
  shift 2
  local -a image=()
  [[ -z "${AENV_COMPOSE_BASE_IMAGE:-}" ]] || image=(--base-image "$AENV_COMPOSE_BASE_IMAGE")
  up_status=0
  (cd "$cwd" && timeout 1800 "$aenv" compose up -p "$project" "$@" "${image[@]}" \
    --cpu 2 --memory 2048 --timeout 900 --planner "$planner") >"$work/stdout" 2>"$work/stderr" ||
    up_status=$?
  up_out=$(<"$work/stdout")
  up_err=$(<"$work/stderr")
}

# Like compose_up, but the sandbox must start. Sets sandbox and statuses.
compose_create() {
  compose_up "$@"
  build_statuses "$up_err"
  [[ "$up_status" == 0 && -n "$up_out" ]] || die "aenv compose up failed (exit ${up_status}): ${up_err}"
  sandbox="$up_out"
  track_sandbox "$sandbox"
}

# Runs `aenv compose build` from the test directory. Sets build_status,
# build_out and build_err.
compose_build() {
  build_status=0
  (cd "$dir" && timeout 900 "$aenv" compose build --progress plain "$@" --planner "$planner") \
    >"$work/stdout" 2>"$work/stderr" || build_status=$?
  build_out=$(<"$work/stdout")
  build_err=$(<"$work/stderr")
}

# Sets statuses to the status of each image build an aenv command reported on
# stderr, and tracks the build records for deletion.
build_statuses() {
  local build
  statuses=""
  while read -r build; do
    track_template "$build"
    api_get "/templates/${build}/builds/${build}/status"
    statuses+="${statuses:+ }$(jq -r '.status' <<<"$HTTP_BODY")"
  done < <(sed -n 's/.*Created image build \([^[:space:]]*\).*/\1/p' <<<"$1")
}

guest() {
  local id="$1"
  shift
  timeout 90 "$aenv" exec "$id" -- "$@"
}

# Fetches / from a sandbox port through the API's sandbox proxy.
proxy() {
  curl -fsS --max-time 60 -H "X-API-Key: ${AENV_API_KEY}" -H "x-agentenv-sandbox-id: $1" \
    -H "x-agentenv-target-port: $2" "${AENV_URL}/"
}

# Prints "SERVICE IMAGE" for each service of an `aenv compose build` override file.
overridden_images() {
  local line service="" service_re='^  ("[^"]*"):$' image_re='^    image: (".*")$'
  while IFS= read -r line; do
    if [[ "$line" =~ $service_re ]]; then
      service=$(jq -r . <<<"${BASH_REMATCH[1]}")
    elif [[ "$line" =~ $image_re ]]; then
      printf '%s %s\n' "$service" "$(jq -r . <<<"${BASH_REMATCH[1]}")"
    fi
  done <"$1"
}

test_lifecycle() {
  local identity child snapshot restored service
  begin_test lifecycle
  compose_create "$dir" e2e -f - --env 'VALUE=$literal' <<<"$COMPOSE"
  assert_eq "$(proxy "$sandbox" 8080)" a "service a serves through the proxy"
  assert_eq "$(proxy "$sandbox" 8081)" b "service b, from the same source image, serves through the proxy"
  assert_eq "$(guest "$sandbox" docker info --format '{{.Driver}}')" plain "Docker uses the plain storage driver"
  for service in a b; do
    assert_eq "$(guest "$sandbox" docker exec "e2e-${service}-1" cat /marker)" "$service" \
      "service ${service} runs its own command"
  done
  assert_eq "$(guest "$sandbox" docker exec e2e-b-1 wget -qO- http://a:8080)" a "services reach each other by name"
  assert_eq "$(guest "$sandbox" docker exec e2e-a-1 printenv VALUE)" '$literal' "--env keeps literal dollars"
  guest "$sandbox" docker exec e2e-a-1 sh -c 'echo persisted >/data/value'
  identity=$(guest "$sandbox" docker inspect --format '{{.Id}} {{.State.Pid}}' e2e-a-1)
  api_post "/sandboxes/${sandbox}/pause"
  assert_status "$HTTP_STATUS" 204 "pause the Compose sandbox"
  api_post "/sandboxes/${sandbox}/resume" '{"timeout":900}'
  assert_status "$HTTP_STATUS" 201 "resume the Compose sandbox"
  assert_eq "$(guest "$sandbox" docker inspect --format '{{.Id}} {{.State.Pid}}' e2e-a-1)" "$identity" \
    "pause/resume preserves running containers"
  assert_eq "$(proxy "$sandbox" 8080)" a "service a serves after resume"

  api_post "/sandboxes/${sandbox}/fork" '{"count":1,"timeout":900}'
  assert_status "$HTTP_STATUS" 201 "fork the Compose sandbox"
  child=$(jq -r '.[0]?.sandbox.sandboxID // empty' <<<"$HTTP_BODY")
  [[ -n "$child" ]] || die "Fork returned no sandbox: ${HTTP_BODY}"
  track_sandbox "$child"
  assert_eq "$(proxy "$child" 8081)" b "the fork serves service b"
  guest "$child" docker exec e2e-a-1 sh -c 'echo child >/marker; echo child >/data/value'
  assert_eq "$(guest "$sandbox" docker exec e2e-a-1 cat /marker)" a "fork isolates service rootfs writes"
  assert_eq "$(guest "$sandbox" docker exec e2e-a-1 cat /data/value)" persisted "fork isolates named volume writes"

  api_post "/sandboxes/${sandbox}/snapshots" '{}'
  assert_status "$HTTP_STATUS" 201 "snapshot the Compose sandbox"
  snapshot=$(jq -r '.snapshotID // empty' <<<"$HTTP_BODY")
  [[ -n "$snapshot" ]] || die "Snapshot returned no ID: ${HTTP_BODY}"
  track_template "$snapshot"
  api_post /sandboxes "$(jq -nc --arg template "$snapshot" '{templateID: $template, timeout: 900}')"
  assert_status "$HTTP_STATUS" 201 "restore the snapshot"
  restored=$(jq -r '.sandboxID // empty' <<<"$HTTP_BODY")
  [[ -n "$restored" ]] || die "Restore returned no sandbox: ${HTTP_BODY}"
  track_sandbox "$restored"
  assert_eq "$(proxy "$restored" 8080)" a "the restored sandbox serves service a"
  assert_eq "$(guest "$restored" docker inspect --format '{{.Id}} {{.State.Pid}}' e2e-a-1)" "$identity" \
    "snapshot restore preserves running containers"
  assert_eq "$(guest "$restored" docker exec e2e-a-1 cat /data/value)" persisted \
    "snapshot restore preserves volume data"
  end_test
}

test_project_loading() {
  begin_test project-loading
  # Like Docker Compose: the default file and its override, the project's .env
  # file and the process environment, which --env extends.
  cat >"$dir/compose.yaml" <<'EOF'
services:
  web:
    image: busybox:${TAG}
    command: [sh, -c, 'echo "$$GREETING" >/greeting; exec sleep infinity']
    environment:
      GREETING: ${GREETING:-unset}
      FROM_SHELL:
EOF
  cat >"$dir/compose.override.yaml" <<'EOF'
services:
  web:
    environment:
      OVERRIDDEN: "yes"
EOF
  printf 'TAG=1.37\nGREETING=from-dotenv\n' >"$dir/.env"
  compose_create "$dir" e2e --env FROM_SHELL=shell </dev/null
  assert_eq "$(guest "$sandbox" docker exec e2e-web-1 cat /greeting)" from-dotenv "the project's .env interpolates the file"
  assert_eq "$(guest "$sandbox" docker exec e2e-web-1 printenv FROM_SHELL)" shell "--env extends the process environment"
  assert_eq "$(guest "$sandbox" docker exec e2e-web-1 printenv OVERRIDDEN)" yes "compose.override.yaml is loaded"
  end_test
}

test_rejection_and_failed_healthcheck_cleanup() {
  local before started
  begin_test rejection
  api_get /sandboxes
  before=$(jq -cS . <<<"$HTTP_BODY")
  compose_up "$dir" e2e -f - <<<'services: {app: {image: busybox:1.37, platform: linux/arm64}}'
  [[ -z "$up_out" ]] || track_sandbox "$up_out"
  assert_not_eq "$up_status" 0 "an unsupported platform fails compose up"
  assert_contains "$up_err" "invalid Compose project" "an invalid project is rejected locally"
  assert_eq "$up_out" "" "a rejected project prints no sandbox"
  started=$SECONDS
  compose_up "$dir" e2e -f - <<'EOF'
services:
  unhealthy:
    image: busybox:1.37
    command: [sleep, '600']
    healthcheck:
      test: [CMD, 'false']
      interval: 1s
      timeout: 1s
      retries: 1
EOF
  [[ -z "$up_out" ]] || track_sandbox "$up_out"
  assert_not_eq "$up_status" 0 "a failed health check fails compose up"
  assert_eq "$up_out" "" "an unhealthy project prints no sandbox"
  check "a failed health check fails within 60s" test $((SECONDS - started)) -lt 60
  api_get /sandboxes
  assert_eq "$(jq -cS . <<<"$HTTP_BODY")" "$before" "the unhealthy sandbox is deleted"
  end_test
}

test_image_defaults() {
  begin_test image-defaults
  # No command/entrypoint overrides: placeholders must carry the original image
  # configuration, including Redis's entrypoint and working directory.
  compose_create "$dir" e2e -f - <<'EOF'
services:
  web:
    image: nginx:1.27-alpine
    ports: ['8080:80']
  cache:
    image: redis:7-alpine
    volumes: [data:/data]
    healthcheck:
      test: [CMD, redis-cli, ping]
      interval: 1s
      timeout: 1s
      retries: 30
volumes:
  data: {}
EOF
  assert_contains "$(proxy "$sandbox" 8080)" "Welcome to nginx" "nginx runs its image command"
  assert_eq "$(guest "$sandbox" docker exec e2e-cache-1 redis-cli ping)" PONG "redis runs its image entrypoint"
  assert_eq "$(guest "$sandbox" docker inspect --format '{{.Config.WorkingDir}}' e2e-cache-1)" /data \
    "the original working directory is preserved"
  assert_eq "$(guest "$sandbox" docker inspect --format '{{json .Config.Entrypoint}}' e2e-cache-1)" \
    '["docker-entrypoint.sh"]' "the original entrypoint is preserved"
  guest "$sandbox" docker exec e2e-cache-1 sh -c 'mkdir /tmp/shared; printf readable >/tmp/shared/value'
  assert_eq "$(guest "$sandbox" docker exec --user 65534 e2e-cache-1 cat /tmp/shared/value)" readable \
    "exec-created files remain readable by non-root container users"
  end_test
}

test_build() {
  local compose dockerfile output project service image
  local -a args
  local -A images=()
  begin_test build
  mkdir -p "$dir/context/docker"
  printf context-upload >"$dir/context/payload"
  printf 'must not be uploaded' >"$dir/context/excluded"
  printf 'excluded\n' >"$dir/context/.dockerignore"
  dockerfile="$dir/context/docker/Custom"
  cat >"$dockerfile" <<'EOF'
FROM busybox:1.37 AS app
ARG VALUE=default
COPY . /context
RUN test ! -e /context/excluded && printf '%s' "$VALUE" >/build-value
HEALTHCHECK CMD false
FROM scratch AS unused
EOF
  compose="$dir/compose.yaml"
  cat >"$compose" <<'EOF'
services:
  app:
    build:
      context: ./context
      dockerfile: docker/Custom
      target: app
      args: {VALUE: "${VALUE}"}
    image: e2e/shared-app:dev
    command: [sh, -c, 'mkdir -p /www; cp /context/payload /www/index.html; exec httpd -f -p 8080 -h /www']
    environment: {LITERAL: "${VALUE}"}
    healthcheck:
      test: [CMD, wget, -qO-, http://localhost:8080]
      interval: 1s
      timeout: 1s
      retries: 10
  client:
    image: busybox:1.37
    command: [sleep, infinity]
    depends_on: {app: {condition: service_healthy}}
  sidecar:
    image: e2e/shared-app:dev
    command: [sleep, infinity]
    healthcheck: {test: [CMD, "true"]}
  inline:
    build:
      dockerfile_inline: |
        FROM busybox:1.37
        RUN printf inline >/build-value
    command: [sleep, infinity]
  worker:
    build:
      context: ./context
      dockerfile: docker/Custom
      target: app
    command: [sleep, infinity]
    healthcheck: {test: [CMD, "true"]}
EOF
  cp "$compose" "$dir/compose.orig"
  output="$dir/built.yaml"
  project="e2e-compose-$(rand_hex)"
  args=(-f "$compose" -p "$project" --output "$output" --env 'VALUE=$literal=a=b')
  if [[ -n "${AENV_BUILD_IMAGE_REPOSITORY:-}" ]]; then
    args+=(--image-repository "$AENV_BUILD_IMAGE_REPOSITORY")
    [[ "${AENV_BUILD_REGISTRY_INSECURE:-}" != "1" ]] || args+=(--registry-insecure)
  fi
  compose_build "${args[@]}"
  build_statuses "$build_err"
  [[ "$build_status" == 0 ]] || die "aenv compose build failed (exit ${build_status}): ${build_err}"
  assert_eq "$build_out" "" "compose build prints nothing on stdout"
  while read -r service image; do
    images[$service]=$image
  done < <(overridden_images "$output")
  assert_eq "$(printf '%s\n' "${!images[@]}" | sort | xargs)" "app inline sidecar worker" \
    "the override file pins the built services and the sidecar"
  assert_eq "$(grep -c 'build: !reset null' "$output" || true)" 3 "the override file resets the three builds"
  # As in Docker Compose, a service runs the image another service builds.
  assert_eq "${images[sidecar]:-}" "${images[app]:-}" "the sidecar runs the image app builds"
  for service in app inline worker; do
    if [[ -n "${AENV_BUILD_IMAGE_REPOSITORY:-}" ]]; then
      image="${images[$service]:-}"
      assert_eq "${image:0:$((${#AENV_BUILD_IMAGE_REPOSITORY} + 6))}" "${AENV_BUILD_IMAGE_REPOSITORY}:aenv-" \
        "service ${service} is pinned to the pushed image"
    else
      check "service ${service} is pinned to a digest" grep -Eqx 'sha256:[0-9a-f]{64}' <<<"${images[$service]:-}"
    fi
  done
  check "compose build leaves the Compose file unchanged" cmp -s "$compose" "$dir/compose.orig"
  assert_eq "$statuses" "ready ready ready" "compose build builds three images"
  for service in app inline worker; do
    assert_contains "$build_err" "Building Compose service ${service} as aenv/${project}-${service}" \
      "compose build names the ${service} image"
  done
  assert_contains "$build_err" "-f ${compose} -f ${output}" "compose build prints the matching up command"

  # The unmodified project with the override file starts without building.
  compose_create "$dir" e2e -f "$compose" -f "$output" --env 'VALUE=$literal=a=b' </dev/null
  refute_contains "$up_err" "Building Compose service" "the override file starts services without building"
  assert_eq "$(guest "$sandbox" docker exec e2e-app-1 cat /build-value)" '$literal=a=b' "build args keep literal dollars"
  assert_eq "$(guest "$sandbox" docker exec e2e-app-1 printenv LITERAL)" '$literal=a=b' \
    "environment values keep literal dollars"
  assert_eq "$(guest "$sandbox" docker exec e2e-client-1 wget -qO- http://app:8080)" context-upload \
    "the build context is uploaded"
  assert_eq "$(guest "$sandbox" docker exec e2e-worker-1 cat /build-value)" default "the worker builds the target"
  assert_eq "$(guest "$sandbox" docker exec e2e-sidecar-1 cat /build-value)" '$literal=a=b' \
    "the sidecar runs the app image"
  assert_eq "$(guest "$sandbox" docker exec e2e-inline-1 cat /build-value)" inline "inline Dockerfiles build"

  # compose up builds services itself; a project read from stdin resolves
  # against the current directory.
  compose_create "$dir" "$project" -f - --env 'VALUE=$literal=a=b' <"$compose"
  assert_eq "$statuses" "ready ready ready" "compose up builds three images"
  assert_eq "$(guest "$sandbox" docker exec "${project}-app-1" cat /build-value)" '$literal=a=b' \
    "compose up builds with build args"
  assert_eq "$(guest "$sandbox" docker exec "${project}-client-1" wget -qO- http://app:8080)" context-upload \
    "compose up resolves a stdin project's context against the current directory"
  assert_eq "$(guest "$sandbox" docker exec "${project}-sidecar-1" cat /build-value)" '$literal=a=b' \
    "compose up shares the app image with the sidecar"

  # --no-build starts the published latest images without building.
  compose_create "$dir" "$project" -f - --env 'VALUE=$literal=a=b' --no-build <"$compose"
  refute_contains "$up_err" "Created image build" "compose up --no-build builds nothing"
  assert_eq "$(guest "$sandbox" docker exec "${project}-app-1" cat /build-value)" '$literal=a=b' \
    "compose up --no-build runs the latest app image"
  assert_eq "$(guest "$sandbox" docker exec "${project}-worker-1" cat /build-value)" default \
    "compose up --no-build runs the latest worker image"
  assert_eq "$(guest "$sandbox" docker exec "${project}-sidecar-1" cat /build-value)" '$literal=a=b' \
    "compose up --no-build runs the latest app image for the sidecar"
  compose_up "$dir" "${project}-missing" -f - --no-build <"$compose"
  [[ -z "$up_out" ]] || track_sandbox "$up_out"
  assert_not_eq "$up_status" 0 "compose up --no-build fails without published images"
  assert_contains "$up_err" "was not found" "compose up --no-build reports the missing image"
  refute_contains "$up_err" "Created image build" "compose up --no-build does not build missing images"

  # A file the command did not write and unsupported builds are refused before
  # allocation.
  printf 'services: {}\n' >"$dir/foreign.yaml"
  compose_build -f "$compose" -p "$project" --output "$dir/foreign.yaml" --env VALUE=x
  assert_not_eq "$build_status" 0 "compose build refuses an output file it did not write"
  assert_contains "$build_err" "was not written by aenv compose build" "compose build names the foreign file"
  refute_contains "$build_err" "Created image build" "a refused output file allocates no builder"
  check "the foreign file is unchanged" cmp -s "$dir/foreign.yaml" <(printf 'services: {}\n')
  printf "services: {app: {build: {context: 'https://example.com/repo.git'}}}\n" >"$dir/remote.yaml"
  compose_build -f "$dir/remote.yaml" -p "$project"
  assert_not_eq "$build_status" 0 "compose build refuses a remote context"
  assert_contains "$build_err" "must be a local directory" "compose build requires a local context"
  refute_contains "$build_err" "Created image build" "a remote context allocates no builder"

  # A failed build reports BuildKit's error and keeps the previous override.
  cp "$output" "$dir/saved.yaml"
  printf 'FROM busybox:1.37 AS app\nRUN exit 42\n' >"$dockerfile"
  compose_build "${args[@]}"
  build_statuses "$build_err"
  assert_not_eq "$build_status" 0 "a failing Dockerfile fails compose build"
  assert_contains "$build_err" "exit code: 42" "compose build reports BuildKit's error"
  check "a failed build keeps the override file" cmp -s "$output" "$dir/saved.yaml"
  assert_eq "$statuses" error "the failed build is recorded as an error"
  end_test
}

test_terminal_bench_task() {
  local checkout environment base project main service
  begin_test terminal-bench
  checkout="$dir/terminal-bench"
  git init -q "$checkout"
  git -C "$checkout" remote add origin "$TERMINAL_BENCH"
  timeout 600 git -C "$checkout" fetch -q --depth 1 --filter blob:none origin "$TERMINAL_BENCH_COMMIT"
  git -C "$checkout" sparse-checkout set "$TERMINAL_BENCH_TASK"
  timeout 600 git -C "$checkout" checkout -q FETCH_HEAD
  environment="$checkout/$TERMINAL_BENCH_TASK"
  base="$dir/docker-compose-build.yaml"
  curl -fsSL --retry 3 --max-time 60 "$HARBOR_BUILD_COMPOSE" -o "$base"

  # Harbor's docker compose -f base -f task --project-directory <environment>.
  project="tb4-$(rand_hex)"
  compose_create "$dir" "$project" -f "$base" -f "$environment/docker-compose.yaml" \
    --project-directory "$environment" --env "CONTEXT_DIR=${environment}" </dev/null
  # The first compose up on a node also builds the bundled guest image.
  assert_eq "$(tr ' ' '\n' <<<"$statuses" | sort -u)" ready "every image build is ready"
  for service in event-feed main; do
    assert_contains "$up_err" "Building Compose service ${service} as aenv/${project}-${service}" \
      "compose up builds ${service}"
  done
  main="${project}-main-1"
  assert_eq "$(guest "$sandbox" docker inspect --format '{{json .Config.Cmd}}' "$main")" \
    '["sh","-c","sleep infinity"]' "main keeps Harbor's command"
  assert_eq "$(guest "$sandbox" docker exec "$main" printenv DISPATCH_EVENT_API_URL)" http://event-feed:8080 \
    "main gets the task's environment"
  check "main gets the task's build context" guest "$sandbox" docker exec "$main" test -f /app/packet/README.md
  check "main reaches event-feed's health endpoint" guest "$sandbox" docker exec "$main" python3 -c \
    "import urllib.request; urllib.request.urlopen('http://event-feed:8080/health', timeout=5).read()"
  end_test
}

if [[ -n "${AENV_COMPOSE_BASE_IMAGE:-}" ]]; then
  test_lifecycle
  test_project_loading
  test_rejection_and_failed_healthcheck_cleanup
  test_image_defaults
fi
if [[ "${AENV_COMPOSE_BUILD_TEST:-}" == "1" ]]; then
  test_build
fi
if [[ "${AENV_COMPOSE_TERMINAL_BENCH_TEST:-}" == "1" ]]; then
  test_terminal_bench_task
fi
suite_summary "19_compose"
