SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:

# Tools
CARGO ?= cargo
DOCKER ?= docker
DOCKER_COMPOSE ?= docker compose
DEPLOY_COMPOSE_FILE ?= deploy/docker-compose.yml
# Guests need upstream DNS, not the host's loopback resolver stub.
HOST_RESOLV_CONF ?= $(firstword $(wildcard /run/systemd/resolve/resolv.conf /etc/resolv.conf))
export HOST_RESOLV_CONF
APT_MIRROR_BASE ?=
KUBECTL ?= kubectl
K8S_NAMESPACE ?= agentenv-system
K8S_RUNTIME_IMAGE ?= agentenv-runtime:latest
K8S_GATEWAY_IMAGE ?= agentenv-gateway:latest
K8S_SCHEDULER_IMAGE ?= agentenv-scheduler:latest
K3S_CTR ?= sudo k3s ctr

# aenv home path.
AENV_HOME_PATH ?= /var/lib/aenv
export AENV_HOME_PATH

# aenv CLI install location. Override with AENV_INSTALL_PREFIX=~/.local for a
# user-local install; AENV_INSTALL_PREFIX=/usr/local requires sudo.
AENV_INSTALL_PREFIX ?= /usr/local
AENV_INSTALL_DIR := $(AENV_INSTALL_PREFIX)/bin
# Runner used to execute the `install` / `rm` commands that write to
# $(AENV_INSTALL_DIR). Defaults to sudo because the default prefix is
# /usr/local; override to empty (AENV_INSTALL_SUDO=) for a user-local prefix.
AENV_INSTALL_SUDO ?= sudo

# Script entrypoints
TEST_SCRIPTS_DIR := ./scripts/tests

# Runner for tests that require AENV's network and namespace capabilities.
CARGO_HOST_TARGET_ENV = $(shell $(CARGO) -vV | sed -n 's/^host: //p' | tr '[:lower:]-' '[:upper:]_')
CAPABILITY_RUNNER = CARGO_TARGET_$(CARGO_HOST_TARGET_ENV)_RUNNER="$(CURDIR)/scripts/run-with-capabilities.sh"
AENV_TEST_STATE_ID ?= $(if $(GITHUB_RUN_ID),$(GITHUB_RUN_ID)-$(GITHUB_RUN_ATTEMPT),local-$$(id -u))
AENV_TEST_STATE_DIR ?= /tmp/aenv-test-$(AENV_TEST_STATE_ID)
AENV_TEST_DEPS_PATH ?= $(if $(AENV_DEPS_PATH),$(AENV_DEPS_PATH),/var/lib/aenv/deps)
CAPABILITY_TEST_ENV := AENV_HOME_PATH="$(AENV_TEST_STATE_DIR)/home" AENV_RUNTIME_PATH="$(AENV_TEST_STATE_DIR)/run" AENV_DEPS_PATH="$(AENV_TEST_DEPS_PATH)"
UVM_UBLK_DAEMON_INSTALL_PATH := $(AENV_HOME_PATH)/ublk/uvm-ublk-daemon
DEBUG_PROFILE_DIR := $${CARGO_TARGET_DIR:-$$(pwd)/target}/debug

# Build profile: debug for dev/test targets, release for explicit release and
# benchmark targets.
PROFILE ?= debug
CARGO_PROFILE_FLAG = $(if $(filter release,$(PROFILE)),--release,)
TARGET_PROFILE_DIR = $${CARGO_TARGET_DIR:-$$(pwd)/target}/$(PROFILE)

.PHONY: all build release \
	build-server build-server-release \
	build-snapshot-image \
	build-aenv build-aenv-release install-aenv uninstall-aenv \
	build-ublk install-ublk \
	fmt clippy \
	mutants coverage \
	test test-unit test-integration prepare-agent-test-state test-agent test-agent-integration test-envd test-ublk \
	test-e2e test-e2e-compose test-e2e-k8s test-e2e-all \
	bench bench-snapshot bench-ublk bench-orchestrator-store \
	ci-deps ci-deps-protoc \
	firecracker-client envd-http-client agentenv-server custom-extension-client start-server start-server-release \
	services gateway scheduler \
	deploy-build deploy-up deploy-up-no-build deploy-down deploy-logs deploy-ps \
	k8s-build k8s-redeploy k8s-load-dev k8s-refresh-dev \
	k8s-render k8s-apply k8s-delete \
	k8s-render-dev k8s-apply-dev k8s-delete-dev \
	docs docs-serve

all: build

build:
	$(CARGO) build

release:
	$(CARGO) build --release

build-server:
	$(CARGO) build -p agentenv --bin server

build-server-release:
	$(CARGO) build --release -p agentenv --bin server

build-snapshot-image:
	$(CARGO) build -p agentenv --bin aenv-snapshot-image

build-aenv:
	$(CARGO) build -p aenv

build-aenv-release:
	$(CARGO) build --release -p aenv

install-aenv: build-aenv-release
	$(AENV_INSTALL_SUDO) install -d "$(AENV_INSTALL_DIR)"
	$(AENV_INSTALL_SUDO) install -m 0755 "$${CARGO_TARGET_DIR:-$$(pwd)/target}/release/aenv" "$(AENV_INSTALL_DIR)/aenv"
	@echo "Installed aenv to $(AENV_INSTALL_DIR)/aenv"

uninstall-aenv:
	$(AENV_INSTALL_SUDO) rm -f "$(AENV_INSTALL_DIR)/aenv"
	@echo "Removed $(AENV_INSTALL_DIR)/aenv"

fmt:
	$(CARGO) fmt --all -- --check

clippy:
	$(CARGO) clippy --workspace --all-targets --all-features -- -D warnings

mutants:
	$(CARGO) adev mutants

coverage:
	$(MAKE) install-ublk PROFILE=debug
	PATH="$${CARGO_TARGET_DIR:-$$(pwd)/target}/debug:$$PATH" \
	$(CARGO) adev coverage

test: test-agent test-envd test-ublk

test-unit:
	$(CARGO) test -p agentenv -p envd -p linux-cap --lib
	$(CARGO) test -p aenv --bin aenv
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p agentenv --lib -- --ignored
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p uvm-ublk -p uvm-ublk-daemon --lib
	bash scripts/tests/verify-capability-runner.sh
	bash scripts/tests/verify-install-service.sh
	bash scripts/tests/verify-install-buildctl.sh

test-integration: test-agent-integration test-envd test-ublk

prepare-agent-test-state:
	$(CAPABILITY_TEST_ENV) $(CARGO) run --bin server -- --setup-only

test-agent: prepare-agent-test-state
	$(MAKE) build-ublk PROFILE=debug
	export PATH="$(DEBUG_PROFILE_DIR):$$PATH"; \
	export AENV_UBLK_DAEMON_BINARY_PATH="$(DEBUG_PROFILE_DIR)/uvm-ublk-daemon"; \
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p agentenv; \
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p agentenv --lib -- --ignored

test-agent-integration: prepare-agent-test-state
	$(MAKE) build-ublk PROFILE=debug
	PATH="$(DEBUG_PROFILE_DIR):$$PATH" \
	AENV_UBLK_DAEMON_BINARY_PATH="$(DEBUG_PROFILE_DIR)/uvm-ublk-daemon" \
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p agentenv \
		--test integration \
		--test orchestrator_integration
	PATH="$(DEBUG_PROFILE_DIR):$$PATH" \
	AENV_UBLK_DAEMON_BINARY_PATH="$(DEBUG_PROFILE_DIR)/uvm-ublk-daemon" \
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p agentenv-e2e-tests --test snapshot_oss_e2e_test -- --ignored

build-ublk:
	$(CARGO) build $(CARGO_PROFILE_FLAG) -p uvm-ublk -p uvm-ublk-daemon

install-ublk: build-ublk
	$(AENV_INSTALL_SUDO) mkdir -p "$$(dirname "$(UVM_UBLK_DAEMON_INSTALL_PATH)")"
	$(AENV_INSTALL_SUDO) cp "$(TARGET_PROFILE_DIR)/uvm-ublk-daemon" "$(UVM_UBLK_DAEMON_INSTALL_PATH)"
	$(AENV_INSTALL_SUDO) chmod 0755 "$(UVM_UBLK_DAEMON_INSTALL_PATH)"

test-envd:
	bash $(TEST_SCRIPTS_DIR)/test_envd.sh

test-ublk:
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p uvm-ublk -p overlaybd -p uvm-ublk-daemon
	$(CAPABILITY_TEST_ENV) $(CAPABILITY_RUNNER) $(CARGO) test -p overlaybd --test oss_backend_minio -- --ignored

bench:
	$(MAKE) install-ublk PROFILE=release
	$(CAPABILITY_RUNNER) $(CARGO) bench -p agentenv-benchmarks

bench-snapshot:
	$(MAKE) install-ublk PROFILE=release
	$(CAPABILITY_RUNNER) $(CARGO) bench -p agentenv-benchmarks --bench snapshot

bench-ublk:
	$(MAKE) install-ublk PROFILE=release
	$(CAPABILITY_RUNNER) $(CARGO) bench -p agentenv-benchmarks --bench ublk_overlaybd

bench-orchestrator-store:
	$(CARGO) bench -p agentenv-benchmarks --bench orchestrator_store

OCI_IMAGE ?=
bench-oci-conversion:
	$(if $(OCI_IMAGE),AGENTENV_BENCH_OCI_IMAGE="$(OCI_IMAGE)") $(CARGO) bench -p agentenv-benchmarks --bench oci_conversion_pipeline

ci-deps:
	$(MAKE) ci-deps-protoc

ci-deps-protoc:
	$(CARGO) adev codegen --ensure-deps-only

firecracker-client:
	$(CARGO) adev codegen firecracker

envd-http-client:
	$(CARGO) adev codegen envd

agentenv-server:
	$(CARGO) adev codegen server

custom-extension-client:
	$(CARGO) adev codegen custom-extension

test-e2e:
	$(MAKE) install-ublk PROFILE=debug
	bash $(TEST_SCRIPTS_DIR)/e2e/run_e2e.sh

test-e2e-compose:
	APT_MIRROR_BASE="$(APT_MIRROR_BASE)" E2E_MODE=compose bash $(TEST_SCRIPTS_DIR)/e2e/run_e2e.sh

test-e2e-k8s:
	APT_MIRROR_BASE="$(APT_MIRROR_BASE)" E2E_MODE=k8s bash $(TEST_SCRIPTS_DIR)/e2e/run_e2e.sh

test-e2e-all: test-e2e test-e2e-compose test-e2e-k8s

start-server:
	$(MAKE) install-ublk PROFILE=debug
	$(CAPABILITY_RUNNER) $(CARGO) run --bin server

start-server-release:
	$(MAKE) install-ublk PROFILE=release
	$(CAPABILITY_RUNNER) $(CARGO) run --release --bin server

deploy-up:
	APT_MIRROR_BASE="$(APT_MIRROR_BASE)" $(DOCKER_COMPOSE) -f $(DEPLOY_COMPOSE_FILE) up --build -d

deploy-build:
	APT_MIRROR_BASE="$(APT_MIRROR_BASE)" $(DOCKER_COMPOSE) -f $(DEPLOY_COMPOSE_FILE) build

deploy-up-no-build:
	$(DOCKER_COMPOSE) -f $(DEPLOY_COMPOSE_FILE) up -d

deploy-down:
	$(DOCKER_COMPOSE) -f $(DEPLOY_COMPOSE_FILE) down --remove-orphans

deploy-logs:
	$(DOCKER_COMPOSE) -f $(DEPLOY_COMPOSE_FILE) logs -f

deploy-ps:
	$(DOCKER_COMPOSE) -f $(DEPLOY_COMPOSE_FILE) ps

k8s-build:
	$(DOCKER) build $(if $(APT_MIRROR_BASE),--build-arg APT_MIRROR_BASE="$(APT_MIRROR_BASE)",) -f deploy/docker/Dockerfile.agentenv -t $(K8S_RUNTIME_IMAGE) .
	$(DOCKER) build -f deploy/docker/Dockerfile.gateway -t $(K8S_GATEWAY_IMAGE) .
	$(DOCKER) build -f deploy/docker/Dockerfile.scheduler -t $(K8S_SCHEDULER_IMAGE) .

k8s-redeploy:
	$(KUBECTL) rollout restart deploy/agentenv-gateway -n $(K8S_NAMESPACE)
	$(KUBECTL) rollout restart deploy/agentenv-scheduler -n $(K8S_NAMESPACE)
	$(KUBECTL) rollout restart ds/agentenv-node -n $(K8S_NAMESPACE)
	$(KUBECTL) rollout status deploy/agentenv-gateway -n $(K8S_NAMESPACE)
	$(KUBECTL) rollout status deploy/agentenv-scheduler -n $(K8S_NAMESPACE)
	$(KUBECTL) rollout status ds/agentenv-node -n $(K8S_NAMESPACE)

k8s-load-dev:
	$(DOCKER) save $(K8S_RUNTIME_IMAGE) | $(K3S_CTR) images import -
	$(DOCKER) save $(K8S_GATEWAY_IMAGE) | $(K3S_CTR) images import -
	$(DOCKER) save $(K8S_SCHEDULER_IMAGE) | $(K3S_CTR) images import -

k8s-refresh-dev: k8s-build k8s-load-dev k8s-redeploy

k8s-render:
	bash deploy/k8s/run.sh render

k8s-apply:
	bash deploy/k8s/run.sh apply

k8s-delete:
	bash deploy/k8s/run.sh delete

k8s-render-dev:
	K8S_OVERLAY=local-dev bash deploy/k8s/run.sh render

k8s-apply-dev:
	K8S_OVERLAY=local-dev bash deploy/k8s/run.sh apply

k8s-delete-dev:
	K8S_OVERLAY=local-dev bash deploy/k8s/run.sh delete

services-%:
	$(MAKE) -C services $*

gateway-%:
	$(MAKE) -C services/gateway $*

scheduler-%:
	$(MAKE) -C services/scheduler $*

docs/src/openapi.yml:
	ln -sf ../../src/api/openapi.yml docs/src/openapi.yml

docs: docs/src/openapi.yml
	mdbook build docs

docs-serve: docs/src/openapi.yml
	mdbook serve docs --open
