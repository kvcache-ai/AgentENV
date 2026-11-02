SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := build

TOOLS_VERSION ?= 0.1.1
ENVD_REF ?= 2026.17
ENVD_UPSTREAM_REPO ?= https://github.com/e2b-dev/infra.git
HOST_MACHINE := $(shell uname -m)
ifeq ($(HOST_MACHINE),x86_64)
DEFAULT_ARCH := amd64
else ifeq ($(HOST_MACHINE),amd64)
DEFAULT_ARCH := amd64
else ifeq ($(HOST_MACHINE),aarch64)
DEFAULT_ARCH := arm64
else ifeq ($(HOST_MACHINE),arm64)
DEFAULT_ARCH := arm64
else
DEFAULT_ARCH := amd64
endif
ARCH ?= $(DEFAULT_ARCH)
ifeq ($(filter $(ARCH),amd64 arm64),)
$(error unsupported ARCH '$(ARCH)' (expected amd64 or arm64))
endif
PLATFORM := linux/$(ARCH)
PUBLISH_PLATFORMS ?= linux/amd64,linux/arm64
IMAGE ?= agentenv-tools:$(TOOLS_VERSION)
DOCKER ?= docker

.PHONY: build publish push check-buildx check-tools-version check-publish-image

check-buildx:
	@"$(DOCKER)" buildx version >/dev/null 2>&1 || { \
		echo "docker buildx is required to build the tools image." >&2; \
		echo "Install Docker Buildx or use a Docker version that includes it." >&2; \
		exit 1; \
	}

check-tools-version:
	@[[ "$(TOOLS_VERSION)" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(\.(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*))?$$ ]] || { \
		echo "TOOLS_VERSION must be SemVer without build metadata (for example 1.2.3 or 1.2.3-custom.1), got '$(TOOLS_VERSION)'." >&2; \
		exit 1; \
	}

# Registry-side immutable tags provide enforcement; the inspect below is only a preflight.
check-publish-image:
	@case "$(IMAGE)" in \
		*/*) ;; \
		*) \
			echo "IMAGE must include a registry/repository for publish, got '$(IMAGE)'." >&2; \
			echo "Example: make publish IMAGE=ghcr.io/kvcache-ai/agentenv-tools:$(TOOLS_VERSION)" >&2; \
			exit 1; \
		;; \
	esac
	@case "$(IMAGE)" in \
		*:$(TOOLS_VERSION)) ;; \
		*) \
			echo "IMAGE tag must match TOOLS_VERSION $(TOOLS_VERSION), got '$(IMAGE)'." >&2; \
			exit 1; \
		;; \
	esac
	@! "$(DOCKER)" buildx imagetools inspect "$(IMAGE)" >/dev/null 2>&1 || { \
		echo "Refusing to overwrite published tools drive $(IMAGE). Bump TOOLS_VERSION." >&2; \
		exit 1; \
	}

build: check-buildx check-tools-version
	"$(DOCKER)" buildx build \
		--platform "$(PLATFORM)" \
		--target artifact \
		--build-arg "TOOLS_VERSION=$(TOOLS_VERSION)" \
		--build-arg "ENVD_REF=$(ENVD_REF)" \
		--build-arg "ENVD_UPSTREAM_REPO=$(ENVD_UPSTREAM_REPO)" \
		-t "$(IMAGE)" \
		--load \
		.
publish: check-buildx check-tools-version check-publish-image
	"$(DOCKER)" buildx build \
		--platform "$(PUBLISH_PLATFORMS)" \
		--target artifact \
		--build-arg "TOOLS_VERSION=$(TOOLS_VERSION)" \
		--build-arg "ENVD_REF=$(ENVD_REF)" \
		--build-arg "ENVD_UPSTREAM_REPO=$(ENVD_UPSTREAM_REPO)" \
		-t "$(IMAGE)" \
		--push \
		.
push:
	@echo "'make push' is no longer supported; use 'make publish' for a multi-platform release." >&2
	@exit 1
