#!/usr/bin/env bash
# Build agentenv-{runtime,gateway,scheduler} from this checkout on the od5
# buildkit-amd64 lane and push them with the same <short-sha> tags CI uses.
# The shared namespace forbids user port-forwards, so buildkit is dialed over
# Tailscale; push auth comes from your local docker keychain.
#
# Usage: .github/scripts/build-od5.sh [runtime|gateway|scheduler ...]   (default: all three)
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

REGISTRY="${REGISTRY:-us-dallas-1.ocir.io/ax3vcxtxsjva/dev/research}"
BUILDKIT_ADDR="${BUILDKIT_ADDR:-tcp://buildkit-amd64.shared.svc.od5.prometheus.co:1234}"

# The tag names a commit, so the build context must be exactly that commit.
[ -z "$(git status --porcelain)" ] || { echo "working tree is dirty; commit first" >&2; exit 1; }
TAG=$(git rev-parse HEAD | cut -c1-7)

COMPONENTS=("$@")
[ ${#COMPONENTS[@]} -eq 0 ] && COMPONENTS=(gateway scheduler runtime)

dockerfile_for() {
  case "$1" in
    runtime)   echo deploy/docker/Dockerfile.agentenv ;;
    gateway)   echo deploy/docker/Dockerfile.gateway ;;
    scheduler) echo deploy/docker/Dockerfile.scheduler ;;
    *) echo "unknown component: $1" >&2; exit 2 ;;
  esac
}

for comp in "${COMPONENTS[@]}"; do
  image="$REGISTRY/agentenv-$comp:$TAG"
  echo ">>> building $image"
  buildctl --addr "$BUILDKIT_ADDR" build \
    --frontend dockerfile.v0 \
    --local context=. \
    --local dockerfile=. \
    --opt "filename=$(dockerfile_for "$comp")" \
    --opt platform=linux/amd64 \
    --output "type=image,name=$image,push=true" \
    --progress plain
  echo ">>> pushed $image"
done
