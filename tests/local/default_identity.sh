#!/usr/bin/env bash
# Usage: tests/local/default_identity.sh /absolute/path/to/product /new/artifacts/path
set -euo pipefail
product=$(cd "$1" && pwd)
artifacts=$2
root=$(cd "$(dirname "$0")/../.." && pwd)
mkdir "$artifacts"
artifacts=$(cd "$artifacts" && pwd)
project="flow-default-$(date +%s)-$$"
export COMPOSE_PROJECT_NAME="$project"
cat > "$artifacts/pg16.yaml" <<'YAML'
services:
  postgres:
    image: postgres:16-bookworm
YAML
compose=(docker compose -f "$root/tests/local/compose.yaml" -f "$artifacts/pg16.yaml")
cleanup() { "${compose[@]}" down --volumes > "$artifacts/cleanup.log" 2>&1; }
trap cleanup EXIT
"${compose[@]}" up -d --wait > "$artifacts/services.log" 2>&1
FLOW_SOURCE_DIR="$root" FLOW_ENGINE_TAG="${project}-engine" "$product/scripts/build-flow.sh" > "$artifacts/engine-build.log" 2>&1
docker build --build-arg FLOW_ENGINE_IMAGE="${project}-engine" -f "$product/apps/flow/Dockerfile" -t "${project}-runner" "$product" > "$artifacts/runner-build.log" 2>&1
docker build --build-arg FLOW_RUNNER_IMAGE="${project}-runner" -f "$root/tests/local/Dockerfile.default-identity" -t "${project}-qualification" "$root" > "$artifacts/qualification-build.log" 2>&1
git -C "$root" rev-parse HEAD > "$artifacts/engine-source.txt"
git -C "$root" diff --binary > "$artifacts/engine-diff.patch"
git -C "$product" rev-parse HEAD > "$artifacts/product-source.txt"
git -C "$product" diff --binary > "$artifacts/product-diff.patch"
docker image inspect "${project}-engine" "${project}-runner" "${project}-qualification" > "$artifacts/images.json"
docker run --rm --network "${project}_default" -v "$artifacts:/artifacts" \
  -e AWS_ACCESS_KEY_ID=demo-access -e AWS_SECRET_ACCESS_KEY=demo-secret-key -e AWS_REGION=us-east-1 \
  "${project}-qualification" 2>&1 | tee "$artifacts/qualification.log"
