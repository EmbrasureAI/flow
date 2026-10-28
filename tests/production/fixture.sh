#!/usr/bin/env bash
# Manage the disposable production fixture used by benchmark.py and ab.py.
#
#   fixture.sh up        start Docker if needed, load or build images, start services
#   fixture.sh status    exit 0 when PostgreSQL, MinIO and REST accept connections
#   fixture.sh gc-slots  drop inactive benchmark replication slots (flow_test_*)
#   fixture.sh down      stop services and delete their volumes
#   fixture.sh reset     down, then up: reclaims the disk that retained runs use
#
# Environment: FLOW_FIXTURE_PROJECT (default flow-production-local), the ports
# in target/production-local/ports.env (created with defaults if absent) and
# FLOW_FIXTURE_REPOSITORY (default EmbrasureAI/flow), whose CI artifacts supply
# prebuilt MinIO images. FLOW_PRODUCTION_WAREHOUSE=s3://bucket/prefix/ points
# the catalog at real S3 with the caller's AWS credentials; see
# compose.aws-s3.yaml.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
project=${FLOW_FIXTURE_PROJECT:-flow-production-local}
ports=$root/target/production-local/ports.env
minio_image=flow-test-minio:RELEASE.2025-04-22T22-12-26Z
mc_image=flow-test-mc:RELEASE.2025-04-16T18-13-26Z
repository=${FLOW_FIXTURE_REPOSITORY:-EmbrasureAI/flow}

ensure_ports() {
  if [[ ! -f $ports ]]; then
    mkdir -p "$(dirname "$ports")"
    printf 'FLOW_PRODUCTION_PG_PORT=55432\nFLOW_PRODUCTION_S3_PORT=59000\nFLOW_PRODUCTION_REST_PORT=58181\n' > "$ports"
  fi
  # shellcheck disable=SC1090
  source "$ports"
}

docker_ready() { docker info >/dev/null 2>&1; }

ensure_docker() {
  docker_ready && return
  if ! command -v dockerd >/dev/null; then
    echo "Docker is not installed; run tests/production/cloud_setup.sh" >&2
    exit 1
  fi
  # Detach from this shell's session: agent tool shells may be reaped after a call.
  sudo setsid nohup dockerd > /tmp/dockerd.log 2>&1 < /dev/null &
  for _ in $(seq 60); do
    sudo docker info >/dev/null 2>&1 && break
    sleep 1
  done
  sudo chmod 666 /var/run/docker.sock
  docker_ready || { echo "dockerd did not start; see /tmp/dockerd.log" >&2; exit 1; }
}

compose() {
  local files=(-f "$root/tests/production/compose.yaml")
  # Some VMs cannot apply cgroup CPU or memory limits. Record that the services then run unbounded.
  if ! docker run --rm --cpus 1 --memory 64m "$minio_image" --version >/dev/null 2>&1; then
    files+=(-f "$root/tests/production/compose.unlimited.yaml")
  fi
  if [[ -n ${FLOW_PRODUCTION_WAREHOUSE:-} ]]; then
    files+=(-f "$root/tests/production/compose.aws-s3.yaml")
  fi
  docker compose --env-file "$ports" -p "$project" "${files[@]}" "$@"
}

load_images() {
  docker image inspect "$minio_image" "$mc_image" >/dev/null 2>&1 && return
  # The service integration workflow publishes the MinIO images it builds from
  # source; reuse the newest unexpired copy (artifact downloads need gh auth).
  if command -v gh >/dev/null && gh auth status >/dev/null 2>&1; then
    local directory artifact
    directory=$(mktemp -d)
    artifact=$(gh api "repos/$repository/actions/artifacts?name=minio-images&per_page=20" \
      --jq '[.artifacts[] | select(.expired | not)] | sort_by(.created_at) | last | .id // empty' 2>/dev/null || true)
    if [[ -n $artifact ]] && gh api "repos/$repository/actions/artifacts/$artifact/zip" > "$directory/images.zip" 2>/dev/null \
        && (cd "$directory" && unzip -oq images.zip) && docker load -i "$directory"/*.tar >/dev/null; then
      echo "loaded MinIO fixture images from CI artifact minio-images/$artifact"
      rm -rf "$directory"
      return
    fi
    rm -rf "$directory"
  fi
  echo "building MinIO fixture images from source (a few minutes)"
  docker build -q -f "$root/tests/production/Dockerfile.minio" --target server -t "$minio_image" "$root/tests/production"
  docker build -q -f "$root/tests/production/Dockerfile.minio" --target client -t "$mc_image" "$root/tests/production"
}

status() {
  ensure_ports
  for port in "$FLOW_PRODUCTION_PG_PORT" "$FLOW_PRODUCTION_S3_PORT" "$FLOW_PRODUCTION_REST_PORT"; do
    (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null || { echo "fixture port $port is closed" >&2; return 1; }
  done
}

case ${1:-} in
  up)
    ensure_ports
    ensure_docker
    load_images
    # Build only the catalog image; --build would also rebuild the loaded MinIO images.
    compose build rest
    compose up -d --wait
    ;;
  status)
    status
    ;;
  gc-slots)
    ensure_docker
    docker exec "$project-postgres-1" psql -U flow -d flow -Atc \
      "SELECT count(pg_drop_replication_slot(slot_name)) FROM pg_replication_slots
       WHERE NOT active AND slot_name LIKE 'flow\_test\_%'"
    ;;
  down)
    ensure_ports
    ensure_docker
    compose down --volumes
    ;;
  reset)
    "$0" down
    "$0" up
    ;;
  *)
    sed -n '2,15p' "$0" >&2
    exit 2
    ;;
esac
