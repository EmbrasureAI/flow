#!/usr/bin/env bash
# Prepare a fresh Linux VM (Amazon Linux 2023 / Fedora family, passwordless sudo)
# for builds, tests and benchmarks. Run it once before benchmarking on a new
# machine; rerunning it is cheap. Steps:
#   1. compilers, libraries, perf and Docker from dnf; rustup and uv
#   2. the production fixture (tests/production/fixture.sh up)
#   3. unless FLOW_SETUP_WARM_BUILD=0, a low-priority background build of
#      origin/main with ab.py's bench-fast settings, so the first A/B build is cached
# Logs: target/cloud-setup.log and target/warm-build.log.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$root"
mkdir -p target
exec > >(tee -a target/cloud-setup.log) 2>&1
echo "== cloud setup $(date -u +%FT%TZ)"

if ! command -v dnf >/dev/null; then
  echo "cloud_setup.sh supports dnf-based Linux only; see tests/production/README.md" >&2
  exit 1
fi
sudo dnf install -y -q gcc gcc-c++ clang clang-devel cmake pkgconf-pkg-config openssl openssl-devel \
  perl make util-linux time unzip git zlib-devel perf docker

if [[ ! -x $HOME/.cargo/bin/rustup ]]; then
  curl -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain none --profile minimal
fi
# shellcheck disable=SC1091
source "$HOME/.cargo/env"
rustup show active-toolchain >/dev/null   # installs the pinned toolchain from rust-toolchain.toml
if ! command -v uv >/dev/null && [[ ! -x $HOME/.local/bin/uv ]]; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi
export PATH=$HOME/.local/bin:$PATH

if ! docker compose version >/dev/null 2>&1 && ! sudo docker compose version >/dev/null 2>&1; then
  sudo mkdir -p /usr/local/lib/docker/cli-plugins
  sudo curl -sSL https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-x86_64 \
    -o /usr/local/lib/docker/cli-plugins/docker-compose
  sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
fi
# Allow perf for profiling builds.
sudo sysctl -q -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0 || true

tests/production/fixture.sh up
uv run --quiet tests/production/benchmark.py --help >/dev/null   # cache the harness's Python dependencies

if [[ ${FLOW_SETUP_WARM_BUILD:-1} != 0 ]]; then
  git fetch -q origin main || true
  setsid nohup nice -n 19 uv run tests/production/ab.py --warm origin/main \
    > target/warm-build.log 2>&1 < /dev/null &
  echo "warming the bench-fast build of origin/main in the background (target/warm-build.log)"
fi
echo "== cloud setup complete; run tests/production/ab.py --help"
