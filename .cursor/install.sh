#!/usr/bin/env bash
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

sudo apt-get update
sudo apt-get install -y --no-install-recommends \
  ca-certificates \
  curl \
  unzip \
  pkg-config \
  g++ \
  libxkbcommon-dev \
  libxkbcommon-x11-dev \
  libwayland-dev \
  libfontconfig1-dev \
  libvulkan-dev \
  postgresql \
  postgresql-contrib \
  redis-server \
  default-jre-headless

if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.98.1
fi
if [ -f "${HOME}/.cargo/env" ]; then
  # shellcheck disable=SC1091
  source "${HOME}/.cargo/env"
fi

export BUN_INSTALL="${BUN_INSTALL:-${HOME}/.bun}"
if ! command -v bun >/dev/null 2>&1 && [ ! -x "${BUN_INSTALL}/bin/bun" ]; then
  curl -fsSL https://bun.sh/install | bash
fi
export PATH="${BUN_INSTALL}/bin:${PATH}"

export BEND_NO_TELEMETRY=1
if [ ! -x "${HOME}/.bend/bin/bend" ] || [ "$(BEND_NO_TELEMETRY=1 "${HOME}/.bend/bin/bend" version)" != "bend 2.0.27" ]; then
  curl -fsSL https://bend-lang.com/install.sh | sh
fi
export PATH="${HOME}/.bend/bin:${PATH}"
bend_version="$(BEND_NO_TELEMETRY=1 "${HOME}/.bend/bin/bend" version)"
if [ "${bend_version}" != "bend 2.0.27" ]; then
  echo "expected bend 2.0.27, found: ${bend_version}" >&2
  exit 1
fi
sudo ln -sfn "${HOME}/.bend/bin/bend" /usr/local/bin/bend

# Same jar URL as the formal job in .github/workflows/pr.yml.
tla_jar="${HOME}/.local/tla/tla2tools.jar"
if [ ! -f "${tla_jar}" ] || ! unzip -t "${tla_jar}" >/dev/null 2>&1; then
  mkdir -p "${HOME}/.local/tla"
  tmp_jar="${tla_jar}.partial"
  curl -fsSL -o "${tmp_jar}" \
    https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
  mv -f "${tmp_jar}" "${tla_jar}"
fi

# Same elan command as the formal job. It does not name a Lean version.
# lake reads formal/**/lean-toolchain when scripts/verify-formal.sh builds.
if [ ! -x "${HOME}/.elan/bin/elan" ]; then
  curl -fsSL https://raw.githubusercontent.com/leanprover/elan/master/elan-init.sh | sh -s -- -y --default-toolchain none
fi
if [ -f "${HOME}/.elan/env" ]; then
  # shellcheck disable=SC1091
  source "${HOME}/.elan/env"
fi
if [ ! -x "${HOME}/.elan/bin/lake" ]; then
  echo "lake was not installed by elan" >&2
  exit 1
fi
sudo ln -sfn "${HOME}/.elan/bin/elan" /usr/local/bin/elan
sudo ln -sfn "${HOME}/.elan/bin/lake" /usr/local/bin/lake
if [ -x "${HOME}/.elan/bin/lean" ]; then
  sudo ln -sfn "${HOME}/.elan/bin/lean" /usr/local/bin/lean
fi

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "${root}"
cargo fetch
(cd coworker && bun install --frozen-lockfile)
