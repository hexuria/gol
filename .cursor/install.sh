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
  redis-server

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
if [ ! -x "${HOME}/.bend/bin/bend" ]; then
  curl -fsSL https://bend-lang.com/install.sh | sh
fi
export PATH="${HOME}/.bend/bin:${PATH}"

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "${root}"
cargo fetch
(cd coworker && bun install --frozen-lockfile)
