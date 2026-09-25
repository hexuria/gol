#!/usr/bin/env bash
# Install the pinned Bend release into ~/.bend/bin. CI and .cursor/install.sh call this, so the pin
# lives in one place. The upstream installer always serves the newest release; when that is not the
# pin, print what it serves and stop instead of installing a different Bend.
set -euo pipefail

pin="2.0.27"
# sha256 of ~/.bend/bin/bend for the pin on linux-x64 (docs/bend.md).
bin_sha256="38330ad07e228ba7a317836f484648cba93a1a3927357edccc65e3f2a0de253a"
bin="${HOME}/.bend/bin/bend"
export BEND_NO_TELEMETRY=1

installed() {
  [ -x "${bin}" ] && [ "$("${bin}" version)" = "bend ${pin}" ]
}

if ! installed; then
  script="$(mktemp)"
  trap 'rm -f "${script}"' EXIT
  curl -fsSL -o "${script}" https://bend-lang.com/install.sh
  served="$(sed -n 's/^VER=["'\'']\{0,1\}\([^"'\'' ]*\).*/\1/p' "${script}" | head -n 1)"
  if [ "${served}" != "${pin}" ]; then
    echo "bend-lang.com/install.sh serves VER=${served:-?}, the pin is ${pin}." >&2
    echo "Version, URL, and checksum lines of the served installer:" >&2
    grep -n -E 'VER|https?://|sha256|[0-9a-f]{64}' "${script}" >&2 || true
    exit 1
  fi
  sh "${script}"
fi

if ! installed; then
  echo "expected bend ${pin}, found: $("${bin}" version 2>&1 || true)" >&2
  exit 1
fi
if [ "$(uname -s)-$(uname -m)" = "Linux-x86_64" ]; then
  actual="$(sha256sum "${bin}" | cut -d ' ' -f 1)"
  if [ "${actual}" != "${bin_sha256}" ]; then
    echo "bend ${pin} binary sha256 is ${actual}, expected ${bin_sha256}" >&2
    exit 1
  fi
fi
echo "bend ${pin} at ${bin}"
