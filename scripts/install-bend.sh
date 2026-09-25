#!/usr/bin/env bash
# Install the pinned Bend release into ~/.bend/bin. CI and .cursor/install.sh call this, so the pin
# lives in one place. bend-lang.com/install.sh always serves the newest release, with VER and one
# SHA_<OS>_<ARCH> per archive. When it serves another release, point VER and SHA_LINUX_X64 back at
# the pin; the installer's own sha256 check then verifies the pinned archive. Other platforms stop.
set -euo pipefail

pin="2.0.27"
# linux-x64 release archive and installed binary for the pin (docs/bend.md).
archive_sha256="58adc86af6605ed0c48f7d84e4c23028f78893ce4a867a20a4f004b11582687b"
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
    if [ "$(uname -s)-$(uname -m)" != "Linux-x86_64" ]; then
      echo "bend-lang.com/install.sh serves ${served:-?}; only linux-x64 has a recorded ${pin} sha256." >&2
      exit 1
    fi
    sed -i -e "s/^VER=.*/VER=\"${pin}\"/" \
      -e "s/^SHA_LINUX_X64=.*/SHA_LINUX_X64=\"${archive_sha256}\"/" "${script}"
    if ! grep -qx "VER=\"${pin}\"" "${script}" \
      || ! grep -qx "SHA_LINUX_X64=\"${archive_sha256}\"" "${script}"; then
      echo "bend-lang.com/install.sh no longer sets VER and SHA_LINUX_X64; cannot pin ${pin}." >&2
      grep -n -E 'VER|REPO|SHA_|https?://' "${script}" >&2 || true
      exit 1
    fi
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
