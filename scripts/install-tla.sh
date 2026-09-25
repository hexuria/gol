#!/usr/bin/env bash
# Install the pinned TLA+ tools jar at ~/.local/tla/tla2tools.jar.
#
# The v1.8.0 release asset is rebuilt every night, so CI ran a different TLC
# every day. v1.7.4 (TLC 2.19) is the newest tagged release. This script
# fetches it and refuses it unless its sha256 matches.
set -euo pipefail

version="1.7.4"
sha256="936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88"
url="https://github.com/tlaplus/tlaplus/releases/download/v${version}/tla2tools.jar"

jar="${HOME}/.local/tla/tla2tools.jar"
if [ -f "${jar}" ] && echo "${sha256}  ${jar}" | sha256sum -c --quiet - >/dev/null 2>&1; then
  echo "tla2tools ${version} is already installed at ${jar}"
  exit 0
fi

mkdir -p "$(dirname "${jar}")"
tmp="$(mktemp "${jar}.XXXXXX")"
trap 'rm -f "${tmp}"' EXIT
curl -fsSL -o "${tmp}" "${url}"
echo "${sha256}  ${tmp}" | sha256sum -c --quiet -
mv -f "${tmp}" "${jar}"
echo "installed tla2tools ${version} at ${jar}"
