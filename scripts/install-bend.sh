#!/usr/bin/env bash
# Install the pinned Bend release into ~/.bend.
#
# bend-lang.com/install.sh installs the newest release. The repo pins 2.0.27
# (verify-bend.sh and workflow-bend's BEND_VERSION), so a new upstream release
# broke every install that used it. This script fetches the pinned release
# archive and refuses it unless its sha256 matches.
set -euo pipefail

version="2.0.27"
sha256="58adc86af6605ed0c48f7d84e4c23028f78893ce4a867a20a4f004b11582687b"
url="https://github.com/bendlang/bend/releases/download/v${version}/bend-${version}-linux-x64.tar.gz"

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) ;;
  *)
    echo "install-bend.sh pins the linux-x64 archive only." >&2
    echo "Install bend ${version} from https://github.com/bendlang/bend/releases/tag/v${version}" >&2
    exit 1
    ;;
esac

prefix="${HOME}/.bend"
if [ -x "${prefix}/bin/bend" ] \
  && [ "$(BEND_NO_TELEMETRY=1 "${prefix}/bin/bend" version)" = "bend ${version}" ]; then
  echo "bend ${version} is already installed at ${prefix}"
  exit 0
fi

tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT
curl -fsSL -o "${tmp}/bend.tar.gz" "${url}"
echo "${sha256}  ${tmp}/bend.tar.gz" | sha256sum -c --quiet -

# The archive holds bend/bin, bend/bend2, and bend/guide. The official
# installer puts them under ~/.bend, which is where the scripts look for bend.
mkdir -p "${prefix}"
rm -rf "${prefix:?}/bin" "${prefix:?}/bend2" "${prefix:?}/guide"
tar -xzf "${tmp}/bend.tar.gz" -C "${prefix}" --strip-components=1 \
  --no-same-owner --warning=no-unknown-keyword

installed="$(BEND_NO_TELEMETRY=1 "${prefix}/bin/bend" version)"
if [ "${installed}" != "bend ${version}" ]; then
  echo "expected bend ${version}, found: ${installed}" >&2
  exit 1
fi
echo "installed ${installed} at ${prefix}"
