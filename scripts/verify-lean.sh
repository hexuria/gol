#!/usr/bin/env bash
# Build every Lean project under formal/.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "${root}"

shopt -s globstar nullglob
lakefiles=("${root}/formal/"**/lakefile.toml)
if ((${#lakefiles[@]})); then
  if ! command -v lake >/dev/null 2>&1; then
    echo "formal/ has a Lean lakefile but lake is not installed" >&2
    exit 1
  fi
  for lakefile in "${lakefiles[@]}"; do
    dir="$(dirname "${lakefile}")"
    echo "lake build ${dir#"${root}/"}"
    (cd "${dir}" && lake build)
  done
fi
