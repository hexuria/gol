#!/usr/bin/env bash
# Run the formal checks that already exist in the tree.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "${root}"

"${root}/scripts/verify-bend.sh"

shopt -s globstar nullglob
configs=("${root}/formal/"**/*.cfg)
if ((${#configs[@]})); then
  jar="${TLA_JAR:-}"
  if [[ -z "${jar}" ]]; then
    for candidate in \
      "${HOME}/.local/tla/tla2tools.jar" \
      /usr/share/java/tla2tools.jar
    do
      if [[ -f "${candidate}" ]]; then
        jar="${candidate}"
        break
      fi
    done
  fi
  if [[ -z "${jar}" || ! -f "${jar}" ]]; then
    echo "formal/ has a TLA+ model but tla2tools.jar was not found" >&2
    echo "set TLA_JAR to the jar from https://github.com/tlaplus/tlaplus/releases" >&2
    exit 1
  fi
  if ! command -v java >/dev/null 2>&1; then
    echo "java is required to model-check formal/*.tla" >&2
    exit 1
  fi
  for cfg in "${configs[@]}"; do
    dir="$(dirname "${cfg}")"
    base="$(basename "${cfg}" .cfg)"
    tla="${dir}/${base}.tla"
    if [[ ! -f "${tla}" ]]; then
      echo "missing ${tla} for ${cfg}" >&2
      exit 1
    fi
    echo "TLC ${base}"
    java -XX:+UseParallelGC -jar "${jar}" -workers 2 -config "${cfg}" -deadlock "${tla}"
  done
fi

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
