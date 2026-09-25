#!/usr/bin/env bash
# Model-check every TLA+ config under formal/ with TLC.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "${root}"

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
    # -workers auto uses every core. -lncheck final checks liveness once, on the complete
    # state graph, instead of pausing the search for a partial check every few minutes.
    # Invariants and [][A]_v properties are still checked on every transition. TLC also
    # checks deadlock, and AGENTS.md forbids CHECK_DEADLOCK FALSE: a reported deadlock is a finding.
    java -XX:+UseParallelGC -jar "${jar}" -workers auto -lncheck final -config "${cfg}" "${tla}"
  done
fi
