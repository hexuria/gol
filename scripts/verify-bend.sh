#!/usr/bin/env bash
# Check Bend syntax, types, laws, proofs, and Rust compatibility.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "${root}"
export BEND_NO_TELEMETRY=1

if [ -n "${BEND:-}" ]; then
  bend_bin="${BEND}"
elif command -v bend >/dev/null 2>&1; then
  bend_bin="$(command -v bend)"
elif [ -x "${HOME}/.bend/bin/bend" ]; then
  bend_bin="${HOME}/.bend/bin/bend"
else
  echo "bend 2.0.27 is not installed." >&2
  echo "curl -fsSL https://bend-lang.com/install.sh | sh" >&2
  exit 1
fi

if [ ! -x /usr/bin/unshare ]; then
  echo "unshare is required to run bend without network" >&2
  exit 1
fi

version="$(/usr/bin/unshare -r -n -- env -i BEND_NO_TELEMETRY=1 "${bend_bin}" version)"
if [ "${version}" != "bend 2.0.27" ]; then
  echo "expected bend 2.0.27, found: ${version}" >&2
  exit 1
fi

src="${root}/experiments/bend"
run_bend() {
  /usr/bin/unshare -r -n -- env -i BEND_NO_TELEMETRY=1 "${bend_bin}" "$@"
}

counter_check="$(run_bend "${src}/counter.bend" --check-only)"
if [ "${counter_check}" != "All terms check." ]; then
  echo "counter.bend failed the checker:" >&2
  printf '%s\n' "${counter_check}" >&2
  exit 1
fi

proof_check="$(run_bend "${src}/PROOF.bend" --check-only)"
if [ "${proof_check}" != "All terms check." ]; then
  echo "PROOF.bend failed:" >&2
  printf '%s\n' "${proof_check}" >&2
  exit 1
fi

encoding="$(run_bend "${src}/counter.bend")"
if [ "${encoding}" != '"v1 on_counter execute complete fail"' ]; then
  echo "counter encoding changed:" >&2
  printf '%s\n' "${encoding}" >&2
  exit 1
fi

cargo test -p workflow-bend
