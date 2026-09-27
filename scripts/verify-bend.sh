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
  echo "bend 2.0.28 is not installed." >&2
  echo "run ./scripts/install-bend.sh" >&2
  exit 1
fi

if [ ! -x /usr/bin/unshare ]; then
  echo "unshare is required to run bend without network" >&2
  exit 1
fi

# Prefer a user namespace so an unprivileged runner can create a network
# namespace. If writing /proc/self/uid_map fails, keep -n when the kernel
# allows a network namespace. Never run bend on the host network.
if /usr/bin/unshare -r -n -- /bin/true >/dev/null 2>&1; then
  unshare_net=(-r -n)
elif /usr/bin/unshare -n -- /bin/true >/dev/null 2>&1; then
  unshare_net=(-n)
else
  echo "unshare cannot create a network namespace (writing /proc/self/uid_map failed and unshare -n was rejected)" >&2
  exit 1
fi

version="$(/usr/bin/unshare "${unshare_net[@]}" -- env -i BEND_NO_TELEMETRY=1 "${bend_bin}" version)"
if [ "${version}" != "bend 2.0.28" ]; then
  echo "expected bend 2.0.28, found: ${version}" >&2
  exit 1
fi

src="${root}/experiments/bend"
run_bend() {
  /usr/bin/unshare "${unshare_net[@]}" -- env -i BEND_NO_TELEMETRY=1 "${bend_bin}" "$@"
}

for file in workflow.bend evals.bend; do
  file_check="$(run_bend "${src}/${file}" --check-only)"
  if [ "${file_check}" != "All terms check." ]; then
    echo "${file} failed the checker:" >&2
    printf '%s\n' "${file_check}" >&2
    exit 1
  fi
done

proof_check="$(run_bend "${src}/PROOF.bend" --check-only)"
if [ "${proof_check}" != "All terms check." ]; then
  echo "PROOF.bend failed:" >&2
  printf '%s\n' "${proof_check}" >&2
  exit 1
fi

encoding="$(run_bend "${src}/workflow.bend")"
if [ "${encoding}" != '"v2 tool.736561726368.71 tool.636f756e746572. spawn.68656c706572.7a65726f complete end seq seq fail on_counter end seq seq"' ]; then
  echo "workflow encoding changed:" >&2
  printf '%s\n' "${encoding}" >&2
  exit 1
fi

cargo test -p workflow-bend
