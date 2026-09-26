#!/usr/bin/env bash
# Run the formal checks that already exist in the tree.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"

"${root}/scripts/verify-bend.sh"
"${root}/scripts/verify-tla.sh"
