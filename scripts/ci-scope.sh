#!/usr/bin/env bash
# Print code=false when a pull request changes only docs, else code=true.
#
# CI jobs skip when this prints code=false. A job skipped by `if:` passes a
# required check, so the answer must be false only when it is certain: pushes,
# a checkout too shallow to diff, a failed diff, or an empty diff all print
# code=true. Run from a pull_request checkout, where HEAD is the merge commit
# and HEAD^1 is the base.
set -uo pipefail

# Paths that no build, test, or check reads. AGENTS.md is not one: the architecture job
# runs the verify-plan self-test against its trigger table.
docs_only='^(README\.md|docs/.*)$'

code=true
if [ "${EVENT:-}" = pull_request ] && git rev-parse --verify --quiet HEAD^1 >/dev/null; then
  if changed="$(git diff --name-only HEAD^1 HEAD)" && [ -n "${changed}" ] \
    && ! printf '%s\n' "${changed}" | grep -qvE "${docs_only}"; then
    code=false
    echo "Only docs changed:" >&2
    printf '%s\n' "${changed}" | sed 's/^/  /' >&2
  fi
fi
echo "code=${code}"
