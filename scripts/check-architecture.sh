#!/usr/bin/env bash
# Fail when workflow or protocol crates take a runtime or server dependency.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "${root}"

meta="$(mktemp)"
trap 'rm -f "${meta}"' EXIT
cargo metadata --format-version 1 --all-features >"${meta}"

python3 - "${meta}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    meta = json.load(handle)

packages = {package["id"]: package for package in meta["packages"]}
nodes = {node["id"]: node for node in meta["resolve"]["nodes"]}
workspace = {
    packages[member]["name"]: member for member in meta["workspace_members"]
}

rules = {
    "workflow-core": {"server", "tokio", "axum"},
    "workflow-rhai": {"server", "tokio", "axum"},
    "workflow-js": {"server", "tokio", "axum"},
    "workflow-bend": {"server", "tokio", "axum"},
    "harness-core": {"tokio", "axum"},
    "protocol": {"tokio", "axum"},
}

def reachable(root_id):
    seen = set()
    stack = [root_id]
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        node = nodes.get(current)
        if node is None:
            continue
        for dep in node["deps"]:
            stack.append(dep["pkg"])
    return seen

failed = False
for crate, banned in rules.items():
    if crate not in workspace:
        print(f"missing workspace crate {crate}", file=sys.stderr)
        failed = True
        continue
    for package_id in reachable(workspace[crate]):
        if package_id == workspace[crate]:
            continue
        name = packages[package_id]["name"]
        if name in banned:
            print(f"{crate} depends on {name}", file=sys.stderr)
            failed = True

if failed:
    sys.exit(1)
print("architecture guard ok")
PY
