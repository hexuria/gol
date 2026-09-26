#!/usr/bin/env bash
# Fail when workflow, protocol, or surface crates take a runtime or server dependency.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd -P)"
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
    "surface-core": {"harness", "server", "tokio", "axum"},
}
# crux_core is the surface's shell contract. No other workspace crate takes it directly.
crux_owner = "surface-core"

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

for crate, package_id in workspace.items():
    if crate == crux_owner:
        continue
    for dep in packages[package_id]["dependencies"]:
        if dep["name"] == "crux_core":
            print(f"{crate} depends on crux_core", file=sys.stderr)
            failed = True

if failed:
    sys.exit(1)
print("architecture guard ok")
PY

# Every crate root forbids unsafe code. workflow-bend denies it and allows it on kill_group alone.
# Crate roots come from cargo metadata, so a [lib] path or a nested bin cannot slip past.
python3 - "${meta}" "${root}" <<'PY'
import json
import os
import re
import sys

meta = json.load(open(sys.argv[1]))
# cargo metadata reports physical paths; compare against the physical root.
root = os.path.realpath(sys.argv[2])
members = set(meta["workspace_members"])
failed = False

crate_kinds = {"lib", "rlib", "dylib", "cdylib", "staticlib", "proc-macro", "bin"}
roots = sorted(
    os.path.relpath(target["src_path"], root)
    for package in meta["packages"]
    if package["id"] in members
    for target in package["targets"]
    if crate_kinds & set(target["kind"])
)
for crate_root in roots:
    want = "#![deny(unsafe_code)]" if crate_root == "crates/workflow-bend/src/lib.rs" else "#![forbid(unsafe_code)]"
    with open(os.path.join(root, crate_root)) as handle:
        if want not in (line.strip() for line in handle):
            print(f"{crate_root} lacks {want}", file=sys.stderr)
            failed = True

# Any attribute that lifts the lint: allow or expect, alone or in a list, inner or outer, or behind cfg_attr.
lift = re.compile(r"#!?\[[^\]]*\b(?:allow|expect)\s*\([^)]*\bunsafe_code\b")
found = []
for directory, _, files in os.walk(os.path.join(root, "crates")):
    for name in files:
        if name.endswith(".rs"):
            path = os.path.join(directory, name)
            with open(path, errors="replace") as handle:
                text = handle.read()
            # Scan the whole file: rustfmt wraps a long attribute list over several lines.
            for match in lift.finditer(text):
                number = text.count("\n", 0, match.start()) + 1
                attribute = " ".join(text[match.start():text.find("]", match.end()) + 1].split())
                found.append((os.path.relpath(path, root), number, attribute))
allowed = [("crates/workflow-bend/src/boundary.rs", "#[allow(unsafe_code)]")]
if [(path, text) for path, _, text in found] != allowed:
    print("unsafe_code may be lifted only by #[allow(unsafe_code)] on kill_group in "
          "crates/workflow-bend/src/boundary.rs; found:", file=sys.stderr)
    for path, number, text in found:
        print(f"  {path}:{number}: {text}", file=sys.stderr)
    failed = True

if failed:
    sys.exit(1)
print("unsafe guard ok")
PY

# The verification planner reads its trigger table from AGENTS.md; keep both in step.
"${root}/scripts/verify-plan.sh" --self-test
