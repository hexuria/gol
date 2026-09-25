use std::fs;
use std::path::Path;
use std::process::Command;

#[test]
fn surface_core_does_not_mint() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    rust_files(root, &mut files);
    let needles = forbidden_spellings();
    let mut hits = Vec::new();
    for path in files {
        let text = fs::read_to_string(&path).unwrap_or_else(|err| {
            panic!("read {}: {err}", path.display());
        });
        for needle in &needles {
            if text.contains(needle) {
                hits.push(format!("{} contains {needle}", path.display()));
            }
        }
    }
    assert!(
        hits.is_empty(),
        "surface-core mints an id:\n{}",
        hits.join("\n")
    );
}

fn forbidden_spellings() -> Vec<String> {
    let mut needles = Vec::new();
    for name in [
        "RunId",
        "EventId",
        "AgentId",
        "StepId",
        "ArtifactId",
        "ToolId",
        "ApprovalId",
        "InvocationId",
    ] {
        needles.push(format!("{name}::{}", "new"));
    }
    for name in [
        "RunId",
        "AgentId",
        "StepId",
        "EventId",
        "ArtifactId",
        "ToolId",
        "ApprovalId",
        "InvocationId",
    ] {
        needles.push(format!("{name}::{}", "default"));
    }
    needles.push(format!("{}::{}", "Timestamp", "now"));
    needles.push(format!("{}::{}", "SystemTime", "now"));
    needles.push(format!("{}::{}", "Uuid", "new_v4"));
    needles.push(format!("{}::{}", "Event", "record"));
    needles.push(format!("{}::{}", "RunSpecBuilder", "build"));
    needles
}

fn rust_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    let entries = fs::read_dir(dir).unwrap_or_else(|err| panic!("read {}: {err}", dir.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|err| panic!("dir entry: {err}"));
        let path = entry.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name == "target" {
            continue;
        }
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn surface_core_metadata_walk() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace = manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let mut command = Command::new(cargo);
    command
        .args(["metadata", "--format-version", "1"])
        .current_dir(workspace);
    clear_cargo_package_env(&mut command);
    let output = command.output().expect("spawn cargo metadata");
    assert!(
        output.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let mut walk = Command::new("python3");
    walk.arg("-c").arg(WALK);
    walk.stdin(std::process::Stdio::piped());
    walk.stdout(std::process::Stdio::piped());
    walk.stderr(std::process::Stdio::piped());
    let mut child = walk.spawn().expect("spawn metadata walk");
    {
        use std::io::Write;
        let stdin = child.stdin.as_mut().expect("walk stdin");
        stdin
            .write_all(&output.stdout)
            .expect("write metadata to walk");
    }
    let walked = child.wait_with_output().expect("walk output");
    assert!(
        walked.status.success(),
        "metadata walk failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&walked.stdout),
        String::from_utf8_lossy(&walked.stderr)
    );
}

fn clear_cargo_package_env(command: &mut Command) {
    for (key, _) in std::env::vars() {
        if key.starts_with("CARGO_PKG_")
            || key.starts_with("CARGO_BIN_")
            || key == "CARGO_MANIFEST_DIR"
            || key == "CARGO_MANIFEST_PATH"
            || key == "CARGO_CRATE_NAME"
            || key == "CARGO_PRIMARY_PACKAGE"
        {
            command.env_remove(key);
        }
    }
}

const WALK: &str = r#"
import json
import sys

BANNED = {"harness", "server", "tokio", "axum"}

def reachable(nodes, root_id):
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

def evaluate(meta):
    packages = {package["id"]: package for package in meta["packages"]}
    nodes = {node["id"]: node for node in meta["resolve"]["nodes"]}
    workspace = {
        packages[member]["name"]: member for member in meta["workspace_members"]
    }
    errors = []
    root_name = "surface-core"
    if root_name not in workspace:
        errors.append("missing workspace crate surface-core")
        return errors
    for package_id in reachable(nodes, workspace[root_name]):
        if package_id == workspace[root_name]:
            continue
        name = packages[package_id]["name"]
        if name in BANNED:
            errors.append(f"surface-core depends on {name}")
    for name, package_id in workspace.items():
        if name == root_name:
            continue
        for dep in packages[package_id]["dependencies"]:
            if dep["name"] == "crux_core":
                errors.append(f"{name} depends on crux_core")
    return errors

def pkg(ident, name, deps):
    return {"id": ident, "name": name, "dependencies": [{"name": dep} for dep in deps]}

def node(ident, deps):
    return {
        "id": ident,
        "deps": [{"name": dep.split()[0], "pkg": dep} for dep in deps],
    }

def meta(packages, members, nodes):
    return {
        "packages": packages,
        "workspace_members": members,
        "resolve": {"nodes": nodes},
    }

tokio_reach = meta(
    [
        pkg("surface-core 0.1.0", "surface-core", ["crux_core"]),
        pkg("crux_core 0.20.0", "crux_core", []),
        pkg("tokio 1.0.0", "tokio", []),
        pkg("protocol 0.1.0", "protocol", []),
    ],
    ["surface-core 0.1.0", "protocol 0.1.0"],
    [
        node("surface-core 0.1.0", ["crux_core 0.20.0", "tokio 1.0.0"]),
        node("crux_core 0.20.0", []),
        node("tokio 1.0.0", []),
        node("protocol 0.1.0", []),
    ],
)
other_crux = meta(
    [
        pkg("surface-core 0.1.0", "surface-core", ["crux_core"]),
        pkg("crux_core 0.20.0", "crux_core", []),
        pkg("protocol 0.1.0", "protocol", ["crux_core"]),
    ],
    ["surface-core 0.1.0", "protocol 0.1.0"],
    [
        node("surface-core 0.1.0", ["crux_core 0.20.0"]),
        node("crux_core 0.20.0", []),
        node("protocol 0.1.0", ["crux_core 0.20.0"]),
    ],
)
clean = meta(
    [
        pkg("surface-core 0.1.0", "surface-core", ["crux_core"]),
        pkg("crux_core 0.20.0", "crux_core", ["serde"]),
        pkg("serde 1.0.0", "serde", []),
        pkg("protocol 0.1.0", "protocol", ["uuid"]),
        pkg("uuid 1.0.0", "uuid", []),
    ],
    ["surface-core 0.1.0", "protocol 0.1.0"],
    [
        node("surface-core 0.1.0", ["crux_core 0.20.0"]),
        node("crux_core 0.20.0", ["serde 1.0.0"]),
        node("serde 1.0.0", []),
        node("protocol 0.1.0", ["uuid 1.0.0"]),
        node("uuid 1.0.0", []),
    ],
)

checks = [
    (tokio_reach, True, "tokio reach"),
    (other_crux, True, "other crate crux_core"),
    (clean, False, "clean resolve"),
]
for fixture, should_fail, label in checks:
    errors = evaluate(fixture)
    failed = bool(errors)
    if failed != should_fail:
        print(f"fixture {label} expected fail={should_fail} got {errors}", file=sys.stderr)
        sys.exit(2)

errors = evaluate(json.load(sys.stdin))
if errors:
    print("\n".join(errors), file=sys.stderr)
    sys.exit(1)
print("surface-core metadata walk ok")
"#;
