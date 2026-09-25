use std::fs;
use std::path::Path;

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
