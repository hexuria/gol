#!/usr/bin/env bash
# Print the verification a change needs, from the trigger table in AGENTS.md.
#
#   scripts/verify-plan.sh [base]     diff the working tree against merge-base(base, HEAD); base defaults to origin/main
#   scripts/verify-plan.sh --self-test
#
# VERIFY_PLAN_TABLE_REF=<ref> reads the table from that commit instead of the working tree, so a
# change cannot relax the rules it is judged by (CI passes the base commit).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "${root}"

python3 - "$@" <<'PY'
import os
import re
import subprocess
import sys

BEGIN, END = "<!-- verify-plan:begin -->", "<!-- verify-plan:end -->"
DEFAULT_LINE_SCOPE = "crates/**"
FLAGS = {"T3", "T9"}


def glob_regex(glob):
    """`**` spans directories, `*` and `?` stay inside one segment, `{a,b}` picks one."""
    out, i = [], 0
    while i < len(glob):
        c = glob[i]
        if glob.startswith("**/", i):
            out.append("(?:.*/)?")
            i += 3
        elif glob.startswith("**", i):
            out.append(".*")
            i += 2
        elif c == "*":
            out.append("[^/]*")
            i += 1
        elif c == "?":
            out.append("[^/]")
            i += 1
        elif c == "{":
            close = glob.index("}", i)
            out.append("(?:" + "|".join(re.escape(a) for a in glob[i + 1:close].split(",")) + ")")
            i = close + 1
        else:
            out.append(re.escape(c))
            i += 1
    return re.compile("^" + "".join(out) + "$")


def ticks(text):
    return re.findall(r"`([^`]+)`", text)


def parse_rule(cell):
    """Clauses are separated by `;`: always | paths: | lines: | lines±: | lines [±] in `glob`: | claim."""
    rule = {"always": False, "claim": False, "paths": [], "lines": []}
    for clause in [c.strip() for c in cell.split(";") if c.strip()]:
        if clause == "always":
            rule["always"] = True
        elif clause.startswith("claim"):
            rule["claim"] = True
        elif clause.startswith("paths:"):
            rule["paths"] += [glob_regex(g) for g in ticks(clause[len("paths:"):])]
        else:
            m = re.match(r"lines(±)?(?: in `([^`]+)`)?:(.*)$", clause)
            if not m:
                raise SystemExit(f"verify-plan: cannot read clause: {clause}")
            scope = glob_regex(m.group(2) or DEFAULT_LINE_SCOPE)
            for pattern in ticks(m.group(3)):
                rule["lines"].append((re.compile(pattern), scope, bool(m.group(1))))
    return rule


def load_table(text):
    if BEGIN not in text or END not in text:
        raise SystemExit("verify-plan: AGENTS.md has no verify-plan table")
    rows = []
    for line in text.split(BEGIN, 1)[1].split(END, 1)[0].splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) != 3 or not re.fullmatch(r"T\d+", cells[0]):
            continue
        rows.append((cells[0], parse_rule(cells[1]), cells[2]))
    if not rows:
        raise SystemExit("verify-plan: the verify-plan table is empty")
    return rows


def fired(rows, changes):
    """changes: {path: (added_lines, removed_lines)}."""
    hits = []
    for row_id, rule, _ in rows:
        hit = rule["always"]
        for path, (added, removed) in changes.items():
            if hit:
                break
            if any(p.match(path) for p in rule["paths"]):
                hit = True
                break
            for pattern, scope, both in rule["lines"]:
                if scope.match(path) and any(
                    pattern.search(l) for l in (added + removed if both else added)
                ):
                    hit = True
                    break
        if hit:
            hits.append(row_id)
    return hits


def git(*args):
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def diff_changes(base):
    merge_base = git("merge-base", base, "HEAD").strip()
    changes = {}
    path = None
    for line in git("diff", "--no-renames", "-U0", merge_base).splitlines():
        if line.startswith("diff --git "):
            path = line.split(" b/", 1)[1]
            changes.setdefault(path, ([], []))
        elif line.startswith("+++ ") or line.startswith("--- "):
            continue
        elif path and line.startswith("+"):
            changes[path][0].append(line[1:])
        elif path and line.startswith("-"):
            changes[path][1].append(line[1:])
    for new in git("ls-files", "--others", "--exclude-standard").splitlines():
        with open(new, errors="replace") as handle:
            changes[new] = (handle.read().splitlines(), [])
    return changes


def table_text():
    ref = os.environ.get("VERIFY_PLAN_TABLE_REF")
    if ref:
        return git("show", f"{ref}:AGENTS.md")
    with open("AGENTS.md") as handle:
        return handle.read()


SELF_TEST = [
    ("rename a local in reduce", {"crates/protocol/src/reduce.rs": (["    let next_state = x;"], ["    let next = x;"])}, {"T0", "T1"}),
    ("MAX_RETRIES 2 -> 3", {"crates/protocol/src/phase.rs": (["pub const MAX_RETRIES: u32 = 3;"], ["pub const MAX_RETRIES: u32 = 2;"])}, {"T0", "T1"}),
    ("read-only GET route", {"crates/server/src/http.rs": (['    .route("/v1/runs/{id}/state", get(get_state))'], [])}, {"T0", "T2"}),
    ("new append in a helper crate", {"crates/memory/src/lib.rs": (["    store.append_events(id, events);"], [])}, {"T0", "T2"}),
    ("delete a sandbox destroy", {"crates/server/src/sandbox.rs": ([], ["        sandbox.destroy(&name)?;"])}, {"T0", "T2"}),
    ("redis consumer with a lease", {"crates/execution/src/queue.rs": (['    let id: Option<String> = cmd("BLMOVE").query(conn)?;', "    const LEASE_TTL: u64 = 30;"], [])}, {"T0", "T3"}),
    ("journal commit order", {"crates/runtime-tokio/src/journal.rs": (["    file.write_all(&record)?;"], [])}, {"T0", "T4"}),
    ("unsafe in the bend scanner", {"crates/workflow-bend/src/boundary.rs": (["    let b = unsafe { *bytes.get_unchecked(i) };"], [])}, {"T0", "T6", "T7", "T9"}),
    ("atomic cancel flag in the driver", {"crates/harness/src/driver.rs": (["    cancel: AtomicBool,"], [])}, {"T0", "T1", "T8"}),
    ("a method named is_terminal", {"crates/protocol/tests/probe.rs": (["    if state.is_terminal() {"], [])}, {"T0"}),
    ("cargo update", {"Cargo.lock": (['version = "1.0.229"'], ['version = "1.0.228"'])}, {"T0"}),
    ("docs only", {"docs/architecture.md": (["A new sentence."], [])}, {"T0"}),
    ("edit the policy", {"AGENTS.md": (["| T13 | always | nothing |"], [])}, {"T0", "T12"}),
    ("ignore a test", {"crates/server/tests/inference.rs": (["#[ignore]"], [])}, {"T0", "T12"}),
    ("change a TLA+ model", {"formal/runlog/RunLog.tla": (["Next == Done"], [])}, {"T0", "T10"}),
    ("a benchmark", {"crates/protocol/benches/fold_reduce.rs": (["c.bench_function(\"fold\", |b| b.iter(f));"], [])}, {"T0", "T11"}),
    ("a rhai frontend construct", {"crates/workflow-rhai/src/lib.rs": (["    Decision::Wait => \"wait\","], [])}, {"T0", "T5"}),
]


def self_test(rows):
    failed = 0
    for label, changes, expected in SELF_TEST:
        got = set(fired(rows, changes))
        if got != expected:
            failed += 1
            print(f"self-test {label}: expected {sorted(expected)}, got {sorted(got)}", file=sys.stderr)
    known = {row_id for row_id, _, _ in rows}
    for _, _, expected in SELF_TEST:
        missing = expected - known
        if missing:
            failed += 1
            print(f"self-test names rows the table lacks: {sorted(missing)}", file=sys.stderr)
    if failed:
        sys.exit(1)
    print(f"verify-plan self-test ok ({len(SELF_TEST)} cases)")


def main(argv):
    rows = load_table(table_text())
    if argv[:1] == ["--self-test"]:
        self_test(rows)
        return
    base = argv[0] if argv else "origin/main"
    changes = diff_changes(base)
    hits = fired(rows, changes)
    required = {row_id: text for row_id, _, text in rows}
    print("Triggers: " + " ".join(hits))
    for row_id in hits:
        tag = " (flag: answer yes or no with a reason)" if row_id in FLAGS else ""
        print(f"{row_id}{tag}: {required[row_id]}")
    unrouted = sorted({p.split("/")[1] for p in changes if p.startswith("crates/") and p.count("/") >= 2}
                      - {"protocol", "harness", "harness-core", "server", "execution", "runtime-tokio",
                         "workflow-core", "workflow-rhai", "workflow-js", "workflow-bend", "gateway",
                         "proxy", "memory", "surface-core"})
    if unrouted:
        print("Crates no row knows about: " + ", ".join(unrouted) + ". Add them to the gol-verify ownership map.")


main(sys.argv[1:])
PY
