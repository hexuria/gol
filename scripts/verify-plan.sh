#!/usr/bin/env bash
# Print the verification a change needs, from the trigger table in AGENTS.md.
#
#   scripts/verify-plan.sh [base]     diff the working tree against merge-base(base, HEAD); base defaults to origin/main
#   scripts/verify-plan.sh --self-test
#
# VERIFY_PLAN_TABLE_REF=<ref> reads the table from that commit instead of the working tree, so a
# change cannot relax the rules it is judged by. A ref whose AGENTS.md has no table falls back to the
# working tree. --self-test always reads the working tree, where a new row and its case land together.
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


def split_outside_ticks(text, sep):
    """Split on `sep` outside backticks, so a regex may contain it."""
    parts, current, quoted = [], [], False
    for c in text:
        if c == "`":
            quoted = not quoted
        if c == sep and not quoted:
            parts.append("".join(current))
            current = []
        else:
            current.append(c)
    parts.append("".join(current))
    return parts


def parse_rule(cell):
    """Clauses are separated by `;`: always | claim | paths: | lines[±-] [in `glob`]:.

    `lines:` matches added lines, `lines-:` removed lines, `lines±:` both.
    """
    rule = {"always": False, "claim": False, "paths": [], "lines": []}
    for clause in [c.strip() for c in split_outside_ticks(cell, ";") if c.strip()]:
        if clause == "always":
            rule["always"] = True
        elif clause.startswith("claim"):
            rule["claim"] = True
        elif clause.startswith("paths:"):
            rule["paths"] += [glob_regex(g) for g in ticks(clause[len("paths:"):])]
        else:
            m = re.match(r"lines([±-])?(?: in `([^`]+)`)?:(.*)$", clause)
            if not m:
                raise SystemExit(f"verify-plan: cannot read clause: {clause}")
            scope = glob_regex(m.group(2) or DEFAULT_LINE_SCOPE)
            side = {None: "added", "-": "removed", "±": "both"}[m.group(1)]
            for pattern in ticks(m.group(3)):
                rule["lines"].append((re.compile(pattern), scope, side))
    return rule


def load_table(text):
    if BEGIN not in text or END not in text:
        raise SystemExit("verify-plan: AGENTS.md has no verify-plan table")
    rows = []
    for line in text.split(BEGIN, 1)[1].split(END, 1)[0].splitlines():
        # GFM escapes a pipe inside a cell as \|; split only on unescaped pipes.
        cells = [c.strip().replace("\\|", "|") for c in re.split(r"(?<!\\)\|", line.strip().strip("|"))]
        if not re.fullmatch(r"T\d+", cells[0]):
            continue
        if len(cells) != 3:
            raise SystemExit(f"verify-plan: row {cells[0]} has {len(cells)} cells, not 3: {line.strip()}")
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
            for pattern, scope, side in rule["lines"]:
                lines = {"added": added, "removed": removed, "both": added + removed}[side]
                if scope.match(path) and any(pattern.search(l) for l in lines):
                    hit = True
                    break
        if hit:
            hits.append(row_id)
    return hits


# Diff output must not depend on the user's git config (color, external diff tools).
DIFF = ("diff", "--no-color", "--no-ext-diff", "--no-renames")


def git(*args):
    out = subprocess.run(["git", *args], check=True, capture_output=True).stdout
    return out.decode("utf-8", errors="replace")


def hunk_lines(merge_base, path):
    """Added and removed lines of one file. Each patch's headers end at its first @@, so a
    content line that starts with -- or ++ is still content. A type change (symlink to file)
    prints two patches, and the second one's headers are skipped the same way."""
    added, removed, in_hunk = [], [], False
    for line in git(*DIFF, "-U0", merge_base, "--", f":(literal){path}").splitlines():
        if line.startswith("diff --git "):
            in_hunk = False
        elif line.startswith("@@"):
            in_hunk = True
        elif in_hunk and line.startswith("+"):
            added.append(line[1:])
        elif in_hunk and line.startswith("-"):
            removed.append(line[1:])
    return added, removed


def diff_changes(base):
    """{path: (added_lines, removed_lines)} for the working tree against merge-base(base, HEAD).
    Paths come from -z listings, so spaces, " b/" and non-ASCII names stay intact; content that
    is not UTF-8 is decoded with replacement."""
    try:
        merge_base = git("merge-base", base, "HEAD").strip()
    except subprocess.CalledProcessError:
        raise SystemExit(f"verify-plan: no merge base between {base} and HEAD; run git fetch origin")
    changes = {}
    for path in filter(None, git(*DIFF, "--name-only", "-z", merge_base).split("\0")):
        changes[path] = hunk_lines(merge_base, path)
    for new in filter(None, git("ls-files", "--others", "--exclude-standard", "-z").split("\0")):
        lines = []
        if os.path.isfile(new):
            with open(new, "rb") as handle:
                lines = handle.read().decode("utf-8", errors="replace").splitlines()
        # A tracked file removed from the index but kept on disk keeps its removed lines.
        changes[new] = (lines, changes.get(new, ([], []))[1])
    return changes


def table_text(for_self_test=False):
    ref = os.environ.get("VERIFY_PLAN_TABLE_REF")
    if ref and not for_self_test:
        try:
            text = git("show", f"{ref}:AGENTS.md")
        except subprocess.CalledProcessError:
            text = ""
        if BEGIN in text and END in text:
            return text
        print(f"verify-plan: {ref}:AGENTS.md has no table; using the working tree", file=sys.stderr)
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
    ("a sql update in memory", {"crates/memory/src/lib.rs": (['    "UPDATE memories SET value = $1 WHERE key = $2"'], [])}, {"T0", "T3"}),
    ("a sql delete in memory", {"crates/memory/src/lib.rs": (['    "DELETE FROM memories WHERE key = $1"'], [])}, {"T0", "T3"}),
    ("a redis producer", {"crates/execution/src/queue.rs": (['    let _: () = conn.lpush("runs", id)?;', '    cmd("XADD").arg("s")'], [])}, {"T0", "T3"}),
    ("remove a sql comment line", {"crates/memory/src/lib.rs": ([], ["-- on conflict do nothing"])}, {"T0", "T3"}),
    ("delete a bounded test", {"crates/protocol/tests/reduce_bounded.rs": ([], ["#[test]", "fn dispatch_reduce_matches_the_table_on_every_pair() {"])}, {"T0", "T12"}),
    ("add a test", {"crates/protocol/tests/probe.rs": (["#[test]", "fn probe() {}"], [])}, {"T0"}),
    ("fewer proptest cases", {"crates/protocol/src/reduce.rs": (["    #![proptest_config(ProptestConfig { cases: 4, ..ProptestConfig::default() })]"], [])}, {"T0", "T1", "T12"}),
    ("tests off for a crate", {"crates/memory/Cargo.toml": (["test = false"], [])}, {"T0", "T12"}),
    ("ignore behind a cfg_attr", {"crates/server/tests/pg_redis.rs": (['#[cfg_attr(not(feature = "pg"), ignore)]'], [])}, {"T0", "T12"}),
    ("autotests off", {"crates/memory/Cargo.toml": (["autotests = false"], [])}, {"T0", "T12"}),
    ("a serde attribute", {"crates/protocol/src/event.rs": (['#[serde(rename = "ignored_field")]'], [])}, {"T0", "T1"}),
    ("toolchain pin", {"rust-toolchain.toml": (['channel = "1.80"'], [])}, {"T0", "T12"}),
    ("ci scope", {"scripts/ci-scope.sh": (["docs_only='^.*$'"], [])}, {"T0", "T12"}),
]

TABLE_PARSE_CASES = [
    ("escaped pipe in a regex", "| T1 | lines: `a\\|b` | x |", {"crates/x/src/y.rs": (["b"], [])}, True),
    ("semicolon in a regex", "| T1 | lines: `a;b` | x |", {"crates/x/src/y.rs": (["a;b"], [])}, True),
    ("removed-only clause ignores additions", "| T1 | lines-: `gone` | x |", {"crates/x/src/y.rs": (["gone"], [])}, False),
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
    for label, row, changes, fires in TABLE_PARSE_CASES:
        got = bool(fired(load_table(f"{BEGIN}\n{row}\n{END}"), changes))
        if got != fires:
            failed += 1
            print(f"self-test {label}: expected fired={fires}, got {got}", file=sys.stderr)
    try:
        load_table(f"{BEGIN}\n| T1 | lines: `a|b` | x |\n{END}")
        failed += 1
        print("self-test: a row with an unescaped pipe was not rejected", file=sys.stderr)
    except SystemExit:
        pass
    failed += diff_self_test()
    if failed:
        sys.exit(1)
    print(f"verify-plan self-test ok ({len(SELF_TEST) + len(TABLE_PARSE_CASES) + 2} cases)")


def diff_self_test():
    """diff_changes on a scratch repo with awkward paths and bytes."""
    import tempfile
    here = os.getcwd()
    # Isolate the scratch repo from the caller: a hook's GIT_INDEX_FILE or GIT_DIR, commit
    # signing and global hooks must not reach it, and it must not reach the caller's repo.
    saved = {k: v for k, v in os.environ.items() if k.startswith("GIT_")}
    for key in saved:
        del os.environ[key]
    with tempfile.TemporaryDirectory() as tmp:
        os.chdir(tmp)
        try:
            def run(*args):
                subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t",
                                "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null", *args],
                               check=True, capture_output=True)
            run("init", "-q")
            os.makedirs("docs/x b/formal")
            with open("docs/x b/formal/M.tla", "w") as f:
                f.write("a\n")
            with open("café.rs", "w") as f:
                f.write("a\n")
            with open("bytes.rs", "wb") as f:
                f.write(b"a\n")
            run("add", "-A")
            run("commit", "-q", "-m", "base")
            with open("docs/x b/formal/M.tla", "w") as f:
                f.write("b\n")
            with open("café.rs", "w") as f:
                f.write("-- on conflict\n")
            with open("bytes.rs", "wb") as f:
                f.write(b"\xff\xfe spawn(\n")
            os.symlink("missing", "dangling")
            os.symlink("bytes.rs", "link.rs")
            run("add", "link.rs")
            run("commit", "-q", "-m", "link")
            os.remove("link.rs")
            with open("link.rs", "w") as f:
                f.write("now a file\n")
            changes = diff_changes("HEAD")
        finally:
            os.chdir(here)
            os.environ.update(saved)
    want = {
        "docs/x b/formal/M.tla": (["b"], ["a"]),
        "café.rs": (["-- on conflict"], ["a"]),
        "bytes.rs": (["\ufffd\ufffd spawn("], ["a"]),
        "dangling": ([], []),
        "link.rs": (["now a file"], ["bytes.rs"]),
    }
    if changes != want:
        print(f"self-test diff parsing: expected {want}, got {changes}", file=sys.stderr)
        return 1
    return 0


def main(argv):
    if argv[:1] == ["--self-test"]:
        self_test(load_table(table_text(for_self_test=True)))
        return
    rows = load_table(table_text())
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
