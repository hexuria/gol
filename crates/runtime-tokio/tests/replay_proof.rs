use std::fs::{self, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use runtime_tokio::{frame, start_frame};
use workflow_core::Record;

/// The bytes of a journal holding `records`.
fn journal_bytes(records: &[Record]) -> Vec<u8> {
    let mut bytes = start_frame();
    for record in records {
        bytes.extend(frame(record).unwrap());
    }
    bytes
}

#[test]
fn kill_after_commit_skips_the_counter() {
    let dir = proof_dir("after-commit");
    let committed = journal_bytes(&[Record::counter(0)]);
    let (log, effect, next) = kill_when(&dir, &["after-commit"], |log, effect, _next| {
        log == committed.as_slice() && effect.len() == 1
    });
    assert_eq!(effect.len(), 1);
    assert_eq!(log, committed);
    assert_eq!(next.len(), 0);

    run_again(&dir);

    assert_eq!(read_file(&dir.join("effect")).len(), 1);
    assert_eq!(read_file(&dir.join("log")), committed);
    assert_eq!(read_file(&dir.join("next")).len(), 1);
}

#[test]
fn kill_before_commit_leaves_the_next_unstarted() {
    let dir = proof_dir("before-commit");
    let started = start_frame();
    let (log, effect, next) = kill_when(&dir, &["before-commit"], |log, effect, _next| {
        effect.len() == 1 && log == started.as_slice()
    });
    assert_eq!(effect.len(), 1);
    assert_eq!(log, started);
    assert_eq!(next.len(), 0);

    run_again(&dir);

    assert_eq!(read_file(&dir.join("effect")).len(), 2);
    assert_eq!(
        read_file(&dir.join("log")),
        journal_bytes(&[Record::counter(0)])
    );
    assert_eq!(read_file(&dir.join("next")).len(), 1);
}

// A commit killed part way leaves a torn record: here cut in its length, just
// after its kind, and one byte short of its checksum. The rerun cuts the torn
// record off and runs the counter again, so the effect runs at least once,
// as when the kill comes before the commit.
#[test]
fn kill_mid_record_runs_the_counter_again() {
    let record = frame(&Record::counter(0)).unwrap();
    for cut in [1, 5, record.len() - 1] {
        let dir = proof_dir(&format!("mid-record-{cut}"));
        let mut torn = start_frame();
        torn.extend_from_slice(&record[..cut]);
        let (log, effect, next) = kill_when(
            &dir,
            &["tear-at", &cut.to_string()],
            |log, effect, _next| log == torn.as_slice() && effect.len() == 1,
        );
        assert_eq!(effect.len(), 1, "cut at {cut}");
        assert_eq!(log, torn, "cut at {cut}");
        assert_eq!(next.len(), 0, "cut at {cut}");

        run_again(&dir);

        assert_eq!(read_file(&dir.join("effect")).len(), 2, "cut at {cut}");
        assert_eq!(
            read_file(&dir.join("log")),
            journal_bytes(&[Record::counter(0)]),
            "cut at {cut}"
        );
        assert_eq!(read_file(&dir.join("next")).len(), 1, "cut at {cut}");
    }
}

// A first open killed part way through its header leaves a torn `Start`
// frame. The rerun writes the header again and runs the workflow from the
// start.
#[test]
fn kill_mid_start_header_rewrites_it() {
    let start = start_frame();
    for cut in [1, start.len() - 1] {
        let dir = proof_dir(&format!("mid-start-{cut}"));
        let (log, effect, next) = kill_when(
            &dir,
            &["tear-start-at", &cut.to_string()],
            |log, _effect, _next| log == &start[..cut],
        );
        assert_eq!(log, &start[..cut], "cut at {cut}");
        assert_eq!(effect.len(), 0, "cut at {cut}");
        assert_eq!(next.len(), 0, "cut at {cut}");

        run_again(&dir);

        assert_eq!(read_file(&dir.join("effect")).len(), 1, "cut at {cut}");
        assert_eq!(
            read_file(&dir.join("log")),
            journal_bytes(&[Record::counter(0)]),
            "cut at {cut}"
        );
        assert_eq!(read_file(&dir.join("next")).len(), 1, "cut at {cut}");
    }
}

/// How long a child may take to reach its window or to finish. A first run
/// of a freshly linked binary can be slow (macOS scans it), so this is a
/// ceiling for a stuck child, not the expected time.
const DEADLINE: Duration = Duration::from_secs(30);

/// The agent the bin's spawning workflow starts.
const CHILD: &str = "00000000-0000-4000-8000-00000000c41d";

/// The request the bin's spawner records: parent, step, agent and input,
/// which together name the child (`RunSpecBuilder::child_of`).
const ASKED: &str =
    "00000000-0000-4000-8000-0000000000aa 0 00000000-0000-4000-8000-00000000c41d draft\n";

fn spawned() -> Record {
    Record::AgentSpawned {
        agent: CHILD.to_string(),
    }
}

// Phase 1.3: killed after the spawner started the child and before
// `AgentSpawned` is committed. The rerun asks the spawner again with the same
// request, so the same child (the owned spawner returns the one it stored,
// `a_redelivered_parent_starts_one_child`), and then commits once.
#[test]
fn kill_between_spawn_and_commit_asks_for_the_same_child() {
    let dir = proof_dir("spawn-before-commit");
    let started = start_frame();
    let (log, effect, next) = kill_when(&dir, &["spawn-before-commit"], |log, effect, _next| {
        effect == ASKED.as_bytes() && log == started.as_slice()
    });
    assert_eq!(effect, ASKED.as_bytes());
    assert_eq!(log, started);
    assert_eq!(next.len(), 0);

    run_again_as(&dir, "run-spawn");

    assert_eq!(read_file(&dir.join("effect")), ASKED.repeat(2).into_bytes());
    assert_eq!(read_file(&dir.join("log")), journal_bytes(&[spawned()]));
    assert_eq!(read_file(&dir.join("next")).len(), 1);
}

// Killed after `AgentSpawned` is committed: the rerun does not ask again.
#[test]
fn kill_after_spawn_commit_does_not_spawn_again() {
    let dir = proof_dir("spawn-after-commit");
    let committed = journal_bytes(&[spawned()]);
    let (log, effect, next) = kill_when(&dir, &["spawn-after-commit"], |log, effect, _next| {
        effect == ASKED.as_bytes() && log == committed.as_slice()
    });
    assert_eq!(effect, ASKED.as_bytes());
    assert_eq!(log, committed);
    assert_eq!(next.len(), 0);

    run_again_as(&dir, "run-spawn");

    assert_eq!(read_file(&dir.join("effect")), ASKED.as_bytes());
    assert_eq!(read_file(&dir.join("log")), committed);
    assert_eq!(read_file(&dir.join("next")).len(), 1);
}

fn proof_dir(case: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("gol-proof-{}-{}", std::process::id(), case));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn kill_when<F>(dir: &Path, mode: &[&str], ready: F) -> (Vec<u8>, Vec<u8>, Vec<u8>)
where
    F: Fn(&[u8], &[u8], &[u8]) -> bool,
{
    let log = dir.join("log");
    let effect = dir.join("effect");
    let next = dir.join("next");
    let mut child = Command::new(env!("CARGO_BIN_EXE_replay_proof"))
        .arg(&log)
        .arg(&effect)
        .arg(&next)
        .args(mode)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdin = child.stdin.take().unwrap();
    let deadline = Instant::now() + DEADLINE;
    loop {
        assert_running(&mut child);
        if let Some((log_bytes, effect_bytes, next_bytes)) = published(&log, &effect, &next) {
            if ready(&log_bytes, &effect_bytes, &next_bytes) {
                assert_running(&mut child);
                std::thread::sleep(Duration::from_millis(20));
                assert_running(&mut child);
                let (log_bytes, effect_bytes, next_bytes) =
                    published(&log, &effect, &next).unwrap();
                assert!(ready(&log_bytes, &effect_bytes, &next_bytes));
                break;
            }
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("child never published the window");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    child.kill().unwrap();
    let status = child.wait().unwrap();
    drop(stdin);
    assert!(!status.success());
    (read_file(&log), read_file(&effect), read_file(&next))
}

fn run_again(dir: &Path) {
    run_again_as(dir, "run");
}

fn run_again_as(dir: &Path, mode: &str) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_replay_proof"))
        .arg(dir.join("log"))
        .arg(dir.join("effect"))
        .arg(dir.join("next"))
        .arg(mode)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + DEADLINE;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            let mut err = String::new();
            if let Some(mut stderr) = child.stderr.take() {
                let _ = stderr.read_to_string(&mut err);
            }
            assert!(status.success(), "run failed: {status} {err}");
            return;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("run did not exit");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn assert_running(child: &mut Child) {
    match child.try_wait() {
        Ok(None) => {}
        Ok(Some(status)) => panic!("child exited before the window: {status}"),
        Err(err) => panic!("try_wait: {err}"),
    }
}

fn published(log: &Path, effect: &Path, next: &Path) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    Some((read_ok(log)?, read_ok(effect)?, read_ok(next)?))
}

fn read_ok(path: &Path) -> Option<Vec<u8>> {
    let mut file = OpenOptions::new().read(true).open(path).ok()?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).ok()?;
    Some(buf)
}

fn read_file(path: &Path) -> Vec<u8> {
    read_ok(path).unwrap()
}
