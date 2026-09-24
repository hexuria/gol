use std::fs::{self, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

#[test]
fn kill_after_commit_skips_the_counter() {
    let dir = proof_dir("after-commit");
    let zeros = 0i64.to_le_bytes();
    let (log, effect, next) = kill_when(&dir, "after-commit", |log, effect, _next| {
        log == zeros.as_slice() && effect.len() == 1
    });
    assert_eq!(effect.len(), 1);
    assert_eq!(log, zeros);
    assert_eq!(next.len(), 0);

    run_again(&dir);

    assert_eq!(read_file(&dir.join("effect")).len(), 1);
    assert_eq!(read_file(&dir.join("log")), zeros);
    assert_eq!(read_file(&dir.join("next")).len(), 1);
}

#[test]
fn kill_before_commit_leaves_the_next_unstarted() {
    let dir = proof_dir("before-commit");
    let (log, effect, next) = kill_when(&dir, "before-commit", |log, effect, _next| {
        effect.len() == 1 && log.is_empty()
    });
    assert_eq!(effect.len(), 1);
    assert_eq!(log.len(), 0);
    assert_eq!(next.len(), 0);

    run_again(&dir);

    assert_eq!(read_file(&dir.join("effect")).len(), 2);
    assert_eq!(read_file(&dir.join("log")), 0i64.to_le_bytes());
    assert_eq!(read_file(&dir.join("next")).len(), 1);
}

fn proof_dir(case: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("gol-proof-{}-{}", std::process::id(), case));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn kill_when<F>(dir: &Path, mode: &str, ready: F) -> (Vec<u8>, Vec<u8>, Vec<u8>)
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
        .arg(mode)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdin = child.stdin.take().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
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
    let mut child = Command::new(env!("CARGO_BIN_EXE_replay_proof"))
        .arg(dir.join("log"))
        .arg(dir.join("effect"))
        .arg(dir.join("next"))
        .arg("run")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
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
