use std::fs::{self, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::FrontendError;

pub const BEND_VERSION: &str = "bend 2.0.27";
const SOURCE_LIMIT: u64 = 64 * 1024;
const STDOUT_LIMIT: usize = 4 * 1024;
const STDERR_LIMIT: usize = 16 * 1024;
const TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub struct Captured {
    pub status: ExitStatus,
    pub stdout: String,
    pub stderr: String,
}

pub struct Limits {
    pub timeout: Duration,
    pub stdout: usize,
    pub stderr: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            timeout: TIMEOUT,
            stdout: STDOUT_LIMIT,
            stderr: STDERR_LIMIT,
        }
    }
}

pub fn bend_program() -> Result<PathBuf, FrontendError> {
    if let Some(path) = std::env::var_os("BEND") {
        if !path.is_empty() {
            return Ok(PathBuf::from(path));
        }
    }
    if let Some(path) = search_path("bend") {
        return Ok(path);
    }
    if let Some(home) = std::env::var_os("HOME") {
        let candidate = PathBuf::from(home).join(".bend/bin/bend");
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(FrontendError::Setup(
        "bend 2.0.27 is not installed; curl -fsSL https://bend-lang.com/install.sh | sh"
            .to_string(),
    ))
}

fn unshare_program() -> Result<PathBuf, FrontendError> {
    let candidate = PathBuf::from("/usr/bin/unshare");
    if candidate.is_file() {
        return Ok(candidate);
    }
    search_path("unshare").ok_or_else(|| {
        FrontendError::Setup("unshare is required to run bend without network".to_string())
    })
}

fn search_path(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

pub struct Staged {
    dir: PathBuf,
}

impl Drop for Staged {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

pub fn stage(source: &Path) -> Result<Staged, FrontendError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!("gol-bend-{}-{nanos}", std::process::id()));
    fs::create_dir_all(&dir).map_err(|error| io_error("create", &dir, &error))?;
    let staged = Staged { dir };
    for name in ["counter.bend", "LAWS.bend", "PROOF.bend"] {
        let bytes = read_source(&source.join(name))?;
        let dest = staged.dir.join(name);
        fs::write(&dest, bytes).map_err(|error| io_error("write", &dest, &error))?;
    }
    let counter = fs::read_to_string(staged.dir.join("counter.bend"))
        .map_err(|error| io_error("read", &staged.dir.join("counter.bend"), &error))?;
    assert_string_main(&counter)?;
    Ok(staged)
}

fn read_source(path: &Path) -> Result<String, FrontendError> {
    let meta = fs::symlink_metadata(path).map_err(|error| io_error("read", path, &error))?;
    if !meta.file_type().is_file() {
        return Err(FrontendError::Setup(format!(
            "{} must be a regular file",
            path.display()
        )));
    }
    if meta.len() > SOURCE_LIMIT {
        return Err(FrontendError::OutputLimit);
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(|error| io_error("read", path, &error))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| io_error("read", path, &error))?;
    if bytes.len() as u64 > SOURCE_LIMIT {
        return Err(FrontendError::OutputLimit);
    }
    String::from_utf8(bytes)
        .map_err(|_| FrontendError::Encoding(format!("{} is not utf-8", path.display())))
}

fn assert_string_main(source: &str) -> Result<(), FrontendError> {
    let mut found = 0;
    for line in source.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("def main") {
            found += 1;
            if line != "def main() -> String:" {
                return Err(FrontendError::Encoding(
                    "counter.bend main must return String; IO is not executed".to_string(),
                ));
            }
        }
    }
    if found != 1 {
        return Err(FrontendError::Encoding(
            "counter.bend needs one def main() -> String:".to_string(),
        ));
    }
    Ok(())
}

pub fn check_version(bend: &Path, dir: &Path) -> Result<(), FrontendError> {
    let captured = run_sandboxed(bend, &["version"], dir, Limits::default())?;
    if !captured.status.success() {
        return Err(FrontendError::Version(failure_text(&captured)));
    }
    if captured.stdout != format!("{BEND_VERSION}\n") || !captured.stderr.is_empty() {
        return Err(FrontendError::Version(format!(
            "expected {BEND_VERSION}, found {}",
            captured.stdout.trim()
        )));
    }
    Ok(())
}

pub fn run_sandboxed(
    program: &Path,
    args: &[&str],
    dir: &Path,
    limits: Limits,
) -> Result<Captured, FrontendError> {
    let unshare = unshare_program()?;
    let mut command = Command::new(&unshare);
    command
        .arg("-r")
        .arg("-n")
        .arg("--")
        .arg(program)
        .args(args)
        .current_dir(dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_clear()
        .env("BEND_NO_TELEMETRY", "1")
        .process_group(0);
    let mut child = command
        .spawn()
        .map_err(|error| io_error("spawn", program, &error))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| FrontendError::Setup("bend stdout was not piped".to_string()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| FrontendError::Setup("bend stderr was not piped".to_string()))?;
    let flag = Arc::new(AtomicBool::new(false));
    let stdout_flag = Arc::clone(&flag);
    let stderr_flag = Arc::clone(&flag);
    let stdout_limit = limits.stdout;
    let stderr_limit = limits.stderr;
    let stdout_thread = thread::spawn(move || read_capped(stdout, stdout_limit, &stdout_flag));
    let stderr_thread = thread::spawn(move || read_capped(stderr, stderr_limit, &stderr_flag));
    let deadline = Instant::now() + limits.timeout;
    let status = loop {
        if flag.load(Ordering::Relaxed) {
            kill_group(&mut child);
            let _ = stdout_thread.join();
            let _ = stderr_thread.join();
            return Err(FrontendError::OutputLimit);
        }
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() >= deadline => {
                kill_group(&mut child);
                let _ = stdout_thread.join();
                let _ = stderr_thread.join();
                return Err(FrontendError::Timeout);
            }
            Ok(None) => thread::sleep(Duration::from_millis(5)),
            Err(error) => return Err(io_error("wait", program, &error)),
        }
    };
    let stdout = join_reader(stdout_thread)?;
    let stderr = join_reader(stderr_thread)?;
    if stdout.overflow || stderr.overflow {
        return Err(FrontendError::OutputLimit);
    }
    Ok(Captured {
        status,
        stdout: bytes_to_string(stdout.bytes)?,
        stderr: bytes_to_string(stderr.bytes)?,
    })
}

struct Capped {
    bytes: Vec<u8>,
    overflow: bool,
}

fn read_capped(mut pipe: impl Read, limit: usize, overflow: &AtomicBool) -> Capped {
    let mut bytes = Vec::new();
    let mut buf = [0u8; 1024];
    loop {
        match pipe.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                let room = limit.saturating_sub(bytes.len());
                if n > room {
                    bytes.extend_from_slice(&buf[..room]);
                    overflow.store(true, Ordering::Relaxed);
                    return Capped {
                        bytes,
                        overflow: true,
                    };
                }
                bytes.extend_from_slice(&buf[..n]);
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => break,
        }
    }
    Capped {
        bytes,
        overflow: false,
    }
}

fn join_reader(handle: thread::JoinHandle<Capped>) -> Result<Capped, FrontendError> {
    handle
        .join()
        .map_err(|_| FrontendError::Setup("bend output reader stopped".to_string()))
}

fn kill_group(child: &mut Child) {
    let pid = child.id() as i32;
    unsafe {
        libc::kill(-pid, libc::SIGKILL);
    }
    let _ = child.wait();
}

fn bytes_to_string(bytes: Vec<u8>) -> Result<String, FrontendError> {
    String::from_utf8(bytes)
        .map_err(|_| FrontendError::Encoding("bend output was not utf-8".to_string()))
}

pub fn failure_text(captured: &Captured) -> String {
    if !captured.stderr.is_empty() {
        return captured.stderr.trim().to_string();
    }
    if !captured.stdout.is_empty() {
        return captured.stdout.trim().to_string();
    }
    format!("bend exited {}", captured.status)
}

fn io_error(action: &str, path: &Path, error: &io::Error) -> FrontendError {
    FrontendError::Setup(format!("{action} {}: {error}", path.display()))
}

pub fn staged_dir(staged: &Staged) -> &Path {
    &staged.dir
}
