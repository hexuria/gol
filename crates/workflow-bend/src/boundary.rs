use std::fs::{self, OpenOptions};
use std::io::{self, Read};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
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

/// Prefer `unshare -r -n` so an unprivileged process can create a network
/// namespace. If writing `/proc/self/uid_map` fails, keep `-n` when the kernel
/// still allows a network namespace. Never fall back to the host network.
fn network_unshare_args(unshare: &Path) -> Result<&'static [&'static str], FrontendError> {
    static ARGS: OnceLock<Result<&'static [&'static str], String>> = OnceLock::new();
    match ARGS.get_or_init(|| {
        if unshare_ok(unshare, &["-r", "-n"]) {
            Ok(&["-r", "-n"][..])
        } else if unshare_ok(unshare, &["-n"]) {
            Ok(&["-n"][..])
        } else {
            Err(
                "unshare cannot create a network namespace (writing /proc/self/uid_map failed and unshare -n was rejected)"
                    .to_string(),
            )
        }
    }) {
        Ok(args) => Ok(*args),
        Err(message) => Err(FrontendError::Setup(message.clone())),
    }
}

fn unshare_ok(unshare: &Path, args: &[&str]) -> bool {
    Command::new(unshare)
        .args(args)
        .arg("--")
        .arg("/bin/true")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
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
    let mut scan = BendScan { src: source, i: 0 };
    let mut mains = 0usize;
    while scan.i < scan.src.len() {
        scan.skip();
        if scan.i >= scan.src.len() {
            break;
        }
        if scan.at_word("def") {
            scan.i += 3;
            if scan.def_name_is_main() {
                mains += 1;
                if !scan.main_returns_string()? {
                    return Err(main_must_be_string());
                }
            }
            continue;
        }
        if scan.at(b'"') {
            scan.skip_string()?;
            continue;
        }
        if scan.at(b'\'') {
            scan.skip_char_lit()?;
            continue;
        }
        scan.bump_char()?;
    }
    if mains != 1 {
        return Err(FrontendError::Encoding(
            "counter.bend needs one def main() -> String:".to_string(),
        ));
    }
    Ok(())
}

fn main_must_be_string() -> FrontendError {
    FrontendError::Encoding("counter.bend main must return String; IO is not executed".to_string())
}

// Bend 2.0.27 lexing for the main guard. Whitespace is space, tab, `\n`, and a
// bare `\r`. A `#` comment runs to the next `\n` only. A `"` string, including
// one that spans lines, hides its text. Escapes match the compiler.
struct BendScan<'a> {
    src: &'a str,
    i: usize,
}

impl BendScan<'_> {
    fn skip(&mut self) {
        let bytes = self.src.as_bytes();
        while self.i < bytes.len() {
            match bytes[self.i] {
                b' ' | b'\n' | b'\r' | b'\t' => self.i += 1,
                b'#' => {
                    self.i += 1;
                    while self.i < bytes.len() && bytes[self.i] != b'\n' {
                        self.i += 1;
                    }
                }
                _ => return,
            }
        }
    }

    fn at(&self, byte: u8) -> bool {
        self.src.as_bytes().get(self.i) == Some(&byte)
    }

    fn at_word(&self, word: &str) -> bool {
        let rest = &self.src[self.i..];
        rest.starts_with(word)
            && !rest[word.len()..]
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    }

    fn take(&mut self, text: &str) -> bool {
        if self.src[self.i..].starts_with(text) {
            self.i += text.len();
            true
        } else {
            false
        }
    }

    fn bump_char(&mut self) -> Result<(), FrontendError> {
        let Some(ch) = self.src[self.i..].chars().next() else {
            return Err(main_must_be_string());
        };
        self.i += ch.len_utf8();
        Ok(())
    }

    fn read_name(&mut self) -> Option<String> {
        let mut chars = self.src[self.i..].chars();
        let head = chars.next()?;
        if !head.is_ascii_alphabetic() && head != '_' {
            return None;
        }
        let mut size = head.len_utf8();
        for ch in chars {
            if !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '.') {
                break;
            }
            size += ch.len_utf8();
        }
        let name = self.src[self.i..self.i + size].to_string();
        self.i += size;
        Some(name)
    }

    fn def_name_is_main(&mut self) -> bool {
        self.skip();
        self.read_name().as_deref() == Some("main")
    }

    fn main_returns_string(&mut self) -> Result<bool, FrontendError> {
        self.skip();
        if self.at(b'?') {
            self.i += 1;
        }
        self.skip();
        if !self.take("(") {
            return Ok(false);
        }
        self.skip_balanced(b'(', b')')?;
        self.skip();
        if !self.take("->") {
            return Ok(false);
        }
        self.skip();
        if !self.at_word("String") {
            return Ok(false);
        }
        self.i += "String".len();
        self.skip();
        Ok(self.take(":"))
    }

    fn skip_balanced(&mut self, open: u8, close: u8) -> Result<(), FrontendError> {
        let mut depth = 1u32;
        while depth > 0 {
            self.skip();
            if self.i >= self.src.len() {
                return Err(main_must_be_string());
            }
            if self.at(b'"') {
                self.skip_string()?;
                continue;
            }
            if self.at(b'\'') {
                self.skip_char_lit()?;
                continue;
            }
            let byte = self.src.as_bytes()[self.i];
            if byte == open {
                depth += 1;
                self.i += 1;
                continue;
            }
            if byte == close {
                depth -= 1;
                self.i += 1;
                continue;
            }
            self.bump_char()?;
        }
        Ok(())
    }

    fn skip_string(&mut self) -> Result<(), FrontendError> {
        if !self.take("\"") {
            return Err(main_must_be_string());
        }
        while !self.take("\"") {
            if self.i >= self.src.len() {
                return Err(main_must_be_string());
            }
            self.take_char_body()?;
        }
        Ok(())
    }

    fn skip_char_lit(&mut self) -> Result<(), FrontendError> {
        if !self.take("'") {
            return Err(main_must_be_string());
        }
        self.take_char_body()?;
        if self.take("'") {
            Ok(())
        } else {
            Err(main_must_be_string())
        }
    }

    fn take_char_body(&mut self) -> Result<(), FrontendError> {
        if self.i >= self.src.len() {
            return Err(main_must_be_string());
        }
        if !self.at(b'\\') {
            return self.bump_char();
        }
        self.i += 1;
        if self.take_unicode_escape() {
            return Ok(());
        }
        let Some(ch) = self.src[self.i..].chars().next() else {
            return Err(main_must_be_string());
        };
        if matches!(ch, 'n' | 't' | 'r' | '0' | '\\' | '\'' | '"') {
            self.i += ch.len_utf8();
            Ok(())
        } else {
            Err(main_must_be_string())
        }
    }

    fn take_unicode_escape(&mut self) -> bool {
        let rest = &self.src.as_bytes()[self.i..];
        let window = &rest[..rest.len().min(11)];
        if window.len() < 4 || !window[0].eq_ignore_ascii_case(&b'u') || window[1] != b'{' {
            return false;
        }
        let mut end = 2;
        while end < window.len() && window[end].is_ascii_hexdigit() {
            end += 1;
        }
        if end == 2 || end >= window.len() || window[end] != b'}' {
            return false;
        }
        self.i += end + 1;
        true
    }
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
    let flags = network_unshare_args(&unshare)?;
    let mut command = Command::new(&unshare);
    command
        .args(flags)
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
