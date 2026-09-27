use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use crate::FrontendError;

pub const BEND_VERSION: &str = "bend 2.0.28";
const SOURCE_LIMIT: u64 = 64 * 1024;
// The v2 line and evals.bend output both fit well under this; it matches the
// source cap.
const STDOUT_LIMIT: usize = 64 * 1024;
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
        "bend 2.0.28 is not installed; run ./scripts/install-bend.sh".to_string(),
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

/// How many random names staging tries before it gives up.
pub(crate) const STAGE_ATTEMPTS: usize = 8;

/// The files staged for Bend, and the ones whose main is run.
pub(crate) const STAGED_FILES: [&str; 4] =
    ["workflow.bend", "evals.bend", "LAWS.bend", "PROOF.bend"];
pub(crate) const RUN_FILES: [&str; 2] = ["workflow.bend", "evals.bend"];

pub fn stage(source: &Path) -> Result<Staged, FrontendError> {
    let dir = create_private_dir(&std::env::temp_dir(), || {
        let suffix = getrandom::u64()
            .map_err(|error| FrontendError::Setup(format!("no random staging name: {error}")))?;
        Ok(format!("gol-bend-{}-{suffix:016x}", std::process::id()))
    })?;
    let staged = Staged { dir };
    for name in STAGED_FILES {
        let text = read_source(&source.join(name))?;
        // Every file whose main is run must have one String main.
        if RUN_FILES.contains(&name) {
            assert_string_main(&text).map_err(|error| match error {
                FrontendError::Encoding(message) => {
                    FrontendError::Encoding(format!("{name} {message}"))
                }
                other => other,
            })?;
        }
        write_new(&staged.dir.join(name), text.as_bytes())?;
    }
    Ok(staged)
}

/// Creates a new directory under `parent`, readable only by this user, named
/// by `name`. The temp dir is shared, so a path that already exists (a
/// directory or a symlink another user made) is never used: `create_dir`
/// refuses it and the next name is tried. This assumes `parent` is sticky or
/// private, as `/tmp` and macOS's per-user `TMPDIR` are, so no other user can
/// rename the new directory away.
pub(crate) fn create_private_dir(
    parent: &Path,
    mut name: impl FnMut() -> Result<String, FrontendError>,
) -> Result<PathBuf, FrontendError> {
    for _ in 0..STAGE_ATTEMPTS {
        let name = name()?;
        // One plain component, so the directory is always inside `parent`.
        let mut parts = Path::new(&name).components();
        if !matches!(
            (parts.next(), parts.next()),
            (Some(std::path::Component::Normal(_)), None)
        ) {
            return Err(FrontendError::Setup(format!(
                "staging name {name:?} is not one path component"
            )));
        }
        let dir = parent.join(name);
        match fs::DirBuilder::new().mode(0o700).create(&dir) {
            Ok(()) => return Ok(dir),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(io_error("create", &dir, &error)),
        }
    }
    Err(FrontendError::Setup(format!(
        "no free staging directory in {} after {STAGE_ATTEMPTS} tries",
        parent.display()
    )))
}

/// Writes a new file, mode 0600. It never follows or truncates something
/// already at `path`.
pub(crate) fn write_new(path: &Path, bytes: &[u8]) -> Result<(), FrontendError> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .and_then(|mut file| file.write_all(bytes))
        .map_err(|error| io_error("write", path, &error))
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
        // Whole names and numbers are skipped, so `def` counts only where a
        // token starts: `xdef` is one name, but `1.5def` is a float and a def.
        if scan.read_name().is_some() || scan.skip_number() {
            continue;
        }
        scan.bump_char()?;
    }
    if mains != 1 {
        return Err(FrontendError::Encoding(
            "needs one def main() -> String:".to_string(),
        ));
    }
    Ok(())
}

fn main_must_be_string() -> FrontendError {
    FrontendError::Encoding("main must return String; IO is not executed".to_string())
}

// Bend 2.0.28 lexing for the main guard. Whitespace is space, tab, `\n`, and a
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

    /// `word` starts here and is not the head of a longer name. The main loop
    /// skips whole names and numbers, so a match is always at a token start.
    fn at_word(&self, word: &str) -> bool {
        let rest = &self.src[self.i..];
        rest.starts_with(word)
            && !rest[word.len()..]
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    }

    /// Skips a Bend 2.0.28 number, `\d+(n|\.\d+([eE][+-]?\d+)?)?`. A float
    /// ends right before a letter, so what follows starts a new token.
    fn skip_number(&mut self) -> bool {
        let bytes = self.src.as_bytes();
        let digits = |from: usize| {
            bytes.get(from..).map_or(0, |rest| {
                rest.iter().take_while(|b| b.is_ascii_digit()).count()
            })
        };
        let mut end = self.i + digits(self.i);
        if end == self.i {
            return false;
        }
        if bytes.get(end) == Some(&b'n') {
            end += 1;
        } else if bytes.get(end) == Some(&b'.') && digits(end + 1) > 0 {
            end += 1 + digits(end + 1);
            if matches!(bytes.get(end), Some(b'e' | b'E')) {
                let sign = usize::from(matches!(bytes.get(end + 1), Some(b'+' | b'-')));
                let exponent = digits(end + 1 + sign);
                if exponent > 0 {
                    end += 1 + sign + exponent;
                }
            }
        }
        self.i = end;
        true
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

// The only unsafe code in the workspace. Every other crate root forbids it.
#[allow(unsafe_code)]
fn kill_group(child: &mut Child) {
    let pid = child.id() as i32;
    // SAFETY: kill(2) takes two integers and touches no memory of this process.
    // The child was spawned with process_group(0), so its pgid equals its pid and
    // -pid names that group. The pid fits in i32 (Linux caps pids at 2^22) and is
    // never 0 or 1. The child is not reaped until the wait below, so its pid and
    // pgid cannot have been reused by another process.
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

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::fs;
    use std::os::unix::fs::{symlink, PermissionsExt};
    use std::path::{Path, PathBuf};

    use proptest::prelude::*;
    use workflow_core::{Decision, WorkflowProgram};

    use super::{
        assert_string_main, create_private_dir, stage, staged_dir, write_new, STAGE_ATTEMPTS,
    };

    /// A private scratch directory, removed when dropped (also after a failed
    /// assertion).
    struct Scratch(PathBuf);

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn scratch(tag: &str) -> Scratch {
        let dir = create_private_dir(&std::env::temp_dir(), || {
            Ok(format!(
                "gol-boundary-test-{tag}-{:016x}",
                getrandom::u64().unwrap()
            ))
        })
        .unwrap();
        Scratch(dir)
    }

    fn mode(path: &Path) -> u32 {
        fs::symlink_metadata(path).unwrap().permissions().mode() & 0o777
    }

    // A path that already exists, as a directory or a symlink, is never used:
    // staging moves on to the next name.
    #[test]
    fn staging_refuses_existing_dir() {
        let parent_dir = scratch("refuse");
        let parent = &parent_dir.0;
        fs::create_dir(parent.join("taken")).unwrap();
        fs::write(parent.join("taken").join("keep"), "keep").unwrap();
        let elsewhere_dir = scratch("elsewhere");
        let elsewhere = &elsewhere_dir.0;
        symlink(elsewhere, parent.join("link")).unwrap();
        // A dangling symlink is refused too.
        symlink(parent.join("nowhere"), parent.join("dangling")).unwrap();

        let names = ["taken", "link", "dangling", "fresh"];
        let calls = Cell::new(0);
        let dir = create_private_dir(parent, || {
            let name = names[calls.get()];
            calls.set(calls.get() + 1);
            Ok(name.to_string())
        })
        .unwrap();
        assert_eq!(dir, parent.join("fresh"));
        assert_eq!(calls.get(), 4);
        assert!(!parent.join("nowhere").exists());
        assert_eq!(mode(&dir), 0o700);
        assert_eq!(
            fs::read_dir(parent.join("taken")).unwrap().count(),
            1,
            "the existing directory was touched"
        );
        assert_eq!(fs::read_dir(elsewhere).unwrap().count(), 0);

        // Every name taken: it gives up after STAGE_ATTEMPTS tries.
        let tries = Cell::new(0);
        let error = create_private_dir(parent, || {
            tries.set(tries.get() + 1);
            Ok("taken".to_string())
        })
        .unwrap_err();
        assert_eq!(tries.get(), STAGE_ATTEMPTS);
        assert!(error.to_string().contains("staging directory"), "{error}");

        // A name that would leave `parent` is refused before anything is made.
        let escape = format!("gol-escape-{:016x}", getrandom::u64().unwrap());
        let up = format!("../{escape}");
        for name in ["/abs", "..", "a/b", up.as_str(), ""] {
            let error = create_private_dir(parent, || Ok(name.to_string())).unwrap_err();
            assert!(
                error.to_string().contains("one path component"),
                "{name}: {error}"
            );
        }
        assert!(!parent.parent().unwrap().join(&escape).exists());
    }

    #[test]
    fn a_staged_file_never_replaces_what_is_there() {
        let dir = scratch("write");
        let kept = dir.0.join("kept");
        fs::write(&kept, "kept").unwrap();
        let target = dir.0.join("target");
        fs::write(&target, "target").unwrap();
        let link = dir.0.join("link");
        symlink(&target, &link).unwrap();
        assert!(write_new(&kept, b"new").is_err());
        assert!(write_new(&link, b"new").is_err());
        assert_eq!(fs::read_to_string(&kept).unwrap(), "kept");
        assert_eq!(fs::read_to_string(&target).unwrap(), "target");
        let fresh = dir.0.join("fresh");
        write_new(&fresh, b"new").unwrap();
        assert_eq!(fs::read_to_string(&fresh).unwrap(), "new");
        assert_eq!(mode(&fresh), 0o600);
    }

    #[test]
    fn a_staged_dir_is_private_and_uniquely_named() {
        let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../experiments/bend");
        let first = stage(&source).unwrap();
        let second = stage(&source).unwrap();
        let (a, b) = (staged_dir(&first), staged_dir(&second));
        assert_eq!(mode(a), 0o700);
        for name in super::STAGED_FILES {
            assert_eq!(mode(&a.join(name)), 0o600, "{name}");
        }
        assert_ne!(a, b);
        let name = a.file_name().unwrap().to_str().unwrap();
        let prefix = format!("gol-bend-{}-", std::process::id());
        let suffix = name.strip_prefix(&prefix).expect("prefix");
        assert_eq!(suffix.len(), 16, "{name}");
        assert!(
            suffix.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "{name}"
        );
    }

    // `xdef main` is not a definition: an identifier that ends in "def" does
    // not start one.
    #[test]
    fn def_inside_a_name_is_not_a_def() {
        let main = "def main() -> String:\n  \"v1\"\n";
        for before in ["xdef", "_def", "x.def", "undef"] {
            let source = format!("{main}{before} main() -> IO(u24):\n  0\n");
            assert!(assert_string_main(&source).is_ok(), "{before}");
        }
        // The right-hand check: `defmain` is a name, not `def main`.
        let joined = format!("{main}defmain() -> IO(u24):\n  0\n");
        assert!(assert_string_main(&joined).is_ok());
        let two = format!("{main}def main() -> IO(u24):\n  0\n");
        assert!(assert_string_main(&two).is_err());
        assert!(assert_string_main("xdef main() -> String:\n  \"v1\"\n").is_err());
    }

    // Bend 2.0.28 ends a float literal right before a letter, so `1.5def main`
    // is a real def. The PR #54 review ran such an IO main on the pinned binary.
    #[test]
    fn a_def_after_a_float_literal_is_a_def() {
        let main = "def main() -> String:\n  \"v1\"\n";
        for literal in ["1.5", "1.5e3", "0.0E+1"] {
            let source = format!("{main}def f() -> F32:\n  {literal}def main() -> IO(u24):\n  0\n");
            assert!(assert_string_main(&source).is_err(), "{literal}");
        }
    }

    // Bend source pieces, each with the number of String mains and other mains
    // it defines. Pieces that only look like a def (a longer name, a string,
    // a comment, a char) define none.
    const PIECES: [(&str, usize, usize); 12] = [
        ("def main() -> String:\n  \"v1\"\n", 1, 0),
        ("def\tmain()\t->\tString:\r\n  \"v1\"\n", 1, 0),
        ("def main() -> IO(u24):\n  0\n", 0, 1),
        ("def f() -> F32:\n  1.5def main() -> IO(u24):\n  0\n", 0, 1),
        (
            "def f() -> F32:\n  2.0e1def main() -> String:\n  \"v1\"\n",
            1,
            0,
        ),
        ("xdef main() -> IO(u24):\n  0\n", 0, 0),
        ("x.def main() -> IO(u24):\n  0\n", 0, 0),
        ("# def main() -> IO(u24):\n", 0, 0),
        ("def s() -> String:\n  \"def main() -> IO(u24):\"\n", 0, 0),
        ("def c() -> Char:\n  'd'\n", 0, 0),
        ("def g(x) -> u24:\n  x + 1\n", 0, 0),
        ("def main2() -> IO(u24):\n  0\n", 0, 0),
    ];

    /// Programs over every decision kind, with names of 1 to 8 characters
    /// and inputs of up to 8, any Unicode.
    fn programs() -> impl Strategy<Value = WorkflowProgram> {
        let text = |min: usize| {
            prop::collection::vec(any::<char>(), min..=8)
                .prop_map(|chars| chars.into_iter().collect::<String>())
        };
        let leaf = prop_oneof![
            (text(1), text(0)).prop_map(|(name, input)| Decision::Tool { name, input }),
            (text(1), text(0)).prop_map(|(agent, input)| Decision::SpawnAgent { agent, input }),
            Just(Decision::Complete),
            Just(Decision::Fail),
        ];
        leaf.prop_recursive(4, 32, 4, |inner| {
            prop_oneof![
                (inner.clone(), inner.clone(), inner.clone()).prop_map(|(missing, zero, other)| {
                    Decision::OnCounter {
                        missing: Box::new(missing),
                        zero: Box::new(zero),
                        other: Box::new(other),
                    }
                }),
                prop::collection::vec(inner, 0..4).prop_map(Decision::Seq),
            ]
        })
        .prop_map(|root| WorkflowProgram { root })
    }

    /// Arbitrary text, encoded programs, and encoded programs with one
    /// character inserted, removed or replaced.
    fn encodings() -> impl Strategy<Value = String> {
        let encoded = programs().prop_map(|program| crate::encode_line(&program));
        let edited = (
            programs(),
            any::<prop::sample::Index>(),
            any::<char>(),
            0u8..4,
        )
            .prop_map(|(program, at, ch, edit)| {
                let mut chars: Vec<char> = crate::encode_line(&program).chars().collect();
                let at = at.index(chars.len() + 1);
                match edit {
                    0 => chars.insert(at, ch),
                    1 if at < chars.len() => {
                        chars.remove(at);
                    }
                    2 if at < chars.len() => chars[at] = ch,
                    _ => {}
                }
                chars.into_iter().collect()
            });
        prop_oneof![any::<String>(), encoded, edited]
    }

    proptest! {
        // The guard accepts a source exactly when it defines one main and that
        // main returns String, over sources built from PIECES.
        #[test]
        fn the_main_guard_counts_real_defs(picks in prop::collection::vec(0..PIECES.len(), 0..6)) {
            let source: String = picks.iter().map(|&pick| PIECES[pick].0).collect();
            let strings: usize = picks.iter().map(|&pick| PIECES[pick].1).sum();
            let others: usize = picks.iter().map(|&pick| PIECES[pick].2).sum();
            let accepted = assert_string_main(&source).is_ok();
            prop_assert_eq!(accepted, strings == 1 && others == 0, "{:?}", source);
        }


        // Every program comes back from its v2 line.
        #[test]
        fn parse_encoding_round_trips(program in programs()) {
            let line = crate::encode_line(&program);
            prop_assert_eq!(crate::parse_encoding(&line).unwrap(), program);
        }

        // Neither hand-written parser panics, and parse_encoding accepts a
        // line only in the exact form the program it names encodes to.
        #[test]
        fn scanner_and_parse_encoding_never_panic(text in encodings(), source in any::<String>()) {
            let _ = assert_string_main(&source);
            let _ = assert_string_main(&text);
            if let Ok(program) = crate::parse_encoding(&text) {
                prop_assert_eq!(crate::encode_line(&program), text);
            }
        }
    }
}
