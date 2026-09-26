use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Component, Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use protocol::{Capability, ToolDescriptor, ToolId};
use serde::Deserialize;
use serde_json::Value;

use crate::{EchoTool, Skill, Tool};

#[derive(Debug)]
pub enum LoadError {
    Io(String),
    Parse(String),
    Mcp(String),
    UnknownTool(String),
    /// Two catalog tools share a name. Effects, the authorizer, and dispatch
    /// name a tool by its name, so one name must mean one tool.
    DuplicateTool(String),
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(message) | Self::Parse(message) | Self::Mcp(message) => f.write_str(message),
            Self::UnknownTool(name) => write!(f, "unknown tool: {name}"),
            Self::DuplicateTool(name) => write!(f, "two catalog tools are named {name}"),
        }
    }
}

pub struct LoadedCatalog {
    tools: Vec<Box<dyn Tool>>,
    pub skills: Vec<Skill>,
}

impl LoadedCatalog {
    pub fn descriptors(&self) -> Vec<ToolDescriptor> {
        self.tools.iter().map(|tool| tool.descriptor()).collect()
    }

    pub fn tools(&self) -> Vec<&dyn Tool> {
        self.tools.iter().map(|tool| tool.as_ref()).collect()
    }

    pub fn into_parts(self) -> (Vec<Box<dyn Tool>>, Vec<Skill>) {
        (self.tools, self.skills)
    }
}

#[derive(Debug, Deserialize)]
struct CatalogFile {
    #[serde(default)]
    tools: Vec<String>,
    #[serde(default)]
    skills: Vec<String>,
    #[serde(default)]
    mcp: Vec<McpServer>,
}

#[derive(Debug, Deserialize)]
struct McpServer {
    name: String,
    command: String,
    #[serde(default)]
    args: Vec<String>,
    /// How long one request may wait for its response, at least 1. Defaults
    /// to `DEFAULT_TIMEOUT_MS`. Starting a session makes several requests
    /// (`initialize`, then each `tools/list` page), each with this limit. It
    /// does not bound a write: a server that stops reading its stdin can still
    /// block a call whose input overflows the pipe.
    timeout_ms: Option<u64>,
    #[serde(default)]
    tools: Vec<McpToolDecl>,
}

#[derive(Debug, Deserialize)]
struct McpToolDecl {
    name: String,
    #[serde(default)]
    description: String,
    /// The JSON schema of the tool's input, as the descriptor reports it.
    /// Loading does not start the server, so the schema is declared here.
    input_schema: Option<String>,
}

/// The longest line, in bytes, the client reads from an MCP server.
const MAX_LINE: usize = 1024 * 1024;

/// How long a request waits for its response when the catalog names no
/// `timeout_ms`.
const DEFAULT_TIMEOUT_MS: u64 = 30_000;

const DEFAULT_INPUT_SCHEMA: &str = "{\"type\":\"object\"}";

/// One line from an MCP server, as the client waiting for response `id` reads
/// it.
#[derive(Clone, Debug, PartialEq)]
enum Frame {
    /// A JSON object whose `id` is the number `id`: the response.
    Response(Value),
    /// Anything else a server may send between responses: a blank line, a
    /// notification or a request of its own (it has a `method`, and its id is
    /// in the server's id space), or a message with another id.
    Skip,
    /// Not UTF-8, not JSON, or not a JSON object.
    Invalid(String),
}

fn parse_frame(line: &[u8], id: i64) -> Frame {
    if line.iter().all(u8::is_ascii_whitespace) {
        return Frame::Skip;
    }
    let value: Value = match serde_json::from_slice(line) {
        Ok(value) => value,
        Err(err) => return Frame::Invalid(err.to_string()),
    };
    let Some(object) = value.as_object() else {
        return Frame::Invalid("not a JSON object".to_string());
    };
    if object.contains_key("method") {
        return Frame::Skip;
    }
    match object.get("id").and_then(Value::as_i64) {
        Some(found) if found == id => Frame::Response(value),
        _ => Frame::Skip,
    }
}

/// Reads one line of at most `cap` bytes, without its newline. `Ok(None)` is
/// end of input. A longer line is an error and leaves the reader mid-line.
fn read_capped_line(reader: &mut impl BufRead, cap: usize) -> Result<Option<Vec<u8>>, String> {
    let too_long = || format!("line over {}", size(cap));
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf().map_err(|err| err.to_string())?;
        if available.is_empty() {
            return Ok((!line.is_empty()).then_some(line));
        }
        match available.iter().position(|byte| *byte == b'\n') {
            Some(end) => {
                if line.len() + end > cap {
                    return Err(too_long());
                }
                line.extend_from_slice(&available[..end]);
                reader.consume(end + 1);
                return Ok(Some(line));
            }
            None => {
                let taken = available.len();
                if line.len() + taken > cap {
                    return Err(too_long());
                }
                line.extend_from_slice(available);
                reader.consume(taken);
            }
        }
    }
}

/// `bytes` in words: whole mebibytes as MiB, anything else in bytes.
fn size(bytes: usize) -> String {
    const MIB: usize = 1024 * 1024;
    if bytes >= MIB && bytes.is_multiple_of(MIB) {
        format!("{} MiB", bytes / MIB)
    } else {
        format!("{bytes} bytes")
    }
}

/// Forwards the server's stdout line by line until it ends, fails or sends an
/// oversized line, or until the session stops listening.
fn forward_lines(stdout: ChildStdout, lines: SyncSender<Result<Vec<u8>, String>>) {
    let mut reader = BufReader::new(stdout);
    loop {
        let (message, last) = match read_capped_line(&mut reader, MAX_LINE) {
            Ok(Some(line)) => (Ok(line), false),
            Ok(None) => (Err("server closed stdout".to_string()), true),
            Err(err) => (Err(err), true),
        };
        if lines.send(message).is_err() || last {
            return;
        }
    }
}

pub fn load_catalog(dir: impl AsRef<Path>) -> Result<LoadedCatalog, LoadError> {
    let dir = dir.as_ref();
    let raw = fs::read_to_string(dir.join("harness.toml"))
        .map_err(|err| LoadError::Io(err.to_string()))?;
    let file: CatalogFile =
        toml::from_str(&raw).map_err(|err| LoadError::Parse(err.to_string()))?;

    let mut tools: Vec<Box<dyn Tool>> = Vec::new();
    for name in &file.tools {
        if name == "echo" {
            tools.push(Box::new(EchoTool));
        } else {
            return Err(LoadError::UnknownTool(name.clone()));
        }
    }

    for server in file.mcp {
        if server.timeout_ms == Some(0) {
            return Err(LoadError::Mcp(format!(
                "{}: timeout_ms must be at least 1",
                server.name
            )));
        }
        if server.tools.is_empty() {
            return Err(LoadError::Mcp(format!(
                "{}: declare tools in the catalog; loading does not start {}",
                server.name, server.command
            )));
        }
        let shared = Arc::new(Mutex::new(PendingMcp {
            dir: dir.to_path_buf(),
            server_name: server.name.clone(),
            command: server.command,
            args: server.args,
            timeout: Duration::from_millis(server.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS)),
            live: None,
        }));
        for declared in server.tools {
            let capability = format!("mcp.{}.{}", server.name, declared.name);
            tools.push(Box::new(McpTool {
                id: ToolId::new(),
                server: server.name.clone(),
                name: declared.name,
                description: declared.description,
                input_schema: declared
                    .input_schema
                    .unwrap_or_else(|| DEFAULT_INPUT_SCHEMA.to_string()),
                capability,
                session: Arc::clone(&shared),
            }));
        }
    }

    let mut names = std::collections::HashSet::new();
    for tool in &tools {
        let name = tool.descriptor().name;
        if !names.insert(name.clone()) {
            return Err(LoadError::DuplicateTool(name));
        }
    }

    let catalog_dir = dir
        .canonicalize()
        .map_err(|err| LoadError::Io(err.to_string()))?;
    let mut skills = Vec::new();
    for relative in &file.skills {
        let rel = Path::new(relative);
        if rel.is_absolute() {
            return Err(LoadError::Io(format!("skill path is absolute: {relative}")));
        }
        if rel
            .components()
            .any(|component| component == Component::ParentDir)
        {
            return Err(LoadError::Io(format!("skill path contains ..: {relative}")));
        }
        let canonical = dir
            .join(rel)
            .canonicalize()
            .map_err(|err| LoadError::Io(err.to_string()))?;
        if !canonical.starts_with(&catalog_dir) {
            return Err(LoadError::Io(format!(
                "skill path is outside the catalog: {relative}"
            )));
        }
        let body = fs::read_to_string(&canonical).map_err(|err| LoadError::Io(err.to_string()))?;
        let name = PathBuf::from(relative)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or(relative)
            .to_string();
        skills.push(Skill { name, body });
    }

    Ok(LoadedCatalog { tools, skills })
}

/// Why an MCP call failed. After a transport failure the session can no longer
/// be trusted (a response may still be in flight), so it is dropped and the
/// next call starts a new server.
#[derive(Debug, PartialEq)]
enum McpFailure {
    /// The server answered: a JSON-RPC error, or a tool result with `isError`.
    Answered(String),
    /// No usable answer: a timeout, an oversized or invalid line, a closed
    /// stream, or a failed write.
    Transport(String),
}

impl McpFailure {
    fn message(self) -> String {
        match self {
            Self::Answered(message) | Self::Transport(message) => message,
        }
    }

    /// The same failure, its message prefixed with the request it came from.
    fn within(self, request: &str) -> Self {
        match self {
            Self::Answered(message) => Self::Answered(format!("{request}: {message}")),
            Self::Transport(message) => Self::Transport(format!("{request}: {message}")),
        }
    }
}

/// The most `tools/list` pages a session start follows.
const MAX_TOOL_PAGES: usize = 16;

struct PendingMcp {
    dir: PathBuf,
    server_name: String,
    command: String,
    args: Vec<String>,
    timeout: Duration,
    live: Option<McpSession>,
}

impl PendingMcp {
    fn call(&mut self, name: &str, input: &str) -> Result<String, String> {
        if self.live.is_none() {
            let session = McpSession::spawn(
                &self.dir,
                &self.server_name,
                &self.command,
                &self.args,
                self.timeout,
            )
            .map_err(McpFailure::message)?;
            self.live = Some(session);
        }
        let Some(session) = self.live.as_mut() else {
            return Err("no session".to_string());
        };
        if !session.listed.iter().any(|listed| listed == name) {
            return Err("not listed by the server".to_string());
        }
        match session.call(name, input) {
            Ok(text) => Ok(text),
            Err(McpFailure::Answered(message)) => Err(message),
            Err(McpFailure::Transport(message)) => {
                self.live = None;
                Err(message)
            }
        }
    }
}

struct McpSession {
    child: Child,
    stdin: ChildStdin,
    lines: Receiver<Result<Vec<u8>, String>>,
    next_id: i64,
    timeout: Duration,
    /// The tool names the server's `tools/list` returned.
    listed: Vec<String>,
}

impl McpSession {
    fn spawn(
        dir: &Path,
        name: &str,
        command: &str,
        args: &[String],
        timeout: Duration,
    ) -> Result<Self, McpFailure> {
        let mut child = Command::new(command)
            .args(args)
            .current_dir(dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|err| McpFailure::Transport(format!("spawn {command} for {name}: {err}")))?;
        let (Some(stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
            let _ = child.kill();
            let _ = child.wait();
            return Err(McpFailure::Transport("no stdio".to_string()));
        };
        // The reader thread ends when stdout ends, which dropping the session
        // (it kills the child) guarantees, or when the session stops listening.
        let (sender, lines) = mpsc::sync_channel(16);
        thread::spawn(move || forward_lines(stdout, sender));
        let mut session = Self {
            child,
            stdin,
            lines,
            next_id: 1,
            timeout,
            listed: Vec::new(),
        };
        session.request(
            "initialize",
            serde_json::json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "gol", "version": "0.1.0"}
            }),
        )?;
        session.notify("notifications/initialized", serde_json::json!({}))?;
        session.listed = session.list_tools()?;
        Ok(session)
    }

    /// Every tool name the server lists, following `nextCursor` pages.
    fn list_tools(&mut self) -> Result<Vec<String>, McpFailure> {
        let malformed = || McpFailure::Transport("tools/list: malformed result".to_string());
        let mut names = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_TOOL_PAGES {
            let params = match &cursor {
                Some(cursor) => serde_json::json!({ "cursor": cursor }),
                None => serde_json::json!({}),
            };
            let page = self
                .request("tools/list", params)
                .map_err(|failure| failure.within("tools/list"))?;
            let tools = page
                .get("tools")
                .and_then(Value::as_array)
                .ok_or_else(malformed)?;
            for tool in tools {
                let name = tool
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(malformed)?;
                names.push(name.to_string());
            }
            match page.get("nextCursor").and_then(Value::as_str) {
                Some(next) => cursor = Some(next.to_string()),
                None => return Ok(names),
            }
        }
        Err(McpFailure::Transport(format!(
            "tools/list: more than {MAX_TOOL_PAGES} pages"
        )))
    }

    fn call(&mut self, name: &str, input: &str) -> Result<String, McpFailure> {
        let result = self.request(
            "tools/call",
            serde_json::json!({
                "name": name,
                "arguments": {"input": input}
            }),
        )?;
        let text = result
            .pointer("/content/0/text")
            .and_then(|value| value.as_str())
            .ok_or_else(|| McpFailure::Answered("tools/call missing text".into()))?;
        // MCP reports a failed tool as a result with isError; it is not an answer.
        if result.get("isError").and_then(|value| value.as_bool()) == Some(true) {
            return Err(McpFailure::Answered(text.to_string()));
        }
        Ok(text.to_string())
    }

    /// Sends a request and waits for the response with its id, skipping
    /// notifications and other ids, for at most `timeout` in all.
    fn request(&mut self, method: &str, params: Value) -> Result<Value, McpFailure> {
        let id = self.next_id;
        self.next_id += 1;
        let message = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        });
        self.send(&message)?;
        let deadline = Instant::now() + self.timeout;
        let value = await_response(&self.lines, id, deadline, self.timeout)?;
        if let Some(error) = value.get("error") {
            let message = error
                .get("message")
                .and_then(|message| message.as_str())
                .map_or_else(|| error.to_string(), str::to_string);
            return Err(McpFailure::Answered(match error.get("code") {
                Some(code) => format!("{message} (code {code})"),
                None => message,
            }));
        }
        value
            .get("result")
            .cloned()
            .ok_or_else(|| McpFailure::Answered("response missing result".into()))
    }

    fn notify(&mut self, method: &str, params: Value) -> Result<(), McpFailure> {
        let message = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        });
        self.send(&message)
    }

    fn send(&mut self, message: &Value) -> Result<(), McpFailure> {
        writeln!(self.stdin, "{message}")
            .and_then(|()| self.stdin.flush())
            .map_err(|err| McpFailure::Transport(err.to_string()))
    }
}

/// Waits for the response to request `id`, skipping stray frames, until
/// `deadline`. The deadline is checked on every pass, so a server that keeps
/// the channel full of notifications cannot hold the call past it.
fn await_response(
    lines: &Receiver<Result<Vec<u8>, String>>,
    id: i64,
    deadline: Instant,
    timeout: Duration,
) -> Result<Value, McpFailure> {
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(McpFailure::Transport(format!(
                "timed out after {} ms",
                timeout.as_millis()
            )));
        }
        let line = match lines.recv_timeout(deadline - now) {
            Ok(Ok(line)) => line,
            Ok(Err(message)) => return Err(McpFailure::Transport(message)),
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => {
                return Err(McpFailure::Transport("server closed stdout".to_string()))
            }
        };
        match parse_frame(&line, id) {
            Frame::Response(value) => return Ok(value),
            Frame::Skip => continue,
            Frame::Invalid(message) => {
                return Err(McpFailure::Transport(format!(
                    "malformed message: {message}"
                )))
            }
        }
    }
}

impl Drop for McpSession {
    /// Kills and reaps the server. The reader thread then sees stdout end and
    /// exits; if the server left a process of its own holding stdout open,
    /// that thread waits on it until it exits.
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct McpTool {
    id: ToolId,
    server: String,
    name: String,
    description: String,
    input_schema: String,
    capability: String,
    session: Arc<Mutex<PendingMcp>>,
}

impl Tool for McpTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            id: self.id,
            name: self.name.clone(),
            description: self.description.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: "{\"type\":\"string\"}".to_string(),
            required_capability: Capability::new(self.capability.clone()),
        }
    }

    fn call(&self, input: &str) -> Result<String, String> {
        let failed = |err: String| format!("mcp {}.{}: {err}", self.server, self.name);
        let mut pending = self
            .session
            .lock()
            .map_err(|_| failed("session lock poisoned".to_string()))?;
        pending.call(&self.name, input).map_err(failed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::json;
    use std::io::Cursor;

    /// The spec of `parse_frame`, written without it: blank lines, objects
    /// with a `method` (the server's own notifications and requests) and
    /// objects whose `id` is not the number `id` are skipped, the object whose
    /// `id` is `id` is the response, and anything that is not a JSON object is
    /// invalid.
    fn oracle(line: &[u8], id: i64) -> &'static str {
        let Ok(text) = std::str::from_utf8(line) else {
            return "invalid";
        };
        if text
            .trim_matches(|c: char| c.is_ascii_whitespace())
            .is_empty()
        {
            return "skip";
        }
        match serde_json::from_str::<Value>(text) {
            Ok(Value::Object(fields)) if fields.contains_key("method") => "skip",
            Ok(Value::Object(fields)) if fields.get("id") == Some(&json!(id)) => "response",
            Ok(Value::Object(_)) => "skip",
            _ => "invalid",
        }
    }

    fn kind(frame: &Frame) -> &'static str {
        match frame {
            Frame::Response(_) => "response",
            Frame::Skip => "skip",
            Frame::Invalid(_) => "invalid",
        }
    }

    fn message() -> impl Strategy<Value = Vec<u8>> {
        let id = prop_oneof![
            Just(None),
            (0i64..4).prop_map(|id| Some(json!(id))),
            Just(Some(json!("1"))),
            Just(Some(json!(1.0))),
            Just(Some(Value::Null)),
        ];
        let body = prop_oneof![
            Just(json!({"result": {"ok": true}})),
            Just(json!({"error": {"code": -1, "message": "no"}})),
            Just(json!({"method": "notifications/progress", "params": {}})),
        ];
        (id, body, any::<Option<usize>>()).prop_map(|(id, body, cut)| {
            let mut object = body;
            if let Some(id) = id {
                object["id"] = id;
            }
            let mut bytes = object.to_string().into_bytes();
            if let Some(cut) = cut {
                bytes.truncate(cut % (bytes.len() + 1));
            }
            bytes
        })
    }

    // proptest's default is the PR budget of 256 cases (AGENTS.md, Budgets).
    proptest! {
        #[test]
        fn parse_frame_never_panics_and_matches_oracle(
            line in prop_oneof![
                prop::collection::vec(any::<u8>(), 0..64),
                message(),
            ],
            id in 0i64..4,
        ) {
            let frame = parse_frame(&line, id);
            prop_assert_eq!(kind(&frame), oracle(&line, id));
            if let Frame::Response(value) = frame {
                prop_assert_eq!(value.get("id"), Some(&json!(id)));
            }
        }
    }

    #[test]
    fn a_line_at_the_cap_is_read_and_one_byte_more_is_not() {
        let mut exact = Cursor::new(b"abcd\nrest".to_vec());
        assert_eq!(read_capped_line(&mut exact, 4), Ok(Some(b"abcd".to_vec())));
        assert_eq!(read_capped_line(&mut exact, 4), Ok(Some(b"rest".to_vec())));
        assert_eq!(read_capped_line(&mut exact, 4), Ok(None));

        let mut over = Cursor::new(b"abcde\n".to_vec());
        assert_eq!(
            read_capped_line(&mut over, 4),
            Err("line over 4 bytes".to_string())
        );
        let mut unterminated = Cursor::new(b"abcde".to_vec());
        assert_eq!(
            read_capped_line(&mut unterminated, 4),
            Err("line over 4 bytes".to_string())
        );
        assert_eq!(size(MAX_LINE), "1 MiB");
    }

    #[test]
    fn a_line_is_capped_across_buffer_refills() {
        let mut exact = BufReader::with_capacity(2, Cursor::new(b"abcd\nxy".to_vec()));
        assert_eq!(read_capped_line(&mut exact, 4), Ok(Some(b"abcd".to_vec())));
        assert_eq!(read_capped_line(&mut exact, 4), Ok(Some(b"xy".to_vec())));
        let mut over = BufReader::with_capacity(2, Cursor::new(b"abcde\n".to_vec()));
        assert_eq!(
            read_capped_line(&mut over, 4),
            Err("line over 4 bytes".to_string())
        );
    }

    fn queued(frames: &[Value]) -> Receiver<Result<Vec<u8>, String>> {
        let (sender, lines) = mpsc::sync_channel(frames.len());
        for frame in frames {
            sender.send(Ok(frame.to_string().into_bytes())).unwrap();
        }
        lines
    }

    // Queued frames do not outlast the deadline: once it has passed, the
    // request times out even though its response is already waiting.
    #[test]
    fn the_deadline_is_checked_before_every_frame() {
        let timeout = Duration::from_millis(200);
        let lines = queued(&[
            json!({"jsonrpc": "2.0", "method": "notifications/progress"}),
            json!({"jsonrpc": "2.0", "id": 3, "result": {}}),
        ]);
        assert_eq!(
            await_response(&lines, 3, Instant::now(), timeout),
            Err(McpFailure::Transport("timed out after 200 ms".to_string()))
        );

        let lines = queued(&[
            json!({"jsonrpc": "2.0", "method": "notifications/progress"}),
            json!({"jsonrpc": "2.0", "id": 3, "method": "ping"}),
            json!({"jsonrpc": "2.0", "id": 3, "result": {}}),
        ]);
        assert_eq!(
            await_response(&lines, 3, Instant::now() + timeout, timeout),
            Ok(json!({"jsonrpc": "2.0", "id": 3, "result": {}}))
        );
    }

    #[test]
    fn a_poisoned_session_lock_is_an_error_not_a_panic() {
        let session = Arc::new(Mutex::new(PendingMcp {
            dir: PathBuf::from("."),
            server_name: "local".to_string(),
            command: "gol-no-such-command".to_string(),
            args: Vec::new(),
            timeout: Duration::from_millis(100),
            live: None,
        }));
        let held = Arc::clone(&session);
        let _ = thread::spawn(move || {
            let _guard = held.lock().unwrap();
            panic!("poison the lock");
        })
        .join();
        assert!(session.is_poisoned());
        let tool = McpTool {
            id: ToolId::new(),
            server: "local".to_string(),
            name: "ping".to_string(),
            description: String::new(),
            input_schema: DEFAULT_INPUT_SCHEMA.to_string(),
            capability: "mcp.local.ping".to_string(),
            session,
        };
        assert_eq!(
            tool.call("hi"),
            Err("mcp local.ping: session lock poisoned".to_string())
        );
    }
}
