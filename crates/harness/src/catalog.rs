use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex};

use protocol::{Capability, ToolDescriptor, ToolId};
use serde::Deserialize;

use crate::{EchoTool, Skill, Tool};

#[derive(Debug)]
pub enum LoadError {
    Io(String),
    Parse(String),
    Mcp(String),
    UnknownTool(String),
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
    #[serde(default)]
    tools: Vec<McpToolDecl>,
}

#[derive(Debug, Deserialize)]
struct McpToolDecl {
    name: String,
    #[serde(default)]
    description: String,
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
            live: None,
        }));
        for declared in server.tools {
            let capability = format!("mcp.{}.{}", server.name, declared.name);
            tools.push(Box::new(McpTool {
                server: server.name.clone(),
                name: declared.name,
                description: declared.description,
                capability,
                session: Arc::clone(&shared),
            }));
        }
    }

    let mut skills = Vec::new();
    for relative in &file.skills {
        let path = dir.join(relative);
        let body = fs::read_to_string(&path).map_err(|err| LoadError::Io(err.to_string()))?;
        let name = PathBuf::from(relative)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or(relative)
            .to_string();
        skills.push(Skill { name, body });
    }

    Ok(LoadedCatalog { tools, skills })
}

struct PendingMcp {
    dir: PathBuf,
    server_name: String,
    command: String,
    args: Vec<String>,
    live: Option<McpSession>,
}

impl PendingMcp {
    fn call(&mut self, name: &str, input: &str) -> Result<String, LoadError> {
        if self.live.is_none() {
            self.live = Some(McpSession::spawn(
                &self.dir,
                &self.server_name,
                &self.command,
                &self.args,
            )?);
        }
        self.live.as_mut().expect("mcp session").call(name, input)
    }
}

struct McpSession {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<std::process::ChildStdout>,
    next_id: i64,
}

impl McpSession {
    fn spawn(dir: &Path, name: &str, command: &str, args: &[String]) -> Result<Self, LoadError> {
        let mut child = Command::new(command)
            .args(args)
            .current_dir(dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|err| LoadError::Mcp(format!("{name}: {err}")))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| LoadError::Mcp("no stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| LoadError::Mcp("no stdout".into()))?;
        let mut session = Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            next_id: 1,
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
        Ok(session)
    }

    fn call(&mut self, name: &str, input: &str) -> Result<String, LoadError> {
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
            .ok_or_else(|| LoadError::Mcp("tools/call missing text".into()))?;
        Ok(text.to_string())
    }

    fn request(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, LoadError> {
        let id = self.next_id;
        self.next_id += 1;
        let message = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        });
        self.send(&message)?;
        let value = self.read_value()?;
        if value.get("id").and_then(|item| item.as_i64()) != Some(id) {
            return Err(LoadError::Mcp("malformed response".into()));
        }
        if let Some(error) = value.get("error") {
            return Err(LoadError::Mcp(error.to_string()));
        }
        value
            .get("result")
            .cloned()
            .ok_or_else(|| LoadError::Mcp("response missing result".into()))
    }

    fn notify(&mut self, method: &str, params: serde_json::Value) -> Result<(), LoadError> {
        let message = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        });
        self.send(&message)
    }

    fn send(&mut self, message: &serde_json::Value) -> Result<(), LoadError> {
        writeln!(self.stdin, "{message}").map_err(|err| LoadError::Mcp(err.to_string()))?;
        self.stdin
            .flush()
            .map_err(|err| LoadError::Mcp(err.to_string()))
    }

    fn read_value(&mut self) -> Result<serde_json::Value, LoadError> {
        let mut line = String::new();
        let read = self
            .stdout
            .read_line(&mut line)
            .map_err(|err| LoadError::Mcp(err.to_string()))?;
        if read == 0 {
            return Err(LoadError::Mcp("server closed stdout".into()));
        }
        serde_json::from_str(&line).map_err(|err| LoadError::Mcp(err.to_string()))
    }
}

impl Drop for McpSession {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct McpTool {
    server: String,
    name: String,
    description: String,
    capability: String,
    session: Arc<Mutex<PendingMcp>>,
}

impl Tool for McpTool {
    fn descriptor(&self) -> ToolDescriptor {
        ToolDescriptor {
            id: ToolId::new(),
            name: self.name.clone(),
            description: self.description.clone(),
            input_schema: "{\"type\":\"object\"}".to_string(),
            output_schema: "{\"type\":\"string\"}".to_string(),
            required_capability: Capability::new(self.capability.clone()),
        }
    }

    fn call(&self, input: &str) -> String {
        let _ = &self.server;
        self.session
            .lock()
            .expect("mcp")
            .call(&self.name, input)
            .unwrap_or_else(|err| format!("mcp error: {err:?}"))
    }
}
