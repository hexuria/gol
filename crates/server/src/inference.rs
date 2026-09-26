use std::collections::HashSet;
use std::process::Command;
use std::sync::{Arc, Mutex};

use protocol::{
    fold, Actor, CredentialSource, Event, EventPayload, EventSource, ExecutionPlacement,
    FailureClass, HarnessState, MessageRole, ModelMessage, RunId, RunSpec, Timestamp,
};

use crate::store::{is_terminal, Append, RunStore, StoredRun};

#[derive(Clone, Debug)]
pub struct GatewayCall {
    pub run_id: RunId,
    pub input: String,
    pub model_name: String,
    pub placement: ExecutionPlacement,
}

pub trait GatewayPoster: Send + Sync {
    fn complete(&self, call: &GatewayCall) -> Result<String, String>;
}

pub struct HttpGatewayPoster {
    pub url: String,
    pub token: String,
}

impl HttpGatewayPoster {
    pub fn from_env() -> Self {
        Self {
            url: std::env::var("GOL_PROXY_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:43124".to_string()),
            token: std::env::var("GOL_GATEWAY_TOKEN")
                .unwrap_or_else(|_| "gol-gateway-local".to_string()),
        }
    }
}

impl GatewayPoster for HttpGatewayPoster {
    fn complete(&self, call: &GatewayCall) -> Result<String, String> {
        ensure_fixture_proxy(&self.url)?;
        let endpoint = format!("{}/v1/gateway/complete", self.url.trim_end_matches('/'));
        let response = ureq::post(&endpoint)
            .set("authorization", &format!("Bearer {}", self.token))
            .set("content-type", "application/json")
            .set("x-gol-caller", "server")
            .send_json(serde_json::json!({
                "input": call.input,
                "model": call.model_name,
                "placement": placement_name(call.placement),
            }))
            .map_err(|error| error.to_string())?;
        let body: serde_json::Value = response.into_json().map_err(|error| error.to_string())?;
        body.get("text")
            .and_then(|value| value.as_str())
            .map(str::to_string)
            .ok_or_else(|| "proxy response missing text".to_string())
    }
}

pub fn ensure_fixture_proxy(url: &str) -> Result<(), String> {
    let lower = url.to_ascii_lowercase();
    for host in [
        "anthropic.com",
        "openai.com",
        "chatgpt.com",
        "x.ai",
        "grok.com",
    ] {
        if lower.contains(host) {
            return Err(format!(
                "refusing to call {host}; the fixture proxy is local"
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct ComputerPlan {
    pub image: String,
    pub started_by: &'static str,
    pub command: String,
    pub started: bool,
    pub name: String,
}

/// One container name per run. A later run must not reuse it.
pub fn box_container_name(run_id: RunId) -> String {
    format!("gol-box-{run_id}")
}

/// Ephemeral workspace for one run. The shared volume `gol-workspace` is not mounted.
pub fn box_workspace_volume(run_id: impl std::fmt::Display) -> String {
    format!("gol-workspace-{run_id}")
}

fn workspace_mount(run_id: impl std::fmt::Display) -> String {
    format!("{}:/workspace", box_workspace_volume(run_id))
}

pub fn computer_plan(placement: ExecutionPlacement, run_id: RunId) -> ComputerPlan {
    computer_for(placement, run_id, false)
}

fn computer_for(placement: ExecutionPlacement, run_id: RunId, started: bool) -> ComputerPlan {
    match placement {
        ExecutionPlacement::Box => {
            let name = box_container_name(run_id);
            ComputerPlan {
                image: "gol-agent:production".to_string(),
                started_by: "server",
                command: box_command(&name, run_id),
                started,
                name,
            }
        }
        ExecutionPlacement::Local | ExecutionPlacement::Reverse => {
            let mount = workspace_mount(run_id);
            ComputerPlan {
                image: "gol-agent:local".to_string(),
                started_by: "desktop",
                command: format!(
                    "docker run --rm -d --name gol-agent-local -v {mount} gol-agent:local"
                ),
                started: false,
                name: "gol-agent-local".to_string(),
            }
        }
    }
}

/// Create, run a finite command, and remove. The image entrypoint is not used:
/// `sleep infinity` would keep the container after `--rm` has nothing to reap.
fn box_command(name: &str, run_id: RunId) -> String {
    let mount = workspace_mount(run_id);
    format!(
        "docker create --name {name} -v {mount} --entrypoint /bin/sh gol-agent:production -c true && docker start -a {name} && docker rm -f {name}"
    )
}

#[derive(Debug)]
pub enum SandboxError {
    Host(String),
}

pub trait SandboxHost: Send + Sync {
    fn provision(&self, name: &str) -> Result<(), SandboxError>;
    fn destroy(&self, name: &str) -> Result<(), SandboxError>;
    fn exists(&self, name: &str) -> bool;
    fn launches_docker(&self) -> bool;
}

#[derive(Default)]
pub struct MemorySandbox {
    live: Mutex<HashSet<String>>,
    provisioned: Mutex<Vec<String>>,
}

impl SandboxHost for MemorySandbox {
    fn provision(&self, name: &str) -> Result<(), SandboxError> {
        if name.is_empty() || name == "gol-agent-box" {
            return Err(SandboxError::Host(format!("refusing sandbox name {name}")));
        }
        {
            let mut live = self.live.lock().expect("sandbox");
            if !live.insert(name.to_string()) {
                return Err(SandboxError::Host(format!(
                    "sandbox {name} is already in use"
                )));
            }
        }
        self.provisioned
            .lock()
            .expect("sandbox")
            .push(name.to_string());
        Ok(())
    }

    fn destroy(&self, name: &str) -> Result<(), SandboxError> {
        let removed = self.live.lock().expect("sandbox").remove(name);
        if removed {
            Ok(())
        } else {
            Err(SandboxError::Host(format!("sandbox {name} is not running")))
        }
    }

    fn exists(&self, name: &str) -> bool {
        self.live.lock().expect("sandbox").contains(name)
    }

    fn launches_docker(&self) -> bool {
        false
    }
}

impl MemorySandbox {
    pub fn provisioned(&self) -> Vec<String> {
        self.provisioned.lock().expect("sandbox").clone()
    }
}

trait RunDocker: Send + Sync {
    fn run(&self, args: &[String]) -> Result<(), String>;
}

impl<F> RunDocker for F
where
    F: Fn(&[String]) -> Result<(), String> + Send + Sync,
{
    fn run(&self, args: &[String]) -> Result<(), String> {
        self(args)
    }
}

pub struct DockerSandbox {
    command: Arc<dyn RunDocker>,
}

impl DockerSandbox {
    pub fn new() -> Self {
        Self::from_command(docker)
    }

    pub fn from_command<F>(command: F) -> Self
    where
        F: Fn(&[String]) -> Result<(), String> + Send + Sync + 'static,
    {
        Self {
            command: Arc::new(command),
        }
    }

    fn command(&self, args: &[&str]) -> Result<(), SandboxError> {
        let owned: Vec<String> = args.iter().map(|arg| (*arg).to_string()).collect();
        self.command.run(&owned).map_err(SandboxError::Host)
    }
}

impl Default for DockerSandbox {
    fn default() -> Self {
        Self::new()
    }
}

impl SandboxHost for DockerSandbox {
    fn provision(&self, name: &str) -> Result<(), SandboxError> {
        if name.is_empty() || name == "gol-agent-box" {
            return Err(SandboxError::Host(format!("refusing sandbox name {name}")));
        }
        let run_id = name.strip_prefix("gol-box-").unwrap_or(name);
        let mount = workspace_mount(run_id);
        self.command(&[
            "create",
            "--name",
            name,
            "-v",
            &mount,
            "--entrypoint",
            "/bin/sh",
            "gol-agent:production",
            "-c",
            "true",
        ])?;
        if let Err(error) = self.command(&["start", "-a", name]) {
            let _ = self.command(&["rm", "-f", name]);
            return Err(error);
        }
        Ok(())
    }

    fn destroy(&self, name: &str) -> Result<(), SandboxError> {
        self.command(&["rm", "-f", name])
    }

    fn exists(&self, name: &str) -> bool {
        self.command(&["inspect", "--type", "container", name])
            .is_ok()
    }

    fn launches_docker(&self) -> bool {
        true
    }
}

fn docker(args: &[String]) -> Result<(), String> {
    let status = Command::new("docker")
        .args(args)
        .status()
        .map_err(|error| error.to_string())?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("docker {args:?} exited {status}"))
    }
}

pub fn sandbox_from_env() -> Arc<dyn SandboxHost> {
    if std::env::var("GOL_START_BOX").ok().as_deref() == Some("1") {
        Arc::new(DockerSandbox::new())
    } else {
        Arc::new(MemorySandbox::default())
    }
}

#[derive(Clone, Debug)]
pub struct TurnOutcome {
    pub spec: RunSpec,
    pub events: Vec<Event>,
    pub completion: Option<String>,
    pub credential_mode: &'static str,
    pub computer: ComputerPlan,
}

#[derive(Debug)]
pub enum TurnError {
    Proxy(String),
    Sandbox(String),
    NotFound,
    Conflict(&'static str),
    BadRequest(&'static str),
}

/// Record the user message, then post to the proxy only for the platform gateway.
/// A Box sandbox is provisioned before that call. Gateway mode removes it only
/// after `RunCompleted` is stored. Subscription mode leaves it until
/// `accept_subscription_completion`. A failed remove rolls the completion back.
pub fn open_turn(
    store: &dyn RunStore,
    spec: RunSpec,
    poster: &dyn GatewayPoster,
    sandbox: &dyn SandboxHost,
) -> Result<TurnOutcome, TurnError> {
    let mut events = Vec::new();
    push(&spec, &mut events, Actor::System, EventPayload::RunCreated);
    push(&spec, &mut events, Actor::System, EventPayload::RunStarted);
    push(
        &spec,
        &mut events,
        Actor::System,
        EventPayload::UserMessage {
            text: spec.input.clone(),
        },
    );
    store.put_run(StoredRun {
        spec: spec.clone(),
        events: events.clone(),
    });

    let box_turn = spec.placement == ExecutionPlacement::Box;
    let name = box_container_name(spec.run_id);
    if box_turn {
        if let Err(SandboxError::Host(message)) = sandbox.provision(&name) {
            // No sandbox is running, so the turn can end: it is over.
            end_failed(
                store,
                &spec,
                FailureClass::Environment,
                format!("provision: {message}"),
            );
            return Err(TurnError::Sandbox(message));
        }
    }
    let completion = match &spec.work_model.credential {
        CredentialSource::PlatformGateway => {
            match poster.complete(&GatewayCall {
                run_id: spec.run_id,
                input: spec.input.clone(),
                model_name: spec.work_model.model_name.clone(),
                placement: spec.placement,
            }) {
                Ok(text) => Some(text),
                Err(message) => {
                    // The sandbox goes first. A turn whose sandbox could not be
                    // removed stays open: it never ends with a sandbox running.
                    let removed = !box_turn || sandbox.destroy(&name).is_ok();
                    if removed {
                        end_failed(
                            store,
                            &spec,
                            FailureClass::Dependency,
                            format!("proxy: {message}"),
                        );
                    }
                    return Err(TurnError::Proxy(message));
                }
            }
        }
        CredentialSource::BringYourOwn { .. } => None,
    };
    let credential_mode = match &spec.work_model.credential {
        CredentialSource::PlatformGateway => "gateway",
        CredentialSource::BringYourOwn { .. } => "subscription",
    };
    let computer = computer_for(
        spec.placement,
        spec.run_id,
        box_turn && sandbox.launches_docker(),
    );
    if let Some(text) = completion.as_deref() {
        // The sandbox goes before the completion is recorded, so a failed destroy
        // leaves an open turn and nothing to take back (formal/runlog/RunLog.tla).
        if box_turn {
            sandbox.destroy(&name).map_err(sandbox_error)?;
        }
        let mut appended = Vec::new();
        append_completion(&spec, &mut appended, text);
        record_completion(store, spec.run_id, appended.clone())?;
        events.extend(appended);
    }
    Ok(TurnOutcome {
        spec,
        events,
        completion,
        credential_mode,
        computer,
    })
}

pub fn accept_subscription_completion(
    store: &dyn RunStore,
    run_id: RunId,
    text: &str,
    sandbox: &dyn SandboxHost,
) -> Result<TurnOutcome, TurnError> {
    let text = text.trim();
    if text.is_empty() {
        return Err(TurnError::BadRequest("completion text is empty"));
    }
    let stored = store.run(run_id).ok_or(TurnError::NotFound)?;
    if matches!(
        stored.spec.work_model.credential,
        CredentialSource::PlatformGateway
    ) {
        return Err(TurnError::Conflict(
            "gateway completions come from the server",
        ));
    }
    check_open_turn(&stored)?;
    let box_turn = stored.spec.placement == ExecutionPlacement::Box;
    let name = box_container_name(stored.spec.run_id);
    if box_turn && !sandbox.exists(&name) {
        return Err(TurnError::Sandbox(
            "box sandbox is gone before the turn completed".to_string(),
        ));
    }
    if box_turn {
        sandbox.destroy(&name).map_err(sandbox_error)?;
    }
    let mut events = stored.events;
    let mut appended = Vec::new();
    append_completion(&stored.spec, &mut appended, text);
    record_completion(store, stored.spec.run_id, appended.clone())?;
    events.extend(appended);
    Ok(TurnOutcome {
        spec: stored.spec.clone(),
        events,
        completion: Some(text.to_string()),
        credential_mode: "subscription",
        computer: computer_plan(stored.spec.placement, stored.spec.run_id),
    })
}

/// A subscription turn is open when nothing has ended it and `open_turn` left the
/// harness running and unanswered. A queued run from `create_run` is idle, and a
/// run waiting on a tool has an answer outstanding; ending either would end a run
/// this turn never owned.
fn check_open_turn(stored: &StoredRun) -> Result<(), TurnError> {
    if stored
        .events
        .iter()
        .any(|event| is_terminal(&event.payload))
    {
        return Err(TurnError::Conflict("turn already completed"));
    }
    if !matches!(
        fold(&stored.spec, &stored.events).harness,
        HarnessState::Running {
            answered: false,
            ..
        }
    ) {
        return Err(TurnError::Conflict("turn is not open"));
    }
    if !stored
        .events
        .iter()
        .any(|event| matches!(event.payload, EventPayload::UserMessage { .. }))
    {
        return Err(TurnError::Conflict("user message is not recorded"));
    }
    Ok(())
}

/// The desktop reports that its turn failed (its model call through the
/// subscription proxy did not answer). Same order as a completion: check the
/// turn is open, remove its sandbox, then append `RunFailed`. A turn another
/// writer ended first is a conflict.
pub fn fail_turn(
    store: &dyn RunStore,
    run_id: RunId,
    message: &str,
    sandbox: &dyn SandboxHost,
) -> Result<TurnOutcome, TurnError> {
    let message = message.trim();
    if message.is_empty() {
        return Err(TurnError::BadRequest("failure message is empty"));
    }
    let stored = store.run(run_id).ok_or(TurnError::NotFound)?;
    if matches!(
        stored.spec.work_model.credential,
        CredentialSource::PlatformGateway
    ) {
        return Err(TurnError::Conflict("gateway turns end on the server"));
    }
    check_open_turn(&stored)?;
    let name = box_container_name(stored.spec.run_id);
    if stored.spec.placement == ExecutionPlacement::Box && sandbox.exists(&name) {
        sandbox.destroy(&name).map_err(sandbox_error)?;
    }
    let failed = run_failed_event(&stored.spec, FailureClass::Dependency, message.to_string());
    record_completion(store, run_id, vec![failed.clone()])?;
    let mut events = stored.events;
    events.push(failed);
    Ok(TurnOutcome {
        spec: stored.spec.clone(),
        events,
        completion: None,
        credential_mode: "subscription",
        computer: computer_plan(stored.spec.placement, stored.spec.run_id),
    })
}

/// Ends a turn that failed before anything else could end it. The store
/// refuses the append if another writer ended the run first.
fn end_failed(store: &dyn RunStore, spec: &RunSpec, class: FailureClass, message: String) {
    store.append_events(spec.run_id, vec![run_failed_event(spec, class, message)]);
}

/// Append the completion events. Two completions racing on one run get one
/// `Appended`; the store refuses the other because the log is already terminal.
fn record_completion(
    store: &dyn RunStore,
    run_id: RunId,
    events: Vec<Event>,
) -> Result<(), TurnError> {
    match store.append_events(run_id, events) {
        Append::Appended => Ok(()),
        Append::Terminal => Err(TurnError::Conflict("turn already completed")),
        Append::Missing => Err(TurnError::NotFound),
    }
}

fn sandbox_error(error: SandboxError) -> TurnError {
    let SandboxError::Host(message) = error;
    TurnError::Sandbox(message)
}

pub fn user_message_event(spec: &RunSpec) -> Event {
    Event::record(
        EventSource::new(
            spec.run_id,
            spec.agent_id,
            &spec.agent_version,
            Actor::System,
            Timestamp::now(),
        ),
        EventPayload::UserMessage {
            text: spec.input.clone(),
        },
    )
}

/// The terminal event for a run the server could not finish. Appending it ends
/// the run, so a failure never leaves the run open.
pub fn run_failed_event(spec: &RunSpec, class: FailureClass, message: String) -> Event {
    Event::record(
        EventSource::new(
            spec.run_id,
            spec.agent_id,
            &spec.agent_version,
            Actor::System,
            Timestamp::now(),
        ),
        EventPayload::RunFailed { class, message },
    )
}

fn append_completion(spec: &RunSpec, events: &mut Vec<Event>, text: &str) {
    push(
        spec,
        events,
        Actor::Gateway,
        EventPayload::ModelResponded {
            message: ModelMessage {
                role: MessageRole::Assistant,
                text: text.to_string(),
            },
        },
    );
    push(
        spec,
        events,
        Actor::System,
        EventPayload::RunCompleted {
            outcome: text.to_string(),
        },
    );
}

fn push(spec: &RunSpec, events: &mut Vec<Event>, actor: Actor, payload: EventPayload) {
    events.push(Event::record(
        EventSource::new(
            spec.run_id,
            spec.agent_id,
            &spec.agent_version,
            actor,
            Timestamp::now(),
        ),
        payload,
    ));
}

fn placement_name(placement: ExecutionPlacement) -> &'static str {
    match placement {
        ExecutionPlacement::Local => "Local",
        ExecutionPlacement::Reverse => "Reverse",
        ExecutionPlacement::Box => "Box",
    }
}

pub type SharedPoster = Arc<dyn GatewayPoster>;
