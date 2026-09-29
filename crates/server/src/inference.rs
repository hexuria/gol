use std::collections::HashSet;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

use harness::StoreError;
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
    /// How long one completion may take, from connect to the last byte of
    /// the answer. A gateway that never answers would otherwise hold its
    /// caller for good: a request, or a queue worker whose heartbeat keeps
    /// renewing its lease (Phase 3.6).
    pub timeout: std::time::Duration,
}

/// `HttpGatewayPoster::from_env`'s timeout: 10 minutes.
pub const GATEWAY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

impl HttpGatewayPoster {
    pub fn from_env() -> Self {
        Self {
            url: std::env::var("GOL_PROXY_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:43124".to_string()),
            token: std::env::var("GOL_GATEWAY_TOKEN")
                .unwrap_or_else(|_| "gol-gateway-local".to_string()),
            timeout: GATEWAY_TIMEOUT,
        }
    }
}

impl GatewayPoster for HttpGatewayPoster {
    fn complete(&self, call: &GatewayCall) -> Result<String, String> {
        ensure_fixture_proxy(&self.url)?;
        let endpoint = format!("{}/v1/gateway/complete", self.url.trim_end_matches('/'));
        let response = ureq::post(&endpoint)
            .timeout(self.timeout)
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
    /// Whether the host reports sandbox `name`. A host that cannot be asked
    /// answers `false`, so this is not a safety answer: deciding whether a turn
    /// may end without removing its sandbox goes through `absent`.
    fn exists(&self, name: &str) -> bool;
    /// `Ok(true)` when the host confirms no sandbox `name` is left, `Ok(false)`
    /// when one is, and an error when the host cannot tell. Only a confirmed
    /// absence lets a turn end without removing its sandbox. Every host answers
    /// it itself: `exists` cannot tell "gone" from "cannot ask".
    fn absent(&self, name: &str) -> Result<bool, SandboxError>;
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

    fn absent(&self, name: &str) -> Result<bool, SandboxError> {
        Ok(!self.exists(name))
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
        self.run(args)
            .map_err(|message| SandboxError::Host(last_line(&message)))
    }

    fn run(&self, args: &[&str]) -> Result<(), String> {
        let owned: Vec<String> = args.iter().map(|arg| (*arg).to_string()).collect();
        self.command.run(&owned)
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

    /// Absent only when Docker answers that there is no such container. Any
    /// other failure (the daemon is unreachable, say) says nothing about it.
    fn absent(&self, name: &str) -> Result<bool, SandboxError> {
        match self.run(&["inspect", "--type", "container", name]) {
            Ok(()) => Ok(false),
            Err(message) if message.contains("No such container") => Ok(true),
            Err(message) => Err(SandboxError::Host(last_line(&message))),
        }
    }

    fn launches_docker(&self) -> bool {
        true
    }
}

/// The longest Docker error, in characters, a failure keeps.
const MAX_DOCKER_ERROR: usize = 512;

/// The last non-empty line of a Docker error, cut to `MAX_DOCKER_ERROR`
/// characters. Docker's stderr can carry a whole image pull before the error,
/// and the error ends up in the run's log.
fn last_line(message: &str) -> String {
    let line = message
        .lines()
        .map(str::trim)
        .rfind(|line| !line.is_empty())
        .unwrap_or("");
    if line.chars().count() <= MAX_DOCKER_ERROR {
        return line.to_string();
    }
    let kept: String = line.chars().take(MAX_DOCKER_ERROR).collect();
    format!("{kept}…")
}

fn docker(args: &[String]) -> Result<(), String> {
    let output = Command::new("docker")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .map_err(|error| error.to_string())?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "docker {args:?} exited {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ))
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
    /// The run store could not answer (StoreError); nothing is retried.
    Store(String),
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
    store
        .put_run(StoredRun {
            spec: spec.clone(),
            events: events.clone(),
        })
        .map_err(store_error)?;
    finish_turn(store, spec, events, poster, sandbox)
}

/// The metadata key the server sets on a background coworker turn (Phase
/// 3.6, decision 62A): a queue worker runs it as a turn, not through the
/// harness. A caller that sends it is refused.
pub const TURN_KEY: &str = "gol.turn";

/// Whether `spec` is a background coworker turn.
pub fn is_turn(spec: &RunSpec) -> bool {
    spec.metadata.contains_key(TURN_KEY)
}

/// The rest of a turn once its user message is stored (`events`, the log so
/// far): a Box sandbox, the gateway's completion, the sandbox removed, then
/// the completion stored; or the turn failed. `open_turn` runs it in the
/// request; a queue worker runs it for a background turn (Phase 3.6).
pub(crate) fn finish_turn(
    store: &dyn RunStore,
    spec: RunSpec,
    mut events: Vec<Event>,
    poster: &dyn GatewayPoster,
    sandbox: &dyn SandboxHost,
) -> Result<TurnOutcome, TurnError> {
    let box_turn = spec.placement == ExecutionPlacement::Box;
    let name = box_container_name(spec.run_id);
    if box_turn {
        if let Err(SandboxError::Host(message)) = sandbox.provision(&name) {
            // The turn is over. It ends only if the host confirms no sandbox
            // was left behind (a failed start whose cleanup also failed leaves
            // one, and an unreachable host cannot say): a turn never ends with
            // its sandbox running.
            if matches!(sandbox.absent(&name), Ok(true)) {
                end_failed(
                    store,
                    &spec,
                    FailureClass::Environment,
                    format!("provision: {message}"),
                );
            }
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
    let stored = store
        .run(run_id)
        .map_err(store_error)?
        .ok_or(TurnError::NotFound)?;
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
    if box_turn && sandbox.absent(&name).map_err(sandbox_error)? {
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
    let stored = store
        .run(run_id)
        .map_err(store_error)?
        .ok_or(TurnError::NotFound)?;
    if matches!(
        stored.spec.work_model.credential,
        CredentialSource::PlatformGateway
    ) {
        return Err(TurnError::Conflict("gateway turns end on the server"));
    }
    check_open_turn(&stored)?;
    let name = box_container_name(stored.spec.run_id);
    if stored.spec.placement == ExecutionPlacement::Box
        && !sandbox.absent(&name).map_err(sandbox_error)?
    {
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
    // Best effort: the caller already reports the failure that led here. A
    // store that cannot take this append leaves the turn open, as a failed
    // destroy does, and the operator sees why.
    if let Err(error) =
        store.append_events(spec.run_id, vec![run_failed_event(spec, class, message)])
    {
        eprintln!("gol: could not end run {}: {error}", spec.run_id);
    }
}

/// Append the completion events. Two completions racing on one run get one
/// `Appended`; the store refuses the other because the log is already terminal.
fn record_completion(
    store: &dyn RunStore,
    run_id: RunId,
    events: Vec<Event>,
) -> Result<(), TurnError> {
    match store.append_events(run_id, events).map_err(store_error)? {
        Append::Appended => Ok(()),
        Append::Terminal => Err(TurnError::Conflict("turn already completed")),
        Append::Missing => Err(TurnError::NotFound),
        // Only `append_events_after` reports a moved log.
        Append::Moved => Err(TurnError::Store("the run log moved".to_string())),
    }
}

fn store_error(error: StoreError) -> TurnError {
    TurnError::Store(error.to_string())
}

fn sandbox_error(error: SandboxError) -> TurnError {
    let SandboxError::Host(message) = error;
    TurnError::Sandbox(message)
}

pub fn user_message_event(spec: &RunSpec) -> Event {
    Event::record(
        EventSource::for_spec(spec, Actor::System, Timestamp::now()),
        EventPayload::UserMessage {
            text: spec.input.clone(),
        },
    )
}

/// A `payload` event from the system, for `spec`'s run.
pub(crate) fn system_event(spec: &RunSpec, payload: EventPayload) -> Event {
    Event::record(
        EventSource::for_spec(spec, Actor::System, Timestamp::now()),
        payload,
    )
}

/// What the Redis path of `POST /v1/runs` stores before it pushes the run:
/// created, queued, and the user's message.
pub fn queued_events(spec: &RunSpec) -> Vec<Event> {
    vec![
        system_event(spec, EventPayload::RunCreated),
        system_event(spec, EventPayload::RunQueued),
        user_message_event(spec),
    ]
}

/// What a worker records ahead of the harness's events: the queued run is
/// scheduled, provisioned and starting, so the driver's `RunStarted` takes
/// dispatch to `Running`.
pub(crate) fn dispatch_events(spec: &RunSpec) -> Vec<Event> {
    vec![
        system_event(spec, EventPayload::RunScheduled),
        system_event(spec, EventPayload::RunProvisioning),
        system_event(spec, EventPayload::RunStarting),
    ]
}

/// The event that ends a stopped run (Phase 3.4).
pub fn run_cancelled_event(spec: &RunSpec) -> Event {
    Event::record(
        EventSource::for_spec(spec, Actor::System, Timestamp::now()),
        EventPayload::RunCancelled,
    )
}

/// The terminal event for a run the server could not finish. Appending it ends
/// the run, so a failure never leaves the run open.
pub fn run_failed_event(spec: &RunSpec, class: FailureClass, message: String) -> Event {
    Event::record(
        EventSource::for_spec(spec, Actor::System, Timestamp::now()),
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
            // A coworker turn's completion arrives as text alone.
            usage: None,
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
        EventSource::for_spec(spec, actor, Timestamp::now()),
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
