use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{FromRequestParts, Path, Query, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use harness::{
    run_to_completion, BootError, Driver, EchoTool, InMemory, JevDecider, Memory, RunMemory,
    StoreError, UnavailableModel,
};
use protocol::{
    fold, AgentId, Capability, Event, EventPayload, ExecutionPlacement, FailureClass, Limits,
    Owner, RunId, RunSpec, RunState, WorkModel,
};
use serde::{Deserialize, Serialize};

use crate::auth::{AuthError, Authenticator, Principal};
use crate::inference::{
    accept_subscription_completion, fail_turn, open_turn, run_failed_event, sandbox_from_env,
    ComputerPlan, GatewayPoster, HttpGatewayPoster, SandboxHost, SharedPoster, TurnError,
    TurnOutcome,
};
use crate::queue::RedisRunQueue;
use crate::store::{AgentManifest, PutAgent, RunStore, StoredAgent, StoredRun};
use crate::surface::{ag_ui_events, json_render_spec};

#[derive(Clone)]
struct AppState {
    store: Arc<dyn RunStore>,
    memory: Arc<dyn Memory>,
    jev_base_url: String,
    redis_url: Option<String>,
    poster: SharedPoster,
    sandbox: Arc<dyn SandboxHost>,
    auth: Arc<dyn Authenticator>,
}

/// `POST /v1/runs`. Capabilities come from the stored manifest, so the body
/// has none, and any unknown field is refused.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunBody {
    agent_id: AgentId,
    agent_version: String,
    input: String,
    placement: ExecutionPlacement,
    work_model: WorkModel,
    limits: Option<Limits>,
    #[serde(default)]
    metadata: BTreeMap<String, String>,
}

/// `POST /v1/coworker/turns`. A desktop turn has no stored manifest, so it
/// names its own capabilities (owner decision for B2).
#[derive(Debug, Deserialize)]
struct TurnRequest {
    agent_id: AgentId,
    agent_version: String,
    input: String,
    placement: ExecutionPlacement,
    work_model: WorkModel,
    #[serde(default)]
    capabilities: Vec<Capability>,
    limits: Option<Limits>,
    #[serde(default)]
    metadata: BTreeMap<String, String>,
}

/// The owner a request's principal becomes.
fn owner_of(principal: &Principal) -> Owner {
    Owner::new(
        principal.issuer.clone(),
        principal.subject.clone(),
        principal.tenant.clone(),
    )
}

/// The run `id`, if the caller owns it. A run another principal owns is
/// reported exactly like a missing one.
async fn owned_run(
    state: &AppState,
    id: RunId,
    principal: &Principal,
) -> Result<StoredRun, ApiError> {
    owned(state, principal, move |store| store.run(id)).await
}

/// `owned_run` with only the events after the first `after`, at most `limit`.
async fn owned_run_page(
    state: &AppState,
    id: RunId,
    principal: &Principal,
    after: usize,
    limit: usize,
) -> Result<StoredRun, ApiError> {
    owned(state, principal, move |store| {
        store.run_page(id, after, limit)
    })
    .await
}

async fn owned(
    state: &AppState,
    principal: &Principal,
    read: impl FnOnce(&dyn RunStore) -> Result<Option<StoredRun>, StoreError> + Send + 'static,
) -> Result<StoredRun, ApiError> {
    let store = state.store.clone();
    let owner = owner_of(principal);
    tokio::task::spawn_blocking(move || read(store.as_ref()))
        .await
        .map_err(|error| ApiError::Decider(error.to_string()))?
        .map_err(ApiError::from)?
        .filter(|run| run.spec.owner.is(&owner))
        .ok_or(ApiError::NotFound)
}

pub fn router(
    store: Arc<dyn RunStore>,
    jev_base_url: impl Into<String>,
    auth: Arc<dyn Authenticator>,
) -> Router {
    router_with_queue(store, jev_base_url, None, auth)
}

pub fn router_with_queue(
    store: Arc<dyn RunStore>,
    jev_base_url: impl Into<String>,
    redis_url: Option<String>,
    auth: Arc<dyn Authenticator>,
) -> Router {
    router_with_parts(
        store,
        Arc::new(InMemory::default()),
        jev_base_url,
        redis_url,
        Arc::new(HttpGatewayPoster::from_env()),
        sandbox_from_env(),
        auth,
    )
}

pub fn router_with_gateway(
    store: Arc<dyn RunStore>,
    jev_base_url: impl Into<String>,
    poster: Arc<dyn GatewayPoster>,
    auth: Arc<dyn Authenticator>,
) -> Router {
    router_with_sandbox(store, jev_base_url, poster, sandbox_from_env(), auth)
}

pub fn router_with_sandbox(
    store: Arc<dyn RunStore>,
    jev_base_url: impl Into<String>,
    poster: Arc<dyn GatewayPoster>,
    sandbox: Arc<dyn SandboxHost>,
    auth: Arc<dyn Authenticator>,
) -> Router {
    router_with_parts(
        store,
        Arc::new(InMemory::default()),
        jev_base_url,
        None,
        poster,
        sandbox,
        auth,
    )
}

/// The server's router: runs in `store`, and every run's memory in `memory`,
/// shared across runs so agent, user and organization memory outlive a run.
/// With `redis_url`, `POST /v1/runs` queues the run for the workers instead
/// of running it.
pub fn router_with_memory(
    store: Arc<dyn RunStore>,
    memory: Arc<dyn Memory>,
    jev_base_url: impl Into<String>,
    redis_url: Option<String>,
    poster: Arc<dyn GatewayPoster>,
    auth: Arc<dyn Authenticator>,
) -> Router {
    router_with_parts(
        store,
        memory,
        jev_base_url,
        redis_url,
        poster,
        sandbox_from_env(),
        auth,
    )
}

fn router_with_parts(
    store: Arc<dyn RunStore>,
    memory: Arc<dyn Memory>,
    jev_base_url: impl Into<String>,
    redis_url: Option<String>,
    poster: SharedPoster,
    sandbox: Arc<dyn SandboxHost>,
    auth: Arc<dyn Authenticator>,
) -> Router {
    Router::new()
        .route("/v1/agents", post(create_agent))
        .route("/v1/runs", post(create_run))
        .route("/v1/runs/{id}", get(get_run))
        .route("/v1/runs/{id}/events", get(get_events))
        .route("/v1/runs/{id}/ag-ui", get(get_ag_ui))
        .route("/v1/runs/{id}/ui", get(get_ui))
        .route("/v1/coworker/turns", post(create_coworker_turn))
        .route(
            "/v1/coworker/turns/{id}/completion",
            post(complete_coworker_turn),
        )
        .route("/v1/coworker/turns/{id}/fail", post(fail_coworker_turn))
        .with_state(AppState {
            store,
            memory,
            jev_base_url: jev_base_url.into(),
            redis_url,
            poster,
            sandbox,
            auth,
        })
}

/// The caller, from a bearer token the server's authenticator accepted.
struct Authenticated(Principal);

impl FromRequestParts<AppState> for Authenticated {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let token = bearer(&parts.headers).ok_or(ApiError::Unauthorized)?;
        let auth = state.auth.clone();
        // The authenticator may fetch the issuer's keys, which blocks.
        let checked = tokio::task::spawn_blocking(move || auth.authenticate(&token))
            .await
            .map_err(|error| ApiError::AuthUnavailable(error.to_string()))?;
        match checked {
            Ok(principal) => Ok(Self(principal)),
            Err(AuthError::Unauthorized) => Err(ApiError::Unauthorized),
            Err(AuthError::Unavailable(message)) => Err(ApiError::AuthUnavailable(message)),
        }
    }
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    let value = header_value(headers, "authorization")?;
    let token = value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))?;
    let token = token.trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_string())
    }
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let value = headers.get(name)?.to_str().ok()?.trim().to_string();
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

async fn create_agent(
    Authenticated(principal): Authenticated,
    State(state): State<AppState>,
    Json(agent): Json<AgentManifest>,
) -> Result<Json<AgentManifest>, ApiError> {
    let store = state.store.clone();
    let stored = StoredAgent {
        manifest: agent.clone(),
        owner: owner_of(&principal),
    };
    match tokio::task::spawn_blocking(move || store.put_agent(stored))
        .await
        .map_err(|error| ApiError::Decider(error.to_string()))?
        .map_err(ApiError::from)?
    {
        PutAgent::Stored => Ok(Json(agent)),
        PutAgent::OwnedByOther => Err(ApiError::Conflict("agent belongs to another principal")),
    }
}

async fn create_run(
    Authenticated(principal): Authenticated,
    State(state): State<AppState>,
    body: Result<Json<RunBody>, JsonRejection>,
) -> Result<Json<RunState>, ApiError> {
    // An unknown field (such as `capabilities`), a bad value or bad JSON is a
    // 400; any other rejection (content type, size) keeps its own status.
    let Json(body) = body.map_err(|rejection| match rejection {
        JsonRejection::JsonDataError(_) | JsonRejection::JsonSyntaxError(_) => {
            ApiError::InvalidBody(rejection.body_text())
        }
        other => ApiError::Rejected(other),
    })?;
    let owner = owner_of(&principal);
    let store = state.store.clone();
    let agent_id = body.agent_id;
    let agent = tokio::task::spawn_blocking(move || store.agent(agent_id))
        .await
        .map_err(|error| ApiError::Decider(error.to_string()))?
        .map_err(ApiError::from)?
        .filter(|agent| agent.owner.is(&owner))
        .ok_or(ApiError::AgentNotFound)?;
    if agent.manifest.version != body.agent_version {
        return Err(ApiError::Conflict(
            "agent_version does not match the stored manifest",
        ));
    }
    let spec = build_spec(
        owner,
        SpecCore {
            agent_id: body.agent_id,
            agent_version: body.agent_version,
            input: body.input,
            placement: body.placement,
            work_model: body.work_model,
            limits: body.limits,
            metadata: body.metadata,
        },
        agent.manifest.required_capabilities,
    )?;
    if let Some(url) = state.redis_url.clone() {
        // Created and queued are on the record before the push, so a worker
        // that takes the run finds them (C4).
        let queued = crate::inference::queued_events(&spec);
        let store = state.store.clone();
        let spec_for_store = spec.clone();
        let queued_for_store = queued.clone();
        tokio::task::spawn_blocking(move || {
            store.put_run(StoredRun {
                spec: spec_for_store,
                events: queued_for_store,
            })
        })
        .await
        .map_err(|error| ApiError::Decider(error.to_string()))?
        .map_err(ApiError::from)?;
        let run_id = spec.run_id;
        let store = state.store.clone();
        let spec_for_queue = spec.clone();
        tokio::task::spawn_blocking(move || {
            RedisRunQueue::open(url).push(run_id).map_err(|error| {
                // The run is stored but will never be picked up: end it, so it is
                // not left open.
                let failed = run_failed_event(
                    &spec_for_queue,
                    FailureClass::Infrastructure,
                    format!("queue push failed: {error}"),
                );
                if let Err(store_error) = store.append_events(run_id, vec![failed]) {
                    eprintln!("gol: could not end run {run_id}: {store_error}");
                }
                error
            })
        })
        .await
        .map_err(|error| ApiError::Decider(error.to_string()))?
        .map_err(ApiError::Decider)?;
        return Ok(Json(fold(&spec, &queued)));
    }

    let jev_base_url = state.jev_base_url.clone();
    let spec_for_run = spec.clone();
    let store_for_run = state.store.clone();
    let memory = state.memory.clone();
    let stored = match tokio::task::spawn_blocking(move || {
        // The user message is on the record before the harness asks Jev.
        let message = crate::inference::user_message_event(&spec_for_run);
        let run_id = spec_for_run.run_id;
        store_for_run
            .put_run(StoredRun {
                spec: spec_for_run.clone(),
                events: vec![message],
            })
            .map_err(RunStartError::Store)?;
        let (events, outcome) = harness_events(&jev_base_url, &spec_for_run, memory.as_ref());
        // Append, never overwrite: anything stored while Jev ran stays. When the
        // run is already terminal the store keeps its log and refuses these.
        store_for_run
            .append_events(run_id, events)
            .map_err(RunStartError::Store)?;
        outcome?;
        store_for_run
            .run(run_id)
            .map_err(RunStartError::Store)?
            .ok_or_else(|| RunStartError::Decider("run is not stored".to_string()))
    })
    .await
    {
        Ok(Ok(stored)) => stored,
        Ok(Err(RunStartError::Unsupported(placement))) => {
            return Err(ApiError::Unsupported(placement));
        }
        Ok(Err(RunStartError::Decider(message))) => return Err(ApiError::Decider(message)),
        Ok(Err(RunStartError::Store(error))) => return Err(ApiError::from(error)),
        Err(error) => return Err(ApiError::Decider(error.to_string())),
    };
    Ok(Json(fold(&stored.spec, &stored.events)))
}

async fn create_coworker_turn(
    Authenticated(principal): Authenticated,
    State(state): State<AppState>,
    Json(body): Json<TurnRequest>,
) -> Result<Json<TurnBody>, ApiError> {
    let spec = build_spec(
        owner_of(&principal),
        SpecCore {
            agent_id: body.agent_id,
            agent_version: body.agent_version,
            input: body.input,
            placement: body.placement,
            work_model: body.work_model,
            limits: body.limits,
            metadata: body.metadata,
        },
        body.capabilities,
    )?;
    let store = state.store.clone();
    let poster = state.poster.clone();
    let sandbox = state.sandbox.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        open_turn(store.as_ref(), spec, poster.as_ref(), sandbox.as_ref())
    })
    .await
    .map_err(|error| ApiError::Decider(error.to_string()))?
    .map_err(ApiError::from)?;
    Ok(Json(TurnBody::from_outcome(outcome)))
}

async fn complete_coworker_turn(
    Authenticated(principal): Authenticated,
    State(state): State<AppState>,
    Path(id): Path<RunId>,
    Json(body): Json<CompletionBody>,
) -> Result<Json<TurnBody>, ApiError> {
    // The owner never changes, so checking it before the completion is exact.
    owned_run(&state, id, &principal).await?;
    let store = state.store.clone();
    let sandbox = state.sandbox.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        accept_subscription_completion(store.as_ref(), id, &body.text, sandbox.as_ref())
    })
    .await
    .map_err(|error| ApiError::Decider(error.to_string()))?
    .map_err(ApiError::from)?;
    Ok(Json(TurnBody::from_outcome(outcome)))
}

#[derive(Debug, Deserialize)]
struct CompletionBody {
    text: String,
}

async fn fail_coworker_turn(
    Authenticated(principal): Authenticated,
    State(state): State<AppState>,
    Path(id): Path<RunId>,
    Json(body): Json<FailBody>,
) -> Result<Json<TurnBody>, ApiError> {
    owned_run(&state, id, &principal).await?;
    let store = state.store.clone();
    let sandbox = state.sandbox.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        fail_turn(store.as_ref(), id, &body.message, sandbox.as_ref())
    })
    .await
    .map_err(|error| ApiError::Decider(error.to_string()))?
    .map_err(ApiError::from)?;
    Ok(Json(TurnBody::from_outcome(outcome)))
}

#[derive(Debug, Deserialize)]
struct FailBody {
    message: String,
}

#[derive(Debug, Serialize)]
struct TurnBody {
    run_id: RunId,
    placement: ExecutionPlacement,
    credential_mode: &'static str,
    user_message: String,
    completion: Option<String>,
    computer: ComputerBody,
}

#[derive(Debug, Serialize)]
struct ComputerBody {
    image: String,
    started_by: &'static str,
    command: String,
    started: bool,
    name: String,
}

impl TurnBody {
    fn from_outcome(outcome: TurnOutcome) -> Self {
        let user_message = outcome
            .events
            .iter()
            .find_map(|event| match &event.payload {
                EventPayload::UserMessage { text } => Some(text.clone()),
                _ => None,
            })
            .unwrap_or_else(|| outcome.spec.input.clone());
        Self {
            run_id: outcome.spec.run_id,
            placement: outcome.spec.placement,
            credential_mode: outcome.credential_mode,
            user_message,
            completion: outcome.completion,
            computer: ComputerBody::from_plan(outcome.computer),
        }
    }
}

impl ComputerBody {
    fn from_plan(plan: ComputerPlan) -> Self {
        Self {
            image: plan.image,
            started_by: plan.started_by,
            command: plan.command,
            started: plan.started,
            name: plan.name,
        }
    }
}

async fn get_run(
    Authenticated(principal): Authenticated,
    State(state): State<AppState>,
    Path(id): Path<RunId>,
) -> Result<Json<RunState>, ApiError> {
    let stored = owned_run(&state, id, &principal).await?;
    Ok(Json(fold(&stored.spec, &stored.events)))
}

/// The most events one page of `GET /v1/runs/{id}/events` holds, and the
/// page size when the caller names none (owner decision 4A for C2).
const EVENTS_PAGE_MAX: usize = 500;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventsQuery {
    /// How many events the caller has already seen.
    #[serde(default)]
    after: usize,
    limit: Option<usize>,
}

async fn get_events(
    Authenticated(principal): Authenticated,
    State(state): State<AppState>,
    Path(id): Path<RunId>,
    query: Result<Query<EventsQuery>, QueryRejection>,
) -> Result<Json<Vec<Event>>, ApiError> {
    let Query(query) = query.map_err(|_| ApiError::BadRequest("after and limit must be counts"))?;
    let limit = query.limit.unwrap_or(EVENTS_PAGE_MAX);
    if limit == 0 || limit > EVENTS_PAGE_MAX {
        return Err(ApiError::BadRequest("limit must be between 1 and 500"));
    }
    let stored = owned_run_page(&state, id, &principal, query.after, limit).await?;
    Ok(Json(stored.events))
}

async fn get_ag_ui(
    Authenticated(principal): Authenticated,
    State(state): State<AppState>,
    Path(id): Path<RunId>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let stored = owned_run(&state, id, &principal).await?;
    Ok(Json(serde_json::Value::Array(ag_ui_events(
        stored.spec.run_id,
        &stored.events,
    ))))
}

async fn get_ui(
    Authenticated(principal): Authenticated,
    State(state): State<AppState>,
    Path(id): Path<RunId>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let stored = owned_run(&state, id, &principal).await?;
    let folded = fold(&stored.spec, &stored.events);
    // A run can end before its harness starts (a failed queue push records
    // RunFailed on a harness still Idle), so fall back to the dispatch phase.
    let outcome = match (&folded.harness, &folded.dispatch) {
        (protocol::HarnessState::Completed { outcome }, _)
        | (_, protocol::DispatchPhase::Completed { outcome }) => outcome.clone(),
        (protocol::HarnessState::Failed { message, .. }, _)
        | (_, protocol::DispatchPhase::Failed { message, .. }) => message.clone(),
        (protocol::HarnessState::Cancelled, _) | (_, protocol::DispatchPhase::Cancelled) => {
            "cancelled".to_string()
        }
        (_, protocol::DispatchPhase::Expired) => "expired".to_string(),
        _ => "running".to_string(),
    };
    Ok(Json(json_render_spec(&stored.spec.input, &outcome)))
}

/// Runs the harness with Jev and returns every event it recorded, with how the
/// run ended. On an error the events are still returned: they are what the
/// harness did before it stopped.
/// Runs the harness with Jev and returns the events to record: what the
/// harness did, and when it could not finish, the `RunFailed` that ends the
/// run instead of leaving it open.
pub(crate) fn harness_events(
    jev_base_url: &str,
    spec: &RunSpec,
    memory: &dyn Memory,
) -> (Vec<Event>, Result<(), RunStartError>) {
    let (mut events, outcome) = run_with_jev(jev_base_url, spec.clone(), memory);
    if let Err(error) = &outcome {
        let (class, message) = match error {
            RunStartError::Unsupported(placement) => (
                FailureClass::Environment,
                format!("unsupported placement: {placement:?}"),
            ),
            RunStartError::Decider(message) => {
                (FailureClass::Dependency, format!("decider: {message}"))
            }
            RunStartError::Store(_) => (
                FailureClass::Infrastructure,
                "store unavailable".to_string(),
            ),
        };
        events.push(run_failed_event(spec, class, message));
    }
    (events, outcome)
}

fn run_with_jev(
    jev_base_url: &str,
    spec: RunSpec,
    memory: &dyn Memory,
) -> (Vec<Event>, Result<(), RunStartError>) {
    let mut driver = match Driver::boot(spec) {
        Ok(driver) => driver,
        Err(BootError::UnsupportedPlacement(placement)) => {
            return (Vec::new(), Err(RunStartError::Unsupported(placement)));
        }
    };
    let client = match jev_client(jev_base_url) {
        Ok(client) => client,
        Err(message) => return (Vec::new(), Err(RunStartError::Decider(message))),
    };
    let mut decider = JevDecider::new(client);
    let echo = EchoTool;
    let outcome = run_to_completion(
        &mut driver,
        &mut decider,
        &[&echo],
        &UnavailableModel,
        &RunMemory::new(memory),
    )
    .map_err(|error| RunStartError::Decider(error.message));
    (driver.events().to_vec(), outcome)
}

fn jev_client(base_url: &str) -> Result<typesafe_sdk::blocking::Client, String> {
    typesafe_sdk::blocking::Client::builder()
        .api_key("gol")
        .base_url(base_url)
        .retry(typesafe_sdk::RetryPolicy::disabled())
        .build()
        .map_err(|error| error.to_string())
}

pub(crate) enum RunStartError {
    Unsupported(ExecutionPlacement),
    Decider(String),
    Store(StoreError),
}

/// The most steps or model calls a client may ask for. With `PlatformGateway` the
/// platform pays for every model call, so a request cannot authorize an unbounded run.
const MAX_LIMIT: u32 = 64;

/// The fields a run and a coworker turn share.
struct SpecCore {
    agent_id: AgentId,
    agent_version: String,
    input: String,
    placement: ExecutionPlacement,
    work_model: WorkModel,
    limits: Option<Limits>,
    metadata: BTreeMap<String, String>,
}

fn build_spec(
    owner: Owner,
    core: SpecCore,
    capabilities: Vec<Capability>,
) -> Result<RunSpec, ApiError> {
    let limits = core.limits.unwrap_or(Limits {
        max_steps: 8,
        max_model_calls: 4,
    });
    let bounded = 1..=MAX_LIMIT;
    if !bounded.contains(&limits.max_steps) || !bounded.contains(&limits.max_model_calls) {
        return Err(ApiError::BadRequest("limits must be between 1 and 64"));
    }
    Ok(RunSpec::builder()
        .owner(owner)
        .agent(core.agent_id, core.agent_version)
        .input(core.input)
        .placement(core.placement)
        .work_model(core.work_model)
        .capabilities(capabilities)
        .limits(limits)
        .metadata(core.metadata)
        .build())
}

enum ApiError {
    Unsupported(ExecutionPlacement),
    Decider(String),
    NotFound,
    AgentNotFound,
    /// The store could not answer: 503, with the detail on stderr only.
    Store(String),
    Proxy(String),
    Sandbox(String),
    Conflict(&'static str),
    BadRequest(&'static str),
    InvalidBody(String),
    Rejected(JsonRejection),
    Unauthorized,
    AuthUnavailable(String),
}

impl From<StoreError> for ApiError {
    fn from(error: StoreError) -> Self {
        Self::Store(error.to_string())
    }
}

impl From<TurnError> for ApiError {
    fn from(error: TurnError) -> Self {
        match error {
            TurnError::Store(message) => Self::Store(message),
            TurnError::Proxy(message) => Self::Proxy(message),
            TurnError::Sandbox(message) => Self::Sandbox(message),
            TurnError::NotFound => Self::NotFound,
            TurnError::Conflict(message) => Self::Conflict(message),
            TurnError::BadRequest(message) => Self::BadRequest(message),
        }
    }
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        match self {
            Self::Unsupported(placement) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({
                    "error": "unsupported placement",
                    "placement": placement,
                })),
            )
                .into_response(),
            Self::Decider(message) => (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": message })),
            )
                .into_response(),
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "run not found" })),
            )
                .into_response(),
            Self::Store(message) => {
                eprintln!("gol: store unavailable: {message}");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({ "error": "store unavailable" })),
                )
                    .into_response()
            }
            Self::AgentNotFound => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "agent not found" })),
            )
                .into_response(),
            Self::Proxy(message) => (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": message })),
            )
                .into_response(),
            Self::Sandbox(message) => (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({ "error": message })),
            )
                .into_response(),
            Self::Conflict(message) => (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": message })),
            )
                .into_response(),
            Self::BadRequest(message) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": message })),
            )
                .into_response(),
            Self::InvalidBody(message) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": message })),
            )
                .into_response(),
            Self::Rejected(rejection) => rejection.into_response(),
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "unauthorized" })),
            )
                .into_response(),
            Self::AuthUnavailable(message) => {
                // The detail is for the operator, not the caller.
                eprintln!("gol: identity provider unavailable: {message}");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({ "error": "identity provider unavailable" })),
                )
                    .into_response()
            }
        }
    }
}
