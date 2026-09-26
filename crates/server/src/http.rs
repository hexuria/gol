use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{FromRequestParts, Path, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use harness::{
    run_to_completion, BootError, Driver, EchoTool, InMemory, JevDecider, UnavailableModel,
};
use protocol::{
    fold, AgentId, Capability, Event, EventPayload, ExecutionPlacement, FailureClass, Limits,
    RunId, RunSpec, RunState, WorkModel,
};
use serde::{Deserialize, Serialize};

use crate::inference::{
    accept_subscription_completion, open_turn, run_failed_event, sandbox_from_env, ComputerPlan,
    GatewayPoster, HttpGatewayPoster, SandboxHost, SharedPoster, TurnError, TurnOutcome,
};
use crate::queue::RedisRunQueue;
use crate::store::{AgentManifest, RunStore, StoredRun};
use crate::surface::{ag_ui_events, json_render_spec};

#[derive(Clone)]
struct AppState {
    store: Arc<dyn RunStore>,
    jev_base_url: String,
    redis_url: Option<String>,
    poster: SharedPoster,
    sandbox: Arc<dyn SandboxHost>,
}

#[derive(Debug, Deserialize)]
struct RunBody {
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

pub fn router(store: Arc<dyn RunStore>, jev_base_url: impl Into<String>) -> Router {
    router_with_queue(store, jev_base_url, None)
}

pub fn router_with_queue(
    store: Arc<dyn RunStore>,
    jev_base_url: impl Into<String>,
    redis_url: Option<String>,
) -> Router {
    router_with_parts(
        store,
        jev_base_url,
        redis_url,
        Arc::new(HttpGatewayPoster::from_env()),
        sandbox_from_env(),
    )
}

pub fn router_with_gateway(
    store: Arc<dyn RunStore>,
    jev_base_url: impl Into<String>,
    poster: Arc<dyn GatewayPoster>,
) -> Router {
    router_with_sandbox(store, jev_base_url, poster, sandbox_from_env())
}

pub fn router_with_sandbox(
    store: Arc<dyn RunStore>,
    jev_base_url: impl Into<String>,
    poster: Arc<dyn GatewayPoster>,
    sandbox: Arc<dyn SandboxHost>,
) -> Router {
    router_with_parts(store, jev_base_url, None, poster, sandbox)
}

fn router_with_parts(
    store: Arc<dyn RunStore>,
    jev_base_url: impl Into<String>,
    redis_url: Option<String>,
    poster: SharedPoster,
    sandbox: Arc<dyn SandboxHost>,
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
        .with_state(AppState {
            store,
            jev_base_url: jev_base_url.into(),
            redis_url,
            poster,
            sandbox,
        })
}

struct Bearer(String);

impl<S> FromRequestParts<S> for Bearer
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        bearer(&parts.headers)
            .map(Self)
            .ok_or(ApiError::Unauthorized)
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
    Bearer(_principal): Bearer,
    State(state): State<AppState>,
    Json(agent): Json<AgentManifest>,
) -> Result<Json<AgentManifest>, ApiError> {
    let store = state.store.clone();
    let saved = agent.clone();
    tokio::task::spawn_blocking(move || store.put_agent(saved))
        .await
        .map_err(|error| ApiError::Decider(error.to_string()))?;
    Ok(Json(agent))
}

async fn create_run(
    Bearer(_principal): Bearer,
    State(state): State<AppState>,
    Json(body): Json<RunBody>,
) -> Result<Json<RunState>, ApiError> {
    let spec = spec_from_body(body)?;
    if let Some(url) = state.redis_url.clone() {
        let message = crate::inference::user_message_event(&spec);
        let store = state.store.clone();
        let spec_for_store = spec.clone();
        let message_for_store = message.clone();
        tokio::task::spawn_blocking(move || {
            store.put_run(StoredRun {
                spec: spec_for_store,
                events: vec![message_for_store],
            });
        })
        .await
        .map_err(|error| ApiError::Decider(error.to_string()))?;
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
                store.append_events(run_id, vec![failed]);
                error
            })
        })
        .await
        .map_err(|error| ApiError::Decider(error.to_string()))?
        .map_err(ApiError::Decider)?;
        return Ok(Json(fold(&spec, &[message])));
    }

    let jev_base_url = state.jev_base_url.clone();
    let spec_for_run = spec.clone();
    let store_for_run = state.store.clone();
    let stored = match tokio::task::spawn_blocking(move || {
        // The user message is on the record before the harness asks Jev.
        let message = crate::inference::user_message_event(&spec_for_run);
        let run_id = spec_for_run.run_id;
        store_for_run.put_run(StoredRun {
            spec: spec_for_run.clone(),
            events: vec![message],
        });
        let (mut events, outcome) = run_with_jev(&jev_base_url, spec_for_run.clone());
        // A run that could not finish still keeps what the harness did, and ends
        // failed instead of being left open.
        if let Err(error) = &outcome {
            let (class, message) = match error {
                RunStartError::Unsupported(placement) => (
                    FailureClass::Environment,
                    format!("unsupported placement: {placement:?}"),
                ),
                RunStartError::Decider(message) => {
                    (FailureClass::Dependency, format!("decider: {message}"))
                }
            };
            events.push(run_failed_event(&spec_for_run, class, message));
        }
        // Append, never overwrite: anything stored while Jev ran stays. When the
        // run is already terminal the store keeps its log and refuses these.
        store_for_run.append_events(run_id, events);
        outcome?;
        store_for_run
            .run(run_id)
            .ok_or_else(|| RunStartError::Decider("run is not stored".to_string()))
    })
    .await
    {
        Ok(Ok(stored)) => stored,
        Ok(Err(RunStartError::Unsupported(placement))) => {
            return Err(ApiError::Unsupported(placement));
        }
        Ok(Err(RunStartError::Decider(message))) => return Err(ApiError::Decider(message)),
        Err(error) => return Err(ApiError::Decider(error.to_string())),
    };
    Ok(Json(fold(&stored.spec, &stored.events)))
}

async fn create_coworker_turn(
    Bearer(_principal): Bearer,
    State(state): State<AppState>,
    Json(body): Json<RunBody>,
) -> Result<Json<TurnBody>, ApiError> {
    let spec = spec_from_body(body)?;
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
    Bearer(_principal): Bearer,
    State(state): State<AppState>,
    Path(id): Path<RunId>,
    Json(body): Json<CompletionBody>,
) -> Result<Json<TurnBody>, ApiError> {
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
    Bearer(_principal): Bearer,
    State(state): State<AppState>,
    Path(id): Path<RunId>,
) -> Result<Json<RunState>, ApiError> {
    let store = state.store.clone();
    let stored = tokio::task::spawn_blocking(move || store.run(id))
        .await
        .map_err(|error| ApiError::Decider(error.to_string()))?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(fold(&stored.spec, &stored.events)))
}

async fn get_events(
    Bearer(_principal): Bearer,
    State(state): State<AppState>,
    Path(id): Path<RunId>,
) -> Result<Json<Vec<Event>>, ApiError> {
    let store = state.store.clone();
    let stored = tokio::task::spawn_blocking(move || store.run(id))
        .await
        .map_err(|error| ApiError::Decider(error.to_string()))?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(stored.events))
}

async fn get_ag_ui(
    Bearer(_principal): Bearer,
    State(state): State<AppState>,
    Path(id): Path<RunId>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let store = state.store.clone();
    let stored = tokio::task::spawn_blocking(move || store.run(id))
        .await
        .map_err(|error| ApiError::Decider(error.to_string()))?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(serde_json::Value::Array(ag_ui_events(
        stored.spec.run_id,
        &stored.events,
    ))))
}

async fn get_ui(
    Bearer(_principal): Bearer,
    State(state): State<AppState>,
    Path(id): Path<RunId>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let store = state.store.clone();
    let stored = tokio::task::spawn_blocking(move || store.run(id))
        .await
        .map_err(|error| ApiError::Decider(error.to_string()))?
        .ok_or(ApiError::NotFound)?;
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
fn run_with_jev(jev_base_url: &str, spec: RunSpec) -> (Vec<Event>, Result<(), RunStartError>) {
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
        &mut InMemory::default(),
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

enum RunStartError {
    Unsupported(ExecutionPlacement),
    Decider(String),
}

/// The most steps or model calls a client may ask for. With `PlatformGateway` the
/// platform pays for every model call, so a request cannot authorize an unbounded run.
const MAX_LIMIT: u32 = 64;

fn spec_from_body(body: RunBody) -> Result<RunSpec, ApiError> {
    let limits = body.limits.unwrap_or(Limits {
        max_steps: 8,
        max_model_calls: 4,
    });
    let bounded = 1..=MAX_LIMIT;
    if !bounded.contains(&limits.max_steps) || !bounded.contains(&limits.max_model_calls) {
        return Err(ApiError::BadRequest("limits must be between 1 and 64"));
    }
    Ok(RunSpec::builder()
        .agent(body.agent_id, body.agent_version)
        .input(body.input)
        .placement(body.placement)
        .work_model(body.work_model)
        .capabilities(body.capabilities)
        .limits(limits)
        .metadata(body.metadata)
        .build())
}

enum ApiError {
    Unsupported(ExecutionPlacement),
    Decider(String),
    NotFound,
    Proxy(String),
    Sandbox(String),
    Conflict(&'static str),
    BadRequest(&'static str),
    Unauthorized,
}

impl From<TurnError> for ApiError {
    fn from(error: TurnError) -> Self {
        match error {
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
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "unauthorized" })),
            )
                .into_response(),
        }
    }
}
