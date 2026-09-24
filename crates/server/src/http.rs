use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use harness::{
    run_to_completion, BootError, Driver, EchoTool, InMemory, JevDecider, UnavailableModel,
};
use protocol::{
    fold, AgentId, Capability, Event, EventPayload, ExecutionPlacement, Limits, RunId, RunSpec,
    RunState, WorkModel,
};
use serde::{Deserialize, Serialize};

use crate::inference::{
    accept_subscription_completion, open_turn, sandbox_from_env, ComputerPlan, GatewayPoster,
    HttpGatewayPoster, SandboxHost, SharedPoster, TurnError, TurnOutcome,
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

async fn create_agent(
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
    State(state): State<AppState>,
    Json(body): Json<RunBody>,
) -> Result<Json<RunState>, ApiError> {
    let spec = spec_from_body(body);
    let jev_base_url = state.jev_base_url.clone();
    let spec_for_run = spec.clone();
    let store_for_run = state.store.clone();
    let events = match tokio::task::spawn_blocking(move || {
        // The user message is on the record before the harness asks Jev.
        let message = crate::inference::user_message_event(&spec_for_run);
        store_for_run.put_run(StoredRun {
            spec: spec_for_run.clone(),
            events: vec![message.clone()],
        });
        let mut events = vec![message];
        events.extend(run_with_jev(&jev_base_url, spec_for_run)?);
        Ok(events)
    })
    .await
    {
        Ok(Ok(events)) => events,
        Ok(Err(RunStartError::Unsupported(placement))) => {
            return Err(ApiError::Unsupported(placement));
        }
        Ok(Err(RunStartError::Decider(message))) => return Err(ApiError::Decider(message)),
        Err(error) => return Err(ApiError::Decider(error.to_string())),
    };
    let folded = fold(&spec, &events);
    let run_id = spec.run_id;
    let store = state.store.clone();
    tokio::task::spawn_blocking(move || store.put_run(StoredRun { spec, events }))
        .await
        .map_err(|error| ApiError::Decider(error.to_string()))?;
    if let Some(url) = state.redis_url.clone() {
        tokio::task::spawn_blocking(move || RedisRunQueue::open(url).push(run_id))
            .await
            .map_err(|error| ApiError::Decider(error.to_string()))?
            .map_err(ApiError::Decider)?;
    }
    Ok(Json(folded))
}

async fn create_coworker_turn(
    State(state): State<AppState>,
    Json(body): Json<RunBody>,
) -> Result<Json<TurnBody>, ApiError> {
    let spec = spec_from_body(body);
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
    State(state): State<AppState>,
    Path(id): Path<RunId>,
    Json(body): Json<CompletionBody>,
) -> Result<Json<TurnBody>, ApiError> {
    let store = state.store.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        accept_subscription_completion(store.as_ref(), id, &body.text)
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
    State(state): State<AppState>,
    Path(id): Path<RunId>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let store = state.store.clone();
    let stored = tokio::task::spawn_blocking(move || store.run(id))
        .await
        .map_err(|error| ApiError::Decider(error.to_string()))?
        .ok_or(ApiError::NotFound)?;
    let folded = fold(&stored.spec, &stored.events);
    let outcome = match &folded.harness {
        protocol::HarnessState::Completed { outcome } => outcome.clone(),
        protocol::HarnessState::Failed { message, .. } => message.clone(),
        protocol::HarnessState::Cancelled => "cancelled".to_string(),
        _ => "running".to_string(),
    };
    Ok(Json(json_render_spec(&stored.spec.input, &outcome)))
}

fn run_with_jev(jev_base_url: &str, spec: RunSpec) -> Result<Vec<Event>, RunStartError> {
    let mut driver = match Driver::boot(spec) {
        Ok(driver) => driver,
        Err(BootError::UnsupportedPlacement(placement)) => {
            return Err(RunStartError::Unsupported(placement));
        }
    };
    let client = jev_client(jev_base_url).map_err(RunStartError::Decider)?;
    let mut decider = JevDecider::new(client);
    let echo = EchoTool;
    run_to_completion(
        &mut driver,
        &mut decider,
        &[&echo],
        &UnavailableModel,
        &mut InMemory::default(),
    )
    .map_err(|error| RunStartError::Decider(error.message))?;
    Ok(driver.events().to_vec())
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

fn spec_from_body(body: RunBody) -> RunSpec {
    RunSpec::builder()
        .agent(body.agent_id, body.agent_version)
        .input(body.input)
        .placement(body.placement)
        .work_model(body.work_model)
        .capabilities(body.capabilities)
        .limits(body.limits.unwrap_or(Limits {
            max_steps: 8,
            max_model_calls: 4,
        }))
        .metadata(body.metadata)
        .build()
}

enum ApiError {
    Unsupported(ExecutionPlacement),
    Decider(String),
    NotFound,
    Proxy(String),
    Sandbox(String),
    Conflict(&'static str),
    BadRequest(&'static str),
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
        }
    }
}
