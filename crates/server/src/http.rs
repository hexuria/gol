use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use harness::{run_to_completion, BootError, Driver, EchoTool, InMemory, UnavailableModel};
use protocol::{
    fold, AgentId, Capability, Event, ExecutionPlacement, Limits, RunId, RunSpec, RunState,
    WorkModel,
};
use serde::Deserialize;

use crate::local::LocalEchoFactory;
use crate::store::{AgentManifest, RunStore, StoredRun};

#[derive(Clone)]
struct AppState {
    store: Arc<dyn RunStore>,
    echo: Arc<LocalEchoFactory>,
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

pub fn router(store: Arc<dyn RunStore>, echo: Arc<LocalEchoFactory>) -> Router {
    Router::new()
        .route("/v1/agents", post(create_agent))
        .route("/v1/runs", post(create_run))
        .route("/v1/runs/{id}", get(get_run))
        .route("/v1/runs/{id}/events", get(get_events))
        .with_state(AppState { store, echo })
}

async fn create_agent(
    State(state): State<AppState>,
    Json(agent): Json<AgentManifest>,
) -> Json<AgentManifest> {
    state.store.put_agent(agent.clone());
    Json(agent)
}

async fn create_run(
    State(state): State<AppState>,
    Json(body): Json<RunBody>,
) -> Result<Json<RunState>, ApiError> {
    let spec = spec_from_body(body);
    let mut driver = match Driver::boot(spec.clone()) {
        Ok(driver) => driver,
        Err(BootError::UnsupportedPlacement(placement)) => {
            return Err(ApiError::Unsupported(placement));
        }
    };
    let mut decider = state.echo.decider();
    let echo = EchoTool;
    run_to_completion(
        &mut driver,
        &mut decider,
        &[&echo],
        &UnavailableModel,
        &mut InMemory::default(),
    )
    .map_err(|error| ApiError::Decider(error.message))?;
    let events = driver.events().to_vec();
    let folded = fold(&spec, &events);
    state.store.put_run(StoredRun { spec, events });
    Ok(Json(folded))
}

async fn get_run(
    State(state): State<AppState>,
    Path(id): Path<RunId>,
) -> Result<Json<RunState>, ApiError> {
    let stored = state.store.run(id).ok_or(ApiError::NotFound)?;
    Ok(Json(fold(&stored.spec, &stored.events)))
}

async fn get_events(
    State(state): State<AppState>,
    Path(id): Path<RunId>,
) -> Result<Json<Vec<Event>>, ApiError> {
    let stored = state.store.run(id).ok_or(ApiError::NotFound)?;
    Ok(Json(stored.events))
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
        }
    }
}
