use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{FromRequestParts, Path, Query, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use harness::{
    run_to_completion, AgentSpawner, BootError, DelegateTarget, Driver, EchoTool, InMemory,
    JevDecider, Memory, RunMemory, StoreError,
};
use protocol::{
    fold, Actor, AgentId, Capability, DispatchPhase, Event, EventPayload, EventSource,
    ExecutionPlacement, FailureClass, HarnessState, Limits, MessageId, Owner, RunId, RunSpec,
    RunState, Timestamp, WorkModel, MAX_MESSAGE_BYTES, SESSION_ID,
};
use serde::{Deserialize, Serialize};

use crate::auth::{AuthError, Authenticator, Principal};
use crate::deliverer::open_question;
use crate::inference::{
    accept_subscription_completion, fail_turn, open_turn, run_failed_event, sandbox_from_env,
    ComputerPlan, GatewayPoster, HttpGatewayPoster, SandboxHost, SharedPoster, TurnError,
    TurnOutcome,
};
use crate::models::ModelsConfig;
use crate::queue::RedisRunQueue;
use crate::store::{
    is_terminal, AgentManifest, Append, PutAgent, RunStore, StopScope, StoredAgent, StoredRun,
};
use crate::stream::{outbox_stream, run_stream, SseBody, Streams};
use crate::surface::{ag_ui_events, json_render_spec};

#[derive(Clone)]
struct AppState {
    store: Arc<dyn RunStore>,
    memory: Arc<dyn Memory>,
    jev_base_url: String,
    /// The work model each run calls (D2).
    models: Arc<ModelsConfig>,
    /// The run queue, one connection shared by every request.
    queue: Option<Arc<RedisRunQueue>>,
    poster: SharedPoster,
    sandbox: Arc<dyn SandboxHost>,
    auth: Arc<dyn Authenticator>,
    /// The streams each principal has open (Phase 3.2).
    streams: Arc<Streams>,
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
    router_with_state(AppState {
        store,
        memory: Arc::new(InMemory::default()),
        jev_base_url: jev_base_url.into(),
        models: Arc::new(ModelsConfig::default()),
        queue: redis_url.map(open_queue),
        poster: Arc::new(HttpGatewayPoster::from_env()),
        sandbox: sandbox_from_env(),
        auth,
        streams: Arc::default(),
    })
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
    router_with_state(AppState {
        store,
        memory: Arc::new(InMemory::default()),
        jev_base_url: jev_base_url.into(),
        models: Arc::new(ModelsConfig::default()),
        queue: None,
        poster,
        sandbox,
        auth,
        streams: Arc::default(),
    })
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
    models: Arc<ModelsConfig>,
    auth: Arc<dyn Authenticator>,
) -> Router {
    router_with_state(AppState {
        store,
        memory,
        jev_base_url: jev_base_url.into(),
        models,
        queue: redis_url.map(open_queue),
        poster,
        sandbox: sandbox_from_env(),
        auth,
        streams: Arc::default(),
    })
}

/// The run queue at `redis_url`, one connection shared by every request.
fn open_queue(redis_url: String) -> Arc<RedisRunQueue> {
    Arc::new(RedisRunQueue::open(redis_url))
}

fn router_with_state(state: AppState) -> Router {
    Router::new()
        .route("/v1/agents", post(create_agent))
        .route("/v1/runs", post(create_run))
        .route("/v1/runs/{id}", get(get_run))
        .route("/v1/runs/{id}/events", get(get_events))
        .route("/v1/runs/{id}/ag-ui", get(get_ag_ui))
        .route("/v1/runs/{id}/ui", get(get_ui))
        .route("/v1/runs/{id}/stream", get(get_run_stream))
        .route("/v1/stream", get(get_stream))
        .route("/v1/threads", post(create_thread).get(list_threads))
        .route("/v1/threads/{id}/messages", post(follow_up))
        .route("/v1/threads/{id}/board", get(get_board))
        .route("/v1/runs/{id}/stop", post(stop_run))
        .route("/v1/runs/{id}/reply", post(reply_run))
        .route("/v1/threads/{id}/stop", post(stop_thread))
        .route("/v1/stop", post(stop_owner))
        .route("/v1/coworker/turns", post(create_coworker_turn))
        .route(
            "/v1/coworker/turns/{id}/completion",
            post(complete_coworker_turn),
        )
        .route("/v1/coworker/turns/{id}/fail", post(fail_coworker_turn))
        .with_state(state)
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
    let Json(body) = body.map_err(body_rejection)?;
    Ok(Json(start_run(&state, owner_of(&principal), body).await?))
}

/// A rejected JSON body: an unknown field, a bad value or bad JSON is a 400;
/// any other rejection (content type, size) keeps its own status.
fn body_rejection(rejection: JsonRejection) -> ApiError {
    match rejection {
        JsonRejection::JsonDataError(_) | JsonRejection::JsonSyntaxError(_) => {
            ApiError::InvalidBody(rejection.body_text())
        }
        other => ApiError::Rejected(other),
    }
}

/// Starts a run of one of `owner`'s agents, as `POST /v1/runs` does: queued
/// when the server has a queue, else run to its end here.
async fn start_run(state: &AppState, owner: Owner, body: RunBody) -> Result<RunState, ApiError> {
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
    if let Some(queue) = state.queue.clone() {
        // Created and queued are on the record before the push, so a worker
        // that takes the run finds them (C4). The run is pending before it
        // is stored, and the push takes it off pending: if this process dies
        // between the store and the push, the sweep pushes it (C6). With
        // Redis down, nothing is stored. The pend, the store, the push and a
        // failed push's RunFailed are one blocking task: it runs to its end
        // even if the client goes away.
        let queued = crate::inference::queued_events(&spec);
        let store = state.store.clone();
        let spec_for_queue = spec.clone();
        tokio::task::spawn_blocking(move || {
            crate::spawner::enqueue(
                store.as_ref(),
                &queue,
                &spec_for_queue,
                crate::spawner::OnPushFailure::End,
            )
            .map_err(|error| match error {
                crate::spawner::EnqueueError::Queue(error) => {
                    ApiError::Decider(format!("queue unavailable: {error}"))
                }
                crate::spawner::EnqueueError::Store(error) => ApiError::from(error),
                crate::spawner::EnqueueError::Push(error) => ApiError::Decider(error),
            })
        })
        .await
        .map_err(|error| ApiError::Decider(error.to_string()))??;
        return Ok(fold(&spec, &queued));
    }

    let jev_base_url = state.jev_base_url.clone();
    let models = state.models.clone();
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
        // Inline: a child needs the run queue, so this run offers no delegation.
        let (events, outcome) =
            harness_events(&jev_base_url, &spec_for_run, memory.as_ref(), &models, None);
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
    Ok(fold(&stored.spec, &stored.events))
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

/// `?after=` for a stream, beside a `Last-Event-ID` header.
#[derive(Debug, Deserialize)]
struct StreamQuery {
    after: Option<u64>,
}

/// Where a stream resumes: the `Last-Event-ID` header, else `?after=`.
fn resume(
    headers: &HeaderMap,
    query: Result<Query<StreamQuery>, QueryRejection>,
) -> Result<Option<u64>, ApiError> {
    let Query(query) = query.map_err(|_| ApiError::BadRequest("after must be a count"))?;
    match headers.get("last-event-id") {
        Some(value) => value
            .to_str()
            .ok()
            .and_then(|value| value.trim().parse().ok())
            .map(Some)
            .ok_or(ApiError::BadRequest("Last-Event-ID must be a count")),
        None => Ok(query.after),
    }
}

/// `GET /v1/stream`: the caller's outbox as server-sent events (Phase 3.2).
async fn get_stream(
    Authenticated(principal): Authenticated,
    State(state): State<AppState>,
    headers: HeaderMap,
    query: Result<Query<StreamQuery>, QueryRejection>,
) -> Result<SseBody, ApiError> {
    let resume = resume(&headers, query)?;
    if state.store.outbox().is_none() {
        return Err(ApiError::NoStreams);
    }
    let owner = owner_of(&principal);
    let slot = state.streams.take(&owner).ok_or(ApiError::TooManyStreams)?;
    Ok(outbox_stream(state.store.clone(), owner, resume, slot))
}

/// `GET /v1/runs/{id}/stream`: one owned run's log as server-sent events,
/// to its end. A run another principal owns is not found. A cursor on the
/// run's terminal event gets 204, which tells a browser's EventSource to
/// stop reconnecting; one past the log is refused.
async fn get_run_stream(
    Authenticated(principal): Authenticated,
    State(state): State<AppState>,
    Path(id): Path<RunId>,
    headers: HeaderMap,
    query: Result<Query<StreamQuery>, QueryRejection>,
) -> Result<axum::response::Response, ApiError> {
    let resume = resume(&headers, query)?.unwrap_or(0);
    // The event at the cursor (the first, with none), which also checks
    // that the caller owns the run.
    let from = usize::try_from(resume.saturating_sub(1)).unwrap_or(usize::MAX);
    let at = owned_run_page(&state, id, &principal, from, 1).await?;
    match at.events.first() {
        Some(event) if resume > 0 && is_terminal(&event.payload) => {
            return Ok(StatusCode::NO_CONTENT.into_response());
        }
        None if resume > 0 => {
            return Err(ApiError::BadRequest("the cursor is past the run's log"));
        }
        _ => {}
    }
    let owner = owner_of(&principal);
    let slot = state.streams.take(&owner).ok_or(ApiError::TooManyStreams)?;
    Ok(run_stream(state.store.clone(), owner, id, resume, slot).into_response())
}

/// `POST /v1/threads`: starts a thread (Phase 3.3, decision 47A), the first
/// coordinator run of the agent named, in a new session the server chooses.
async fn create_thread(
    Authenticated(principal): Authenticated,
    State(state): State<AppState>,
    body: Result<Json<RunBody>, JsonRejection>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let Json(mut body) = body.map_err(body_rejection)?;
    if body.metadata.contains_key(SESSION_ID) {
        return Err(ApiError::BadRequest(
            "a thread's session is the server's to choose",
        ));
    }
    let thread_id = uuid::Uuid::new_v4().to_string();
    body.metadata
        .insert(SESSION_ID.to_string(), thread_id.clone());
    let run = start_run(&state, owner_of(&principal), body).await?;
    Ok(Json(
        serde_json::json!({ "thread_id": thread_id, "run": run }),
    ))
}

/// A follow-up's body: its input, and its limits if not the thread's.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FollowUpBody {
    input: String,
    limits: Option<Limits>,
}

/// The most runs a board reads for one thread; a board of a longer thread
/// says it is `truncated`.
const BOARD_RUNS: usize = 1000;

/// The most threads, or cards, in a page (decision 50A).
const PAGE_MAX: usize = 50;

/// `POST /v1/threads/{id}/messages`: a follow-up, the next coordinator run
/// of the thread's agent in its session, with the thread's placement, work
/// model and limits unless the body gives limits.
async fn follow_up(
    Authenticated(principal): Authenticated,
    State(state): State<AppState>,
    Path(id): Path<String>,
    body: Result<Json<FollowUpBody>, JsonRejection>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let Json(body) = body.map_err(body_rejection)?;
    let owner = owner_of(&principal);
    // The coordinator's spec, and its agent's manifest as stored now: a
    // follow-up runs the version the owner keeps, not the thread's first.
    let (store, thread_owner, thread) = (state.store.clone(), owner.clone(), id.clone());
    let found = tokio::task::spawn_blocking(move || {
        let Some(threads) = store.threads() else {
            return Ok(Err(ApiError::NoThreads));
        };
        let Some(root) = threads.thread_root(&thread_owner, &thread)? else {
            return Ok(Err(ApiError::ThreadNotFound));
        };
        let agent = store.agent(root.agent_id)?;
        let runs = threads.runs_of_thread(&thread_owner, &thread, BOARD_RUNS)?;
        Ok::<_, StoreError>(Ok((root, agent, waiting_on_user(&runs))))
    })
    .await
    .map_err(|error| ApiError::Store(error.to_string()))?
    .map_err(ApiError::from)?;
    let (root, agent, waiting) = found?;
    // A task waits on the user: the message answers it (decision 61A).
    if !waiting.is_empty() {
        let (task, text) = match addressed(&body.input) {
            Some((label, text)) => match waiting.iter().find(|task| task.label == label) {
                Some(task) => (task, text),
                None => return Err(ApiError::WhichTask(waiting)),
            },
            None if waiting.len() == 1 => (&waiting[0], body.input.as_str()),
            None => return Err(ApiError::WhichTask(waiting)),
        };
        if body.limits.is_some() {
            return Err(ApiError::BadRequest(
                "an answer takes no limits: they are for a follow-up",
            ));
        }
        let run_id = task.run_id;
        answer_question(&state, run_id, text, Some(task.question_id)).await?;
        return Ok(Json(serde_json::json!({
            "thread_id": id,
            "answered": { "label": format!("T{}", task.label), "run_id": run_id },
        })));
    }
    let agent = agent
        .filter(|agent| agent.owner.is(&owner))
        .ok_or(ApiError::AgentNotFound)?;
    let run = start_run(
        &state,
        owner,
        RunBody {
            agent_id: root.agent_id,
            agent_version: agent.manifest.version,
            input: body.input,
            placement: root.placement,
            work_model: root.work_model,
            limits: Some(body.limits.unwrap_or(root.limits)),
            metadata: root.metadata,
        },
    )
    .await?;
    Ok(Json(serde_json::json!({ "thread_id": id, "run": run })))
}

/// A task of a thread that waits on its user (Phase 3.5).
#[derive(Debug)]
struct WaitingTask {
    /// Its number in the thread: "T<label>" (decision 60A).
    label: usize,
    run_id: RunId,
    question_id: MessageId,
    question: String,
}

/// The tasks among a thread's `runs` (in the thread's order) that wait on
/// the user, labelled by their place in that order.
fn waiting_on_user(runs: &[StoredRun]) -> Vec<WaitingTask> {
    runs.iter()
        .enumerate()
        .filter_map(|(at, run)| {
            open_question(&run.spec, &run.events).map(|(question_id, question)| WaitingTask {
                label: at + 1,
                run_id: run.spec.run_id,
                question_id,
                question,
            })
        })
        .collect()
}

/// A message addressed to a task, "T<n>: text", as (n, text): `T` or `t`,
/// 1 to 9 digits naming a task from 1, a colon, and the text after it,
/// trimmed. Leading white space is allowed.
fn addressed(input: &str) -> Option<(usize, &str)> {
    let rest = input.trim_start();
    let rest = rest.strip_prefix(['T', 't'])?;
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 || digits > 9 {
        return None;
    }
    let (number, rest) = rest.split_at(digits);
    let text = rest.strip_prefix(':')?;
    let number: usize = number.parse().ok()?;
    (number >= 1).then(|| (number, text.trim()))
}

/// Appends the user's `text` as the answer to the question run `run_id`
/// waits on, onto the log it read, and wakes the run. A log another writer
/// moved in between is read again, up to three times; a run that no longer
/// waits on its user is 409. A failed wake is left to the ask sweep, which
/// wakes a parked run whose log no longer waits.
async fn answer_question(
    state: &AppState,
    run_id: RunId,
    text: &str,
    asked: Option<MessageId>,
) -> Result<(), ApiError> {
    if text.trim().is_empty() {
        return Err(ApiError::BadRequest("the answer is empty"));
    }
    if text.len() > MAX_MESSAGE_BYTES {
        return Err(ApiError::TooLarge("the answer is too long"));
    }
    let (store, queue, text) = (state.store.clone(), state.queue.clone(), text.to_string());
    tokio::task::spawn_blocking(move || {
        // The question answered: the one the caller saw, else the one first
        // read. A task that went on to another question is not answered
        // with text meant for this one.
        let mut asked = asked;
        for _ in 0..3 {
            let Some(run) = store.run(run_id)? else {
                return Ok(Err(ApiError::NotFound));
            };
            let Some((question, _)) = open_question(&run.spec, &run.events) else {
                return Ok(Err(ApiError::Conflict("the task is not waiting for you")));
            };
            if *asked.get_or_insert(question) != question {
                return Ok(Err(ApiError::Conflict(
                    "the task is waiting on another question",
                )));
            }
            let answer = Event::record(
                EventSource::for_spec(&run.spec, Actor::System, Timestamp::now()),
                EventPayload::UserAnswered {
                    message_id: question,
                    text: text.clone(),
                },
            );
            match store.append_events_after(run_id, run.events.len(), vec![answer])? {
                Append::Appended => {
                    if let Some(queue) = &queue {
                        if let Err(error) = queue.wake(run_id, question) {
                            eprintln!("gol: reply: wake run {run_id}: {error}");
                        }
                    }
                    return Ok(Ok(()));
                }
                Append::Moved => continue,
                Append::Missing => return Ok(Err(ApiError::NotFound)),
                Append::Terminal => {
                    return Ok(Err(ApiError::Conflict("the task is not waiting for you")))
                }
            }
        }
        Ok::<_, StoreError>(Err(ApiError::Store(
            "the task kept changing while it was answered".to_string(),
        )))
    })
    .await
    .map_err(|error| ApiError::Store(error.to_string()))?
    .map_err(ApiError::from)?
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplyBody {
    text: String,
    /// The question answered, as its card showed it; without it, the one
    /// the run waits on when the reply is read.
    question_id: Option<MessageId>,
}

/// `POST /v1/runs/{id}/reply`: the user's answer to the question the run
/// waits on (Phase 3.5, decision 56A).
async fn reply_run(
    Authenticated(principal): Authenticated,
    State(state): State<AppState>,
    Path(id): Path<RunId>,
    body: Result<Json<ReplyBody>, JsonRejection>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let Json(body) = body.map_err(body_rejection)?;
    owned_run_page(&state, id, &principal, 0, 1).await?;
    answer_question(&state, id, &body.text, body.question_id).await?;
    Ok(Json(serde_json::json!({ "answered": id })))
}

/// A page of threads: `after` of them seen, at most `limit`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ThreadsQuery {
    after: Option<usize>,
    limit: Option<usize>,
}

/// A page of a board's cards, of one state if `state` is given.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BoardQuery {
    after: Option<usize>,
    limit: Option<usize>,
    state: Option<CardState>,
}

/// `after` and a `limit` from 1 to `PAGE_MAX` (default `PAGE_MAX`).
fn page_bounds(after: Option<usize>, limit: Option<usize>) -> Result<(usize, usize), ApiError> {
    let limit = limit.unwrap_or(PAGE_MAX);
    if limit == 0 || limit > PAGE_MAX {
        return Err(ApiError::BadRequest("limit must be between 1 and 50"));
    }
    Ok((after.unwrap_or(0), limit))
}

/// `GET /v1/threads`: the caller's threads, newest first.
async fn list_threads(
    Authenticated(principal): Authenticated,
    State(state): State<AppState>,
    query: Result<Query<ThreadsQuery>, QueryRejection>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let Query(query) = query.map_err(|_| ApiError::BadRequest("after and limit must be counts"))?;
    let (after, limit) = page_bounds(query.after, query.limit)?;
    let (store, owner) = (state.store.clone(), owner_of(&principal));
    let threads = tokio::task::spawn_blocking(move || {
        store
            .threads()
            .map(|threads| threads.threads_of(&owner, after, limit))
    })
    .await
    .map_err(|error| ApiError::Store(error.to_string()))?
    .ok_or(ApiError::NoThreads)?
    .map_err(ApiError::from)?;
    Ok(Json(serde_json::json!({
        "threads": threads.iter().map(|thread| serde_json::json!({
            "thread_id": thread.thread_id,
            "agent_id": thread.agent_id,
            "runs": thread.runs,
            "started_at": Timestamp::unix_millis(thread.started_ms),
        })).collect::<Vec<_>>(),
    })))
}

/// A card's state, from the fold of its run's log.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum CardState {
    Queued,
    Running,
    Waiting,
    Completed,
    Failed,
    Cancelled,
    Expired,
}

impl CardState {
    fn of(state: &RunState) -> Self {
        match &state.dispatch {
            DispatchPhase::Created | DispatchPhase::Queued => Self::Queued,
            DispatchPhase::Completed { .. } => Self::Completed,
            DispatchPhase::Failed { .. } => Self::Failed,
            DispatchPhase::Cancelled => Self::Cancelled,
            DispatchPhase::Expired => Self::Expired,
            DispatchPhase::Waiting { .. }
            | DispatchPhase::AwaitingApproval { .. }
            | DispatchPhase::Paused => Self::Waiting,
            _ if matches!(state.harness, HarnessState::WaitingForMessage { .. }) => Self::Waiting,
            _ => Self::Running,
        }
    }
}

/// One run's card on a board, with its state: the thread's `label`-th run.
fn card(label: usize, run: &StoredRun) -> (CardState, serde_json::Value) {
    let state = fold(&run.spec, &run.events);
    let card_state = CardState::of(&state);
    // Once each, in the order started: a resumed run may log one twice.
    let mut children: Vec<RunId> = Vec::new();
    for event in &run.events {
        if let EventPayload::ChildStarted { run_id, .. } = event.payload {
            if !children.contains(&run_id) {
                children.push(run_id);
            }
        }
    }
    let outcome = match &state.dispatch {
        DispatchPhase::Completed { outcome } => Some(outcome.clone()),
        _ => None,
    };
    let at = |wanted: fn(&EventPayload) -> bool| {
        run.events
            .iter()
            .find(|event| wanted(&event.payload))
            .map(|event| event.envelope.at)
    };
    let question = open_question(&run.spec, &run.events);
    let card = serde_json::json!({
        "label": format!("T{label}"),
        "question_id": question.as_ref().map(|(id, _)| *id),
        "question": question.map(|(_, question)| question),
        "run_id": run.spec.run_id,
        "agent_id": run.spec.agent_id,
        "parent": run.spec.lineage.parent,
        "children": children,
        "state": card_state,
        "outcome": outcome,
        "steps": state.steps,
        "model_calls": state.model_calls,
        "created_at": run.events.first().map(|event| event.envelope.at),
        "started_at": at(|payload| matches!(payload, EventPayload::RunStarted)),
        "ended_at": at(is_terminal),
    });
    (card_state, card)
}

/// `GET /v1/threads/{id}/board`: a card per run of the thread (every
/// coordinator run and every task at any depth), oldest first, filtered by
/// `?state=` and paged. Without a filter only the page's runs are folded;
/// with one, each run read is, to know its state. Folded off the async
/// workers.
async fn get_board(
    Authenticated(principal): Authenticated,
    State(state): State<AppState>,
    Path(id): Path<String>,
    query: Result<Query<BoardQuery>, QueryRejection>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let Query(query) = query.map_err(|_| {
        ApiError::BadRequest("after and limit must be counts, and state a card state")
    })?;
    let (after, limit) = page_bounds(query.after, query.limit)?;
    let wanted = query.state;
    let (store, owner) = (state.store.clone(), owner_of(&principal));
    let board = tokio::task::spawn_blocking(move || {
        let Some(threads) = store.threads() else {
            return Ok(Err(ApiError::NoThreads));
        };
        let mut runs = threads.runs_of_thread(&owner, &id, BOARD_RUNS + 1)?;
        if runs.is_empty() {
            return Ok(Err(ApiError::ThreadNotFound));
        }
        let truncated = runs.len() > BOARD_RUNS;
        runs.truncate(BOARD_RUNS);
        let cards: Vec<serde_json::Value> = match wanted {
            None => runs
                .iter()
                .enumerate()
                .skip(after)
                .take(limit)
                .map(|(at, run)| card(at + 1, run).1)
                .collect(),
            Some(wanted) => runs
                .iter()
                .enumerate()
                .map(|(at, run)| card(at + 1, run))
                .filter(|(state, _)| *state == wanted)
                .skip(after)
                .take(limit)
                .map(|(_, card)| card)
                .collect(),
        };
        Ok::<_, StoreError>(Ok(
            serde_json::json!({ "cards": cards, "truncated": truncated }),
        ))
    })
    .await
    .map_err(|error| ApiError::Store(error.to_string()))?
    .map_err(ApiError::from)??;
    Ok(Json(board))
}

/// Records `owner`'s stop of `scope` (Phase 3.4) and cancels the covered
/// runs no worker holds (decision 55A): those still queued, and those parked
/// on an ask, which it also wakes so their worker acknowledges them. A run a
/// worker holds is cancelled by that worker at its next step boundary.
///
/// The cancel appends only onto the log it read, so a worker that moved the
/// run on in between is left to cancel it itself (`formal/runlog`'s
/// conditional append). Without a queue a run is carried out inside its
/// request and is never queued or parked: the stop is recorded and nothing
/// is cancelled here. Subscription turns do not run through the worker and
/// are not stopped. A run that could not be cancelled here, or a failed read
/// of the parked runs, answers 503 after the rest are done: the stop is
/// recorded, and asking again is safe.
async fn stop(
    state: &AppState,
    owner: Owner,
    scope: StopScope,
) -> Result<Json<serde_json::Value>, ApiError> {
    let (store, queue) = (state.store.clone(), state.queue.clone());
    let cancelled = tokio::task::spawn_blocking(move || {
        let Some(stops) = store.stops() else {
            return Ok(Err(ApiError::NoStops));
        };
        stops.put_stop(&owner, &scope)?;
        let Some(queue) = queue else {
            return Ok(Ok(Vec::new()));
        };
        let mut failed = false;
        let parked = queue.parked().unwrap_or_else(|error| {
            eprintln!("gol: stop: parked runs: {error}");
            failed = true;
            Vec::new()
        });
        let mut cancelled = Vec::new();
        for run in stops.open_runs_under(&owner, &scope)? {
            let run_id = run.spec.run_id;
            let ask = parked
                .iter()
                .find(|(parked, _)| *parked == run_id)
                .map(|(_, ask)| *ask);
            let queued = matches!(
                fold(&run.spec, &run.events).dispatch,
                DispatchPhase::Created | DispatchPhase::Queued
            );
            if !queued && ask.is_none() {
                continue;
            }
            // A thread or owner stop does not cover a run stored after it.
            match stops.stopped(run_id) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(error) => {
                    eprintln!("gol: stop: run {run_id}: {error}");
                    failed = true;
                    continue;
                }
            }
            let appended = store.append_events_after(
                run_id,
                run.events.len(),
                vec![crate::inference::run_cancelled_event(&run.spec)],
            );
            match appended {
                Ok(Append::Appended) => {
                    cancelled.push(run_id);
                    if let Some(ask) = ask {
                        if let Err(error) = queue.wake(run_id, ask) {
                            eprintln!("gol: stop: wake run {run_id}: {error}");
                            failed = true;
                        }
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    eprintln!("gol: stop: cancel run {run_id}: {error}");
                    failed = true;
                }
            }
        }
        if failed {
            return Ok(Err(ApiError::Store(
                "a stop could not cancel every run".to_string(),
            )));
        }
        Ok::<_, StoreError>(Ok(cancelled))
    })
    .await
    .map_err(|error| ApiError::Store(error.to_string()))?
    .map_err(ApiError::from)??;
    Ok(Json(serde_json::json!({ "cancelled": cancelled })))
}

/// `POST /v1/runs/{id}/stop`: the run and every run under it.
async fn stop_run(
    Authenticated(principal): Authenticated,
    State(state): State<AppState>,
    Path(id): Path<RunId>,
) -> Result<Json<serde_json::Value>, ApiError> {
    owned_run_page(&state, id, &principal, 0, 1).await?;
    stop(&state, owner_of(&principal), StopScope::Run(id)).await
}

/// `POST /v1/threads/{id}/stop`: every run of the thread.
async fn stop_thread(
    Authenticated(principal): Authenticated,
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let owner = owner_of(&principal);
    let (store, thread_owner, thread) = (state.store.clone(), owner.clone(), id.clone());
    let found = tokio::task::spawn_blocking(move || {
        store
            .threads()
            .map(|threads| threads.thread_root(&thread_owner, &thread))
            .transpose()
    })
    .await
    .map_err(|error| ApiError::Store(error.to_string()))?
    .map_err(ApiError::from)?;
    match found {
        None => return Err(ApiError::NoThreads),
        Some(None) => return Err(ApiError::ThreadNotFound),
        Some(Some(_)) => {}
    }
    stop(&state, owner, StopScope::Thread(id)).await
}

/// `POST /v1/stop`: everything the caller's principal owns.
async fn stop_owner(
    Authenticated(principal): Authenticated,
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, ApiError> {
    stop(&state, owner_of(&principal), StopScope::Owner).await
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

/// A spawner for a run's delegations, and the agents its decider is offered.
pub(crate) type Delegation = (Arc<dyn AgentSpawner>, Vec<DelegateTarget>);

/// Runs the harness with Jev and returns the events to record: what the
/// harness did, and when it could not finish, the `RunFailed` that ends the
/// run instead of leaving it open. Without `delegation` every delegation is
/// refused and none is offered.
pub(crate) fn harness_events(
    jev_base_url: &str,
    spec: &RunSpec,
    memory: &dyn Memory,
    models: &ModelsConfig,
    delegation: Option<Delegation>,
) -> (Vec<Event>, Result<(), RunStartError>) {
    let (mut events, outcome) =
        run_with_jev(jev_base_url, spec.clone(), memory, models, delegation);
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

/// Runs the harness with Jev and returns every event it recorded, with how the
/// run ended. On an error the events are still returned: they are what the
/// harness did before it stopped.
fn run_with_jev(
    jev_base_url: &str,
    spec: RunSpec,
    memory: &dyn Memory,
    models: &ModelsConfig,
    delegation: Option<Delegation>,
) -> (Vec<Event>, Result<(), RunStartError>) {
    let model = models.model_for(&spec);
    let mut driver = match Driver::boot(spec) {
        Ok(driver) => driver,
        Err(BootError::UnsupportedPlacement(placement)) => {
            return (Vec::new(), Err(RunStartError::Unsupported(placement)));
        }
    };
    if let Some((spawner, targets)) = delegation {
        driver = driver.with_spawner(spawner, targets);
    }
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
        &model,
        &RunMemory::new(memory),
    )
    .map_err(|error| RunStartError::Decider(error.message));
    (driver.events().to_vec(), outcome)
}

pub(crate) fn jev_client(base_url: &str) -> Result<typesafe_sdk::blocking::Client, String> {
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
    TooManyStreams,
    /// 413, with the reason.
    TooLarge(&'static str),
    /// A message to a thread where several tasks wait on the user, and it
    /// names none of them: 409 with each task's label and question.
    WhichTask(Vec<WaitingTask>),
    ThreadNotFound,
    /// The store keeps no threads.
    NoThreads,
    /// The store keeps no stops.
    NoStops,
    /// The store keeps no outbox, so there is nothing to stream.
    NoStreams,
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
            Self::NoStreams => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "streams are not available" })),
            )
                .into_response(),
            Self::NoStops => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "stops are not available" })),
            )
                .into_response(),
            Self::NoThreads => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "threads are not available" })),
            )
                .into_response(),
            Self::ThreadNotFound => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "thread not found" })),
            )
                .into_response(),
            Self::TooLarge(message) => (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(serde_json::json!({ "error": message })),
            )
                .into_response(),
            Self::WhichTask(waiting) => (
                StatusCode::CONFLICT,
                Json(serde_json::json!({
                    "error": "which task?",
                    "waiting": waiting
                        .iter()
                        .map(|task| serde_json::json!({
                            "label": format!("T{}", task.label),
                            "run_id": task.run_id,
                            "question_id": task.question_id,
                            "question": task.question,
                        }))
                        .collect::<Vec<_>>(),
                })),
            )
                .into_response(),
            Self::TooManyStreams => (
                StatusCode::TOO_MANY_REQUESTS,
                Json(serde_json::json!({ "error": "too many open streams" })),
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

#[cfg(test)]
mod addressed_tests {
    use super::addressed;
    use proptest::prelude::*;

    #[test]
    fn a_label_names_a_task_and_its_text() {
        assert_eq!(addressed("T1: SFO"), Some((1, "SFO")));
        assert_eq!(addressed("  t12:LAX  "), Some((12, "LAX")));
        assert_eq!(addressed("T2:"), Some((2, "")));
        for plain in [
            "SFO",
            "T: x",
            "T0: x",
            "T1 x",
            "T1234567890: x",
            "Tx: y",
            "",
            "T",
        ] {
            assert_eq!(addressed(plain), None, "{plain:?}");
        }
    }

    proptest! {
        // Any input: no panic, and a label is read only from its exact form.
        #[test]
        fn any_input_is_read_only_in_its_exact_form(input in any::<String>()) {
            let oracle = {
                let rest = input.trim_start();
                let rest = rest.strip_prefix('T').or_else(|| rest.strip_prefix('t'));
                rest.and_then(|rest| {
                    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
                    let after = &rest[digits.len()..];
                    let number: usize = digits.parse().ok()?;
                    (!digits.is_empty() && digits.len() <= 9 && number >= 1)
                        .then_some(())
                        .and(after.strip_prefix(':'))
                        .map(|text| (number, text.trim()))
                })
            };
            prop_assert_eq!(addressed(&input), oracle);
        }

        #[test]
        fn a_formed_label_reads_back(number in 1usize..=999_999_999, text in "[^\\s].*|") {
            let input = format!("T{number}: {text}");
            prop_assert_eq!(addressed(&input), Some((number, text.trim())));
        }
    }
}
