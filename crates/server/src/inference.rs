use std::sync::Arc;

use protocol::{
    Actor, CredentialSource, Event, EventPayload, ExecutionPlacement, MessageRole, ModelMessage,
    RunId, RunSpec, Timestamp,
};

use crate::store::{RunStore, StoredRun};

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
}

pub fn computer_plan(placement: ExecutionPlacement) -> ComputerPlan {
    match placement {
        ExecutionPlacement::Box => {
            let command = "docker run --rm -d --name gol-agent-box -v gol-workspace:/workspace gol-agent:production".to_string();
            let started = std::env::var("GOL_START_BOX").ok().as_deref() == Some("1")
                && std::process::Command::new("docker")
                    .args([
                        "run",
                        "--rm",
                        "-d",
                        "--name",
                        "gol-agent-box",
                        "-v",
                        "gol-workspace:/workspace",
                        "gol-agent:production",
                    ])
                    .status()
                    .map(|status| status.success())
                    .unwrap_or(false);
            ComputerPlan {
                image: "gol-agent:production".to_string(),
                started_by: "server",
                command,
                started,
            }
        }
        ExecutionPlacement::Local | ExecutionPlacement::Reverse => ComputerPlan {
            image: "gol-agent:local".to_string(),
            started_by: "desktop",
            command: "docker run --rm -d --name gol-agent-local -v gol-workspace:/workspace gol-agent:local".to_string(),
            started: false,
        },
    }
}

#[derive(Clone, Debug)]
pub struct TurnOutcome {
    pub spec: RunSpec,
    pub events: Vec<Event>,
    pub completion: Option<String>,
    pub credential_mode: &'static str,
}

pub enum TurnError {
    Proxy(String),
    NotFound,
    Conflict(&'static str),
    BadRequest(&'static str),
}

/// Record the user message, then post to the proxy only for the platform gateway.
pub fn open_turn(
    store: &dyn RunStore,
    spec: RunSpec,
    poster: &dyn GatewayPoster,
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

    match &spec.work_model.credential {
        CredentialSource::PlatformGateway => {
            let text = poster
                .complete(&GatewayCall {
                    run_id: spec.run_id,
                    input: spec.input.clone(),
                    model_name: spec.work_model.model_name.clone(),
                    placement: spec.placement,
                })
                .map_err(TurnError::Proxy)?;
            append_completion(&spec, &mut events, &text);
            store.put_run(StoredRun {
                spec: spec.clone(),
                events: events.clone(),
            });
            Ok(TurnOutcome {
                spec,
                events,
                completion: Some(text),
                credential_mode: "gateway",
            })
        }
        CredentialSource::BringYourOwn { .. } => Ok(TurnOutcome {
            spec,
            events,
            completion: None,
            credential_mode: "subscription",
        }),
    }
}

pub fn accept_subscription_completion(
    store: &dyn RunStore,
    run_id: RunId,
    text: &str,
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
    if stored
        .events
        .iter()
        .any(|event| matches!(event.payload, EventPayload::RunCompleted { .. }))
    {
        return Err(TurnError::Conflict("turn already completed"));
    }
    if !stored
        .events
        .iter()
        .any(|event| matches!(event.payload, EventPayload::UserMessage { .. }))
    {
        return Err(TurnError::Conflict("user message is not recorded"));
    }
    let mut events = stored.events;
    append_completion(&stored.spec, &mut events, text);
    store.put_run(StoredRun {
        spec: stored.spec.clone(),
        events: events.clone(),
    });
    Ok(TurnOutcome {
        spec: stored.spec,
        events,
        completion: Some(text.to_string()),
        credential_mode: "subscription",
    })
}

pub fn user_message_event(spec: &RunSpec) -> Event {
    Event::record(
        spec.run_id,
        spec.agent_id,
        &spec.agent_version,
        None,
        Actor::System,
        None,
        Timestamp::now(),
        EventPayload::UserMessage {
            text: spec.input.clone(),
        },
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
        spec.run_id,
        spec.agent_id,
        &spec.agent_version,
        None,
        actor,
        None,
        Timestamp::now(),
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
