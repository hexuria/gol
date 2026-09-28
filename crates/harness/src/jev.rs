use protocol::{
    AgentId, Capability, Effect, HarnessState, InvocationId, MAX_CHILDREN, MAX_DELEGATION_HOPS,
};
use serde_json::{json, Value};
use typesafe_sdk::blocking::Client;
use typesafe_sdk::Question;

use crate::{Decider, DeciderError, DecisionView, DelegateTarget};

/// How many of the latest events `jev_state` shows Jev.
pub const RECENT_EVENTS: usize = 16;

/// The longest string, in characters, `jev_state` keeps inside a recent event.
pub const MAX_EVENT_TEXT: usize = 2048;

const MODEL: &str = "model";
const COMPLETE: &str = "complete";
const DELEGATE: &str = "delegate:";

pub struct JevDecider {
    client: Client,
}

impl JevDecider {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

/// The run as Jev sees it: the input, a summary of the fold, the latest
/// `RECENT_EVENTS` event payloads (each string capped at `MAX_EVENT_TEXT`
/// characters), the tool catalog and the skills. The input is sent once, in
/// full.
pub fn jev_state(view: &DecisionView<'_>) -> Value {
    let limits = &view.spec.limits;
    let recent = &view.events[view.events.len().saturating_sub(RECENT_EVENTS)..];
    json!({
        "input": view.spec.input,
        "run": {
            "harness": view.state.harness,
            "steps": view.state.steps,
            "max_steps": limits.max_steps,
            "model_calls": view.state.model_calls,
            "max_model_calls": limits.max_model_calls,
            "steps_exhausted": view.steps_exhausted,
            "model_calls_exhausted": view.model_calls_exhausted,
        },
        "recent_events": recent
            .iter()
            .map(|event| capped(json!(event.payload)))
            .collect::<Vec<_>>(),
        "tools": view.tools.iter().map(|tool| json!({
            "name": tool.name,
            "description": tool.description,
            "input_schema": tool.input_schema,
        })).collect::<Vec<_>>(),
        "skills": view.skills.iter().map(|skill| json!({
            "name": skill.name,
            "body": skill.body,
        })).collect::<Vec<_>>(),
    })
}

/// `value` with every string longer than `MAX_EVENT_TEXT` characters cut to
/// that length and marked. Event payloads repeat the input and tool outputs, so
/// without the cap each step would resend them in full.
fn capped(value: Value) -> Value {
    match value {
        Value::String(text) => {
            let length = text.chars().count();
            if length <= MAX_EVENT_TEXT {
                return Value::String(text);
            }
            let kept: String = text.chars().take(MAX_EVENT_TEXT).collect();
            Value::String(format!(
                "{kept}… [truncated {} chars]",
                length - MAX_EVENT_TEXT
            ))
        }
        Value::Array(items) => Value::Array(items.into_iter().map(capped).collect()),
        Value::Object(fields) => Value::Object(
            fields
                .into_iter()
                .map(|(key, field)| (key, capped(field)))
                .collect(),
        ),
        other => other,
    }
}

/// The choices Jev is offered, as (label, description). Once the step budget
/// is spent only `complete` is offered: while the harness is running it is the
/// one decision that is still free.
/// Otherwise each catalog tool is offered under its own name, then each
/// delegation `delegate_choices` allows, then `model` while model calls
/// remain, then `complete`. A tool named `model` or `complete`, or starting
/// with `delegate:`, is left out, since its label would be ambiguous.
pub fn jev_choices(view: &DecisionView<'_>) -> Vec<(String, Option<String>)> {
    let complete = (COMPLETE.to_string(), Some("Finish the run.".to_string()));
    if view.steps_exhausted {
        return vec![complete];
    }
    let mut choices: Vec<(String, Option<String>)> = view
        .tools
        .iter()
        .filter(|tool| {
            tool.name != MODEL && tool.name != COMPLETE && !tool.name.starts_with(DELEGATE)
        })
        .map(|tool| (tool.name.clone(), Some(tool.description.clone())))
        .collect();
    choices.extend(
        delegate_choices(view)
            .into_iter()
            .map(|(label, description, _)| (label, Some(description))),
    );
    if !view.model_calls_exhausted {
        choices.push((
            MODEL.to_string(),
            Some("Ask the work model about the input.".to_string()),
        ));
    }
    choices.push(complete);
    choices
}

/// The `delegate:<name>` choices, as (label, description, agent): one per
/// target other than the run's own agent, and none unless a delegation could
/// start a child. The driver refuses a delegation from a run that is not
/// running, has 10 children, or has fewer than 2 steps or 2 model calls left
/// to give once this decision has taken its step; the authorizer refuses one
/// without `agent.delegate` or 8 hops deep. A name several targets share gets
/// the first 8 characters of each id; an empty name is the id.
fn delegate_choices(view: &DecisionView<'_>) -> Vec<(String, String, AgentId)> {
    let (spec, state) = (view.spec, view.state);
    let left = |max: u32, used: u32, given: u32| max.saturating_sub(used.saturating_add(given));
    let steps_left = left(spec.limits.max_steps, state.steps, state.given_steps);
    let model_calls_left = left(
        spec.limits.max_model_calls,
        state.model_calls,
        state.given_model_calls,
    );
    let open = matches!(state.harness, HarnessState::Running { .. })
        && spec
            .capabilities
            .contains(&Capability::new("agent.delegate"))
        && spec.lineage.hop < MAX_DELEGATION_HOPS
        && state.children < MAX_CHILDREN
        && steps_left > 2
        && model_calls_left >= 2;
    if !open {
        return Vec::new();
    }
    let name = |target: &DelegateTarget| {
        if target.name.is_empty() {
            target.agent_id.to_string()
        } else {
            target.name.clone()
        }
    };
    let targets: Vec<&DelegateTarget> = view
        .agents
        .iter()
        .filter(|target| target.agent_id != spec.agent_id)
        .collect();
    targets
        .iter()
        .map(|target| {
            let named = name(target);
            let shared = targets.iter().filter(|other| name(other) == named).count() > 1;
            let label = if shared {
                let id = target.agent_id.to_string();
                format!("{DELEGATE}{named}-{}", &id[..8])
            } else {
                format!("{DELEGATE}{named}")
            };
            let description = if target.description.is_empty() {
                format!("Hand the input to agent {named}.")
            } else {
                target.description.clone()
            };
            (label, description, target.agent_id)
        })
        .collect()
}

impl Decider for JevDecider {
    fn decide(&mut self, view: &DecisionView<'_>) -> Result<Effect, DeciderError> {
        let choices = jev_choices(view);
        let criteria = choices
            .iter()
            .map(|(label, description)| (label.clone(), description.clone().map(Into::into)));
        let response = self
            .client
            .system_one(
                jev_state(view),
                [(
                    "effect",
                    Question::choice("Choose the next effect for this run.", criteria),
                )],
            )
            .map_err(|error| DeciderError {
                message: error.to_string(),
            })?;
        let choice = response
            .choice("effect")
            .map_err(|error| DeciderError {
                message: error.to_string(),
            })?
            .choice
            .clone();
        if !choices.iter().any(|(label, _)| *label == choice) {
            return Err(DeciderError {
                message: format!("unknown effect choice: {choice}"),
            });
        }
        if let Some((_, _, agent_id)) = delegate_choices(view)
            .into_iter()
            .find(|(label, _, _)| *label == choice)
        {
            return Ok(Effect::Delegate {
                agent_id,
                input: view.spec.input.clone(),
            });
        }
        Ok(match choice.as_str() {
            MODEL => Effect::ModelCall {
                prompt: view.spec.input.clone(),
            },
            COMPLETE => Effect::Complete {
                outcome: "done".to_string(),
            },
            tool => Effect::ToolCall {
                name: tool.to_string(),
                input: view.spec.input.clone(),
                invocation: InvocationId::new(),
            },
        })
    }
}
