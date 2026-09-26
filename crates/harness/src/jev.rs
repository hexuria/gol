use protocol::{Effect, InvocationId};
use serde_json::{json, Value};
use typesafe_sdk::blocking::Client;
use typesafe_sdk::Question;

use crate::{Decider, DeciderError, DecisionView};

/// How many of the latest events `jev_state` shows Jev.
pub const RECENT_EVENTS: usize = 16;

/// The longest string, in characters, `jev_state` keeps inside a recent event.
pub const MAX_EVENT_TEXT: usize = 2048;

const MODEL: &str = "model";
const COMPLETE: &str = "complete";

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
/// Otherwise each catalog tool is offered under its own name, then `model`
/// while model calls remain, then `complete`. A tool named `model` or
/// `complete` is left out, since its label would be ambiguous.
pub fn jev_choices(view: &DecisionView<'_>) -> Vec<(String, Option<String>)> {
    let complete = (COMPLETE.to_string(), Some("Finish the run.".to_string()));
    if view.steps_exhausted {
        return vec![complete];
    }
    let mut choices: Vec<(String, Option<String>)> = view
        .tools
        .iter()
        .filter(|tool| tool.name != MODEL && tool.name != COMPLETE)
        .map(|tool| (tool.name.clone(), Some(tool.description.clone())))
        .collect();
    if !view.model_calls_exhausted {
        choices.push((
            MODEL.to_string(),
            Some("Ask the work model about the input.".to_string()),
        ));
    }
    choices.push(complete);
    choices
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
