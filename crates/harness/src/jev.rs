use protocol::{Effect, InvocationId};
use serde_json::{json, Value};
use typesafe_sdk::blocking::Client;
use typesafe_sdk::Question;

use crate::{Decider, DeciderError, DecisionView};

/// How many of the latest events `jev_state` shows Jev.
pub const RECENT_EVENTS: usize = 16;

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
/// `RECENT_EVENTS` event payloads, the tool catalog and the skills.
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
        "recent_events": recent.iter().map(|event| &event.payload).collect::<Vec<_>>(),
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

/// The choices Jev is offered, as (label, description). Once the step budget
/// is spent only `complete` is offered, the one decision that is still free.
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
