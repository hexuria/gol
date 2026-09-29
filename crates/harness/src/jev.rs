use protocol::{
    AgentId, Capability, Effect, HarnessState, InvocationId, MAX_CHILDREN, MAX_DELEGATION_HOPS,
    MAX_MESSAGE_BYTES,
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
const TELL: &str = "tell:";
const ASK: &str = "ask:";

/// How long an ask Jev chooses waits for its answer (decision 28A): the
/// ask's task usually answers first, with its outcome (decision 31A).
pub const JEV_ASK_TIMEOUT_SECS: u32 = 3600;

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
/// delegation `delegate_choices` allows, then each tell and each ask
/// `message_choices` allows, then `model` while model calls remain, then
/// `complete`. A tool named `model` or `complete`, or starting with
/// `delegate:`, `tell:` or `ask:`, is left out, since its label would be
/// ambiguous.
pub fn jev_choices(view: &DecisionView<'_>) -> Vec<(String, Option<String>)> {
    let complete = (COMPLETE.to_string(), Some("Finish the run.".to_string()));
    if view.steps_exhausted {
        return vec![complete];
    }
    let mut choices: Vec<(String, Option<String>)> = view
        .tools
        .iter()
        .filter(|tool| {
            tool.name != MODEL
                && tool.name != COMPLETE
                && ![DELEGATE, TELL, ASK]
                    .iter()
                    .any(|prefix| tool.name.starts_with(prefix))
        })
        .map(|tool| (tool.name.clone(), Some(tool.description.clone())))
        .collect();
    choices.extend(
        delegate_choices(view)
            .into_iter()
            .map(|(label, description, _)| (label, Some(description))),
    );
    choices.extend(
        message_choices(view)
            .into_iter()
            .map(|(label, description, _, _)| (label, Some(description))),
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
/// the first 8 characters of each id; an empty name is the id. The chosen
/// label is looked up exactly, so a label that still names more than one
/// target is not offered at all.
fn delegate_choices(view: &DecisionView<'_>) -> Vec<(String, String, AgentId)> {
    if !starts_task(view, "agent.delegate") {
        return Vec::new();
    }
    target_choices(view, DELEGATE, |named, description| {
        if description.is_empty() {
            format!("Hand the input to agent {named}.")
        } else {
            description.to_string()
        }
    })
}

/// The `tell:<name>` then the `ask:<name>` choices (decision 32A), as
/// (label, description, agent, whether it asks), labelled and filtered as
/// the delegate choices are: a tell or a new ask starts a task whose budget
/// is carved as a delegation's (decision 33A). None without a deliverer,
/// without `agent.message`, or with an input longer than a message may be
/// (the body is the input, and the authorizer would deny it).
fn message_choices(view: &DecisionView<'_>) -> Vec<(String, String, AgentId, bool)> {
    if !view.messaging
        || view.spec.input.len() > MAX_MESSAGE_BYTES
        || !starts_task(view, "agent.message")
    {
        return Vec::new();
    }
    let described = |verb: String, description: &str| {
        if description.is_empty() {
            format!("{verb}.")
        } else {
            format!("{verb}: {description}")
        }
    };
    let tells = target_choices(view, TELL, |named, description| {
        described(
            format!("Tell agent {named} the input, without waiting"),
            description,
        )
    });
    let asks = target_choices(view, ASK, |named, description| {
        described(
            format!("Ask agent {named} about the input, and wait for its answer"),
            description,
        )
    });
    tells
        .into_iter()
        .map(|(label, description, agent)| (label, description, agent, false))
        .chain(
            asks.into_iter()
                .map(|(label, description, agent)| (label, description, agent, true)),
        )
        .collect()
}

/// Whether a decision could start a task in another agent now: the run
/// holds `capability`, is running below the hop limit with fewer than 10
/// children, and after this decision's step has 2 steps and 2 model calls
/// to give.
fn starts_task(view: &DecisionView<'_>, capability: &str) -> bool {
    let (spec, state) = (view.spec, view.state);
    let left = |max: u32, used: u32, given: u32| max.saturating_sub(used.saturating_add(given));
    let steps_left = left(spec.limits.max_steps, state.steps, state.given_steps);
    let model_calls_left = left(
        spec.limits.max_model_calls,
        state.model_calls,
        state.given_model_calls,
    );
    matches!(state.harness, HarnessState::Running { .. })
        && spec.capabilities.contains(&Capability::new(capability))
        && spec.lineage.hop < MAX_DELEGATION_HOPS
        && state.children < MAX_CHILDREN
        && steps_left > 2
        && model_calls_left >= 2
}

/// One `<prefix><name>` choice per target other than the run's own agent,
/// as (label, description, agent), described by `describe(name,
/// description)`. A name several targets share gets the first 8 characters
/// of each id; an empty name is the id; a label that still names more than
/// one target is left out.
fn target_choices(
    view: &DecisionView<'_>,
    prefix: &str,
    describe: impl Fn(&str, &str) -> String,
) -> Vec<(String, String, AgentId)> {
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
        .filter(|target| target.agent_id != view.spec.agent_id)
        .collect();
    let choices: Vec<(String, String, AgentId)> = targets
        .iter()
        .map(|target| {
            let named = name(target);
            let shared = targets.iter().filter(|other| name(other) == named).count() > 1;
            let label = if shared {
                let id = target.agent_id.to_string();
                format!("{prefix}{named}-{}", &id[..8])
            } else {
                format!("{prefix}{named}")
            };
            (
                label,
                describe(&named, &target.description),
                target.agent_id,
            )
        })
        .collect();
    choices
        .iter()
        .filter(|(label, _, _)| {
            choices
                .iter()
                .filter(|(other, _, _)| other == label)
                .count()
                == 1
        })
        .cloned()
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
        if let Some((_, _, to, asks)) = message_choices(view)
            .into_iter()
            .find(|(label, _, _, _)| *label == choice)
        {
            return Ok(Effect::SendMessage {
                to,
                body: view.spec.input.clone(),
                expects_reply: asks,
                reply_to: None,
                timeout_secs: asks.then_some(JEV_ASK_TIMEOUT_SECS),
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
