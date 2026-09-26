use std::collections::VecDeque;

use protocol::{Effect, Event, RunSpec, RunState, ToolDescriptor};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Skill {
    pub name: String,
    pub body: String,
}

pub struct DecisionView<'a> {
    pub spec: &'a RunSpec,
    pub state: &'a RunState,
    pub events: &'a [Event],
    pub tools: &'a [ToolDescriptor],
    pub skills: &'a [Skill],
    /// The run has spent its step budget. Only a `Complete` that finishes the
    /// run is still allowed; any other decision fails the run with `Budget`.
    /// While a tool call is outstanding even a `Complete` cannot finish the
    /// run, so it fails too.
    pub steps_exhausted: bool,
    /// The run has spent its model-call budget. A model call fails the run
    /// with `Budget`; other effects are still allowed while steps remain.
    pub model_calls_exhausted: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeciderError {
    pub message: String,
}

pub trait Decider {
    fn decide(&mut self, view: &DecisionView<'_>) -> Result<Effect, DeciderError>;
}

pub struct ScriptedDecider {
    effects: VecDeque<Effect>,
}

impl ScriptedDecider {
    pub fn new(effects: impl IntoIterator<Item = Effect>) -> Self {
        Self {
            effects: effects.into_iter().collect(),
        }
    }
}

impl Decider for ScriptedDecider {
    fn decide(&mut self, _view: &DecisionView<'_>) -> Result<Effect, DeciderError> {
        self.effects.pop_front().ok_or_else(|| DeciderError {
            message: "scripted decider has no further effect".to_string(),
        })
    }
}
