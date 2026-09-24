use protocol::{Effect, InvocationId};
use typesafe_sdk::blocking::Client;
use typesafe_sdk::Question;

use crate::{Decider, DeciderError, DecisionView};

pub struct JevDecider {
    client: Client,
}

impl JevDecider {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

impl Decider for JevDecider {
    fn decide(&mut self, view: &DecisionView<'_>) -> Result<Effect, DeciderError> {
        let response = self
            .client
            .system_one(
                view.spec.input.clone(),
                [(
                    "effect",
                    Question::choice(
                        "effect",
                        [("echo", None), ("model", None), ("complete", None)],
                    ),
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
        match choice.as_str() {
            "echo" => Ok(Effect::ToolCall {
                name: "echo".to_string(),
                input: view.spec.input.clone(),
                invocation: InvocationId::new(),
            }),
            "model" => Ok(Effect::ModelCall {
                prompt: view.spec.input.clone(),
            }),
            "complete" => Ok(Effect::Complete {
                outcome: "done".to_string(),
            }),
            other => Err(DeciderError {
                message: format!("unknown effect choice: {other}"),
            }),
        }
    }
}
