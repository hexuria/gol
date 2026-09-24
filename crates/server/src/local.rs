use harness::{Decider, DeciderError, DecisionView};
use protocol::{Effect, InvocationId};

pub struct LocalEchoFactory;

pub struct LocalEchoDecider {
    calls: u8,
}

impl Decider for LocalEchoDecider {
    fn decide(&mut self, view: &DecisionView<'_>) -> Result<Effect, DeciderError> {
        let effect = if self.calls == 0 {
            Effect::ToolCall {
                name: "echo".to_string(),
                input: view.spec.input.clone(),
                invocation: InvocationId::new(),
            }
        } else {
            Effect::Complete {
                outcome: "done".to_string(),
            }
        };
        self.calls += 1;
        Ok(effect)
    }
}

impl LocalEchoFactory {
    pub fn decider(&self) -> LocalEchoDecider {
        LocalEchoDecider { calls: 0 }
    }
}
