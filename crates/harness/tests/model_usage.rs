//! D1: the driver records the tokens a model call used on `ModelResponded`,
//! when the model reports them.
use harness::{run_to_completion, Driver, InMemory, ModelCompletion, ScriptedDecider};
use protocol::{
    AgentId, Capability, CredentialSource, Effect, EventPayload, ExecutionPlacement, Limits,
    MessageRole, ModelMessage, ModelProvider, ModelRequest, Owner, RunSpec, Usage, WorkModel,
};

struct Reporting;

impl ModelCompletion for Reporting {
    fn complete(&self, request: &ModelRequest) -> Result<ModelMessage, String> {
        self.complete_with_usage(request)
            .map(|(message, _)| message)
    }

    fn complete_with_usage(
        &self,
        _request: &ModelRequest,
    ) -> Result<(ModelMessage, Option<Usage>), String> {
        Ok((
            ModelMessage {
                role: MessageRole::Assistant,
                text: "ok".to_string(),
            },
            Some(Usage {
                input_tokens: 21,
                output_tokens: 4,
            }),
        ))
    }
}

struct Silent;

impl ModelCompletion for Silent {
    fn complete(&self, _request: &ModelRequest) -> Result<ModelMessage, String> {
        Ok(ModelMessage {
            role: MessageRole::Assistant,
            text: "ok".to_string(),
        })
    }
}

fn usage_recorded(models: &dyn ModelCompletion) -> Vec<Option<Usage>> {
    let spec = RunSpec::builder()
        .owner(Owner::new("https://issuer.test", "user-1", "tenant-1"))
        .agent(AgentId::new(), "1")
        .input("hello")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::Anthropic,
            model_name: "m".to_string(),
            credential: CredentialSource::PlatformGateway,
        })
        .capabilities(vec![Capability::new("model.call")])
        .limits(Limits {
            max_steps: 4,
            max_model_calls: 2,
        })
        .build();
    let mut driver = Driver::boot(spec).unwrap();
    let mut decider = ScriptedDecider::new([
        Effect::ModelCall {
            prompt: "hello".to_string(),
        },
        Effect::Complete {
            outcome: "done".to_string(),
        },
    ]);
    run_to_completion(&mut driver, &mut decider, &[], models, &InMemory::default()).unwrap();
    driver
        .events()
        .iter()
        .filter_map(|event| match &event.payload {
            EventPayload::ModelResponded { usage, .. } => Some(*usage),
            _ => None,
        })
        .collect()
}

#[test]
fn a_reported_usage_is_recorded_on_the_response() {
    assert_eq!(
        usage_recorded(&Reporting),
        [Some(Usage {
            input_tokens: 21,
            output_tokens: 4,
        })]
    );
}

#[test]
fn a_model_that_reports_no_usage_records_none() {
    assert_eq!(usage_recorded(&Silent), [None]);
}
