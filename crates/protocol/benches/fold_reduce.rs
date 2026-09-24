use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use protocol::{
    Actor, AgentId, CredentialSource, Effect, Event, EventPayload, ExecutionPlacement,
    HarnessState, Limits, ModelProvider, RunSpec, Timestamp, WorkModel,
};

fn spec() -> RunSpec {
    RunSpec::builder()
        .agent(AgentId::new(), "1")
        .input("hello")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::OpenAI,
            model_name: "gpt-test".to_string(),
            credential: CredentialSource::PlatformGateway,
        })
        .limits(Limits {
            max_steps: 8,
            max_model_calls: 4,
        })
        .build()
}

fn event(spec: &RunSpec, payload: EventPayload) -> Event {
    Event::record(
        spec.run_id,
        spec.agent_id,
        &spec.agent_version,
        None,
        Actor::System,
        None,
        Timestamp::unix_millis(0),
        payload,
    )
}

fn completed_log(spec: &RunSpec) -> Vec<Event> {
    vec![
        event(spec, EventPayload::RunStarted),
        event(
            spec,
            EventPayload::EffectAuthorized {
                effect: Effect::ToolCall {
                    name: "echo".to_string(),
                    input: "hello".to_string(),
                },
            },
        ),
        event(
            spec,
            EventPayload::ToolResult {
                name: "echo".to_string(),
                output: "hello".to_string(),
            },
        ),
        event(
            spec,
            EventPayload::RunCompleted {
                outcome: "done".to_string(),
            },
        ),
    ]
}

fn bench_fold(c: &mut Criterion) {
    let spec = spec();
    let events = completed_log(&spec);
    c.bench_function("fold", |b| {
        b.iter(|| black_box(protocol::fold(black_box(&spec), black_box(&events))))
    });
}

fn bench_reduce(c: &mut Criterion) {
    let spec = spec();
    let state = HarnessState::Running {
        step: 1,
        attempt: 0,
        answered: false,
    };
    let event = event(
        &spec,
        EventPayload::EffectAuthorized {
            effect: Effect::ToolCall {
                name: "echo".to_string(),
                input: "hello".to_string(),
            },
        },
    );
    c.bench_function("reduce", |b| {
        b.iter(|| {
            black_box(protocol::reduce(
                black_box(state.clone()),
                black_box(&event),
            ))
        })
    });
}

criterion_group!(benches, bench_fold, bench_reduce);
criterion_main!(benches);
