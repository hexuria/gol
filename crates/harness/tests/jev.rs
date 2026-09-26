//! `JevDecider` shows System One the run as it stands (its input, a fold
//! summary, the recent events, the tool catalog and the skills) and offers only
//! choices the driver can take. Jev is mocked with wiremock on
//! `/v1/systemone`; the requests it receives are read back and checked.

use harness::{
    jev_choices, jev_state, run_to_completion, DeciderError, DecisionView, Driver, EchoTool,
    InMemory, JevDecider, ModelCompletion, Skill, Tool, MAX_EVENT_TEXT,
};
use protocol::{
    AgentId, Capability, CredentialSource, Event, EventPayload, ExecutionPlacement, HarnessState,
    Limits, MessageRole, ModelMessage, ModelProvider, ModelRequest, RunSpec, ToolDescriptor,
    WorkModel,
};
use serde_json::{json, Value};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A model that always answers.
struct Answering;

impl ModelCompletion for Answering {
    fn complete(&self, _request: &ModelRequest) -> Result<ModelMessage, String> {
        Ok(ModelMessage {
            role: MessageRole::Assistant,
            text: "ok".into(),
        })
    }
}

fn spec(limits: Limits) -> RunSpec {
    spec_with_input(limits, "hi")
}

fn spec_with_input(limits: Limits, input: &str) -> RunSpec {
    RunSpec::builder()
        .agent(AgentId::new(), "1")
        .input(input)
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::Anthropic,
            model_name: "m".into(),
            credential: CredentialSource::PlatformGateway,
        })
        .capabilities(vec![
            Capability::new("tool.echo"),
            Capability::new("model.call"),
        ])
        .limits(limits)
        .build()
}

fn limits(max_steps: u32, max_model_calls: u32) -> Limits {
    Limits {
        max_steps,
        max_model_calls,
    }
}

/// A System One response that picks `label` for the `effect` question.
fn answer(label: &str) -> Value {
    json!({
        "model": "jev-latest",
        "usage": {"input_tokens": 1, "output_tokens": 1},
        "answers": {
            "effect": {
                "type": "choice",
                "choice": label,
                "confidence": 1.0,
                "probabilities": {label: 1.0}
            }
        }
    })
}

/// Jev answers each label once, in order, and repeats the last one.
async fn jev(labels: &[&str]) -> MockServer {
    let server = MockServer::start().await;
    let (last, first) = labels.split_last().expect("at least one answer");
    for label in first {
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .respond_with(ResponseTemplate::new(200).set_body_json(answer(label)))
            .up_to_n_times(1)
            .mount(&server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(answer(last)))
        .mount(&server)
        .await;
    server
}

/// A finished run: its events and final state.
struct Ran {
    events: Vec<Event>,
    state: protocol::RunState,
}

/// Runs a fresh driver against Jev at `base_url` until the run ends.
async fn run(base_url: String, limits: Limits) -> Ran {
    let (ended, ran) = try_run(base_url, limits, "hi").await;
    ended.unwrap();
    ran
}

/// Runs a fresh driver with `input` against Jev at `base_url`, and returns how
/// the loop ended with the run as it then stood.
async fn try_run(base_url: String, limits: Limits, input: &str) -> (Result<(), DeciderError>, Ran) {
    let input = input.to_string();
    tokio::task::spawn_blocking(move || {
        let client = typesafe_sdk::blocking::Client::builder()
            .api_key("gol")
            .base_url(base_url)
            .retry(typesafe_sdk::RetryPolicy::disabled())
            .build()
            .unwrap();
        let mut decider = JevDecider::new(client);
        let mut driver = Driver::boot(spec_with_input(limits, &input)).unwrap();
        let echo = EchoTool;
        let tools: [&dyn Tool; 1] = [&echo];
        let ended = run_to_completion(
            &mut driver,
            &mut decider,
            &tools,
            &Answering,
            &mut InMemory::default(),
        );
        let ran = Ran {
            events: driver.events().to_vec(),
            state: driver.state(),
        };
        (ended, ran)
    })
    .await
    .unwrap()
}

/// The JSON bodies Jev received, in order.
async fn requests(server: &MockServer) -> Vec<Value> {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|request| serde_json::from_slice(&request.body).unwrap())
        .collect()
}

fn criteria(request: &Value) -> &Value {
    &request["questions"]["effect"]["criteria"]
}

fn completed(ran: &Ran) -> bool {
    ran.state.harness
        == HarnessState::Completed {
            outcome: "done".into(),
        }
}

#[tokio::test]
async fn jev_offers_catalog_tools() {
    let server = jev(&["complete"]).await;
    let driver = run(server.uri(), limits(4, 4)).await;
    assert!(completed(&driver));

    let sent = requests(&server).await;
    assert_eq!(sent.len(), 1);
    assert_eq!(
        sent[0]["questions"]["effect"]["instructions"],
        "Choose the next effect for this run."
    );
    assert_eq!(
        *criteria(&sent[0]),
        json!({
            "echo": "Returns the input text.",
            "model": "Ask the work model about the input.",
            "complete": "Finish the run."
        })
    );
    assert_eq!(sent[0]["state"]["input"], "hi");
    assert_eq!(
        sent[0]["state"]["tools"],
        json!([{
            "name": "echo",
            "description": "Returns the input text.",
            "input_schema": "{\"type\":\"string\"}"
        }])
    );
}

#[tokio::test]
async fn jev_state_includes_last_tool_result() {
    let server = jev(&["echo", "complete"]).await;
    let driver = run(server.uri(), limits(4, 4)).await;
    assert!(completed(&driver));

    let result = driver
        .events
        .iter()
        .find(|event| matches!(event.payload, EventPayload::ToolResult { .. }))
        .expect("echo ran");
    let sent = requests(&server).await;
    assert_eq!(sent.len(), 2);
    let recent = sent[1]["state"]["recent_events"].as_array().unwrap();
    assert!(
        recent.contains(&serde_json::to_value(&result.payload).unwrap()),
        "{recent:?}"
    );
    assert_eq!(sent[1]["state"]["run"]["steps"], 1);
}

#[tokio::test]
async fn jev_at_budget_offers_only_complete() {
    let server = jev(&["echo", "complete"]).await;
    let driver = run(server.uri(), limits(1, 4)).await;
    assert!(completed(&driver), "{:?}", driver.state.harness);

    let sent = requests(&server).await;
    assert_eq!(sent.len(), 2);
    assert_eq!(*criteria(&sent[1]), json!({"complete": "Finish the run."}));
    assert_eq!(sent[1]["state"]["run"]["steps_exhausted"], true);
}

#[tokio::test]
async fn jev_leaves_out_model_after_the_model_budget() {
    let server = jev(&["model", "complete"]).await;
    let driver = run(server.uri(), limits(4, 1)).await;
    assert!(completed(&driver), "{:?}", driver.state.harness);

    let sent = requests(&server).await;
    assert_eq!(sent.len(), 2);
    assert_eq!(
        *criteria(&sent[1]),
        json!({"echo": "Returns the input text.", "complete": "Finish the run."})
    );
}

fn decided(ran: &Ran) -> usize {
    ran.events
        .iter()
        .filter(|event| matches!(event.payload, EventPayload::EffectDecided { .. }))
        .count()
}

// Jev may only pick what it was offered. After the last model call `model` is
// not offered, so answering it anyway is an error, not a model call that would
// fail the run on its budget.
#[tokio::test]
async fn an_answer_that_was_not_offered_is_an_error() {
    let server = jev(&["model", "model"]).await;
    let (ended, ran) = try_run(server.uri(), limits(4, 1), "hi").await;
    assert_eq!(
        ended,
        Err(DeciderError {
            message: "unknown effect choice: model".into()
        })
    );
    assert_eq!(ran.state.model_calls, 1);
    assert_eq!(decided(&ran), 1);
}

// A label no one offered is not taken as a tool name.
#[tokio::test]
async fn an_invented_label_is_an_error() {
    let server = jev(&["shell"]).await;
    let (ended, ran) = try_run(server.uri(), limits(4, 4), "hi").await;
    assert_eq!(
        ended,
        Err(DeciderError {
            message: "unknown effect choice: shell".into()
        })
    );
    assert_eq!(decided(&ran), 0);
}

fn strings(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(text) => out.push(text.clone()),
        Value::Array(items) => items.iter().for_each(|item| strings(item, out)),
        Value::Object(fields) => fields.values().for_each(|field| strings(field, out)),
        _ => {}
    }
}

// The input is sent once in full; the copies of it inside recent events are
// cut, so a long input does not grow the request with every step.
#[tokio::test]
async fn long_event_text_is_capped_in_the_state() {
    let input = "x".repeat(100_000);
    let server = jev(&["echo", "complete"]).await;
    let (ended, _) = try_run(server.uri(), limits(4, 4), &input).await;
    ended.unwrap();

    let sent = requests(&server).await;
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[1]["state"]["input"], input.as_str());
    let mut texts = Vec::new();
    strings(&sent[1]["state"]["recent_events"], &mut texts);
    let longest = texts.iter().map(|text| text.chars().count()).max().unwrap();
    assert!(
        longest < MAX_EVENT_TEXT + 64,
        "longest event text: {longest}"
    );
    assert!(
        texts
            .iter()
            .any(|text| text.ends_with("… [truncated 97952 chars]")),
        "{:?}",
        texts
            .iter()
            .map(|text| text.chars().count())
            .collect::<Vec<_>>()
    );
}

fn view<'a>(
    spec: &'a RunSpec,
    state: &'a protocol::RunState,
    events: &'a [Event],
    tools: &'a [ToolDescriptor],
    skills: &'a [Skill],
) -> DecisionView<'a> {
    DecisionView {
        spec,
        state,
        events,
        tools,
        skills,
        steps_exhausted: false,
        model_calls_exhausted: false,
    }
}

#[test]
fn a_tool_named_like_a_builtin_choice_is_left_out() {
    let spec = spec(limits(4, 4));
    let driver = Driver::boot(spec.clone()).unwrap();
    let state = driver.state();
    let mut complete = EchoTool.descriptor();
    complete.name = "complete".into();
    let tools = [EchoTool.descriptor(), complete];
    let view = view(&spec, &state, driver.events(), &tools, &[]);

    let labels: Vec<String> = jev_choices(&view)
        .into_iter()
        .map(|(label, _)| label)
        .collect();
    assert_eq!(labels, ["echo", "model", "complete"]);
}

#[test]
fn jev_state_carries_skills_and_the_last_16_events() {
    let spec = spec(limits(4, 4));
    let driver = Driver::boot(spec.clone()).unwrap();
    let state = driver.state();
    let events: Vec<Event> = driver.events().iter().cycle().take(20).cloned().collect();
    let skills = [Skill {
        name: "tone".into(),
        body: "Be brief.".into(),
    }];
    let view = view(&spec, &state, &events, &[], &skills);

    let rendered = jev_state(&view);
    let expected: Vec<Value> = events[4..]
        .iter()
        .map(|event| serde_json::to_value(&event.payload).unwrap())
        .collect();
    assert_eq!(rendered["recent_events"], Value::Array(expected));
    assert_eq!(
        rendered["skills"],
        json!([{"name": "tone", "body": "Be brief."}])
    );
}
