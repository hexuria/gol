//! `JevDecider` shows System One the run as it stands (its input, a fold
//! summary, the recent events, the tool catalog and the skills) and offers only
//! choices the driver can take. Jev is mocked with wiremock on
//! `/v1/systemone`; the requests it receives are read back and checked.

use std::sync::Arc;

use harness::{
    jev_choices, jev_state, run_to_completion, AgentSpawner, ChildRequest, Decider, DeciderError,
    DecisionView, DelegateTarget, Driver, EchoTool, InMemory, JevDecider, ModelCompletion, Skill,
    StartedChild, Tool, JEV_ASK_TIMEOUT_SECS, MAX_EVENT_TEXT,
};
use protocol::{
    AgentId, Capability, CredentialSource, Effect, Event, EventPayload, ExecutionPlacement,
    HarnessState, Limits, MessageRole, ModelMessage, ModelProvider, ModelRequest, RunId, RunSpec,
    ToolDescriptor, WorkModel,
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
        .owner(protocol::Owner::new(
            "https://issuer.test",
            "user-1",
            "tenant-1",
        ))
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
            &InMemory::default(),
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
        agents: &[],
        messaging: false,
        asking: false,
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

fn target(agent_id: AgentId, name: &str, description: &str) -> DelegateTarget {
    DelegateTarget {
        agent_id,
        name: name.to_string(),
        description: description.to_string(),
    }
}

fn delegating(limits: Limits) -> RunSpec {
    let mut spec = spec(limits);
    spec.capabilities.push(Capability::new("agent.delegate"));
    spec
}

fn labels(view: &DecisionView<'_>) -> Vec<String> {
    jev_choices(view)
        .into_iter()
        .map(|(label, _)| label)
        .collect()
}

// Phase 1.2b: each of the owner's other agents is offered as
// `delegate:<name>`, and only while a delegation could start a child: the run
// holds `agent.delegate`, is running below the hop limit with fewer than 10
// children, and after this decision's step still has 2 steps and 2 model
// calls to give. A shared name gets the first 8 characters of the id; an
// empty name is the id.
#[test]
fn jev_offers_delegate_choices_only_when_allowed() {
    let editor_a: AgentId = "aaaaaaaa-0000-4000-8000-000000000001".parse().unwrap();
    let editor_b: AgentId = "bbbbbbbb-0000-4000-8000-000000000002".parse().unwrap();
    let unnamed: AgentId = "cccccccc-0000-4000-8000-000000000003".parse().unwrap();
    let writer = AgentId::new();
    let spec = delegating(limits(3, 2));
    let targets = [
        target(writer, "writer", "Writes things up."),
        target(spec.agent_id, "me", "This run's own agent."),
        target(editor_a, "editor", ""),
        target(editor_b, "editor", ""),
        target(unnamed, "", ""),
    ];
    let driver = Driver::boot(spec.clone()).unwrap();
    let state = driver.state();
    let tools = [EchoTool.descriptor()];
    let mut offered = view(&spec, &state, driver.events(), &tools, &[]);
    offered.agents = &targets;
    assert_eq!(
        labels(&offered),
        [
            "echo",
            "delegate:writer",
            "delegate:editor-aaaaaaaa",
            "delegate:editor-bbbbbbbb",
            "delegate:cccccccc-0000-4000-8000-000000000003",
            "model",
            "complete",
        ]
    );
    let choices = jev_choices(&offered);
    assert_eq!(choices[1].1.as_deref(), Some("Writes things up."));
    assert_eq!(
        choices[2].1.as_deref(),
        Some("Hand the input to agent editor.")
    );

    let delegates = |view: &DecisionView<'_>| {
        labels(view)
            .into_iter()
            .filter(|label| label.starts_with("delegate:"))
            .count()
    };
    assert_eq!(delegates(&offered), 4);

    // No capability.
    let plain = spec_with_input(limits(3, 2), "hi");
    let mut refused = view(&plain, &state, driver.events(), &tools, &[]);
    refused.agents = &targets;
    assert_eq!(delegates(&refused), 0);

    // Two steps left: this decision takes one, leaving one to give.
    let short = delegating(limits(2, 2));
    let mut refused = view(&short, &state, driver.events(), &tools, &[]);
    refused.agents = &targets;
    assert_eq!(delegates(&refused), 0);

    // One model call left.
    let short = delegating(limits(3, 1));
    let mut refused = view(&short, &state, driver.events(), &tools, &[]);
    refused.agents = &targets;
    assert_eq!(delegates(&refused), 0);

    // What earlier children were given is spent.
    let mut given = state.clone();
    given.given_model_calls = 1;
    let mut refused = view(&spec, &given, driver.events(), &tools, &[]);
    refused.agents = &targets;
    assert_eq!(delegates(&refused), 0);
    // Steps and model calls already taken are spent.
    let mut taken = state.clone();
    taken.steps = 1;
    let mut refused = view(&spec, &taken, driver.events(), &tools, &[]);
    refused.agents = &targets;
    assert_eq!(delegates(&refused), 0);
    let mut taken = state.clone();
    taken.model_calls = 1;
    let mut refused = view(&spec, &taken, driver.events(), &tools, &[]);
    refused.agents = &targets;
    assert_eq!(delegates(&refused), 0);
    let mut given = state.clone();
    given.given_steps = 1;
    let mut refused = view(&spec, &given, driver.events(), &tools, &[]);
    refused.agents = &targets;
    assert_eq!(delegates(&refused), 0);

    // Ten children already.
    let mut full = state.clone();
    full.children = 10;
    let mut refused = view(&spec, &full, driver.events(), &tools, &[]);
    refused.agents = &targets;
    assert_eq!(delegates(&refused), 0);

    // Already 8 hops deep.
    let mut deep = spec.clone();
    deep.lineage.hop = 8;
    let mut refused = view(&deep, &state, driver.events(), &tools, &[]);
    refused.agents = &targets;
    assert_eq!(delegates(&refused), 0);

    // Not running.
    let mut waiting = state.clone();
    waiting.harness = HarnessState::Cancelled;
    let mut refused = view(&spec, &waiting, driver.events(), &tools, &[]);
    refused.agents = &targets;
    assert_eq!(delegates(&refused), 0);

    // A tool named like a delegate choice is left out.
    let mut clash = EchoTool.descriptor();
    clash.name = "delegate:writer".into();
    let tools = [clash];
    let mut offered = view(&spec, &state, driver.events(), &tools, &[]);
    offered.agents = &targets;
    assert_eq!(
        labels(&offered)
            .iter()
            .filter(|label| *label == "delegate:writer")
            .count(),
        1
    );
}

/// Starts every child it is asked for, and remembers the requests.
#[derive(Default)]
struct Starts {
    asked: std::sync::Mutex<Vec<AgentId>>,
}

impl AgentSpawner for Starts {
    fn start(&self, request: ChildRequest<'_>) -> Result<StartedChild, String> {
        self.asked.lock().unwrap().push(request.agent_id);
        Ok(StartedChild {
            run_id: RunId::new(),
            limits: request.limits,
        })
    }
}

// Jev picks `delegate:writer`: the driver decides a Delegate of the writer
// agent with the run's input, and the spawner starts it.
#[tokio::test(flavor = "multi_thread")]
async fn a_delegate_choice_maps_to_the_named_agent() {
    let server = jev(&["delegate:writer", "complete"]).await;
    let writer = AgentId::new();
    let reader = AgentId::new();
    let spawner = Arc::new(Starts::default());
    let base_url = server.uri();
    let started = spawner.clone();
    let events = tokio::task::spawn_blocking(move || {
        let client = typesafe_sdk::blocking::Client::builder()
            .api_key("gol")
            .base_url(base_url)
            .retry(typesafe_sdk::RetryPolicy::disabled())
            .build()
            .unwrap();
        let mut decider = JevDecider::new(client);
        let mut driver = Driver::boot(delegating(limits(8, 4)))
            .unwrap()
            .with_spawner(
                started,
                vec![
                    target(reader, "reader", "Reads."),
                    target(writer, "writer", "Writes."),
                ],
            );
        run_to_completion(
            &mut driver,
            &mut decider,
            &[],
            &Answering,
            &InMemory::default(),
        )
        .unwrap();
        driver.events().to_vec()
    })
    .await
    .unwrap();

    assert_eq!(*spawner.asked.lock().unwrap(), [writer]);
    assert!(events.iter().any(|event| matches!(
        &event.payload,
        EventPayload::EffectDecided {
            effect: Effect::Delegate { agent_id, input },
        } if *agent_id == writer && input == "hi"
    )));
    assert!(events.iter().any(|event| matches!(
        &event.payload,
        EventPayload::ChildStarted { agent_id, .. } if *agent_id == writer
    )));
    let sent = requests(&server).await;
    let offered = criteria(&sent[0]);
    assert!(offered.to_string().contains("delegate:writer"), "{offered}");
}

// Labels are looked up exactly, so two targets must never share one. A label
// that still collides after the id suffix is not offered: no choice can reach
// the wrong agent.
#[test]
fn an_ambiguous_delegate_label_is_not_offered() {
    let spec = delegating(limits(3, 2));
    let same_prefix_a: AgentId = "dddddddd-0000-4000-8000-000000000001".parse().unwrap();
    let same_prefix_b: AgentId = "dddddddd-0000-4000-8000-000000000002".parse().unwrap();
    let editor: AgentId = "eeeeeeee-0000-4000-8000-000000000001".parse().unwrap();
    let editor_too: AgentId = "ffffffff-0000-4000-8000-000000000001".parse().unwrap();
    let posing: AgentId = "11111111-0000-4000-8000-000000000001".parse().unwrap();
    let writer = AgentId::new();
    let targets = [
        target(same_prefix_a, "x", ""),
        target(same_prefix_b, "x", ""),
        target(editor, "editor", ""),
        target(editor_too, "editor", ""),
        target(posing, "editor-eeeeeeee", ""),
        target(writer, "writer", ""),
    ];
    let driver = Driver::boot(spec.clone()).unwrap();
    let state = driver.state();
    let mut offered = view(&spec, &state, driver.events(), &[], &[]);
    offered.agents = &targets;
    assert_eq!(
        labels(&offered),
        [
            "delegate:editor-ffffffff",
            "delegate:writer",
            "model",
            "complete"
        ]
    );
}

fn messaging(limits: Limits) -> RunSpec {
    let mut spec = spec(limits);
    spec.capabilities.push(Capability::new("agent.message"));
    spec
}

// Decision 32A: each of the owner's other agents is offered as `tell:<name>`
// and `ask:<name>`, under the delegate choices' labels and filters, while a
// message could start a task: a deliverer is present, the run holds
// `agent.message`, is running below the hop limit with fewer than 10
// children, and after this decision's step still has 2 steps and 2 model
// calls to give (decision 33A carves them as a delegation does).
#[test]
fn jev_offers_tell_and_ask_choices_only_when_allowed() {
    let editor_a: AgentId = "aaaaaaaa-0000-4000-8000-000000000001".parse().unwrap();
    let editor_b: AgentId = "bbbbbbbb-0000-4000-8000-000000000002".parse().unwrap();
    let writer = AgentId::new();
    let spec = messaging(limits(3, 2));
    let targets = [
        target(writer, "writer", "Writes things up."),
        target(spec.agent_id, "me", ""),
        target(editor_a, "editor", ""),
        target(editor_b, "editor", ""),
    ];
    let driver = Driver::boot(spec.clone()).unwrap();
    let state = driver.state();
    let tools = [EchoTool.descriptor()];
    let mut offered = view(&spec, &state, driver.events(), &tools, &[]);
    offered.agents = &targets;
    offered.messaging = true;
    assert_eq!(
        labels(&offered),
        [
            "echo",
            "tell:writer",
            "tell:editor-aaaaaaaa",
            "tell:editor-bbbbbbbb",
            "ask:writer",
            "ask:editor-aaaaaaaa",
            "ask:editor-bbbbbbbb",
            "model",
            "complete",
        ]
    );
    let choices = jev_choices(&offered);
    assert_eq!(
        choices[1].1.as_deref(),
        Some("Tell agent writer the input, without waiting: Writes things up.")
    );
    assert_eq!(
        choices[5].1.as_deref(),
        Some("Ask agent editor about the input, and wait for its answer.")
    );

    let messages = |view: &DecisionView<'_>| {
        labels(view)
            .into_iter()
            .filter(|label| label.starts_with("tell:") || label.starts_with("ask:"))
            .count()
    };
    assert_eq!(messages(&offered), 6);

    // An input longer than a message may be: the authorizer would deny it.
    let long = {
        let mut long = messaging(limits(3, 2));
        long.input = "x".repeat(protocol::MAX_MESSAGE_BYTES + 1);
        long
    };
    let mut refused = offered;
    refused.spec = &long;
    assert_eq!(messages(&refused), 0);

    // No deliverer.
    let mut refused = offered;
    refused.messaging = false;
    assert_eq!(messages(&refused), 0);

    // No capability: `agent.delegate` alone is not enough.
    let plain = delegating(limits(3, 2));
    let mut refused = offered;
    refused.spec = &plain;
    assert_eq!(messages(&refused), 0);

    // Two steps left: this decision takes one, leaving one to give.
    let short = messaging(limits(2, 2));
    let mut refused = offered;
    refused.spec = &short;
    assert_eq!(messages(&refused), 0);

    // One model call left.
    let short = messaging(limits(3, 1));
    let mut refused = offered;
    refused.spec = &short;
    assert_eq!(messages(&refused), 0);

    // Ten children already.
    let mut full = state.clone();
    full.children = 10;
    let mut refused = offered;
    refused.state = &full;
    assert_eq!(messages(&refused), 0);

    // Already 8 hops deep.
    let mut deep = spec.clone();
    deep.lineage.hop = 8;
    let mut refused = offered;
    refused.spec = &deep;
    assert_eq!(messages(&refused), 0);

    // Not running.
    let mut waiting = state.clone();
    waiting.harness = HarnessState::Cancelled;
    let mut refused = offered;
    refused.state = &waiting;
    assert_eq!(messages(&refused), 0);

    // A tool named like a message choice is left out.
    let mut clash = EchoTool.descriptor();
    clash.name = "ask:writer".into();
    let tools = [clash];
    let mut clashing = offered;
    clashing.tools = &tools;
    assert_eq!(
        labels(&clashing)
            .iter()
            .filter(|label| *label == "ask:writer")
            .count(),
        1
    );
}

// Jev picks `tell:writer` or `ask:writer`: the decision is a message to the
// writer agent with the run's input; an ask waits `JEV_ASK_TIMEOUT_SECS`.
#[tokio::test(flavor = "multi_thread")]
async fn tell_and_ask_choices_map_to_messages() {
    let writer = AgentId::new();
    for (label, expects_reply, timeout_secs) in [
        ("tell:writer", false, None),
        ("ask:writer", true, Some(JEV_ASK_TIMEOUT_SECS)),
    ] {
        let server = jev(&[label]).await;
        let base_url = server.uri();
        let effect = tokio::task::spawn_blocking(move || {
            let client = typesafe_sdk::blocking::Client::builder()
                .api_key("gol")
                .base_url(base_url)
                .retry(typesafe_sdk::RetryPolicy::disabled())
                .build()
                .unwrap();
            let spec = messaging(limits(8, 4));
            let driver = Driver::boot(spec.clone()).unwrap();
            let state = driver.state();
            let targets = [target(writer, "writer", "")];
            let mut offered = view(&spec, &state, driver.events(), &[], &[]);
            offered.agents = &targets;
            offered.messaging = true;
            JevDecider::new(client).decide(&offered)
        })
        .await
        .unwrap();
        assert_eq!(
            effect,
            Ok(Effect::SendMessage {
                to: writer,
                body: "hi".to_string(),
                expects_reply,
                reply_to: None,
                timeout_secs,
            }),
            "{label}"
        );
    }
}

fn asking(input: &str) -> RunSpec {
    let mut spec = spec_with_input(limits(8, 4), input);
    spec.capabilities.push(Capability::new("user.ask"));
    spec
}

// Phase 3.5: `ask_user` is offered only while the run holds user.ask, can
// wait for the answer, and its step is not yet answered.
#[test]
fn jev_offers_ask_user_only_when_allowed() {
    let with = asking("hi");
    let without = spec(limits(8, 4));
    for (spec, can_wait, offered) in [
        (&with, true, true),
        (&with, false, false),
        (&without, true, false),
    ] {
        let driver = Driver::boot(spec.clone()).unwrap();
        let state = driver.state();
        let mut offered_view = view(spec, &state, driver.events(), &[], &[]);
        offered_view.asking = can_wait;
        assert_eq!(
            labels(&offered_view).contains(&"ask_user".to_string()),
            offered,
            "can_wait={can_wait}"
        );
    }
    let driver = Driver::boot(with.clone()).unwrap();
    let mut state = driver.state();
    state.harness = protocol::HarnessState::Running {
        step: 1,
        attempt: 0,
        answered: true,
    };
    let mut answered = view(&with, &state, driver.events(), &[], &[]);
    answered.asking = true;
    assert!(!labels(&answered).contains(&"ask_user".to_string()));
    // A tool named ask_user would be ambiguous: it is left out.
    let mut tool = EchoTool.descriptor();
    tool.name = "ask_user".into();
    let tools = [tool];
    let fresh = driver.state();
    let mut named = view(&with, &fresh, driver.events(), &tools, &[]);
    named.asking = true;
    assert_eq!(
        labels(&named)
            .iter()
            .filter(|label| *label == "ask_user")
            .count(),
        1
    );
}

fn responded(spec: &RunSpec, role: protocol::MessageRole, text: &str) -> Event {
    Event::record(
        protocol::EventSource::for_spec(spec, protocol::Actor::Gateway, protocol::Timestamp::now()),
        protocol::EventPayload::ModelResponded {
            message: protocol::ModelMessage {
                role,
                text: text.to_string(),
            },
            usage: None,
        },
    )
}

// 59A: the question is the run's last non-empty assistant text, else its
// input.
#[tokio::test]
async fn an_ask_user_choice_asks_the_last_assistant_text_else_the_input() {
    for (texts, expected) in [
        (vec![], "book a flight"),
        (
            vec![("assistant", "Which airport?"), ("assistant", "   ")],
            "Which airport?",
        ),
        (
            vec![
                ("assistant", "first"),
                ("assistant", "Which day?"),
                ("user", "not this"),
            ],
            "Which day?",
        ),
    ] {
        let server = jev(&["ask_user"]).await;
        let base_url = server.uri();
        let effect = tokio::task::spawn_blocking(move || {
            let client = typesafe_sdk::blocking::Client::builder()
                .api_key("gol")
                .base_url(base_url)
                .retry(typesafe_sdk::RetryPolicy::disabled())
                .build()
                .unwrap();
            let spec = asking("book a flight");
            let driver = Driver::boot(spec.clone()).unwrap();
            let state = driver.state();
            let mut events = driver.events().to_vec();
            for (role, text) in texts {
                let role = if role == "assistant" {
                    protocol::MessageRole::Assistant
                } else {
                    protocol::MessageRole::User
                };
                events.push(responded(&spec, role, text));
            }
            let mut offered = view(&spec, &state, &events, &[], &[]);
            offered.asking = true;
            JevDecider::new(client).decide(&offered)
        })
        .await
        .unwrap();
        assert_eq!(
            effect,
            Ok(Effect::AskUser {
                prompt: expected.to_string(),
            })
        );
    }
}
