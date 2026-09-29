//! Asks between agents on the queue (Phase 2.3, decisions 31A to 34A): a
//! run that asks is parked, the end of the task it started is its reply,
//! and the reply wakes it. The sweep finishes what a crash leaves and times
//! out a due ask. `formal/runqueue` (`RunQueueMail.cfg`) models the park
//! and the wake. On both stores; needs Postgres and Redis, as
//! `queue_worker.rs` does.
use std::sync::Arc;

use harness::{AgentSpawner, ChildRequest, InMemory, MessageDeliverer, MessageRequest};
use protocol::{
    Actor, AgentId, Capability, CredentialSource, Event, EventPayload, EventSource,
    ExecutionPlacement, FailureClass, Limits, MessageId, MessageRole, ModelMessage, ModelProvider,
    Owner, RunId, RunSpec, Timestamp, WorkModel,
};
use serde_json::json;
use server::{
    is_terminal, queued_events, sweep_asks, AgentManifest, Append, InMemoryStore, MessageStore,
    OwnedDeliverer, OwnedSpawner, PostgresStore, Prepared, PutMessage, QueueTiming, RedisRunQueue,
    RunStore, StoreError, StoredAgent, StoredMessage, StoredRun, Worker,
};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const POSTGRES_URL: &str = "postgres://gol:gol@127.0.0.1/gol";
const REDIS_URL: &str = "redis://127.0.0.1/";

type Stores = (Arc<dyn RunStore>, Arc<dyn MessageStore>);

/// The memory store, then Postgres: each keeps both runs and messages.
fn stores(which: usize) -> Stores {
    if which == 0 {
        let store = Arc::new(InMemoryStore::default());
        (store.clone(), store)
    } else {
        let store = Arc::new(PostgresStore::connect(POSTGRES_URL).expect("connect"));
        (store.clone(), store)
    }
}

/// An owner of its own, so agents other tests store are not listed.
fn fresh_owner() -> Owner {
    Owner::new(
        "https://issuer.test",
        format!("user-{}", RunId::new()),
        "tenant-1",
    )
}

fn put_agent(store: &dyn RunStore, owner: &Owner, name: &str, capabilities: &[&str]) -> AgentId {
    let id = AgentId::new();
    store
        .put_agent(StoredAgent {
            manifest: AgentManifest {
                id,
                version: "1".to_string(),
                name: name.to_string(),
                description: format!("The {name}."),
                instructions: "Do it.".to_string(),
                tools: Vec::new(),
                required_capabilities: capabilities
                    .iter()
                    .map(|name| Capability::new(*name))
                    .collect(),
            },
            owner: owner.clone(),
        })
        .expect("put agent");
    id
}

/// Jev answers each label once, in order, and repeats the last one.
async fn jev(labels: &[&str]) -> MockServer {
    let server = MockServer::start().await;
    let answer = |label: &str| {
        json!({
            "model": "jev-latest",
            "usage": {"input_tokens": 1, "output_tokens": 1},
            "answers": {"effect": {"type": "choice", "choice": label,
                "confidence": 1.0, "probabilities": {label: 1.0}}}
        })
    };
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

/// A researcher that may message, the owner's writer, and a queue of its
/// own with the researcher's run pushed on it.
struct Setup {
    runs: Arc<dyn RunStore>,
    messages: Arc<dyn MessageStore>,
    key: String,
    queue: RedisRunQueue,
    owner: Owner,
    researcher: RunSpec,
    writer: AgentId,
}

fn setup(which: usize) -> Setup {
    let (runs, messages) = stores(which);
    let owner = fresh_owner();
    let asker = put_agent(runs.as_ref(), &owner, "researcher", &["agent.message"]);
    let writer = put_agent(runs.as_ref(), &owner, "writer", &[]);
    let researcher = RunSpec::builder()
        .owner(owner.clone())
        .agent(asker, "1")
        .input("what is the plan?")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::OpenAI,
            model_name: "gpt-test".to_string(),
            credential: CredentialSource::PlatformGateway,
        })
        .capabilities(vec![Capability::new("agent.message")])
        .limits(Limits {
            max_steps: 8,
            max_model_calls: 4,
        })
        .build();
    let key = format!("gol:test:{}", RunId::new());
    let queue = RedisRunQueue::with_key(REDIS_URL, &key);
    runs.put_run(StoredRun {
        events: queued_events(&researcher),
        spec: researcher.clone(),
    })
    .expect("put run");
    queue.push(researcher.run_id).expect("push");
    Setup {
        runs,
        messages,
        key,
        queue,
        owner,
        researcher,
        writer,
    }
}

impl Setup {
    /// A worker that delivers messages, or, with `messages` false, one that
    /// crashed before it could deliver a task's end.
    fn worker(&self, uri: &str, messages: bool) -> Worker {
        let builder = Worker::builder()
            .queue(RedisRunQueue::with_key(REDIS_URL, &self.key))
            .store(self.runs.clone())
            .memory(Arc::new(InMemory::default()))
            .jev(uri);
        if messages {
            builder.messages(self.messages.clone()).build()
        } else {
            builder.build()
        }
    }

    /// A delivering worker whose lease runs out in 400 ms.
    fn fast_worker(&self, uri: &str) -> Worker {
        Worker::builder()
            .queue(RedisRunQueue::with_key(REDIS_URL, &self.key))
            .store(self.runs.clone())
            .memory(Arc::new(InMemory::default()))
            .jev(uri)
            .timing(QueueTiming {
                lease: std::time::Duration::from_millis(400),
                heartbeat: std::time::Duration::from_millis(100),
                ..QueueTiming::default()
            })
            .messages(self.messages.clone())
            .build()
    }

    fn events(&self, run_id: RunId) -> Vec<Event> {
        self.runs.run(run_id).expect("read").expect("run").events
    }

    /// The ask the researcher sent and the task it started.
    fn asked(&self) -> (MessageId, RunId) {
        let events = self.events(self.researcher.run_id);
        let ask = events.iter().find_map(|event| match &event.payload {
            EventPayload::MessageSent {
                message_id,
                to,
                expects_reply: true,
            } if *to == self.writer => Some(*message_id),
            _ => None,
        });
        let task = events.iter().find_map(|event| match &event.payload {
            EventPayload::ChildStarted {
                run_id, agent_id, ..
            } if *agent_id == self.writer => Some(*run_id),
            _ => None,
        });
        (ask.expect("an ask"), task.expect("a task"))
    }

    /// The answers the researcher's log holds: (reply_to, body) of each
    /// reply, and `None` bodies for timeouts.
    fn answers(&self) -> Vec<(MessageId, Option<String>)> {
        self.events(self.researcher.run_id)
            .into_iter()
            .filter_map(|event| match event.payload {
                EventPayload::MessageReceived {
                    reply_to: Some(ask),
                    body,
                    ..
                } => Some((ask, Some(body))),
                EventPayload::AskTimedOut { message_id } => Some((message_id, None)),
                _ => None,
            })
            .collect()
    }

    /// Logs that the researcher sent `ask`, as its driver records it: the
    /// ask is then open in its log.
    fn log_sent(&self, ask: MessageId) {
        let sent = Event::record(
            EventSource::for_spec(&self.researcher, Actor::System, Timestamp::now()),
            EventPayload::MessageSent {
                message_id: ask,
                to: self.writer,
                expects_reply: true,
            },
        );
        self.runs
            .append_events(self.researcher.run_id, vec![sent])
            .expect("append");
    }

    fn deliverer(&self) -> OwnedDeliverer {
        OwnedDeliverer::builder()
            .store(self.runs.clone())
            .messages(self.messages.clone())
            .queue(Arc::new(RedisRunQueue::with_key(REDIS_URL, &self.key)))
            .build()
    }
}

/// Runs `work` on a plain thread: the stores and the queue block, which the
/// Postgres client refuses on a Tokio worker.
fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::spawn(work).join().expect("thread")
}

fn completed(events: &[Event]) -> bool {
    matches!(
        events
            .iter()
            .filter(|event| is_terminal(&event.payload))
            .map(|event| &event.payload)
            .collect::<Vec<_>>()
            .as_slice(),
        [EventPayload::RunCompleted { .. }]
    )
}

// Decisions 31A and 34A: the researcher's Jev picks `ask:writer`. The worker
// starts the writer's task, stores the ask, and parks the researcher instead
// of acknowledging it. The task completes; its outcome is the reply, which
// is appended to the researcher's log and wakes it; the researcher then
// completes.
#[tokio::test(flavor = "multi_thread")]
async fn a_researcher_asks_the_writer_and_is_woken_by_the_reply() {
    for which in 0..2 {
        let server = jev(&["ask:writer", "complete"]).await;
        let uri = server.uri();
        blocking(move || {
            let setup = setup(which);
            let worker = setup.worker(&uri, true);
            let researcher = setup.researcher.run_id;

            assert_eq!(worker.work_one().expect("work"), Some(researcher));
            let (ask, task) = setup.asked();
            assert!(!setup
                .events(researcher)
                .iter()
                .any(|event| is_terminal(&event.payload)));
            assert_eq!(setup.queue.parked().expect("parked"), [(researcher, ask)]);
            assert_eq!(setup.queue.queued().expect("queued"), [task]);

            assert_eq!(worker.work_one().expect("work"), Some(task));
            assert!(completed(&setup.events(task)));
            assert_eq!(setup.answers(), [(ask, Some("done".to_string()))]);
            assert_eq!(setup.messages.ask_of_task(task), Ok(None));
            assert_eq!(setup.queue.parked().expect("parked"), []);
            assert_eq!(setup.queue.queued().expect("queued"), [researcher]);

            assert_eq!(worker.work_one().expect("work"), Some(researcher));
            assert!(completed(&setup.events(researcher)));
            assert_eq!(setup.queue.queued().expect("queued"), []);
        });
        let sent = server.received_requests().await.expect("requests");
        let first: serde_json::Value = serde_json::from_slice(&sent[0].body).expect("json");
        let offered = first["questions"]["effect"]["criteria"].to_string();
        assert!(offered.contains("tell:writer"), "{offered}");
        assert!(offered.contains("ask:writer"), "{offered}");
        assert!(!offered.contains("ask:researcher"), "{offered}");
    }
}

// Decision 34A, `formal/runqueue` `Recheck` (the lostWake negative control):
// the task ends and its reply is appended while the researcher's worker has
// stored the ask but not yet parked it, so the replier's wake finds nothing
// parked. The park then rereads the log, finds the reply, and wakes the
// researcher itself.
#[tokio::test(flavor = "multi_thread")]
async fn a_reply_before_the_park_still_wakes_the_asker() {
    for which in 0..2 {
        let server = jev(&["ask:writer", "complete"]).await;
        let uri = server.uri();
        blocking(move || {
            let setup = setup(which);
            let researcher = setup.researcher.run_id;
            let asking = setup.worker(&uri, true);
            let replying = setup.worker(&uri, true);

            let claim = asking.claim().expect("claim").expect("a run");
            let Prepared::Open(open) = claim.prepare().expect("prepare") else {
                panic!("the researcher is open")
            };
            let done = open.execute().record().expect("record");
            let (ask, task) = setup.asked();

            // The task runs and replies before the researcher is parked.
            assert_eq!(replying.work_one().expect("work"), Some(task));
            assert_eq!(setup.answers(), [(ask, Some("done".to_string()))]);
            assert_eq!(setup.queue.queued().expect("queued"), []);

            assert_eq!(done.ack().expect("ack"), researcher);
            assert_eq!(setup.queue.parked().expect("parked"), []);
            assert_eq!(setup.queue.queued().expect("queued"), [researcher]);
            assert_eq!(asking.work_one().expect("work"), Some(researcher));
            assert!(completed(&setup.events(researcher)));
        });
    }
}

// Decision 28A: an open ask past its deadline is answered by the sweep with
// `AskTimedOut`; before its deadline it is left alone, and the task's later
// end is no second answer. The ask waits 60 s; every other test's ask here
// waits an hour, so the sweep at 61 s reaches none of them.
#[tokio::test(flavor = "multi_thread")]
async fn an_ask_past_its_deadline_times_out() {
    for which in 0..2 {
        let server = jev(&["complete"]).await;
        let uri = server.uri();
        blocking(move || {
            let setup = setup(which);
            let researcher = setup.researcher.run_id;
            let sent = setup
                .deliverer()
                .send(MessageRequest {
                    from: &setup.researcher,
                    decision: 1,
                    to: setup.writer,
                    body: "what is the plan?",
                    expects_reply: true,
                    reply_to: None,
                    timeout_secs: Some(60),
                    limits: Some(Limits {
                        max_steps: 3,
                        max_model_calls: 2,
                    }),
                })
                .expect("sent");
            let ask = sent.message_id;
            setup.log_sent(ask);
            // Swept at the stored deadline's own clock: a millisecond before
            // it the ask is not due, however slow the machine; at it, it is.
            let deadline = setup
                .messages
                .message(ask)
                .expect("read")
                .expect("stored")
                .deadline
                .expect("an ask with a timeout")
                .as_unix_millis();
            let sweep = |at: i64| {
                sweep_asks(
                    &setup.queue,
                    setup.runs.as_ref(),
                    setup.messages.as_ref(),
                    Timestamp::unix_millis(at),
                )
                .expect("sweep")
            };
            assert!(!sweep(deadline - 1).contains(&researcher));
            assert_eq!(setup.answers(), []);
            assert!(sweep(deadline).contains(&researcher));
            assert_eq!(setup.answers(), [(ask, None)]);
            assert!(!sweep(deadline + 1_000).contains(&researcher));

            // The task still runs, and its end answers nothing.
            let worker = setup.worker(&uri, true);
            while worker.work_one().expect("work").is_some() {}
            assert!(completed(&setup.events(sent.task.expect("a task").run_id)));
            assert_eq!(setup.answers(), [(ask, None)]);

            a_due_ask_whose_task_ended_gets_the_tasks_answer(which, &uri);
            an_ask_never_logged_is_closed_at_its_deadline(which);
        });
    }
}

// Decision 34A: a crash can leave a task ended with its end undelivered (its
// worker died before delivering), or a reply appended with the asker still
// parked (the replier died before waking it). The sweep finishes both.
#[tokio::test(flavor = "multi_thread")]
async fn the_sweep_finishes_an_answer_a_crash_left() {
    for which in 0..2 {
        let server = jev(&["ask:writer", "complete"]).await;
        let uri = server.uri();
        blocking(move || {
            // The task's end was never delivered.
            let setup = setup(which);
            let researcher = setup.researcher.run_id;
            assert_eq!(
                setup.worker(&uri, true).work_one().expect("work"),
                Some(researcher)
            );
            let (ask, task) = setup.asked();
            assert_eq!(
                setup.worker(&uri, false).work_one().expect("work"),
                Some(task)
            );
            assert_eq!(setup.answers(), []);
            let sweep = |setup: &Setup| {
                sweep_asks(
                    &setup.queue,
                    setup.runs.as_ref(),
                    setup.messages.as_ref(),
                    Timestamp::now(),
                )
                .expect("sweep")
            };
            assert!(sweep(&setup).contains(&researcher));
            assert_eq!(setup.answers(), [(ask, Some("done".to_string()))]);
            assert_eq!(setup.queue.parked().expect("parked"), []);
            assert_eq!(setup.queue.queued().expect("queued"), [researcher]);
            assert!(!sweep(&setup).contains(&researcher));
        });
        let server = jev(&["ask:writer", "complete"]).await;
        let uri = server.uri();
        blocking(move || {
            // The reply was appended; the wake never came.
            let setup = setup(which);
            let researcher = setup.researcher.run_id;
            assert_eq!(
                setup.worker(&uri, true).work_one().expect("work"),
                Some(researcher)
            );
            let (ask, task) = setup.asked();
            let reply = Event::record(
                EventSource::for_spec(&setup.researcher, Actor::System, Timestamp::now()),
                EventPayload::MessageReceived {
                    message_id: MessageId::new(),
                    from_agent: setup.writer,
                    from_run: task,
                    body: "later".to_string(),
                    reply_to: Some(ask),
                },
            );
            setup
                .runs
                .append_events(researcher, vec![reply])
                .expect("append");
            assert_eq!(setup.queue.parked().expect("parked"), [(researcher, ask)]);
            let swept = sweep_asks(
                &setup.queue,
                setup.runs.as_ref(),
                setup.messages.as_ref(),
                Timestamp::now(),
            )
            .expect("sweep");
            assert!(swept.contains(&researcher));
            assert_eq!(setup.queue.parked().expect("parked"), []);
            assert_eq!(setup.queue.queued().expect("queued"), [researcher, task]);
        });
    }
}

// Decision 30A: a resumed run that sends again, at the same decision, sends
// the same message and starts the same task.
#[test]
fn a_resent_message_is_stored_and_started_once() {
    for which in 0..2 {
        let setup = setup(which);
        let deliverer = setup.deliverer();
        let send = || {
            deliverer
                .send(MessageRequest {
                    from: &setup.researcher,
                    decision: 1,
                    to: setup.writer,
                    body: "what is the plan?",
                    expects_reply: true,
                    reply_to: None,
                    timeout_secs: Some(3600),
                    limits: Some(Limits {
                        max_steps: 3,
                        max_model_calls: 2,
                    }),
                })
                .expect("sent")
        };
        let first = send();
        let again = send();
        assert_eq!(first.message_id, again.message_id);
        let task = first.task.expect("a task").run_id;
        assert_eq!(again.task.expect("a task").run_id, task);
        let queued = setup.queue.queued().expect("queued");
        assert_eq!(queued.iter().filter(|run| **run == task).count(), 1);
        assert_eq!(
            setup
                .messages
                .ask_of_task(task)
                .expect("ask")
                .map(|ask| ask.id),
            Some(first.message_id)
        );
    }
}

// Decision 29A: a message reaches only the sender's owner's agents; one to
// another owner's agent starts nothing and stores nothing.
#[test]
fn a_message_to_another_owners_agent_is_refused() {
    for which in 0..2 {
        let setup = setup(which);
        let stranger = put_agent(setup.runs.as_ref(), &fresh_owner(), "stranger", &[]);
        assert_ne!(setup.owner, fresh_owner());
        let sent = setup.deliverer().send(MessageRequest {
            from: &setup.researcher,
            decision: 1,
            to: stranger,
            body: "hello",
            expects_reply: true,
            reply_to: None,
            timeout_secs: Some(60),
            limits: Some(Limits {
                max_steps: 3,
                max_model_calls: 2,
            }),
        });
        assert!(sent.is_err(), "{sent:?}");
        assert_eq!(
            setup.queue.queued().expect("queued"),
            [setup.researcher.run_id]
        );
        let stored = setup
            .messages
            .open_asks_due(Timestamp::unix_millis(i64::MAX))
            .expect("asks");
        assert!(stored
            .iter()
            .all(|message| message.from_run != setup.researcher.run_id));
    }
}

// Decision 31A: the task may also reply before it ends. The reply reaches
// the asker at once and answers the ask; a reply from a run the ask did not
// start is refused.
#[tokio::test(flavor = "multi_thread")]
async fn an_explicit_reply_from_the_task_answers_the_ask() {
    for which in 0..2 {
        let server = jev(&["ask:writer", "complete"]).await;
        let uri = server.uri();
        blocking(move || {
            let setup = setup(which);
            let researcher = setup.researcher.run_id;
            assert_eq!(
                setup.worker(&uri, true).work_one().expect("work"),
                Some(researcher)
            );
            let (ask, task) = setup.asked();
            let task_spec = setup.runs.run(task).expect("read").expect("task").spec;
            let reply = |from: &RunSpec| {
                setup.deliverer().send(MessageRequest {
                    from,
                    decision: 1,
                    to: setup.researcher.agent_id,
                    body: "the plan",
                    expects_reply: false,
                    reply_to: Some(ask),
                    timeout_secs: None,
                    limits: None,
                })
            };
            assert_eq!(
                reply(&setup.researcher).map(|sent| sent.message_id),
                Err("not a reply to an ask this run was sent".to_string())
            );
            // A reply goes to the agent that asked.
            let astray = setup.deliverer().send(MessageRequest {
                from: &task_spec,
                decision: 1,
                to: setup.writer,
                body: "the plan",
                expects_reply: false,
                reply_to: Some(ask),
                timeout_secs: None,
                limits: None,
            });
            assert_eq!(
                astray.map(|sent| sent.message_id),
                Err("not a reply to an ask this run was sent".to_string())
            );
            let sent = reply(&task_spec).expect("replied");
            assert_eq!(sent.task, None);
            // The ask is answered: a second reply, at another decision, is
            // refused and appends nothing.
            let again = setup.deliverer().send(MessageRequest {
                from: &task_spec,
                decision: 2,
                to: setup.researcher.agent_id,
                body: "more",
                expects_reply: false,
                reply_to: Some(ask),
                timeout_secs: None,
                limits: None,
            });
            assert_eq!(
                again.map(|sent| sent.message_id),
                Err("not a reply to an ask this run was sent".to_string())
            );
            assert_eq!(setup.answers(), [(ask, Some("the plan".to_string()))]);
            assert_eq!(setup.messages.ask_of_task(task), Ok(None));
            assert_eq!(setup.queue.parked().expect("parked"), []);
            assert_eq!(setup.queue.queued().expect("queued"), [researcher, task]);
        });
    }
}

// An ask whose asking run is not stored has no one to answer: the sweep
// closes it instead of retrying it on every pass.
#[test]
fn an_ask_without_its_asker_is_closed() {
    for which in 0..2 {
        let setup = setup(which);
        let now = Timestamp::now();
        let orphan = StoredMessage {
            id: MessageId::new(),
            owner: setup.owner.clone(),
            from_run: RunId::new(),
            from_agent: setup.researcher.agent_id,
            decision: 1,
            to_agent: setup.writer,
            body: "anyone?".to_string(),
            expects_reply: true,
            reply_to: None,
            task_run: Some(RunId::new()),
            deadline: Some(Timestamp::unix_millis(now.as_unix_millis() - 1_000)),
            timeout_secs: None,
            hop: 0,
        };
        setup.messages.put_message(orphan.clone()).expect("put");
        let open = |setup: &Setup| {
            setup
                .messages
                .open_asks_due(now)
                .expect("due")
                .iter()
                .any(|message| message.id == orphan.id)
        };
        sweep_asks(
            &setup.queue,
            setup.runs.as_ref(),
            setup.messages.as_ref(),
            now,
        )
        .expect("sweep");
        assert!(!open(&setup));
    }
}

// Decision 31A over 28A: an ask past its deadline whose task has ended (its
// end never delivered) is answered with that end, not with a timeout. Run
// from `an_ask_past_its_deadline_times_out`: both sweep past a 60 s deadline,
// which would reach each other's ask if they ran at once.
fn a_due_ask_whose_task_ended_gets_the_tasks_answer(which: usize, uri: &str) {
    let setup = setup(which);
    let researcher = setup.researcher.run_id;
    let sent = setup
        .deliverer()
        .send(MessageRequest {
            from: &setup.researcher,
            decision: 1,
            to: setup.writer,
            body: "what is the plan?",
            expects_reply: true,
            reply_to: None,
            timeout_secs: Some(60),
            limits: Some(Limits {
                max_steps: 3,
                max_model_calls: 2,
            }),
        })
        .expect("sent");
    setup.log_sent(sent.message_id);
    let task = sent.task.expect("a task").run_id;
    // A worker that cannot deliver runs the task; the researcher's
    // claim is held so that it stays open.
    let worker = setup.worker(uri, false);
    let held = worker.claim().expect("claim").expect("a run");
    assert_eq!(held.run_id(), researcher);
    assert_eq!(worker.work_one().expect("work"), Some(task));
    assert!(completed(&setup.events(task)));
    assert_eq!(setup.answers(), []);

    let later = Timestamp::unix_millis(Timestamp::now().as_unix_millis() + 61_000);
    sweep_asks(
        &setup.queue,
        setup.runs.as_ref(),
        setup.messages.as_ref(),
        later,
    )
    .expect("sweep");
    assert_eq!(
        setup.answers(),
        [(sent.message_id, Some("done".to_string()))]
    );
}

// Decision 30A: a resumed run that sends something else at the same decision
// is refused, before any task starts: the stored message stays the one its
// log will name.
#[test]
fn a_different_message_at_the_same_decision_is_refused() {
    for which in 0..2 {
        let setup = setup(which);
        let send = |expects_reply: bool| {
            setup.deliverer().send(MessageRequest {
                from: &setup.researcher,
                decision: 3,
                to: setup.writer,
                body: "what is the plan?",
                expects_reply,
                reply_to: None,
                timeout_secs: expects_reply.then_some(3600),
                limits: Some(Limits {
                    max_steps: 3,
                    max_model_calls: 2,
                }),
            })
        };
        let told = send(false).expect("told");
        let queued = setup.queue.queued().expect("queued");
        assert_eq!(
            send(true).map(|sent| sent.message_id),
            Err("another message was sent at this decision".to_string())
        );
        assert_eq!(setup.queue.queued().expect("queued"), queued);
        let task = told.task.expect("a task").run_id;
        assert_eq!(setup.messages.ask_of_task(task), Ok(None));
        // The same ask with another timeout is another message too.
        let ask = |timeout_secs: u32| {
            setup.deliverer().send(MessageRequest {
                from: &setup.researcher,
                decision: 4,
                to: setup.writer,
                body: "what is the plan?",
                expects_reply: true,
                reply_to: None,
                timeout_secs: Some(timeout_secs),
                limits: Some(Limits {
                    max_steps: 3,
                    max_model_calls: 2,
                }),
            })
        };
        let asked = ask(3600).expect("asked");
        assert_eq!(ask(3600).map(|sent| sent.message_id), Ok(asked.message_id));
        assert_eq!(
            ask(1800).map(|sent| sent.message_id),
            Err("another message was sent at this decision".to_string())
        );
    }
}

// A message's task and a delegation's child at the same number, to the same
// agent with the same input, are different runs.
#[test]
fn a_message_task_is_not_a_delegation_child() {
    for which in 0..2 {
        let setup = setup(which);
        let told = setup
            .deliverer()
            .send(MessageRequest {
                from: &setup.researcher,
                decision: 2,
                to: setup.writer,
                body: "what is the plan?",
                expects_reply: false,
                reply_to: None,
                timeout_secs: None,
                limits: Some(Limits {
                    max_steps: 3,
                    max_model_calls: 2,
                }),
            })
            .expect("told");
        let spawner = OwnedSpawner::new(
            setup.runs.clone(),
            Some(Arc::new(RedisRunQueue::with_key(REDIS_URL, &setup.key))),
        );
        let child = spawner
            .start(ChildRequest {
                parent: &setup.researcher,
                step: 2,
                agent_id: setup.writer,
                input: "what is the plan?",
                limits: Limits {
                    max_steps: 3,
                    max_model_calls: 2,
                },
            })
            .expect("delegated");
        assert_ne!(told.task.expect("a task").run_id, child.run_id);
    }
}

// The asker's worker died after its ask was stored and its task started,
// before its log said so; the task ended meanwhile. Its answer waits for the
// ask to be in the asker's log instead of landing before it (where the
// asker's harness, waiting after it, would never see it). The resumed asker
// resends the same ask, is parked, and the sweep delivers the task's end.
#[tokio::test(flavor = "multi_thread")]
async fn an_answer_waits_for_its_ask_to_be_logged() {
    for which in 0..2 {
        let server = jev(&["complete", "ask:writer", "complete"]).await;
        let uri = server.uri();
        blocking(move || {
            let setup = setup(which);
            let researcher = setup.researcher.run_id;
            // What the dead worker did: the ask the researcher's first
            // decision sends, stored with its task.
            let sent = setup
                .deliverer()
                .send(MessageRequest {
                    from: &setup.researcher,
                    decision: 1,
                    to: setup.writer,
                    body: "what is the plan?",
                    expects_reply: true,
                    reply_to: None,
                    timeout_secs: Some(harness::JEV_ASK_TIMEOUT_SECS),
                    limits: Some(Limits {
                        max_steps: 4,
                        max_model_calls: 2,
                    }),
                })
                .expect("sent");
            let task = sent.task.expect("a task").run_id;
            // The task runs first (Jev: complete) and ends.
            let worker = setup.fast_worker(&uri);
            let held = worker.claim().expect("claim").expect("a run");
            assert_eq!(held.run_id(), researcher);
            assert_eq!(worker.work_one().expect("work"), Some(task));
            assert!(completed(&setup.events(task)));
            assert_eq!(setup.answers(), []);

            // The researcher's claim runs out, and it is run again (Jev:
            // ask:writer): the same ask, and it is parked.
            drop(held);
            std::thread::sleep(std::time::Duration::from_millis(500));
            setup.queue.reap().expect("reap");
            assert_eq!(worker.work_one().expect("work"), Some(researcher));
            assert_eq!(setup.asked(), (sent.message_id, task));
            assert_eq!(
                setup.queue.parked().expect("parked"),
                [(researcher, sent.message_id)]
            );

            let swept = sweep_asks(
                &setup.queue,
                setup.runs.as_ref(),
                setup.messages.as_ref(),
                Timestamp::now(),
            )
            .expect("sweep");
            assert!(swept.contains(&researcher));
            assert_eq!(
                setup.answers(),
                [(sent.message_id, Some("done".to_string()))]
            );
            assert_eq!(worker.work_one().expect("work"), Some(researcher));
            assert!(completed(&setup.events(researcher)));
        });
    }
}

// An ask whose asker never logged it (its worker died first, and the
// resumed run did something else) is closed at its deadline, and nothing is
// appended to the asker's log: no harness waits on it. Run from
// `an_ask_past_its_deadline_times_out`, as the test above.
fn an_ask_never_logged_is_closed_at_its_deadline(which: usize) {
    let setup = setup(which);
    let sent = setup
        .deliverer()
        .send(MessageRequest {
            from: &setup.researcher,
            decision: 1,
            to: setup.writer,
            body: "what is the plan?",
            expects_reply: true,
            reply_to: None,
            timeout_secs: Some(60),
            limits: Some(Limits {
                max_steps: 3,
                max_model_calls: 2,
            }),
        })
        .expect("sent");
    let task = sent.task.expect("a task").run_id;
    let later = Timestamp::unix_millis(Timestamp::now().as_unix_millis() + 61_000);
    let swept = sweep_asks(
        &setup.queue,
        setup.runs.as_ref(),
        setup.messages.as_ref(),
        later,
    )
    .expect("sweep");
    assert!(swept.contains(&setup.researcher.run_id));
    assert_eq!(setup.answers(), []);
    assert_eq!(setup.messages.ask_of_task(task), Ok(None));
}

// Decision 26A and the plan's "checks same owner and hop": a tell or a new
// ask starts a task one hop further, so the deliverer refuses one from a run
// already 8 hops deep, starting and storing nothing. The eighth hop is sent,
// and the message keeps its sender's hop.
#[test]
fn a_ninth_hop_is_refused() {
    for which in 0..2 {
        let setup = setup(which);
        let deliverer = setup.deliverer();
        let mut deep = setup.researcher.clone();
        let send = |from: &RunSpec, decision: u32| {
            deliverer.send(MessageRequest {
                from,
                decision,
                to: setup.writer,
                body: "what is the plan?",
                expects_reply: true,
                reply_to: None,
                timeout_secs: Some(3600),
                limits: Some(Limits {
                    max_steps: 3,
                    max_model_calls: 2,
                }),
            })
        };

        deep.lineage.hop = 7;
        let sent = send(&deep, 1).expect("the eighth hop");
        let stored = setup
            .messages
            .message(sent.message_id)
            .expect("read")
            .expect("stored");
        assert_eq!(stored.hop, 7);
        let task = sent.task.expect("a task").run_id;
        let task = setup.runs.run(task).expect("read").expect("task");
        assert_eq!(task.spec.lineage.hop, 8);

        deep.lineage.hop = 8;
        let queued = setup.queue.queued().expect("queued");
        assert_eq!(
            send(&deep, 2).map(|sent| sent.message_id),
            Err("messaging is already 8 hops deep".to_string())
        );
        assert_eq!(setup.queue.queued().expect("queued"), queued);
        let due = setup
            .messages
            .open_asks_due(Timestamp::unix_millis(i64::MAX))
            .expect("asks");
        assert_eq!(
            due.iter()
                .filter(|message| message.from_run == deep.run_id)
                .count(),
            1
        );
    }
}

/// A messages store whose open-ask lookup by task fails, as a Postgres
/// query can while the others succeed.
struct AskLookupFails(Arc<dyn MessageStore>);

impl MessageStore for AskLookupFails {
    fn put_message(&self, message: StoredMessage) -> Result<PutMessage, StoreError> {
        self.0.put_message(message)
    }
    fn message(&self, id: MessageId) -> Result<Option<StoredMessage>, StoreError> {
        self.0.message(id)
    }
    fn ask_of_task(&self, _task_run: RunId) -> Result<Option<StoredMessage>, StoreError> {
        Err(StoreError::new("connection reset"))
    }
    fn answer(&self, ask: MessageId) -> Result<bool, StoreError> {
        self.0.answer(ask)
    }
    fn open_asks_due(&self, now: Timestamp) -> Result<Vec<StoredMessage>, StoreError> {
        self.0.open_asks_due(now)
    }
}

// A timeout answers an ask for good, so the sweep gives none on a store
// error: a parked run whose task still runs stays parked, its ask open,
// for the next sweep (and the task's end).
#[tokio::test(flavor = "multi_thread")]
async fn a_store_error_does_not_time_out_a_parked_ask() {
    for which in 0..2 {
        let server = jev(&["ask:writer", "complete"]).await;
        let uri = server.uri();
        blocking(move || {
            let setup = setup(which);
            let researcher = setup.researcher.run_id;
            assert_eq!(
                setup.worker(&uri, true).work_one().expect("work"),
                Some(researcher)
            );
            let (ask, task) = setup.asked();
            let failing = AskLookupFails(setup.messages.clone());
            let swept = sweep_asks(
                &setup.queue,
                setup.runs.as_ref(),
                &failing,
                Timestamp::now(),
            )
            .expect("sweep");
            assert!(!swept.contains(&researcher));
            assert_eq!(setup.answers(), []);
            assert_eq!(setup.queue.parked().expect("parked"), [(researcher, ask)]);
            assert_eq!(
                setup
                    .messages
                    .ask_of_task(task)
                    .map(|open| open.map(|ask| ask.id)),
                Ok(Some(ask))
            );
        });
    }
}

// An explicit reply that cannot reach the asker yet (its ask is not in the
// asker's log) is refused, not reported as sent; the same reply at the same
// decision, once the ask is logged, is delivered.
#[test]
fn a_reply_before_its_ask_is_logged_is_refused_and_retried() {
    for which in 0..2 {
        let setup = setup(which);
        let sent = setup
            .deliverer()
            .send(MessageRequest {
                from: &setup.researcher,
                decision: 1,
                to: setup.writer,
                body: "what is the plan?",
                expects_reply: true,
                reply_to: None,
                timeout_secs: Some(3600),
                limits: Some(Limits {
                    max_steps: 3,
                    max_model_calls: 2,
                }),
            })
            .expect("sent");
        let task = sent.task.expect("a task").run_id;
        let task_spec = setup.runs.run(task).expect("read").expect("task").spec;
        let reply = || {
            setup.deliverer().send(MessageRequest {
                from: &task_spec,
                decision: 1,
                to: setup.researcher.agent_id,
                body: "the plan",
                expects_reply: false,
                reply_to: Some(sent.message_id),
                timeout_secs: None,
                limits: None,
            })
        };
        assert_eq!(
            reply().map(|sent| sent.message_id),
            Err("the asker has not logged the ask yet".to_string())
        );
        assert_eq!(setup.answers(), []);

        setup.log_sent(sent.message_id);
        let replied = reply().expect("replied");
        assert_eq!(replied.task, None);
        assert_eq!(
            setup.answers(),
            [(sent.message_id, Some("the plan".to_string()))]
        );
    }
}

// Owner decision on #74, follow-up: an ask answered by its task's end
// carries the task's last non-blank assistant response when it completed (a
// Jev task's outcome is the fixed word "done"); a task that failed, was
// cancelled or expired says so instead, whatever it answered before. Every
// answer is cut to a message's 32 KiB (30A) at a character boundary.
#[tokio::test(flavor = "multi_thread")]
async fn a_task_answers_with_its_last_model_response() {
    let max = protocol::MAX_MESSAGE_BYTES;
    let completed = || EventPayload::RunCompleted {
        outcome: "done".to_string(),
    };
    let assistant = MessageRole::Assistant;
    // (the last response's role and text, how the task ended, the answer)
    let cases = [
        (
            assistant,
            "the plan is to ship on Friday".to_string(),
            completed(),
            "the plan is to ship on Friday".to_string(),
        ),
        // An empty, blank or non-assistant response answers nothing: the
        // one before it does.
        (
            assistant,
            String::new(),
            completed(),
            "a first thought".to_string(),
        ),
        (
            assistant,
            " \n\t".to_string(),
            completed(),
            "a first thought".to_string(),
        ),
        (
            MessageRole::User,
            "not an answer".to_string(),
            completed(),
            "a first thought".to_string(),
        ),
        (
            MessageRole::System,
            "not an answer".to_string(),
            completed(),
            "a first thought".to_string(),
        ),
        (assistant, "a".repeat(max), completed(), "a".repeat(max)),
        (assistant, "a".repeat(max + 1), completed(), "a".repeat(max)),
        // 1 + 2 * 20,000 bytes: the cap falls inside a two-byte character.
        (
            assistant,
            format!("a{}", "é".repeat(20_000)),
            completed(),
            format!("a{}", "é".repeat((max - 1) / 2)),
        ),
        // A task that did not complete says so, whatever it answered before.
        (
            assistant,
            "almost".to_string(),
            EventPayload::RunFailed {
                class: FailureClass::Dependency,
                message: "decider: down".to_string(),
            },
            "the task failed: decider: down".to_string(),
        ),
        // The failure text is cut as a response is.
        (
            assistant,
            "almost".to_string(),
            EventPayload::RunFailed {
                class: FailureClass::Dependency,
                message: "x".repeat(max),
            },
            format!("the task failed: {}", "x".repeat(max))[..max].to_string(),
        ),
        (
            assistant,
            "almost".to_string(),
            EventPayload::RunCancelled,
            "the task was cancelled".to_string(),
        ),
        (
            assistant,
            "almost".to_string(),
            EventPayload::RunExpired,
            "the task expired".to_string(),
        ),
    ];
    for (which, (role, text, end, expected)) in cases
        .into_iter()
        .flat_map(|case| [(0, case.clone()), (1, case)])
    {
        let server = jev(&["ask:writer", "complete"]).await;
        let uri = server.uri();
        blocking(move || {
            let setup = setup(which);
            let researcher = setup.researcher.run_id;
            assert_eq!(
                setup.worker(&uri, true).work_one().expect("work"),
                Some(researcher)
            );
            let (ask, task) = setup.asked();
            let task_spec = setup.runs.run(task).expect("read").expect("task").spec;
            let record = |payload| {
                Event::record(
                    EventSource::for_spec(&task_spec, Actor::System, Timestamp::now()),
                    payload,
                )
            };
            let reply = |role: MessageRole, text: &str| EventPayload::ModelResponded {
                message: ModelMessage {
                    role,
                    text: text.to_string(),
                },
                usage: None,
            };
            let appended = setup
                .runs
                .append_events(
                    task,
                    vec![
                        record(reply(MessageRole::Assistant, "a first thought")),
                        record(reply(role, &text)),
                        record(end),
                    ],
                )
                .expect("append");
            assert_eq!(appended, Append::Appended);

            let swept = sweep_asks(
                &setup.queue,
                setup.runs.as_ref(),
                setup.messages.as_ref(),
                Timestamp::now(),
            )
            .expect("sweep");
            assert!(swept.contains(&researcher));
            let answers = setup.answers();
            let [(answered, Some(body))] = answers.as_slice() else {
                panic!("one answer: {answers:?}")
            };
            assert_eq!(*answered, ask);
            assert_eq!(*body, expected);
        });
    }
}
