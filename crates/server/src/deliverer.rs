//! Messages between agents (Phase 2.3): `OwnedDeliverer`, the queue
//! worker's `MessageDeliverer`, and the delivery of a reply (or an ask's
//! timeout) to the run that asked, which the worker and the sweep share.
//! `formal/runqueue` models the park and wake around it (decision 34A).
use std::marker::PhantomData;
use std::sync::Arc;

use harness::{ChildRequest, MessageDeliverer, MessageRequest, SentMessage};
use protocol::{
    Actor, AgentId, Event, EventPayload, EventSource, MessageId, MessageRole, RunId, Timestamp,
    MAX_DELEGATION_HOPS, MAX_MESSAGE_BYTES,
};

use crate::queue::RedisRunQueue;
use crate::spawner::OwnedSpawner;
use crate::store::{is_terminal, Append, MessageStore, PutMessage, RunStore, StoredMessage};
use crate::worker::{Given, Missing};

/// The step a message's task is derived from: the sending decision with the
/// high bit set, so a task never shares its id with a delegation's child at
/// the same number (to the same agent with the same input). Decisions are
/// bounded by `max_steps`, far below the bit.
fn task_step(decision: u32) -> u32 {
    decision | 1 << 31
}

/// Delivers a run's messages to other agents of its owner. A tell or a new
/// ask is stored first, with the task it names, then the task is started
/// through the owned spawner (same owner, the carved limits, decision 29A
/// and 33A). A reply answers an open ask whose task is the sending run, and
/// reaches the asker at once (decision 31A allows an explicit reply beside
/// the task's end).
pub struct OwnedDeliverer {
    store: Arc<dyn RunStore>,
    messages: Arc<dyn MessageStore>,
    queue: Arc<RedisRunQueue>,
    spawner: OwnedSpawner,
}

/// An `OwnedDeliverer` whose run store, messages store and queue are each
/// required before `build` exists.
pub struct OwnedDelivererBuilder<S, M, Q> {
    store: Option<Arc<dyn RunStore>>,
    messages: Option<Arc<dyn MessageStore>>,
    queue: Option<Arc<RedisRunQueue>>,
    states: PhantomData<(S, M, Q)>,
}

impl<S, M, Q> OwnedDelivererBuilder<S, M, Q> {
    fn to<S2, M2, Q2>(self) -> OwnedDelivererBuilder<S2, M2, Q2> {
        OwnedDelivererBuilder {
            store: self.store,
            messages: self.messages,
            queue: self.queue,
            states: PhantomData,
        }
    }
}

impl<M, Q> OwnedDelivererBuilder<Missing, M, Q> {
    pub fn store(mut self, store: Arc<dyn RunStore>) -> OwnedDelivererBuilder<Given, M, Q> {
        self.store = Some(store);
        self.to()
    }
}

impl<S, Q> OwnedDelivererBuilder<S, Missing, Q> {
    pub fn messages(
        mut self,
        messages: Arc<dyn MessageStore>,
    ) -> OwnedDelivererBuilder<S, Given, Q> {
        self.messages = Some(messages);
        self.to()
    }
}

impl<S, M> OwnedDelivererBuilder<S, M, Missing> {
    pub fn queue(mut self, queue: Arc<RedisRunQueue>) -> OwnedDelivererBuilder<S, M, Given> {
        self.queue = Some(queue);
        self.to()
    }
}

impl OwnedDelivererBuilder<Given, Given, Given> {
    pub fn build(self) -> OwnedDeliverer {
        let (Some(store), Some(messages), Some(queue)) = (self.store, self.messages, self.queue)
        else {
            unreachable!("every required input is given in this state")
        };
        let spawner = OwnedSpawner::new(store.clone(), Some(queue.clone()));
        OwnedDeliverer {
            store,
            messages,
            queue,
            spawner,
        }
    }
}

impl OwnedDeliverer {
    pub fn builder() -> OwnedDelivererBuilder<Missing, Missing, Missing> {
        OwnedDelivererBuilder {
            store: None,
            messages: None,
            queue: None,
            states: PhantomData,
        }
    }

    /// Stores `message` once for its run and decision, and returns the id
    /// it was stored under: a resumed run's resend gets the first id. A
    /// different message at the same decision is refused (decision 30A).
    fn put(&self, message: StoredMessage) -> Result<MessageId, String> {
        match self.messages.put_message(message.clone()) {
            Ok(PutMessage::Stored) => Ok(message.id),
            Ok(PutMessage::Existed(stored)) => {
                let same = stored.to_agent == message.to_agent
                    && stored.body == message.body
                    && stored.expects_reply == message.expects_reply
                    && stored.reply_to == message.reply_to
                    && stored.task_run == message.task_run
                    && stored.timeout_secs == message.timeout_secs;
                if same {
                    Ok(stored.id)
                } else {
                    Err("another message was sent at this decision".to_string())
                }
            }
            Err(error) => {
                eprintln!("gol: message from run {}: {error}", message.from_run);
                Err("store unavailable".to_string())
            }
        }
    }
}

impl MessageDeliverer for OwnedDeliverer {
    /// A refusal's reason goes into the sender's run log, which its owner
    /// reads: fixed words, and a store or queue error's detail to stderr.
    fn send(&self, request: MessageRequest<'_>) -> Result<SentMessage, String> {
        let from = request.from;
        let Some(limits) = request.limits else {
            return self.reply(request);
        };
        // The authorizer denies this too; the deliverer does not start a
        // task past the cap on another caller's word.
        if from.lineage.hop >= MAX_DELEGATION_HOPS {
            return Err(format!(
                "messaging is already {MAX_DELEGATION_HOPS} hops deep"
            ));
        }
        let task = self.spawner.child_spec(ChildRequest {
            parent: from,
            step: task_step(request.decision),
            agent_id: request.to,
            input: request.body,
            limits,
        })?;
        let deadline = request.timeout_secs.map(|seconds| {
            Timestamp::unix_millis(
                Timestamp::now()
                    .as_unix_millis()
                    .saturating_add(i64::from(seconds) * 1000),
            )
        });
        // Stored before the task exists, so the task's end always finds it.
        // A task that then fails to start leaves an ask its asker never
        // logged; the sweep closes it at its deadline.
        let message_id = self.put(StoredMessage {
            id: MessageId::new(),
            owner: from.owner.clone(),
            from_run: from.run_id,
            from_agent: from.agent_id,
            decision: request.decision,
            to_agent: request.to,
            body: request.body.to_string(),
            expects_reply: request.expects_reply,
            reply_to: request.reply_to,
            task_run: Some(task.run_id),
            deadline,
            timeout_secs: request.timeout_secs,
            hop: from.lineage.hop,
        })?;
        let task = self.spawner.enqueue_child(from, task)?;
        Ok(SentMessage {
            message_id,
            task: Some(task),
        })
    }
}

impl OwnedDeliverer {
    /// A reply: to the agent that asked, answering the open ask whose task
    /// is the sending run, and to no other (the authorizer's hop exemption
    /// rests on this).
    fn reply(&self, request: MessageRequest<'_>) -> Result<SentMessage, String> {
        let from = request.from;
        let not_asked = || "not a reply to an ask this run was sent".to_string();
        let ask = match self.messages.ask_of_task(from.run_id) {
            Ok(Some(ask)) if Some(ask.id) == request.reply_to && ask.from_agent == request.to => {
                ask
            }
            Ok(_) => return Err(not_asked()),
            Err(error) => {
                eprintln!("gol: reply from run {}: {error}", from.run_id);
                return Err("store unavailable".to_string());
            }
        };
        let message_id = self.put(StoredMessage {
            id: MessageId::new(),
            owner: from.owner.clone(),
            from_run: from.run_id,
            from_agent: from.agent_id,
            decision: request.decision,
            to_agent: ask.from_agent,
            body: request.body.to_string(),
            expects_reply: false,
            reply_to: Some(ask.id),
            task_run: None,
            deadline: None,
            timeout_secs: None,
            hop: from.lineage.hop,
        })?;
        // Stored, but sent only once it reaches the asker. Until its ask is
        // in the asker's log it cannot, and the sender is told so; the same
        // reply at the same decision retries it.
        let delivered = deliver(
            self.store.as_ref(),
            self.messages.as_ref(),
            &self.queue,
            ask.from_run,
            ask.id,
            EventPayload::MessageReceived {
                message_id,
                from_agent: from.agent_id,
                from_run: from.run_id,
                body: request.body.to_string(),
                reply_to: Some(ask.id),
            },
            IfUnsent::Leave,
        );
        match delivered {
            Ok(true) => {}
            Ok(false) => return Err("the asker has not logged the ask yet".to_string()),
            Err(error) => {
                eprintln!("gol: reply {message_id} from run {}: {error}", from.run_id);
                return Err("store unavailable".to_string());
            }
        }
        Ok(SentMessage {
            message_id,
            task: None,
        })
    }
}

/// Where an ask stands in its asker's log, read in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AskState {
    /// No `MessageSent` names it: the asker's worker died before its log
    /// said so, or the asker sent something else.
    Unsent,
    /// Sent, and no answer after it: the asker's harness waits on it.
    Open,
    /// Answered (a reply or `AskTimedOut`) after it was sent.
    Answered,
}

/// Where `ask` stands in `events`. An answer before the ask's `MessageSent`
/// does not count: the harness only takes an answer while it waits.
pub(crate) fn ask_state(events: &[Event], ask: MessageId) -> AskState {
    events
        .iter()
        .fold(AskState::Unsent, |state, event| match &event.payload {
            EventPayload::MessageSent { message_id, .. } if *message_id == ask => AskState::Open,
            EventPayload::MessageReceived {
                reply_to: Some(reply_to),
                ..
            } if *reply_to == ask && state == AskState::Open => AskState::Answered,
            EventPayload::AskTimedOut { message_id }
                if *message_id == ask && state == AskState::Open =>
            {
                AskState::Answered
            }
            _ => state,
        })
}

/// What `deliver` does with an ask its asker has not logged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IfUnsent {
    /// Leaves it open: the asker may still log it (a resumed run resends).
    Leave,
    /// Closes it: its deadline has passed. An asker that logs it later is
    /// parked on a closed ask, and the sweep answers it then.
    Close,
}

/// Delivers `answer` (a reply to `ask`, or its `AskTimedOut`) to `asker`,
/// the run that asked: appended to its log while the ask is open there,
/// then the ask marked answered, then the asker woken if it is parked on it
/// (`formal/runqueue` `ReplyAppend` then `ReplyWake`). An ask already
/// answered in the log is only marked and woken; an asker that ended, or is
/// not stored, needs no answer. `true` when the ask is settled, `false`
/// when it was left for later. Once the append is made, a failure to mark
/// or wake goes to stderr: the sweep finishes it.
pub(crate) fn deliver(
    store: &dyn RunStore,
    messages: &dyn MessageStore,
    queue: &RedisRunQueue,
    asker: RunId,
    ask: MessageId,
    answer: EventPayload,
    if_unsent: IfUnsent,
) -> Result<bool, String> {
    let failed = |error: String| format!("answer to ask {ask} of run {asker}: {error}");
    let run = store
        .run(asker)
        .map_err(|error| failed(error.to_string()))?;
    if let Some(run) = &run {
        let ended = run.events.iter().any(|event| is_terminal(&event.payload));
        match ask_state(&run.events, ask) {
            _ if ended => {}
            AskState::Unsent if if_unsent == IfUnsent::Leave => return Ok(false),
            AskState::Unsent | AskState::Answered => {}
            AskState::Open => {
                let event = Event::record(
                    EventSource::for_spec(&run.spec, Actor::System, Timestamp::now()),
                    answer,
                );
                match store.append_events(asker, vec![event]) {
                    Ok(Append::Appended | Append::Terminal | Append::Missing) => {}
                    Ok(other) => return Err(failed(format!("{other:?}"))),
                    Err(error) => return Err(failed(error.to_string())),
                }
            }
        }
    }
    if let Err(error) = messages.answer(ask) {
        eprintln!("gol: {}", failed(error.to_string()));
    }
    if let Err(error) = queue.wake(asker, ask) {
        eprintln!("gol: {}", failed(error));
    }
    Ok(true)
}

/// The reply a task gives `ask` when it ends (decision 31A): its last
/// non-empty assistant response if it completed after one (a Jev task's
/// outcome is the fixed word "done"), else its outcome, or that it failed, was cancelled or
/// expired. `None` while it runs. The body is cut to `MAX_MESSAGE_BYTES`,
/// as a message's is (30A).
pub(crate) fn task_answer(
    task: RunId,
    task_agent: AgentId,
    ask: MessageId,
    events: &[Event],
) -> Option<EventPayload> {
    let responded = || {
        events.iter().rev().find_map(|event| match &event.payload {
            EventPayload::ModelResponded { message, .. }
                if message.role == MessageRole::Assistant && !message.text.trim().is_empty() =>
            {
                Some(message.text.clone())
            }
            _ => None,
        })
    };
    let body = events.iter().rev().find_map(|event| match &event.payload {
        EventPayload::RunCompleted { outcome } => Some(responded().unwrap_or(outcome.clone())),
        EventPayload::RunFailed { message, .. } => Some(format!("the task failed: {message}")),
        EventPayload::RunCancelled => Some("the task was cancelled".to_string()),
        EventPayload::RunExpired => Some("the task expired".to_string()),
        _ => None,
    })?;
    Some(EventPayload::MessageReceived {
        message_id: MessageId::new(),
        from_agent: task_agent,
        from_run: task,
        body: capped(body),
        reply_to: Some(ask),
    })
}

/// `body` cut to at most `MAX_MESSAGE_BYTES`, at a character boundary.
fn capped(mut body: String) -> String {
    if body.len() > MAX_MESSAGE_BYTES {
        let mut end = MAX_MESSAGE_BYTES;
        while !body.is_char_boundary(end) {
            end -= 1;
        }
        body.truncate(end);
    }
    body
}
