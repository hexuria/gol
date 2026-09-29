//! Messages between agents (Phase 2.3): `OwnedDeliverer`, the queue
//! worker's `MessageDeliverer`, and the delivery of a reply (or an ask's
//! timeout) to the run that asked, which the worker and the sweep share.
//! `formal/runqueue` models the park and wake around it (decision 34A).
use std::sync::Arc;

use harness::{AgentSpawner, ChildRequest, MessageDeliverer, MessageRequest, SentMessage};
use protocol::{Actor, Event, EventPayload, EventSource, MessageId, RunId, Timestamp};

use crate::queue::RedisRunQueue;
use crate::spawner::OwnedSpawner;
use crate::store::{Append, MessageStore, PutMessage, RunStore, StoredMessage};

/// Delivers a run's messages to other agents of its owner. A tell or a new
/// ask starts the target's task through the owned spawner (same owner, the
/// carved limits, decision 29A and 33A) and is stored with it; a reply
/// answers an open ask whose task is the sending run, and reaches the asker
/// at once (decision 31A allows an explicit reply beside the task's end).
pub struct OwnedDeliverer {
    store: Arc<dyn RunStore>,
    messages: Arc<dyn MessageStore>,
    queue: Arc<RedisRunQueue>,
    spawner: OwnedSpawner,
}

impl OwnedDeliverer {
    pub fn new(
        store: Arc<dyn RunStore>,
        messages: Arc<dyn MessageStore>,
        queue: Arc<RedisRunQueue>,
    ) -> Self {
        let spawner = OwnedSpawner::new(store.clone(), Some(queue.clone()));
        Self {
            store,
            messages,
            queue,
            spawner,
        }
    }

    /// Stores `message` once for its run and decision, and returns the id
    /// it was stored under (a resumed run's resend gets the first id).
    fn put(&self, message: StoredMessage) -> Result<MessageId, String> {
        match self.messages.put_message(message.clone()) {
            Ok(PutMessage::Stored) => Ok(message.id),
            Ok(PutMessage::Existed(stored)) => Ok(stored.id),
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
        let task = self.spawner.start(ChildRequest {
            parent: from,
            step: request.decision,
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
        })?;
        Ok(SentMessage {
            message_id,
            task: Some(task),
        })
    }
}

impl OwnedDeliverer {
    /// A reply: to an open ask whose task is the sending run, and to no
    /// other (the authorizer's hop exemption rests on this).
    fn reply(&self, request: MessageRequest<'_>) -> Result<SentMessage, String> {
        let from = request.from;
        let not_asked = || "not a reply to an ask this run was sent".to_string();
        let ask_id = request.reply_to.ok_or_else(not_asked)?;
        let ask = match self.messages.message(ask_id) {
            Ok(Some(ask)) if ask.expects_reply && ask.task_run == Some(from.run_id) => ask,
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
        })?;
        deliver(
            self.store.as_ref(),
            self.messages.as_ref(),
            &self.queue,
            &ask,
            EventPayload::MessageReceived {
                message_id,
                from_agent: from.agent_id,
                from_run: from.run_id,
                body: request.body.to_string(),
                reply_to: Some(ask.id),
            },
        )?;
        Ok(SentMessage {
            message_id,
            task: None,
        })
    }
}

/// Delivers `answer` (the reply to `ask`, or its `AskTimedOut`) to the run
/// that asked: appended to its log first, then the ask marked answered,
/// then the asker woken if it is parked (`formal/runqueue` `ReplyAppend`
/// then `ReplyWake`). Done twice it appends a second answer, which the
/// asker's harness no longer waits on and ignores, and wakes nothing.
pub(crate) fn deliver(
    store: &dyn RunStore,
    messages: &dyn MessageStore,
    queue: &RedisRunQueue,
    ask: &StoredMessage,
    answer: EventPayload,
) -> Result<(), String> {
    let failed = |error: String| {
        eprintln!(
            "gol: answer to ask {} of run {}: {error}",
            ask.id, ask.from_run
        );
        "store unavailable".to_string()
    };
    let asker = store
        .run(ask.from_run)
        .map_err(|error| failed(error.to_string()))?
        .ok_or_else(|| failed("the asking run is not stored".to_string()))?;
    let event = Event::record(
        EventSource::for_spec(&asker.spec, Actor::System, Timestamp::now()),
        answer,
    );
    match store.append_events(ask.from_run, vec![event]) {
        // An asker that already ended needs no answer.
        Ok(Append::Appended | Append::Terminal) => {}
        Ok(other) => return Err(failed(format!("{other:?}"))),
        Err(error) => return Err(failed(error.to_string())),
    }
    messages
        .answer(ask.id)
        .map_err(|error| failed(error.to_string()))?;
    queue.wake(ask.from_run).map_err(failed)?;
    Ok(())
}

/// Whether `events` hold the answer (reply or timeout) to `ask`.
pub(crate) fn answered(events: &[Event], ask: MessageId) -> bool {
    events.iter().any(|event| {
        matches!(
            &event.payload,
            EventPayload::MessageReceived {
                reply_to: Some(reply_to),
                ..
            } if *reply_to == ask
        ) || matches!(
            &event.payload,
            EventPayload::AskTimedOut { message_id } if *message_id == ask
        )
    })
}

/// The reply a task gives its ask when it ends (decision 31A): its outcome,
/// or that it failed, was cancelled or expired. `None` while it runs.
pub(crate) fn task_answer(
    task: RunId,
    task_agent: protocol::AgentId,
    ask: &StoredMessage,
    events: &[Event],
) -> Option<EventPayload> {
    let body = events.iter().rev().find_map(|event| match &event.payload {
        EventPayload::RunCompleted { outcome } => Some(outcome.clone()),
        EventPayload::RunFailed { message, .. } => Some(format!("the task failed: {message}")),
        EventPayload::RunCancelled => Some("the task was cancelled".to_string()),
        EventPayload::RunExpired => Some("the task expired".to_string()),
        _ => None,
    })?;
    Some(EventPayload::MessageReceived {
        message_id: MessageId::new(),
        from_agent: task_agent,
        from_run: task,
        body,
        reply_to: Some(ask.id),
    })
}
