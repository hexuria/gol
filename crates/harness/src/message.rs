use protocol::{AgentId, Limits, MessageId, RunSpec};

use crate::StartedChild;

/// A message the driver asks a deliverer to accept (Phase 2): from `from`'s
/// run, sent by its `decision`th decision, to agent `to`. The run and the
/// decision name the message (decision 30A): two messages in one harness
/// step are two decisions, and a resumed run that performs the same
/// decision again sends the same message.
#[derive(Clone, Copy, Debug)]
pub struct MessageRequest<'a> {
    pub from: &'a RunSpec,
    pub decision: u32,
    pub to: AgentId,
    pub body: &'a str,
    pub expects_reply: bool,
    pub reply_to: Option<MessageId>,
    pub timeout_secs: Option<u32>,
    /// The budget carved for the task a tell or a new ask starts (decision
    /// 33A); `None` for a reply, which starts none.
    pub limits: Option<Limits>,
}

/// A message a deliverer accepted: its id, and the task it started for the
/// target (a tell or a new ask), with the limits that task was stored with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SentMessage {
    pub message_id: MessageId,
    pub task: Option<StartedChild>,
}

/// Accepts messages for delivery. The driver records an accepted one as
/// `MessageSent` (after `ChildStarted` for the task it started), and the
/// error as the reason of `MessageRefused`.
pub trait MessageDeliverer: Send + Sync {
    fn send(&self, request: MessageRequest<'_>) -> Result<SentMessage, String>;
}
