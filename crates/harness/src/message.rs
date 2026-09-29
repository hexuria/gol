use protocol::{AgentId, MessageId, RunSpec};

/// A message the driver asks a deliverer to accept (Phase 2): from `from`'s
/// run, sent by its `step`, to agent `to`. The run and the step name the
/// message, so a resumed run that sends again sends the same message
/// (decision 30A).
#[derive(Clone, Copy, Debug)]
pub struct MessageRequest<'a> {
    pub from: &'a RunSpec,
    pub step: u32,
    pub to: AgentId,
    pub body: &'a str,
    pub expects_reply: bool,
    pub reply_to: Option<MessageId>,
    pub timeout_secs: Option<u32>,
}

/// Accepts messages for delivery. The driver records an accepted one as
/// `MessageSent`, and the error as the reason of `MessageRefused`.
pub trait MessageDeliverer: Send + Sync {
    fn send(&self, request: MessageRequest<'_>) -> Result<MessageId, String>;
}
