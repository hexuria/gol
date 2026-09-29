//! The live stream (Phase 3.2, decisions 40A-45A): `GET /v1/stream` sends a
//! principal's outbox and `GET /v1/runs/{id}/stream` one run's log, as
//! server-sent events that resume after `Last-Event-ID`. Each client has a
//! task that reads a page (at most `PAGE`), sends it, and waits for a hint:
//! its principal's, `ALL`, a lag, or `POLL` without one. A hint carries no
//! state, so a lost one costs a read, never an event.
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::response::sse::{Event as SseEvent, KeepAlive, KeepAliveStream, Sse};
use protocol::{Event, Owner, RunId};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use crate::store::{is_terminal, principal_hint, Hint, OutboxEntry, RunStore, ALL};

/// Events per read, and the most a client's task holds before the client
/// takes them (decision 43A): a slow client stalls only its own stream.
pub(crate) const PAGE: usize = 500;

/// Open streams per principal (decision 45A).
pub(crate) const MAX_STREAMS: usize = 16;

/// A waiting stream re-reads this often without a hint (decision 40A).
const POLL: Duration = Duration::from_secs(5);

/// A comment this often keeps proxies from closing an idle stream.
const HEARTBEAT: Duration = Duration::from_secs(15);

pub(crate) type SseBody = Sse<KeepAliveStream<ReceiverStream<Result<SseEvent, Infallible>>>>;

/// The streams each principal has open.
#[derive(Default)]
pub(crate) struct Streams(Mutex<HashMap<(String, String), usize>>);

/// One open stream, counted until it drops (its client went away, or it
/// ended).
pub(crate) struct Slot {
    streams: Arc<Streams>,
    key: (String, String),
}

impl Streams {
    /// A slot for another stream of `owner`'s principal, unless it has
    /// `MAX_STREAMS` open.
    pub(crate) fn take(self: &Arc<Self>, owner: &Owner) -> Option<Slot> {
        let key = (owner.issuer.clone(), owner.subject.clone());
        let mut open = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let count = open.entry(key.clone()).or_insert(0);
        if *count >= MAX_STREAMS {
            return None;
        }
        *count += 1;
        Some(Slot {
            streams: self.clone(),
            key,
        })
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        let mut open = self
            .streams
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(count) = open.get_mut(&self.key) {
            *count -= 1;
            if *count == 0 {
                open.remove(&self.key);
            }
        }
    }
}

type Sender = mpsc::Sender<Result<SseEvent, Infallible>>;

fn sse(receiver: mpsc::Receiver<Result<SseEvent, Infallible>>) -> SseBody {
    Sse::new(ReceiverStream::new(receiver)).keep_alive(KeepAlive::new().interval(HEARTBEAT))
}

/// An event of `run_id`'s log at `run_seq`, sent with the number `id`.
fn event(id: u64, run_id: RunId, run_seq: u64, event: &Event) -> SseEvent {
    let data = serde_json::json!({ "run_id": run_id, "run_seq": run_seq, "event": event });
    SseEvent::default()
        .id(id.to_string())
        .event(event.payload.event_type())
        .data(data.to_string())
}

/// Waits for a reason to read again: `hint` or `ALL`, a lag (hints were
/// dropped), or `POLL` without one. True when the client went away.
async fn wait(hints: Option<&mut broadcast::Receiver<Hint>>, hint: Hint, sender: &Sender) -> bool {
    let poll = tokio::time::sleep(POLL);
    tokio::pin!(poll);
    let Some(hints) = hints else {
        return tokio::select! {
            _ = sender.closed() => true,
            _ = &mut poll => false,
        };
    };
    loop {
        tokio::select! {
            _ = sender.closed() => return true,
            _ = &mut poll => return false,
            got = hints.recv() => match got {
                Ok(got) if got == hint || got == ALL => return false,
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(_)) => return false,
                Err(broadcast::error::RecvError::Closed) => {
                    return tokio::select! {
                        _ = sender.closed() => true,
                        _ = &mut poll => false,
                    };
                }
            },
        }
    }
}

/// `owner`'s outbox after `resume`, then as it grows. A client with no
/// cursor starts after what was pruned; one whose cursor is below it is sent
/// `reset` with `pruned_through`, and the stream ends (decision 41A). A store
/// error ends the stream; the client resumes from its last event.
pub(crate) fn outbox_stream(
    store: Arc<dyn RunStore>,
    owner: Owner,
    resume: Option<u64>,
    slot: Slot,
) -> SseBody {
    let (sender, receiver) = mpsc::channel(PAGE);
    // Subscribed before the first read, so no hint falls between them.
    let mut hints = store.outbox().map(|outbox| outbox.hints());
    tokio::spawn(async move {
        let _slot = slot;
        let hint = principal_hint(&owner);
        let mut cursor = resume;
        loop {
            let read = {
                let (store, owner, after) = (store.clone(), owner.clone(), cursor.unwrap_or(0));
                tokio::task::spawn_blocking(move || {
                    store
                        .outbox()
                        .map(|outbox| outbox.outbox_after(&owner, after, PAGE))
                })
                .await
            };
            let page = match read {
                Ok(Some(Ok(page))) => page,
                Ok(Some(Err(error))) => {
                    eprintln!("gol: stream: {error}");
                    return;
                }
                Ok(None) | Err(_) => return,
            };
            match cursor {
                Some(after) if after < page.pruned_through => {
                    let reset = SseEvent::default().event("reset").data(
                        serde_json::json!({ "pruned_through": page.pruned_through }).to_string(),
                    );
                    let _ = sender.send(Ok(reset)).await;
                    return;
                }
                Some(_) => {}
                None => cursor = Some(page.pruned_through),
            }
            let full = page.entries.len() == PAGE;
            for OutboxEntry {
                seq,
                run_id,
                run_seq,
                event: stored,
            } in page.entries
            {
                if sender
                    .send(Ok(event(seq, run_id, run_seq, &stored)))
                    .await
                    .is_err()
                {
                    return;
                }
                cursor = Some(seq);
            }
            if !full && wait(hints.as_mut(), hint, &sender).await {
                return;
            }
        }
    });
    sse(receiver)
}

/// Run `run_id`'s log after its first `resume` events, then as it grows, to
/// its terminal event, after which the stream ends (decision 42A). Each
/// event's id is its place in the run.
pub(crate) fn run_stream(
    store: Arc<dyn RunStore>,
    owner: Owner,
    run_id: RunId,
    resume: u64,
    slot: Slot,
) -> SseBody {
    let (sender, receiver) = mpsc::channel(PAGE);
    let mut hints = store.outbox().map(|outbox| outbox.hints());
    tokio::spawn(async move {
        let _slot = slot;
        let hint = principal_hint(&owner);
        let mut cursor = resume;
        loop {
            let read = {
                let store = store.clone();
                // One event before the cursor too, to see whether the log
                // already ended there.
                let from = cursor.saturating_sub(1);
                tokio::task::spawn_blocking(move || store.run_page(run_id, from as usize, PAGE + 1))
                    .await
            };
            let events = match read {
                Ok(Ok(Some(run))) => run.events,
                Ok(Ok(None)) | Err(_) => return,
                Ok(Err(error)) => {
                    eprintln!("gol: stream: {error}");
                    return;
                }
            };
            let mut events = events.into_iter();
            if cursor > 0 {
                match events.next() {
                    Some(seen) if is_terminal(&seen.payload) => return,
                    Some(_) => {}
                    // The log is shorter than the cursor: nothing to add yet.
                    None => {}
                }
            }
            let mut sent = 0;
            for stored in events.take(PAGE) {
                let run_seq = cursor + 1;
                let ended = is_terminal(&stored.payload);
                if sender
                    .send(Ok(event(run_seq, run_id, run_seq, &stored)))
                    .await
                    .is_err()
                {
                    return;
                }
                cursor = run_seq;
                sent += 1;
                if ended {
                    return;
                }
            }
            if sent < PAGE && wait(hints.as_mut(), hint, &sender).await {
                return;
            }
        }
    });
    sse(receiver)
}
