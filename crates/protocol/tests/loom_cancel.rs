#![cfg(loom)]

use std::sync::Arc;

use loom::sync::Mutex;
use loom::thread;
use protocol::{
    reduce, Actor, AgentId, Event, EventPayload, EventSource, HarnessState, InvocationId, RunId,
    Timestamp,
};
use uuid::Uuid;

fn source() -> EventSource<'static> {
    EventSource::new(
        RunId::from_uuid(Uuid::from_u128(9)),
        AgentId::from_uuid(Uuid::from_u128(8)),
        "1",
        Actor::System,
        Timestamp::unix_millis(0),
    )
}

fn waiting() -> HarnessState {
    HarnessState::WaitingForTool {
        step: 1,
        attempt: 0,
        name: "echo".to_string(),
        invocation: InvocationId::from_uuid(Uuid::from_u128(1)),
    }
}

fn cancel_event() -> Event {
    Event::record(source(), EventPayload::RunCancelled)
}

fn tool_result() -> Event {
    Event::record(
        source(),
        EventPayload::ToolResult {
            name: "echo".to_string(),
            invocation: InvocationId::from_uuid(Uuid::from_u128(1)),
            step: 1,
            attempt: 0,
            output: "late".to_string(),
        },
    )
}

fn apply(state: &Mutex<HarnessState>, event: &Event) {
    let mut guard = state.lock().unwrap();
    let (next, effects) = reduce(guard.clone(), event);
    assert!(effects.is_empty());
    *guard = next;
}

#[test]
fn late_tool_result_races_cancel() {
    loom::model(|| {
        let state = Arc::new(Mutex::new(waiting()));
        let cancel_state = Arc::clone(&state);
        let result_state = Arc::clone(&state);
        let cancel_thread = thread::spawn(move || apply(&cancel_state, &cancel_event()));
        let result_thread = thread::spawn(move || apply(&result_state, &tool_result()));
        cancel_thread.join().unwrap();
        result_thread.join().unwrap();
        assert_eq!(*state.lock().unwrap(), HarnessState::Cancelled);
    });
}
