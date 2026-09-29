//! The messages store (Phase 2.1): a message between agents is stored once
//! per sending run and decision (decision 30A), read back by id and by the
//! task it started, answered once, and an open ask past its deadline is
//! found for its timeout. On both stores; needs Postgres, as `pg_redis.rs`
//! does.
use std::sync::Arc;

use protocol::{AgentId, MessageId, Owner, RunId, Timestamp};
use server::{InMemoryStore, MessageStore, PostgresStore, PutMessage, StoredMessage};

const POSTGRES_URL: &str = "postgres://gol:gol@127.0.0.1/gol";

fn stores() -> Vec<Arc<dyn MessageStore>> {
    vec![
        Arc::new(InMemoryStore::default()),
        Arc::new(PostgresStore::connect(POSTGRES_URL).expect("connect")),
    ]
}

fn ask(from_run: RunId, decision: u32, deadline: Option<Timestamp>) -> StoredMessage {
    StoredMessage {
        id: MessageId::new(),
        owner: Owner::new("https://issuer.test", "user-1", "tenant-1"),
        from_run,
        from_agent: AgentId::new(),
        decision,
        to_agent: AgentId::new(),
        body: "what is it?".to_string(),
        expects_reply: true,
        reply_to: None,
        task_run: Some(RunId::new()),
        deadline,
    }
}

// Decision 30A: a resumed run that sends again sends the same message.
#[test]
fn a_message_is_stored_once_per_run_and_decision() {
    for store in stores() {
        let from = RunId::new();
        let first = ask(from, 3, None);
        assert_eq!(store.put_message(first.clone()), Ok(PutMessage::Stored));
        let again = ask(from, 3, None);
        assert_eq!(
            store.put_message(again),
            Ok(PutMessage::Existed(Box::new(first.clone())))
        );
        let next = ask(from, 4, None);
        assert_eq!(store.put_message(next), Ok(PutMessage::Stored));
        assert_eq!(store.message(first.id), Ok(Some(first)));
        assert_eq!(store.message(MessageId::new()), Ok(None));
    }
}

#[test]
fn an_ask_is_found_by_the_task_it_started_and_answered_once() {
    for store in stores() {
        let asked = ask(RunId::new(), 1, None);
        store.put_message(asked.clone()).expect("put");
        let task = asked.task_run.expect("task");
        assert_eq!(store.ask_of_task(task), Ok(Some(asked.clone())));
        assert_eq!(store.ask_of_task(RunId::new()), Ok(None));
        assert_eq!(store.answer(asked.id), Ok(true));
        assert_eq!(store.answer(asked.id), Ok(false));
        assert_eq!(store.answer(MessageId::new()), Ok(false));
    }
}

// Decision 28A: an open ask past its deadline is due for its timeout; an
// answered one, or one with time left or no deadline, is not.
#[test]
fn open_asks_past_their_deadline_are_due() {
    for store in stores() {
        let from = RunId::new();
        let due = ask(from, 1, Some(Timestamp::unix_millis(1_000)));
        let answered = ask(from, 2, Some(Timestamp::unix_millis(1_000)));
        let later = ask(from, 3, Some(Timestamp::unix_millis(9_000)));
        let never = ask(from, 4, None);
        for message in [&due, &answered, &later, &never] {
            store.put_message(message.clone()).expect("put");
        }
        store.answer(answered.id).expect("answer");
        let found: Vec<MessageId> = store
            .open_asks_due(Timestamp::unix_millis(5_000))
            .expect("due")
            .into_iter()
            .filter(|message| message.from_run == from)
            .map(|message| message.id)
            .collect();
        assert_eq!(found, [due.id]);
    }
}
