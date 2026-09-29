//! The per-owner outbox (Phase 3.1, decisions 16A and 36A-38A): every event
//! a run store keeps also gets one outbox entry, numbered by its owner's
//! counter in the same step, so a reader resuming after a number misses
//! nothing. `formal/outbox` models the writers. On both stores; needs
//! Postgres, as `pg_redis.rs` does. Pruning is `outbox_prune.rs`.
use std::sync::Arc;

use protocol::{
    Actor, AgentId, CredentialSource, Event, EventPayload, EventSource, ExecutionPlacement, Limits,
    ModelProvider, Owner, RunId, RunSpec, Timestamp, WorkModel,
};
use server::{
    queued_events, Append, InMemoryStore, OutboxEntry, OutboxStore, PostgresStore, PutRun,
    RunStore, StoredRun,
};

const POSTGRES_URL: &str = "postgres://gol:gol@127.0.0.1/gol";

type Stores = (Arc<dyn RunStore>, Arc<dyn OutboxStore>);

fn stores() -> Vec<Stores> {
    let memory = Arc::new(InMemoryStore::default());
    let postgres = Arc::new(PostgresStore::connect(POSTGRES_URL).expect("connect"));
    vec![(memory.clone(), memory), (postgres.clone(), postgres)]
}

/// A principal of its own, so other tests' events are not in its outbox.
fn fresh_owner() -> Owner {
    Owner::new(
        "https://issuer.test",
        format!("user-{}", RunId::new()),
        "tenant-1",
    )
}

fn spec(owner: &Owner) -> RunSpec {
    RunSpec::builder()
        .owner(owner.clone())
        .agent(AgentId::new(), "1")
        .input("hello")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::OpenAI,
            model_name: "gpt-test".to_string(),
            credential: CredentialSource::PlatformGateway,
        })
        .limits(Limits {
            max_steps: 4,
            max_model_calls: 1,
        })
        .build()
}

fn event(spec: &RunSpec, payload: EventPayload) -> Event {
    Event::record(
        EventSource::for_spec(spec, Actor::System, Timestamp::now()),
        payload,
    )
}

fn message(spec: &RunSpec, text: &str) -> Event {
    event(
        spec,
        EventPayload::UserMessage {
            text: text.to_string(),
        },
    )
}

/// Stores `spec`'s run as the producer does: created and queued.
fn put(store: &dyn RunStore, spec: &RunSpec) -> Vec<Event> {
    let events = queued_events(spec);
    assert_eq!(
        store.put_run(StoredRun {
            spec: spec.clone(),
            events: events.clone(),
        }),
        Ok(PutRun::Stored)
    );
    events
}

fn all(outbox: &dyn OutboxStore, owner: &Owner) -> Vec<OutboxEntry> {
    outbox.outbox_after(owner, 0, usize::MAX).expect("outbox")
}

fn numbers(entries: &[OutboxEntry]) -> Vec<(u64, RunId, u64)> {
    entries
        .iter()
        .map(|entry| (entry.seq, entry.run_id, entry.run_seq))
        .collect()
}

// A put writes one entry per event it stores, and each append one per event
// it appends, numbered on from the owner's last, with the event itself.
#[test]
fn every_stored_event_leaves_one_outbox_entry() {
    for (runs, outbox) in stores() {
        let owner = fresh_owner();
        let spec = spec(&owner);
        let mut stored = put(runs.as_ref(), &spec);
        let more = vec![
            message(&spec, "one"),
            message(&spec, "two"),
            message(&spec, "three"),
        ];
        assert_eq!(
            runs.append_events(spec.run_id, more.clone()),
            Ok(Append::Appended)
        );
        stored.extend(more);
        let count = stored.len() as u64;
        let entries = all(outbox.as_ref(), &owner);
        let run = spec.run_id;
        assert_eq!(
            numbers(&entries),
            (1..=count).map(|n| (n, run, n)).collect::<Vec<_>>()
        );
        assert_eq!(
            entries
                .into_iter()
                .map(|entry| entry.event)
                .collect::<Vec<_>>(),
            stored
        );
        // A second put of the same run stores nothing, and numbers nothing.
        assert_eq!(
            runs.put_run(StoredRun {
                spec: spec.clone(),
                events: queued_events(&spec),
            }),
            Ok(PutRun::Existed)
        );
        assert_eq!(all(outbox.as_ref(), &owner).len() as u64, count);
    }
}

// An append the store refuses (the log ended, moved on, or is not there)
// writes no entry.
#[test]
fn a_refused_append_leaves_no_entry() {
    for (runs, outbox) in stores() {
        let owner = fresh_owner();
        let spec = spec(&owner);
        let stored = put(runs.as_ref(), &spec).len() as u64;
        assert_eq!(
            runs.append_events_after(spec.run_id, 1, vec![message(&spec, "stale")]),
            Ok(Append::Moved)
        );
        let ended = event(
            &spec,
            EventPayload::RunCompleted {
                outcome: "done".to_string(),
            },
        );
        assert_eq!(
            runs.append_events(spec.run_id, vec![ended]),
            Ok(Append::Appended)
        );
        assert_eq!(
            runs.append_events(spec.run_id, vec![message(&spec, "late")]),
            Ok(Append::Terminal)
        );
        let missing = self::spec(&owner);
        assert_eq!(
            runs.append_events(missing.run_id, vec![message(&missing, "lost")]),
            Ok(Append::Missing)
        );
        let run = spec.run_id;
        assert_eq!(
            numbers(&all(outbox.as_ref(), &owner)),
            (1..=stored + 1).map(|n| (n, run, n)).collect::<Vec<_>>()
        );
    }
}

// Decision 16A (formal/outbox UniqueSeqs, Contiguous, RunOrder): appends
// racing on two runs of one owner, and on one run, take the owner's numbers
// 1..n with no gap or repeat, and each run's events keep their order.
#[test]
fn racing_appends_for_one_owner_get_consecutive_numbers() {
    for (runs, outbox) in stores() {
        let owner = fresh_owner();
        let specs = [spec(&owner), spec(&owner)];
        let per_run = specs
            .iter()
            .map(|spec| put(runs.as_ref(), spec).len() as u64)
            .max()
            .expect("two runs");
        // Two writers per run: 4 threads, 10 appends of 2 events each.
        std::thread::scope(|scope| {
            for spec in specs.iter().chain(specs.iter()) {
                let runs = runs.clone();
                scope.spawn(move || {
                    for n in 0..10 {
                        let pair = vec![message(spec, &n.to_string()), message(spec, "and")];
                        assert_eq!(runs.append_events(spec.run_id, pair), Ok(Append::Appended));
                    }
                });
            }
        });
        let entries = all(outbox.as_ref(), &owner);
        let per_run = per_run + 2 * 10 * 2;
        assert_eq!(
            entries.iter().map(|entry| entry.seq).collect::<Vec<_>>(),
            (1..=2 * per_run).collect::<Vec<u64>>()
        );
        for spec in &specs {
            let run_seqs: Vec<u64> = entries
                .iter()
                .filter(|entry| entry.run_id == spec.run_id)
                .map(|entry| entry.run_seq)
                .collect();
            assert_eq!(run_seqs, (1..=per_run).collect::<Vec<u64>>());
        }
        // An append's two events take consecutive numbers: each "and" comes
        // right after its pair's first event, in the same run.
        for (at, entry) in entries.iter().enumerate() {
            let EventPayload::UserMessage { text } = &entry.event.payload else {
                continue;
            };
            if text == "and" {
                let before = &entries[at - 1];
                assert_eq!(before.run_id, entry.run_id);
                assert_eq!(before.run_seq + 1, entry.run_seq);
            }
        }
    }
}

// Decision 36A: the outbox is the principal's. Another principal counts
// from 1 on its own; the same principal in another tenant shares the count.
#[test]
fn each_principal_has_its_own_sequence() {
    for (runs, outbox) in stores() {
        let owner = fresh_owner();
        let other = fresh_owner();
        let elsewhere = Owner::new(owner.issuer.clone(), owner.subject.clone(), "tenant-2");
        let first = spec(&owner);
        let theirs = spec(&other);
        let second = spec(&elsewhere);
        let n = put(runs.as_ref(), &first).len() as u64;
        put(runs.as_ref(), &theirs);
        put(runs.as_ref(), &second);
        let expected: Vec<(u64, RunId, u64)> = (1..=n)
            .map(|k| (k, first.run_id, k))
            .chain((1..=n).map(|k| (n + k, second.run_id, k)))
            .collect();
        assert_eq!(numbers(&all(outbox.as_ref(), &owner)), expected);
        assert_eq!(
            all(outbox.as_ref(), &elsewhere),
            all(outbox.as_ref(), &owner)
        );
        assert_eq!(
            numbers(&all(outbox.as_ref(), &other)),
            (1..=n).map(|k| (k, theirs.run_id, k)).collect::<Vec<_>>()
        );
    }
}

// A reader resumes after the last number it saw, a page at a time.
#[test]
fn the_outbox_lists_in_order_after_a_number() {
    for (runs, outbox) in stores() {
        let owner = fresh_owner();
        let spec = spec(&owner);
        let last = put(runs.as_ref(), &spec).len() as u64 + 3;
        runs.append_events(
            spec.run_id,
            vec![
                message(&spec, "a"),
                message(&spec, "b"),
                message(&spec, "c"),
            ],
        )
        .expect("append");
        let page = |after, limit| {
            outbox
                .outbox_after(&owner, after, limit)
                .expect("outbox")
                .iter()
                .map(|entry| entry.seq)
                .collect::<Vec<_>>()
        };
        assert_eq!(page(1, 2), [2, 3]);
        assert_eq!(page(last - 1, 10), [last]);
        assert_eq!(page(last, 10), Vec::<u64>::new());
        assert_eq!(page(0, 0), Vec::<u64>::new());
    }
}
