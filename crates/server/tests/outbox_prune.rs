//! Outbox retention (Phase 3.1, decision 38A): pruning removes entries stored
//! before a cutoff, and the owner's counter stays, so numbers never restart.
//! A test binary of its own, with one test: a prune is store-wide, and a
//! test running beside it would lose entries to it.
use std::sync::Arc;

use protocol::{
    AgentId, CredentialSource, ExecutionPlacement, Limits, ModelProvider, Owner, RunId, RunSpec,
    Timestamp, WorkModel,
};
use server::{queued_events, InMemoryStore, OutboxStore, PostgresStore, RunStore, StoredRun};

const POSTGRES_URL: &str = "postgres://gol:gol@127.0.0.1/gol";

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

#[test]
fn pruning_removes_old_entries_and_keeps_the_count() {
    let memory = Arc::new(InMemoryStore::default());
    let postgres = Arc::new(PostgresStore::connect(POSTGRES_URL).expect("connect"));
    let stores: Vec<(Arc<dyn RunStore>, Arc<dyn OutboxStore>)> =
        vec![(memory.clone(), memory), (postgres.clone(), postgres)];
    for (runs, outbox) in stores {
        let owner = fresh_owner();
        let put = |spec: &RunSpec| {
            runs.put_run(StoredRun {
                spec: spec.clone(),
                events: queued_events(spec),
            })
            .expect("put");
        };
        let old = spec(&owner);
        put(&old);
        let first = outbox
            .outbox_after(&owner, 0, usize::MAX)
            .expect("outbox")
            .entries
            .len() as u64;
        let cutoff = Timestamp::unix_millis(Timestamp::now().as_unix_millis() + 1);
        std::thread::sleep(std::time::Duration::from_millis(5));
        let pruned = outbox.prune_outbox(cutoff).expect("prune");
        assert!(pruned >= first, "pruned {pruned}");
        // A reader is told what was pruned: a cursor below it is stale.
        let page = outbox.outbox_after(&owner, 0, usize::MAX).expect("outbox");
        assert_eq!(page.pruned_through, first);
        assert_eq!(page.entries, Vec::new());
        // The count goes on from where it was.
        let new = spec(&owner);
        put(&new);
        let page = outbox.outbox_after(&owner, 0, usize::MAX).expect("outbox");
        assert_eq!(page.pruned_through, first);
        assert_eq!(
            page.entries
                .into_iter()
                .map(|entry| (entry.seq, entry.run_id))
                .collect::<Vec<_>>(),
            (first + 1..=2 * first)
                .map(|seq| (seq, new.run_id))
                .collect::<Vec<_>>()
        );
        // Entries stored after the cutoff stay.
        assert_eq!(outbox.prune_outbox(cutoff), Ok(0));
    }
    pruning_takes_a_prefix_across_clock_skew();
}

// Decision 38A with writers on hosts whose clocks differ: an entry can be
// stored with an earlier time than the one numbered before it. Pruning takes
// a prefix of the principal's numbers all the same, so what stays is a suffix
// and a reader is never handed a gap it cannot see. Postgres only: one
// process's store has one clock. Run after the test above, not beside it: a
// prune is store-wide.
fn pruning_takes_a_prefix_across_clock_skew() {
    let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
    let owner = fresh_owner();
    let spec = spec(&owner);
    store
        .put_run(StoredRun {
            spec: spec.clone(),
            events: queued_events(&spec),
        })
        .expect("put");
    let count = store
        .outbox_after(&owner, 0, usize::MAX)
        .expect("outbox")
        .entries
        .len() as u64;
    assert!(count >= 3, "{count}");
    // The second entry's host was behind: it looks older than the first.
    let mut admin = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("admin");
    admin
        .execute(
            "update outbox set stored_ms = case when seq = 2 then 1000 else 9000000000000 end
             where owner_issuer = $1 and owner_subject = $2",
            &[&owner.issuer, &owner.subject],
        )
        .expect("skew");
    store
        .prune_outbox(Timestamp::unix_millis(5000))
        .expect("prune");
    let page = store.outbox_after(&owner, 0, usize::MAX).expect("outbox");
    assert_eq!(page.pruned_through, 2);
    assert_eq!(
        page.entries
            .iter()
            .map(|entry| entry.seq)
            .collect::<Vec<_>>(),
        (3..=count).collect::<Vec<u64>>()
    );
}

fn fresh_owner() -> Owner {
    Owner::new(
        "https://issuer.test",
        format!("user-{}", RunId::new()),
        "tenant-1",
    )
}
