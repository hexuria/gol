//! Outbox retention (Phase 3.1, decision 38A): pruning removes entries stored
//! before a cutoff, and the owner's counter stays, so numbers never restart.
//! A prune is store-wide, and Postgres is shared with the other tests, which
//! may run beside these (another binary, nextest, another job). So the
//! Postgres prunes here only reach rows this file set to negative stored
//! times, which no real write has, with negative cutoffs.
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
    // The in-memory store is this test's own: its real clock will do.
    let memory = InMemoryStore::default();
    let owner = fresh_owner();
    let old = spec(&owner);
    put(&memory, &old);
    let first = entries(&memory, &owner).len() as u64;
    let cutoff = Timestamp::unix_millis(Timestamp::now().as_unix_millis() + 1);
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert_eq!(memory.prune_outbox(cutoff), Ok(first));
    prunes_and_keeps_the_count(&memory, &owner, first, cutoff);

    // Postgres: this owner's rows set to a negative time, pruned alone.
    let postgres = PostgresStore::connect(POSTGRES_URL).expect("connect");
    let owner = fresh_owner();
    let old = spec(&owner);
    put(&postgres, &old);
    let first = entries(&postgres, &owner).len() as u64;
    stored_at(&owner, "-5000");
    let cutoff = Timestamp::unix_millis(-1000);
    let pruned = postgres.prune_outbox(cutoff).expect("prune");
    // Rows an earlier run of this test left negative go too.
    assert!(pruned >= first, "pruned {pruned}");
    prunes_and_keeps_the_count(&postgres, &owner, first, cutoff);
    // After, not beside: its prune would reach this one's negative rows.
    pruning_takes_a_prefix_across_clock_skew();
}

/// After `owner`'s first run (`first` entries) was pruned: a reader is told
/// so, the count goes on from where it was, and newer entries stay.
fn prunes_and_keeps_the_count<S: RunStore + OutboxStore>(
    store: &S,
    owner: &Owner,
    first: u64,
    cutoff: Timestamp,
) {
    // A reader is told what was pruned: a cursor below it is stale.
    let page = store.outbox_after(owner, 0, usize::MAX).expect("outbox");
    assert_eq!(page.pruned_through, first);
    assert_eq!(page.entries, Vec::new());
    let new = spec(owner);
    put(store, &new);
    let page = store.outbox_after(owner, 0, usize::MAX).expect("outbox");
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
    assert_eq!(store.prune_outbox(cutoff), Ok(0));
}

// Decision 38A with writers on hosts whose clocks differ: an entry can be
// stored with an earlier time than the one numbered before it. Pruning takes
// a prefix of the principal's numbers all the same, so what stays is a suffix
// and a reader is never handed a gap it cannot see. The in-memory store's
// prefix rule is a unit test in `store.rs`, where its times can be set.
fn pruning_takes_a_prefix_across_clock_skew() {
    let store = PostgresStore::connect(POSTGRES_URL).expect("connect");
    let owner = fresh_owner();
    put(&store, &spec(&owner));
    let count = entries(&store, &owner).len() as u64;
    assert!(count >= 3, "{count}");
    // The second entry's host was behind: it looks older than the first,
    // and older than the cutoff, which the others are not.
    stored_at(&owner, "case when seq = 2 then -3000 else -500 end");
    store
        .prune_outbox(Timestamp::unix_millis(-1000))
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

fn put(store: &dyn RunStore, spec: &RunSpec) {
    store
        .put_run(StoredRun {
            spec: spec.clone(),
            events: queued_events(spec),
        })
        .expect("put");
}

fn entries(store: &dyn OutboxStore, owner: &Owner) -> Vec<server::OutboxEntry> {
    store
        .outbox_after(owner, 0, usize::MAX)
        .expect("outbox")
        .entries
}

/// Sets `owner`'s outbox rows' stored time to the SQL expression `ms`.
fn stored_at(owner: &Owner, ms: &str) {
    let mut admin = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("admin");
    admin
        .execute(
            &format!(
                "update outbox set stored_ms = {ms}
                 where owner_issuer = $1 and owner_subject = $2"
            ),
            &[&owner.issuer, &owner.subject],
        )
        .expect("stored time");
}

fn fresh_owner() -> Owner {
    Owner::new(
        "https://issuer.test",
        format!("user-{}", RunId::new()),
        "tenant-1",
    )
}
