//! Outbox retention (Phase 3.1, decision 38A): pruning removes entries stored
//! before a cutoff, and the owner's counter stays, so numbers never restart.
//! A test binary of its own: a prune is store-wide, and the tests in
//! `outbox.rs` would lose entries to it if they ran beside it.
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
        let owner = Owner::new(
            "https://issuer.test",
            format!("user-{}", RunId::new()),
            "tenant-1",
        );
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
            .len() as u64;
        let cutoff = Timestamp::unix_millis(Timestamp::now().as_unix_millis() + 1);
        std::thread::sleep(std::time::Duration::from_millis(5));
        let pruned = outbox.prune_outbox(cutoff).expect("prune");
        assert!(pruned >= first, "pruned {pruned}");
        assert_eq!(
            outbox.outbox_after(&owner, 0, usize::MAX).expect("outbox"),
            Vec::new()
        );
        // The count goes on from where it was.
        let new = spec(&owner);
        put(&new);
        let numbers: Vec<(u64, RunId)> = outbox
            .outbox_after(&owner, 0, usize::MAX)
            .expect("outbox")
            .into_iter()
            .map(|entry| (entry.seq, entry.run_id))
            .collect();
        assert_eq!(
            numbers,
            (first + 1..=2 * first)
                .map(|seq| (seq, new.run_id))
                .collect::<Vec<_>>()
        );
        // Entries stored after the cutoff stay.
        assert_eq!(outbox.prune_outbox(cutoff), Ok(0));
    }
}
