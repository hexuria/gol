use harness::MemoryKey;
use harness::{memory_scenarios, Memory};
use memory::PostgresMemory;
use protocol::MemoryScope;

const POSTGRES_URL: &str = "postgres://gol:gol@127.0.0.1/gol";

#[test]
fn postgres_recalls_a_value_on_a_new_connection() {
    let key = format!("topic-recall-{}", uuid_key());
    {
        let store = PostgresMemory::connect(POSTGRES_URL).expect("connect");
        assert_eq!(store.read(&owner(MemoryScope::Run), &key), Ok(None));
        store
            .write(&owner(MemoryScope::Run), &key, "rust")
            .expect("write");
    }
    let store = PostgresMemory::connect(POSTGRES_URL).expect("reconnect");
    assert_eq!(
        store.read(&owner(MemoryScope::Run), &key),
        Ok(Some("rust".to_string()))
    );
    assert_eq!(store.read(&owner(MemoryScope::Agent), &key), Ok(None));
}

// A dead connection fails the call that finds it, and the next call
// reconnects (owner decision 1A).
#[test]
fn a_killed_backend_fails_one_read_then_reconnects() {
    let key = format!("topic-idle-kill-{}", uuid_key());
    let application = format!("gol_c1_memory_{key}").replace('-', "_");
    let store = PostgresMemory::connect(&format!("{POSTGRES_URL}?application_name={application}"))
        .expect("connect");
    store
        .write(&owner(MemoryScope::Run), &key, "rust")
        .expect("write");

    let mut admin = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("admin");
    let killed: i64 = admin
        .query_one(
            "select count(pg_terminate_backend(pid)) from pg_stat_activity
             where application_name = $1",
            &[&application],
        )
        .expect("terminate")
        .get(0);
    assert_eq!(killed, 1);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while admin
        .query_one(
            "select count(*) from pg_stat_activity where application_name = $1",
            &[&application],
        )
        .expect("activity")
        .get::<_, i64>(0)
        > 0
    {
        assert!(std::time::Instant::now() < deadline, "backend still alive");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    assert!(store.read(&owner(MemoryScope::Run), &key).is_err());
    assert_eq!(
        store.read(&owner(MemoryScope::Run), &key),
        Ok(Some("rust".to_string()))
    );
}

// A backend killed while a statement runs reports a database error, not a
// closed connection. The call that saw it fails, and the next call still
// reconnects (owner decision 1A).
#[test]
fn a_backend_killed_mid_statement_is_replaced_on_the_next_call() {
    let key = format!("topic-mid-kill-{}", uuid_key());
    let application = format!("gol_c1_mid_{key}").replace('-', "_");
    // Timeouts long enough that the kill, not the lock timeout, ends the
    // write.
    let url = format!(
        "{POSTGRES_URL}?application_name={application}\
         &options=-clock_timeout%3D60s%20-cstatement_timeout%3D60s"
    );
    let scope = serde_json::to_string(&MemoryScope::Run).unwrap();
    let mut admin = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("admin");
    PostgresMemory::connect(POSTGRES_URL).expect("schema");
    let mut holder = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("holder");
    let mut tx = holder.transaction().expect("begin");
    tx.execute(
        "insert into memories (scope, owner_id, key, value) values ($1, 'recall', $2, 'held')",
        &[&scope, &key],
    )
    .expect("insert");
    // Connect first: connect waits on the schema lock too, and the kill must
    // land in the write.
    let store = PostgresMemory::connect(&url).expect("connect");
    let blocked = {
        let key = key.clone();
        std::thread::spawn(move || {
            let result = store.write(&owner(MemoryScope::Run), &key, "rust");
            (store, result)
        })
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while admin
        .query_one(
            "select count(*) from pg_stat_activity
             where application_name = $1 and wait_event_type = 'Lock'",
            &[&application],
        )
        .expect("activity")
        .get::<_, i64>(0)
        == 0
    {
        assert!(std::time::Instant::now() < deadline, "write never waited");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let killed: i64 = admin
        .query_one(
            "select count(pg_terminate_backend(pid)) from pg_stat_activity
             where application_name = $1",
            &[&application],
        )
        .expect("terminate")
        .get(0);
    assert_eq!(killed, 1);
    let (store, result) = blocked.join().unwrap();
    assert!(result.is_err(), "the killed write");
    tx.rollback().expect("rollback");
    assert_eq!(store.read(&owner(MemoryScope::Run), &key), Ok(None));
}

/// The owner these tests keep their entries under.
fn owner(scope: MemoryScope) -> MemoryKey {
    MemoryKey {
        scope,
        owner_id: "recall".to_string(),
    }
}

// The shared scoping scenarios (harness::memory_scenarios) against Postgres:
// the same contract InMemory passes in crates/harness/tests/memory_scopes.rs.
#[test]
fn run_memory_isolated_between_runs() {
    memory_scenarios::run_memory_isolated_between_runs(&connect());
}

#[test]
fn step_memory_isolated_between_steps() {
    memory_scenarios::step_memory_isolated_between_steps(&connect());
}

#[test]
fn agent_memory_survives_across_runs() {
    memory_scenarios::agent_memory_survives_across_runs(&connect());
}

#[test]
fn user_memory_isolated_between_tenants() {
    memory_scenarios::user_memory_isolated_between_tenants(&connect());
}

#[test]
fn session_memory_belongs_to_its_user() {
    memory_scenarios::session_memory_belongs_to_its_user(&connect());
}

#[test]
fn workspace_memory_belongs_to_its_organization() {
    memory_scenarios::workspace_memory_belongs_to_its_organization(&connect());
}

#[test]
fn global_memory_is_denied() {
    memory_scenarios::global_memory_is_denied(&connect());
}

// The counterexample trace of formal/memory, forced.
#[test]
fn no_cross_scope_read_in_the_model_trace() {
    memory_scenarios::no_cross_scope_read_in_the_model_trace(&connect());
}

// The Rust test of `NoCrossScopeRead` in formal/memory/Memory.tla.
#[test]
fn no_cross_scope_read() {
    memory_scenarios::no_cross_scope_read(&connect());
}

fn connect() -> PostgresMemory {
    PostgresMemory::connect(POSTGRES_URL).expect("connect")
}

// A memories table from before owners is refused at connect (owner decision
// 4A for C3): keeping its rows under an empty owner would let any owner read
// them.
#[test]
fn a_memories_table_without_owners_is_refused_at_connect() {
    let mut admin = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("connect");
    let schema = format!("c3_old_{}", std::process::id());
    admin
        .batch_execute(&format!(
            "drop schema if exists {schema} cascade;
             create schema {schema};
             create table {schema}.memories (scope text not null, key text not null,
                 value text not null, primary key (scope, key));"
        ))
        .expect("old schema");
    let url = format!("{POSTGRES_URL}?options=-csearch_path%3D{schema}");
    let connected = PostgresMemory::connect(&url).map(|_| ());
    admin
        .batch_execute(&format!("drop schema {schema} cascade"))
        .expect("drop");
    let error = connected.expect_err("connect must fail");
    assert!(error.to_string().contains("drop table memories"), "{error}");
}

// A table given the owner column by hand but still keyed by (scope, key) is
// refused too: every write to it would fail at runtime.
#[test]
fn a_memories_table_with_the_old_key_is_refused_at_connect() {
    let mut admin = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("connect");
    let schema = format!("c3_rekeyed_{}", std::process::id());
    admin
        .batch_execute(&format!(
            "drop schema if exists {schema} cascade;
             create schema {schema};
             create table {schema}.memories (scope text not null, key text not null,
                 value text not null, primary key (scope, key));
             alter table {schema}.memories add column owner_id text not null default '';"
        ))
        .expect("old schema");
    let url = format!("{POSTGRES_URL}?options=-csearch_path%3D{schema}");
    let connected = PostgresMemory::connect(&url).map(|_| ());
    admin
        .batch_execute(&format!("drop schema {schema} cascade"))
        .expect("drop");
    let error = connected.expect_err("connect must fail");
    assert!(error.to_string().contains("drop table memories"), "{error}");
}

// Every run shares one connection, so a write stuck behind a row lock gives
// up at the lock timeout (SQLSTATE 55P03) instead of holding the rest up. The
// write runs on its own thread, so a missing timeout fails the test instead
// of hanging it.
#[test]
fn a_write_waiting_on_a_lock_gives_up() {
    let key = format!("topic-locked-{}", uuid_key());
    // Connecting creates the table the holder inserts into, whichever test
    // runs first on a fresh database.
    drop(connect());
    let scope = serde_json::to_string(&MemoryScope::Run).unwrap();
    let mut holder = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("holder");
    let mut tx = holder.transaction().expect("begin");
    tx.execute(
        "insert into memories (scope, owner_id, key, value) values ($1, 'recall', $2, 'held')",
        &[&scope, &key],
    )
    .expect("insert");
    let writer = {
        let key = key.clone();
        std::thread::spawn(move || {
            let store = connect();
            let result = store.write(&owner(MemoryScope::Run), &key, "rust");
            (store, result)
        })
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while !writer.is_finished() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let gave_up = writer.is_finished();
    tx.rollback().expect("rollback");
    let (store, result) = writer.join().unwrap();
    assert!(gave_up, "the write still waited on the lock after 15 s");
    let error = result.expect_err("the write waited out the lock");
    assert!(error.to_string().contains("55P03"), "{error}");
    assert_eq!(
        store.write(&owner(MemoryScope::Run), &key, "rust"),
        Ok(()),
        "the next write, once the lock is gone"
    );
}

/// Coarse on macOS, so each test also names its own key prefix.
fn uuid_key() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos()
        .to_string()
}
