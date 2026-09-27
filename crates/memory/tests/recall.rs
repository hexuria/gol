use harness::Memory;
use memory::PostgresMemory;
use protocol::MemoryScope;

const POSTGRES_URL: &str = "postgres://gol:gol@127.0.0.1/gol";

#[test]
fn postgres_recalls_a_value_on_a_new_connection() {
    let key = format!("topic-{}", uuid_key());
    {
        let mut store = PostgresMemory::connect(POSTGRES_URL).expect("connect");
        assert_eq!(store.read(MemoryScope::Run, &key), Ok(None));
        store.write(MemoryScope::Run, &key, "rust").expect("write");
    }
    let store = PostgresMemory::connect(POSTGRES_URL).expect("reconnect");
    assert_eq!(
        store.read(MemoryScope::Run, &key),
        Ok(Some("rust".to_string()))
    );
    assert_eq!(store.read(MemoryScope::Agent, &key), Ok(None));
}

// A dead connection fails the call that finds it, and the next call
// reconnects (owner decision 1A).
#[test]
fn a_killed_backend_fails_one_read_then_reconnects() {
    let key = format!("topic-{}", uuid_key());
    let application = format!("gol_c1_memory_{key}").replace('-', "_");
    let mut store =
        PostgresMemory::connect(&format!("{POSTGRES_URL}?application_name={application}"))
            .expect("connect");
    store.write(MemoryScope::Run, &key, "rust").expect("write");

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

    assert!(store.read(MemoryScope::Run, &key).is_err());
    assert_eq!(
        store.read(MemoryScope::Run, &key),
        Ok(Some("rust".to_string()))
    );
}

fn uuid_key() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos()
        .to_string()
}
