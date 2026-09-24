use harness::Memory;
use memory::PostgresMemory;
use protocol::MemoryScope;

const POSTGRES_URL: &str = "postgres://gol:gol@127.0.0.1/gol";

#[test]
fn postgres_recalls_a_value_on_a_new_connection() {
    let key = format!("topic-{}", uuid_key());
    {
        let mut store = PostgresMemory::connect(POSTGRES_URL).expect("connect");
        assert!(store.read(MemoryScope::Run, &key).is_none());
        store.write(MemoryScope::Run, &key, "rust");
    }
    let store = PostgresMemory::connect(POSTGRES_URL).expect("reconnect");
    assert_eq!(store.read(MemoryScope::Run, &key).as_deref(), Some("rust"));
    assert!(store.read(MemoryScope::Agent, &key).is_none());
}

fn uuid_key() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos()
        .to_string()
}
