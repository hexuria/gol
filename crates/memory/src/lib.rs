use std::sync::Mutex;

use harness::Memory;
use postgres::NoTls;
use protocol::MemoryScope;

pub struct PostgresMemory {
    client: Mutex<postgres::Client>,
}

impl PostgresMemory {
    pub fn connect(url: &str) -> Result<Self, postgres::Error> {
        let mut client = postgres::Client::connect(url, NoTls)?;
        client.batch_execute(
            "
            create table if not exists memories (
                scope text not null,
                key text not null,
                value text not null,
                primary key (scope, key)
            );
            ",
        )?;
        Ok(Self {
            client: Mutex::new(client),
        })
    }
}

impl Memory for PostgresMemory {
    fn read(&self, scope: MemoryScope, key: &str) -> Option<String> {
        let scope = serde_json::to_string(&scope).expect("scope");
        let row = self
            .client
            .lock()
            .expect("memory")
            .query_opt(
                "select value from memories where scope = $1 and key = $2",
                &[&scope, &key],
            )
            .expect("select memory")?;
        Some(row.get(0))
    }

    fn write(&mut self, scope: MemoryScope, key: &str, value: &str) {
        let scope = serde_json::to_string(&scope).expect("scope");
        self.client
            .lock()
            .expect("memory")
            .execute(
                "insert into memories (scope, key, value) values ($1, $2, $3)
                 on conflict (scope, key) do update set value = excluded.value",
                &[&scope, &key, &value],
            )
            .expect("upsert memory");
    }
}
