use std::collections::BTreeMap;

use protocol::MemoryScope;

pub trait Memory {
    fn read(&self, scope: MemoryScope, key: &str) -> Option<String>;
    fn write(&mut self, scope: MemoryScope, key: &str, value: &str);
}

#[derive(Clone, Debug, Default)]
pub struct InMemory {
    values: BTreeMap<(MemoryScope, String), String>,
}

impl Memory for InMemory {
    fn read(&self, scope: MemoryScope, key: &str) -> Option<String> {
        self.values.get(&(scope, key.to_string())).cloned()
    }

    fn write(&mut self, scope: MemoryScope, key: &str, value: &str) {
        self.values
            .insert((scope, key.to_string()), value.to_string());
    }
}
