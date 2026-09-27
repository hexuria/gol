use std::collections::BTreeMap;

use protocol::MemoryScope;

/// A store could not answer: it is unreachable, or a write's outcome is
/// unknown. Callers report it; they do not retry a write, which may have
/// committed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreError {
    message: String,
}

impl StoreError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for StoreError {}

pub trait Memory {
    fn read(&self, scope: MemoryScope, key: &str) -> Result<Option<String>, StoreError>;
    fn write(&mut self, scope: MemoryScope, key: &str, value: &str) -> Result<(), StoreError>;
}

#[derive(Clone, Debug, Default)]
pub struct InMemory {
    values: BTreeMap<(MemoryScope, String), String>,
}

impl Memory for InMemory {
    fn read(&self, scope: MemoryScope, key: &str) -> Result<Option<String>, StoreError> {
        Ok(self.values.get(&(scope, key.to_string())).cloned())
    }

    fn write(&mut self, scope: MemoryScope, key: &str, value: &str) -> Result<(), StoreError> {
        self.values
            .insert((scope, key.to_string()), value.to_string());
        Ok(())
    }
}
