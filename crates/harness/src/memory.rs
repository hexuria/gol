use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};

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

/// Whose memory an entry is: its scope, and the id that
/// `protocol::memory_owner_id` names for a run (owner decision 10 for C3).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MemoryKey {
    pub scope: MemoryScope,
    pub owner_id: String,
}

/// Memory shared by every run of a server, so it takes `&self`: an
/// implementation keeps itself consistent across callers.
pub trait Memory: Send + Sync {
    fn read(&self, owner: &MemoryKey, key: &str) -> Result<Option<String>, StoreError>;
    fn write(&self, owner: &MemoryKey, key: &str, value: &str) -> Result<(), StoreError>;
}

/// Memory in a map. A poisoned lock (a writer panicked holding it) still
/// serves reads, since every write completes before its guard drops; a write
/// under it is refused, as `InMemoryStore` does.
#[derive(Debug, Default)]
pub struct InMemory {
    values: Mutex<BTreeMap<(MemoryKey, String), String>>,
}

impl Memory for InMemory {
    fn read(&self, owner: &MemoryKey, key: &str) -> Result<Option<String>, StoreError> {
        let values = self.values.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(values.get(&(owner.clone(), key.to_string())).cloned())
    }

    fn write(&self, owner: &MemoryKey, key: &str, value: &str) -> Result<(), StoreError> {
        self.values
            .lock()
            .map_err(|_| StoreError::new("memory lock poisoned"))?
            .insert((owner.clone(), key.to_string()), value.to_string());
        Ok(())
    }
}
