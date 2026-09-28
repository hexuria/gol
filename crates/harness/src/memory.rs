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

/// Memory in a map. After a writer panicked holding the lock, reads still
/// answer and writes are refused, as `InMemoryStore` does.
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

/// One run's memory: run and step memory kept with the run, so it is freed
/// when the run ends, and every other scope in `shared`, which outlives it.
pub struct RunMemory<'a> {
    shared: &'a dyn Memory,
    own: InMemory,
}

impl<'a> RunMemory<'a> {
    pub fn new(shared: &'a dyn Memory) -> Self {
        Self {
            shared,
            own: InMemory::default(),
        }
    }

    fn holder(&self, owner: &MemoryKey) -> &dyn Memory {
        match owner.scope {
            MemoryScope::Run | MemoryScope::Step => &self.own,
            _ => self.shared,
        }
    }
}

impl Memory for RunMemory<'_> {
    fn read(&self, owner: &MemoryKey, key: &str) -> Result<Option<String>, StoreError> {
        self.holder(owner).read(owner, key)
    }

    fn write(&self, owner: &MemoryKey, key: &str, value: &str) -> Result<(), StoreError> {
        self.holder(owner).write(owner, key, value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(scope: MemoryScope) -> MemoryKey {
        MemoryKey {
            scope,
            owner_id: "o".to_string(),
        }
    }

    // Run and step memory stay with the run; the other scopes reach the
    // shared memory.
    #[test]
    fn a_run_keeps_only_its_run_and_step_memory() {
        let shared = InMemory::default();
        {
            let run = RunMemory::new(&shared);
            for scope in [MemoryScope::Run, MemoryScope::Step, MemoryScope::Agent] {
                run.write(&key(scope), "k", "v").unwrap();
                assert_eq!(run.read(&key(scope), "k"), Ok(Some("v".to_string())));
            }
        }
        assert_eq!(shared.read(&key(MemoryScope::Run), "k"), Ok(None));
        assert_eq!(shared.read(&key(MemoryScope::Step), "k"), Ok(None));
        assert_eq!(
            shared.read(&key(MemoryScope::Agent), "k"),
            Ok(Some("v".to_string()))
        );
    }
}
