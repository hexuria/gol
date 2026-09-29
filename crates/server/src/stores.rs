//! Where the server keeps runs and memory, from its environment.
use std::collections::BTreeMap;
use std::sync::Arc;

use harness::{InMemory, Memory};
use memory::PostgresMemory;

use crate::postgres::{PoolOptions, PostgresStore};
use crate::store::{InMemoryStore, MessageStore, RunStore};

/// The server's run store, the memory its runs share, and the messages
/// between its agents (Phase 2.1).
pub struct Stores {
    pub runs: Arc<dyn RunStore>,
    pub memory: Arc<dyn Memory>,
    pub messages: Arc<dyn MessageStore>,
}

/// Postgres when `GOL_DATABASE_URL` is set and not empty, with
/// `GOL_DATABASE_POOL_SIZE`
/// connections (8 by default) and one kept idle; memory otherwise. An error
/// says why the server cannot start. It connects, so call it from a blocking
/// thread.
pub fn stores_from_env(env: &BTreeMap<String, String>) -> Result<Stores, String> {
    let Some(url) = env.get("GOL_DATABASE_URL").filter(|url| !url.is_empty()) else {
        let runs = Arc::new(InMemoryStore::default());
        return Ok(Stores {
            runs: runs.clone(),
            memory: Arc::new(InMemory::default()),
            messages: runs,
        });
    };
    let max_size = match env
        .get("GOL_DATABASE_POOL_SIZE")
        .filter(|size| !size.is_empty())
    {
        None => PoolOptions::default().max_size,
        Some(size) => size
            .parse::<u32>()
            .ok()
            .filter(|size| *size > 0)
            .ok_or_else(|| {
                format!("GOL_DATABASE_POOL_SIZE must be a positive count, not {size:?}")
            })?,
    };
    let runs = PostgresStore::connect_with(
        url,
        PoolOptions {
            max_size,
            min_idle: 1,
        },
    )
    .map_err(|error| format!("run store: {error}"))?;
    let memory = PostgresMemory::connect(url).map_err(|error| format!("memory: {error}"))?;
    let runs = Arc::new(runs);
    Ok(Stores {
        runs: runs.clone(),
        memory: Arc::new(memory),
        messages: runs,
    })
}
