//! Where the server keeps runs and memory, from its environment.
use std::collections::BTreeMap;
use std::sync::Arc;

use harness::{InMemory, Memory};
use memory::PostgresMemory;

use crate::postgres::{PoolOptions, PostgresStore};
use crate::store::{InMemoryStore, RunStore};

/// The server's run store and the memory its runs share.
pub struct Stores {
    pub runs: Arc<dyn RunStore>,
    pub memory: Arc<dyn Memory>,
}

/// Postgres when `GOL_DATABASE_URL` is set, with `GOL_DATABASE_POOL_SIZE`
/// connections (8 by default) and one kept idle; memory otherwise. An error
/// says why the server cannot start. It connects, so call it from a blocking
/// thread.
pub fn stores_from_env(env: &BTreeMap<String, String>) -> Result<Stores, String> {
    let Some(url) = env.get("GOL_DATABASE_URL") else {
        return Ok(Stores {
            runs: Arc::new(InMemoryStore::default()),
            memory: Arc::new(InMemory::default()),
        });
    };
    let max_size = match env.get("GOL_DATABASE_POOL_SIZE") {
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
    Ok(Stores {
        runs: Arc::new(runs),
        memory: Arc::new(memory),
    })
}
