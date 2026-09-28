//! The Redis run queue (C4). `POST /v1/runs` pushes a run id onto the runs
//! list. A worker claims it: one script moves it to the processing list,
//! takes its lease and counts the delivery, so no run is ever in processing
//! without a lease. The worker renews the lease while it runs the harness
//! and acknowledges the run only once the run's terminal event is stored.
//! The reaper moves a run whose lease ran out back to the front of the runs
//! list. `formal/runqueue` models it.
//!
//! Every key shares the hash tag `{<key>}`, so the scripts also run on one
//! Redis Cluster slot. The scripts touch lease keys they build from their
//! arguments, which a single node allows. Redis must keep what it is given:
//! `noeviction`, and AOF persistence so a restart does not drop the lists.
use std::sync::Mutex;
use std::time::Duration;

use protocol::RunId;
use redis::{Commands, Script};

/// How long a claim is held, how often it is renewed and reaped, and how
/// many times a run is delivered before it is failed (owner decision 2A for
/// C4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueTiming {
    /// How long a lease lasts unless its worker renews it.
    pub lease: Duration,
    /// How often a working worker renews its lease. Shorter than `lease`.
    pub heartbeat: Duration,
    /// How often the reaper looks for runs whose lease ran out.
    pub reap_every: Duration,
    /// How long a worker waits before it looks again at an empty queue, and
    /// the first wait after an error, which doubles up to `max_backoff`.
    pub idle_wait: Duration,
    /// The longest wait after repeated errors.
    pub max_backoff: Duration,
    /// Deliveries after which a run that never ends is failed instead.
    pub max_deliveries: u32,
}

impl Default for QueueTiming {
    /// A 30 s lease, renewed every 10 s, reaped every 15 s: a crashed
    /// worker's run is back at the front of the queue within about 45 s. A
    /// run delivered five times without ending is failed.
    fn default() -> Self {
        Self {
            lease: Duration::from_secs(30),
            heartbeat: Duration::from_secs(10),
            reap_every: Duration::from_secs(15),
            idle_wait: Duration::from_millis(500),
            max_backoff: Duration::from_secs(30),
            max_deliveries: 5,
        }
    }
}

/// How long a connect, a read or a write to Redis may take.
const REDIS_TIMEOUT: Duration = Duration::from_secs(5);

/// Moves the oldest run to processing, leases it to ARGV[2] for ARGV[3] ms
/// and counts the delivery, in one step. A run already leased (an id queued
/// twice) is dropped from processing instead.
const CLAIM: &str = r"
local id = redis.call('LMOVE', KEYS[1], KEYS[2], 'RIGHT', 'LEFT')
if not id then return false end
if not redis.call('SET', ARGV[1] .. id, ARGV[2], 'NX', 'PX', ARGV[3]) then
  redis.call('LREM', KEYS[2], 1, id)
  return false
end
return {id, redis.call('HINCRBY', KEYS[3], id, 1)}
";

/// Extends the lease KEYS[1] by ARGV[2] ms while ARGV[1] holds it.
const RENEW: &str = r"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
";

/// Removes ARGV[1] from processing and its delivery count, and its lease
/// KEYS[2] if ARGV[2] holds it.
const ACK: &str = r"
redis.call('LREM', KEYS[1], 1, ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
if redis.call('GET', KEYS[2]) == ARGV[2] then
  redis.call('DEL', KEYS[2])
end
return 1
";

/// Moves every run in processing whose lease is gone back to the front of
/// the runs list, the end claims take from.
const REAP: &str = r"
local moved = {}
for _, id in ipairs(redis.call('LRANGE', KEYS[1], 0, -1)) do
  if redis.call('EXISTS', ARGV[1] .. id) == 0 then
    redis.call('LREM', KEYS[1], 1, id)
    redis.call('RPUSH', KEYS[2], id)
    table.insert(moved, id)
  end
end
return moved
";

/// A run a worker claimed, and how many times it has been delivered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Delivery {
    pub run_id: RunId,
    pub count: u32,
}

pub struct RedisRunQueue {
    url: String,
    runs: String,
    processing: String,
    deliveries: String,
    lease_prefix: String,
    /// One connection, opened on first use and dropped after any error.
    connection: Mutex<Option<redis::Connection>>,
}

impl RedisRunQueue {
    /// The server's queue, under `{gol:runs}`.
    pub fn open(url: impl Into<String>) -> Self {
        Self::with_key(url, "gol:runs")
    }

    /// A queue under `{key}`, with `{key}:processing`, `{key}:deliveries`
    /// and `{key}:lease:<run>`.
    pub fn with_key(url: impl Into<String>, key: impl Into<String>) -> Self {
        let tag = format!("{{{}}}", key.into());
        Self {
            url: url.into(),
            processing: format!("{tag}:processing"),
            deliveries: format!("{tag}:deliveries"),
            lease_prefix: format!("{tag}:lease:"),
            runs: tag,
            connection: Mutex::new(None),
        }
    }

    /// Whether Redis answers.
    pub fn ping(&self) -> Result<(), String> {
        self.with_connection(|connection| redis::cmd("PING").query::<String>(connection))
            .map(|_| ())
    }

    pub fn push(&self, id: RunId) -> Result<(), String> {
        self.with_connection(|connection| connection.lpush::<_, _, ()>(&self.runs, id.to_string()))
    }

    /// Takes the oldest run off the queue for good, with no lease: a crash
    /// loses it. For tests; a worker claims instead.
    pub fn pop(&self) -> Result<Option<RunId>, String> {
        let value: Option<String> = self.with_connection(|connection| {
            connection.rpop(&self.runs, None::<std::num::NonZeroUsize>)
        })?;
        value.map(|text| parse(&text)).transpose()
    }

    /// The runs waiting to be claimed, newest first. Entries that are not
    /// run ids are left out.
    pub fn queued(&self) -> Result<Vec<RunId>, String> {
        self.list(&self.runs)
    }

    /// The runs claimed and not yet acknowledged, newest first.
    pub fn processing(&self) -> Result<Vec<RunId>, String> {
        self.list(&self.processing)
    }

    /// Claims the oldest run for `token` with a lease of `lease`. An entry
    /// that is not a run id is dropped, with an error.
    pub fn claim(&self, token: &str, lease: Duration) -> Result<Option<Delivery>, String> {
        let claimed: Option<(String, u32)> = self.with_connection(|connection| {
            Script::new(CLAIM)
                .key(&self.runs)
                .key(&self.processing)
                .key(&self.deliveries)
                .arg(&self.lease_prefix)
                .arg(token)
                .arg(millis(lease))
                .invoke(connection)
        })?;
        let Some((text, count)) = claimed else {
            return Ok(None);
        };
        match parse(&text) {
            Ok(run_id) => Ok(Some(Delivery { run_id, count })),
            Err(error) => {
                self.forget(&text, token)?;
                Err(format!("dropped queue entry {text:?}: {error}"))
            }
        }
    }

    /// Extends `id`'s lease by `lease` while `token` holds it. False when the
    /// lease is gone or another worker's.
    pub fn renew(&self, id: RunId, token: &str, lease: Duration) -> Result<bool, String> {
        let renewed: i64 = self.with_connection(|connection| {
            Script::new(RENEW)
                .key(self.lease_key(&id.to_string()))
                .arg(token)
                .arg(millis(lease))
                .invoke(connection)
        })?;
        Ok(renewed == 1)
    }

    /// Acknowledges `id`: off processing, its delivery count gone, and its
    /// lease released if `token` holds it. Call it only once the run's log is
    /// terminal.
    pub fn ack(&self, id: RunId, token: &str) -> Result<(), String> {
        self.forget(&id.to_string(), token)
    }

    /// Moves every claimed run whose lease ran out back to the front of the
    /// queue, and returns them.
    pub fn reap(&self) -> Result<Vec<RunId>, String> {
        let moved: Vec<String> = self.with_connection(|connection| {
            Script::new(REAP)
                .key(&self.processing)
                .key(&self.runs)
                .arg(&self.lease_prefix)
                .invoke(connection)
        })?;
        Ok(moved.iter().filter_map(|text| parse(text).ok()).collect())
    }

    fn forget(&self, entry: &str, token: &str) -> Result<(), String> {
        self.with_connection(|connection| {
            Script::new(ACK)
                .key(&self.processing)
                .key(self.lease_key(entry))
                .key(&self.deliveries)
                .arg(entry)
                .arg(token)
                .invoke::<i64>(connection)
        })
        .map(|_| ())
    }

    fn lease_key(&self, entry: &str) -> String {
        format!("{}{entry}", self.lease_prefix)
    }

    fn list(&self, key: &str) -> Result<Vec<RunId>, String> {
        let values: Vec<String> =
            self.with_connection(|connection| connection.lrange(key, 0, -1))?;
        Ok(values.iter().filter_map(|text| parse(text).ok()).collect())
    }

    /// Runs `op` on the queue's connection, opening one first when there is
    /// none. Any error drops the connection, so the next call reconnects.
    fn with_connection<T>(
        &self,
        op: impl FnOnce(&mut redis::Connection) -> redis::RedisResult<T>,
    ) -> Result<T, String> {
        let mut slot = self.connection.lock().unwrap_or_else(|poisoned| {
            self.connection.clear_poison();
            let mut slot = poisoned.into_inner();
            *slot = None;
            slot
        });
        if slot.is_none() {
            *slot = Some(connect(&self.url).map_err(|error| error.to_string())?);
        }
        let Some(connection) = slot.as_mut() else {
            return Err("redis connection missing".to_string());
        };
        let result = op(connection);
        if result.is_err() {
            *slot = None;
        }
        result.map_err(|error| error.to_string())
    }
}

fn connect(url: &str) -> redis::RedisResult<redis::Connection> {
    let connection = redis::Client::open(url)?.get_connection_with_timeout(REDIS_TIMEOUT)?;
    connection.set_read_timeout(Some(REDIS_TIMEOUT))?;
    connection.set_write_timeout(Some(REDIS_TIMEOUT))?;
    Ok(connection)
}

fn parse(text: &str) -> Result<RunId, String> {
    text.parse().map_err(|error: uuid::Error| error.to_string())
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis())
        .unwrap_or(u64::MAX)
        .max(1)
}
