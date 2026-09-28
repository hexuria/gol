//! The Redis run queue (C4). `POST /v1/runs` pushes a run id onto the runs
//! list. A worker claims it: one script moves it to the processing list and
//! takes its lease, so no run is ever in processing without a lease. The
//! worker renews the lease while it runs the harness and acknowledges the
//! run only once the run's terminal event is stored. The reaper moves a run
//! whose lease ran out back onto the runs list. `formal/runqueue` models it.
use std::time::Duration;

use protocol::RunId;
use redis::{Commands, Script};

/// How long a claim is held, and how often it is renewed and reaped (owner
/// decision 2A for C4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueTiming {
    /// How long a lease lasts unless its worker renews it.
    pub lease: Duration,
    /// How often a working worker renews its lease.
    pub heartbeat: Duration,
    /// How often the reaper looks for runs whose lease ran out.
    pub reap_every: Duration,
    /// How long a worker waits before it looks again at an empty queue.
    pub idle_wait: Duration,
}

impl Default for QueueTiming {
    /// A 30 s lease, renewed every 10 s, reaped every 15 s: a crashed
    /// worker's run is back on the queue within about 45 s.
    fn default() -> Self {
        Self {
            lease: Duration::from_secs(30),
            heartbeat: Duration::from_secs(10),
            reap_every: Duration::from_secs(15),
            idle_wait: Duration::from_millis(500),
        }
    }
}

/// Moves the oldest run to processing and leases it to ARGV[2] for ARGV[3]
/// ms, in one step.
const CLAIM: &str = r"
local id = redis.call('LMOVE', KEYS[1], KEYS[2], 'RIGHT', 'LEFT')
if not id then return false end
redis.call('SET', ARGV[1] .. id, ARGV[2], 'PX', ARGV[3])
return id
";

/// Extends the lease KEYS[1] by ARGV[2] ms while ARGV[1] holds it.
const RENEW: &str = r"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
";

/// Removes ARGV[1] from processing, and its lease KEYS[2] if ARGV[2] holds it.
const ACK: &str = r"
redis.call('LREM', KEYS[1], 1, ARGV[1])
if redis.call('GET', KEYS[2]) == ARGV[2] then
  redis.call('DEL', KEYS[2])
end
return 1
";

/// Moves every run in processing whose lease is gone back onto the runs list.
const REAP: &str = r"
local moved = {}
for _, id in ipairs(redis.call('LRANGE', KEYS[1], 0, -1)) do
  if redis.call('EXISTS', ARGV[1] .. id) == 0 then
    redis.call('LREM', KEYS[1], 1, id)
    redis.call('LPUSH', KEYS[2], id)
    table.insert(moved, id)
  end
end
return moved
";

pub struct RedisRunQueue {
    url: String,
    runs: String,
    processing: String,
    lease_prefix: String,
}

impl RedisRunQueue {
    /// The server's queue, under `gol:runs`.
    pub fn open(url: impl Into<String>) -> Self {
        Self::with_key(url, "gol:runs")
    }

    /// A queue under `key`, with `key:processing` and `key:lease:<run>`.
    pub fn with_key(url: impl Into<String>, key: impl Into<String>) -> Self {
        let key = key.into();
        Self {
            url: url.into(),
            processing: format!("{key}:processing"),
            lease_prefix: format!("{key}:lease:"),
            runs: key,
        }
    }

    pub fn push(&self, id: RunId) -> Result<(), String> {
        let mut connection = self.connection()?;
        connection
            .lpush::<_, _, ()>(&self.runs, id.to_string())
            .map_err(|error| error.to_string())
    }

    /// Takes the oldest run off the queue without a lease. For inspection:
    /// a worker claims instead, so a crash cannot lose the run.
    pub fn pop(&self) -> Result<Option<RunId>, String> {
        let mut connection = self.connection()?;
        let value: Option<String> = connection
            .rpop(&self.runs, None::<std::num::NonZeroUsize>)
            .map_err(|error| error.to_string())?;
        value.map(|text| parse(&text)).transpose()
    }

    /// The runs waiting to be claimed, newest first.
    pub fn queued(&self) -> Result<Vec<RunId>, String> {
        self.list(&self.runs)
    }

    /// The runs claimed and not yet acknowledged, newest first.
    pub fn processing(&self) -> Result<Vec<RunId>, String> {
        self.list(&self.processing)
    }

    /// Claims the oldest run for `token` with a lease of `lease`.
    pub fn claim(&self, token: &str, lease: Duration) -> Result<Option<RunId>, String> {
        let mut connection = self.connection()?;
        let id: Option<String> = Script::new(CLAIM)
            .key(&self.runs)
            .key(&self.processing)
            .arg(&self.lease_prefix)
            .arg(token)
            .arg(millis(lease))
            .invoke(&mut connection)
            .map_err(|error| error.to_string())?;
        id.map(|text| parse(&text)).transpose()
    }

    /// Extends `id`'s lease by `lease` while `token` holds it. False when the
    /// lease is gone or another worker's.
    pub fn renew(&self, id: RunId, token: &str, lease: Duration) -> Result<bool, String> {
        let mut connection = self.connection()?;
        let renewed: i64 = Script::new(RENEW)
            .key(self.lease_key(id))
            .arg(token)
            .arg(millis(lease))
            .invoke(&mut connection)
            .map_err(|error| error.to_string())?;
        Ok(renewed == 1)
    }

    /// Acknowledges `id`: off processing, and its lease gone if `token` holds
    /// it. Call it only once the run's terminal event is stored.
    pub fn ack(&self, id: RunId, token: &str) -> Result<(), String> {
        let mut connection = self.connection()?;
        Script::new(ACK)
            .key(&self.processing)
            .key(self.lease_key(id))
            .arg(id.to_string())
            .arg(token)
            .invoke::<i64>(&mut connection)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    /// Moves every claimed run whose lease ran out back onto the queue, and
    /// returns them.
    pub fn reap(&self) -> Result<Vec<RunId>, String> {
        let mut connection = self.connection()?;
        let moved: Vec<String> = Script::new(REAP)
            .key(&self.processing)
            .key(&self.runs)
            .arg(&self.lease_prefix)
            .invoke(&mut connection)
            .map_err(|error| error.to_string())?;
        moved.iter().map(|text| parse(text)).collect()
    }

    fn lease_key(&self, id: RunId) -> String {
        format!("{}{id}", self.lease_prefix)
    }

    fn list(&self, key: &str) -> Result<Vec<RunId>, String> {
        let mut connection = self.connection()?;
        let values: Vec<String> = connection
            .lrange(key, 0, -1)
            .map_err(|error| error.to_string())?;
        values.iter().map(|text| parse(text)).collect()
    }

    fn connection(&self) -> Result<redis::Connection, String> {
        redis::Client::open(self.url.as_str())
            .map_err(|error| error.to_string())?
            .get_connection()
            .map_err(|error| error.to_string())
    }
}

fn parse(text: &str) -> Result<RunId, String> {
    text.parse().map_err(|error: uuid::Error| error.to_string())
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis())
        .unwrap_or(u64::MAX)
        .max(1)
}
