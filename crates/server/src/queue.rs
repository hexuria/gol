//! The Redis run queue (C4). `POST /v1/runs` pushes a run id onto the runs
//! list. A worker claims it: one script moves it to the processing list and
//! takes its lease, so no run is ever in processing without a lease. The
//! worker counts a start when it opens the run to execute it, renews the
//! lease while it runs the harness, and acknowledges the run only once the
//! run's terminal event is stored. The reaper moves a run whose lease ran out
//! back to the front of the runs list. `formal/runqueue` models it.
//!
//! This is standalone Redis: the client follows no cluster redirects. Every
//! key shares the hash tag `{<key>}`, and the scripts touch lease keys they
//! build from their arguments. Redis must keep what it is given:
//! `noeviction`, and AOF persistence so a restart does not drop the lists.
use std::sync::Mutex;
use std::time::{Duration, Instant};

use protocol::RunId;
use redis::{Commands, Script};

/// How long a claim is held and how often it is renewed and reaped (owner
/// decision 2A for C4), and how many starts a run gets before it is failed.
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
    /// Starts after which a run that never ends is failed instead of run
    /// again. Only a claim that opens the run to execute it counts; a claim
    /// that cannot load the run does not.
    pub max_deliveries: u32,
}

impl Default for QueueTiming {
    /// A 30 s lease, renewed every 10 s, reaped every 15 s: a crashed
    /// worker's run is back at the front of the queue within about 45 s. A
    /// run started five times without ending is failed.
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

/// Moves the oldest run to processing and leases it to ARGV[2] for ARGV[3]
/// ms, in one step. A run already leased (an id queued twice) is dropped from
/// processing instead.
const CLAIM: &str = r"
local id = redis.call('LMOVE', KEYS[1], KEYS[2], 'RIGHT', 'LEFT')
if not id then return false end
if not redis.call('SET', ARGV[1] .. id, ARGV[2], 'NX', 'PX', ARGV[3]) then
  redis.call('LREM', KEYS[2], 1, id)
  return false
end
return id
";

/// Counts a start of ARGV[1] and returns how many there have been.
const START: &str = r"
return redis.call('HINCRBY', KEYS[1], ARGV[1], 1)
";

/// Hands a claim back while ARGV[2] still holds its lease KEYS[2]: ARGV[1]
/// off processing, onto the back of the runs list, and the lease released.
/// A claim whose lease another worker holds now changes nothing: its entry
/// in processing is the new holder's.
const RELEASE: &str = r"
if redis.call('GET', KEYS[2]) == ARGV[2] then
  redis.call('LREM', KEYS[1], 1, ARGV[1])
  redis.call('LPUSH', KEYS[3], ARGV[1])
  redis.call('DEL', KEYS[2])
end
return 1
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

pub struct RedisRunQueue {
    url: String,
    runs: String,
    processing: String,
    deliveries: String,
    lease_prefix: String,
    /// A connection for the next caller, and when a connect last failed.
    slot: Mutex<Slot>,
}

/// The queue's idle connection. A caller takes it out while it works, so no
/// caller waits on another's command or connect.
#[derive(Default)]
struct Slot {
    idle: Option<redis::Connection>,
    failed_at: Option<Instant>,
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
            slot: Mutex::new(Slot::default()),
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

    /// Claims the oldest run for `token` with a lease of `lease`. `None` when
    /// the queue is empty, or when the oldest entry was a run already leased
    /// (an id queued twice), which is dropped. An entry that is not a run id
    /// is dropped, with an error.
    pub fn claim(&self, token: &str, lease: Duration) -> Result<Option<RunId>, String> {
        let claimed: Option<String> = self.with_connection(|connection| {
            Script::new(CLAIM)
                .key(&self.runs)
                .key(&self.processing)
                .arg(&self.lease_prefix)
                .arg(token)
                .arg(millis(lease))
                .invoke(connection)
        })?;
        let Some(text) = claimed else {
            return Ok(None);
        };
        match parse(&text) {
            Ok(run_id) => Ok(Some(run_id)),
            Err(error) => {
                self.forget(&text, token)?;
                Err(format!("dropped queue entry {text:?}: {error}"))
            }
        }
    }

    /// Counts a start of `id`, and returns how many there have been.
    pub fn start(&self, id: RunId) -> Result<u32, String> {
        self.with_connection(|connection| {
            Script::new(START)
                .key(&self.deliveries)
                .arg(id.to_string())
                .invoke(connection)
        })
    }

    /// Hands `id` back to the back of the queue, without waiting for its
    /// lease to run out, if `token` still holds the lease: for a claim that
    /// could not load or start its run. Behind the other runs, a run that
    /// cannot be loaded does not hold up the queue.
    pub fn release(&self, id: RunId, token: &str) -> Result<(), String> {
        self.with_connection(|connection| {
            Script::new(RELEASE)
                .key(&self.processing)
                .key(self.lease_key(&id.to_string()))
                .key(&self.runs)
                .arg(id.to_string())
                .arg(token)
                .invoke::<i64>(connection)
        })
        .map(|_| ())
    }

    /// How many starts `id` has had since its last acknowledgement.
    pub fn starts(&self, id: RunId) -> Result<u32, String> {
        let count: Option<u32> =
            self.with_connection(|connection| connection.hget(&self.deliveries, id.to_string()))?;
        Ok(count.unwrap_or(0))
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

    /// Runs `op` on the queue's idle connection, or on a new one. The lock is
    /// held only to take or return the idle connection, never across a
    /// command or a connect. For `REDIS_TIMEOUT` after a failed connect, calls
    /// fail at once instead of each waiting out a connect of their own. A
    /// connection that errs is dropped; one that works goes back for the next
    /// call.
    fn with_connection<T>(
        &self,
        op: impl FnOnce(&mut redis::Connection) -> redis::RedisResult<T>,
    ) -> Result<T, String> {
        let taken = {
            let mut slot = self.lock_slot();
            if slot
                .failed_at
                .is_some_and(|failed| failed.elapsed() < REDIS_TIMEOUT)
            {
                return Err("redis is unavailable: a connect failed moments ago".to_string());
            }
            slot.idle.take()
        };
        let mut connection = match taken {
            Some(connection) => connection,
            None => match connect(&self.url) {
                Ok(connection) => connection,
                Err(error) => {
                    self.lock_slot().failed_at = Some(Instant::now());
                    return Err(error.to_string());
                }
            },
        };
        let result = op(&mut connection);
        if result.is_ok() {
            let mut slot = self.lock_slot();
            slot.failed_at = None;
            slot.idle.get_or_insert(connection);
        }
        result.map_err(|error| error.to_string())
    }

    fn lock_slot(&self) -> std::sync::MutexGuard<'_, Slot> {
        self.slot.lock().unwrap_or_else(|poisoned| {
            self.slot.clear_poison();
            let mut slot = poisoned.into_inner();
            slot.idle = None;
            slot
        })
    }
}

/// A connection with read and write timeouts. The redis client bounds only
/// the TCP connect; its handshake then reads with no timeout, so the whole
/// connect runs on a helper thread with a deadline. On a Redis that accepts
/// and never answers, the helper waits on until the server gives up; the
/// caller does not.
fn connect(url: &str) -> redis::RedisResult<redis::Connection> {
    let client = redis::Client::open(url)?;
    let (sent, received) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("gol-redis-conn".to_string())
        .spawn(move || {
            let connection =
                client
                    .get_connection_with_timeout(REDIS_TIMEOUT)
                    .and_then(|connection| {
                        connection.set_read_timeout(Some(REDIS_TIMEOUT))?;
                        connection.set_write_timeout(Some(REDIS_TIMEOUT))?;
                        Ok(connection)
                    });
            let _ = sent.send(connection);
        })
        .map_err(redis::RedisError::from)?;
    received.recv_timeout(REDIS_TIMEOUT).unwrap_or_else(|_| {
        Err(redis::RedisError::from((
            redis::ErrorKind::IoError,
            "redis did not answer the connection handshake in time",
        )))
    })
}

fn parse(text: &str) -> Result<RunId, String> {
    text.parse().map_err(|error: uuid::Error| error.to_string())
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis())
        .unwrap_or(u64::MAX)
        .max(1)
}
