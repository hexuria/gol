//! The Redis run queue (C4). `POST /v1/runs` pushes a run id onto the runs
//! list. A worker claims it: one script moves it to the processing list and
//! takes its lease, so no run is ever in processing without a lease. The
//! worker counts a start when it opens the run to execute it, renews the
//! lease while it runs the harness, and acknowledges the run only once the
//! run's terminal event is stored. The reaper moves a run whose lease ran out
//! back to the front of the runs list. `formal/runqueue` models it.
//!
//! A run is pending from before its producer stores it until it is pushed
//! (C6): the push takes it off pending in the same step. A producer that dies
//! in between leaves it pending, and the sweep (`crate::worker::sweep`)
//! pushes it once it has been pending for `QueueTiming::sweep_after`.
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
    /// How long a run may stay pending before the sweep takes its producer
    /// for dead and pushes it (owner decision 2A for C6).
    pub sweep_after: Duration,
}

impl Default for QueueTiming {
    /// A 30 s lease, renewed every 10 s, reaped every 15 s: a crashed
    /// worker's run is back at the front of the queue within about 45 s. A
    /// run started five times without ending is failed. A run left pending
    /// for 60 s is pushed by the sweep, which runs with the reaper.
    fn default() -> Self {
        Self {
            lease: Duration::from_secs(30),
            heartbeat: Duration::from_secs(10),
            reap_every: Duration::from_secs(15),
            idle_wait: Duration::from_millis(500),
            max_backoff: Duration::from_secs(30),
            max_deliveries: 5,
            sweep_after: Duration::from_secs(60),
        }
    }
}

/// How long a connect, a read or a write to Redis may take.
const REDIS_TIMEOUT: Duration = Duration::from_secs(5);

/// How many idle connections a queue keeps for its next callers (owner
/// decision 3A for C6).
const IDLE_CONNECTIONS: usize = 4;

/// Marks ARGV[1] pending, scored with the Redis clock in ms.
const PEND: &str = r"
local now = redis.call('TIME')
redis.call('ZADD', KEYS[1], now[1] * 1000 + math.floor(now[2] / 1000), ARGV[1])
return 1
";

/// Queues ARGV[1] on the runs list and takes it off pending, in one step.
const PUSH: &str = r"
redis.call('LPUSH', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
";

/// The entries pending for at least ARGV[1] ms, by the Redis clock.
const PENDING_FOR: &str = r"
local now = redis.call('TIME')
local cutoff = now[1] * 1000 + math.floor(now[2] / 1000) - tonumber(ARGV[1])
return redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', cutoff)
";

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
    pending: String,
    lease_prefix: String,
    /// Idle connections for the next callers, and how connecting has gone.
    slot: Mutex<Slot>,
}

/// The queue's idle connections. A caller takes one out while it works, so
/// no caller waits on another's command or connect.
#[derive(Default)]
struct Slot {
    idle: Vec<redis::Connection>,
    /// When a connect last failed, until one works.
    failed_at: Option<Instant>,
    /// A connect after a failure is under way: other callers fail at once
    /// instead of each waiting out a connect of their own.
    probing: bool,
}

impl RedisRunQueue {
    /// The server's queue, under `{gol:runs}`.
    pub fn open(url: impl Into<String>) -> Self {
        Self::with_key(url, "gol:runs")
    }

    /// A queue under `{key}`, with `{key}:processing`, `{key}:deliveries`,
    /// `{key}:pending` and `{key}:lease:<run>`.
    pub fn with_key(url: impl Into<String>, key: impl Into<String>) -> Self {
        let tag = format!("{{{}}}", key.into());
        Self {
            url: url.into(),
            processing: format!("{tag}:processing"),
            deliveries: format!("{tag}:deliveries"),
            pending: format!("{tag}:pending"),
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

    /// Marks `id` pending: its producer is about to store it and push it.
    pub fn pend(&self, id: RunId) -> Result<(), String> {
        self.with_connection(|connection| {
            Script::new(PEND)
                .key(&self.pending)
                .arg(id.to_string())
                .invoke::<i64>(connection)
        })
        .map(|_| ())
    }

    /// Queues `id` and takes it off pending, in one step.
    pub fn push(&self, id: RunId) -> Result<(), String> {
        self.with_connection(|connection| {
            Script::new(PUSH)
                .key(&self.runs)
                .key(&self.pending)
                .arg(id.to_string())
                .invoke::<i64>(connection)
        })
        .map(|_| ())
    }

    /// The pending runs, longest pending first. Entries that are not run ids
    /// are left out.
    pub fn pending(&self) -> Result<Vec<RunId>, String> {
        let values: Vec<String> =
            self.with_connection(|connection| connection.zrange(&self.pending, 0, -1))?;
        Ok(values.iter().filter_map(|text| parse(text).ok()).collect())
    }

    /// The runs pending for at least `age`, longest pending first. An entry
    /// that is not a run id is dropped.
    pub fn pending_for(&self, age: Duration) -> Result<Vec<RunId>, String> {
        let values: Vec<String> = self.with_connection(|connection| {
            Script::new(PENDING_FOR)
                .key(&self.pending)
                .arg(u64::try_from(age.as_millis()).unwrap_or(u64::MAX))
                .invoke(connection)
        })?;
        let mut ids = Vec::new();
        for text in values {
            match parse(&text) {
                Ok(id) => ids.push(id),
                Err(_) => self.unpend_entry(&text)?,
            }
        }
        Ok(ids)
    }

    /// Takes `id` off pending without queueing it.
    pub fn unpend(&self, id: RunId) -> Result<(), String> {
        self.unpend_entry(&id.to_string())
    }

    fn unpend_entry(&self, entry: &str) -> Result<(), String> {
        self.with_connection(|connection| connection.zrem::<_, _, i64>(&self.pending, entry))
            .map(|_| ())
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

    /// Runs `op` on an idle connection, or on a new one. The lock is held
    /// only to take or return a connection, never across a command or a
    /// connect. An idle connection is used whenever there is one. Without
    /// one, for `REDIS_TIMEOUT` after a failed connect calls fail at once;
    /// after that one caller connects again while the others still fail at
    /// once, so a Redis that is down costs one wait, not one per caller. A
    /// connection that errs is dropped; one that works goes back for the next
    /// call, up to `IDLE_CONNECTIONS` of them.
    fn with_connection<T>(
        &self,
        op: impl FnOnce(&mut redis::Connection) -> redis::RedisResult<T>,
    ) -> Result<T, String> {
        let taken = {
            let mut slot = self.lock_slot();
            match slot.idle.pop() {
                Some(connection) => Some(connection),
                None if slot.probing => {
                    return Err("redis is unavailable: a connect is being retried".to_string());
                }
                None if slot
                    .failed_at
                    .is_some_and(|failed| failed.elapsed() < REDIS_TIMEOUT) =>
                {
                    return Err("redis is unavailable: a connect failed moments ago".to_string());
                }
                None => {
                    slot.probing = slot.failed_at.is_some();
                    None
                }
            }
        };
        let mut connection = match taken {
            Some(connection) => connection,
            None => {
                let connected = connect(&self.url);
                let mut slot = self.lock_slot();
                slot.probing = false;
                match connected {
                    Ok(connection) => {
                        slot.failed_at = None;
                        connection
                    }
                    Err(error) => {
                        slot.failed_at = Some(Instant::now());
                        return Err(error.to_string());
                    }
                }
            }
        };
        let result = op(&mut connection);
        if result.is_ok() {
            let mut slot = self.lock_slot();
            slot.failed_at = None;
            if slot.idle.len() < IDLE_CONNECTIONS {
                slot.idle.push(connection);
            }
        }
        result.map_err(|error| error.to_string())
    }

    fn lock_slot(&self) -> std::sync::MutexGuard<'_, Slot> {
        self.slot.lock().unwrap_or_else(|poisoned| {
            self.slot.clear_poison();
            let mut slot = poisoned.into_inner();
            slot.idle.clear();
            slot.probing = false;
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

#[cfg(test)]
mod tests {
    use super::{RedisRunQueue, REDIS_TIMEOUT};
    use std::collections::BTreeSet;
    use std::net::TcpListener;
    use std::sync::{Arc, Barrier, Mutex};
    use std::time::{Duration, Instant};

    /// The Redis client ids of the connections that `callers` concurrent
    /// calls use, each holding its connection until all of them have one.
    fn client_ids(queue: &Arc<RedisRunQueue>, callers: usize) -> BTreeSet<i64> {
        let barrier = Arc::new(Barrier::new(callers));
        let handles: Vec<_> = (0..callers)
            .map(|_| {
                let queue = queue.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    queue
                        .with_connection(|connection| {
                            barrier.wait();
                            redis::cmd("CLIENT").arg("ID").query::<i64>(connection)
                        })
                        .expect("client id")
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("caller"))
            .collect()
    }

    // Four idle connections are kept for the next callers, and no more.
    #[test]
    fn four_idle_connections_serve_four_concurrent_callers() {
        let queue = Arc::new(RedisRunQueue::with_key(
            "redis://127.0.0.1/",
            "gol:test:pool",
        ));
        let first = client_ids(&queue, 4);
        assert_eq!(first.len(), 4);
        assert_eq!(client_ids(&queue, 4), first);
        let five = client_ids(&queue, 5);
        assert_eq!(five.len(), 5);
        assert!(first.is_subset(&five));
        let again = client_ids(&queue, 5);
        assert_eq!(
            again.intersection(&five).count(),
            4,
            "only four idle connections are kept"
        );
    }

    // After a failed connect, once the fail-fast window is over, one caller
    // tries Redis again while the others fail at once, instead of each
    // waiting out a connect of its own.
    #[test]
    fn one_probe_connects_while_the_others_fail_fast() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        // Refuse (close at once) until `hold`; then accept and never answer.
        let hold = Arc::new(Mutex::new(false));
        let held = Arc::new(Mutex::new(Vec::new()));
        {
            let hold = hold.clone();
            let held = held.clone();
            std::thread::spawn(move || {
                for stream in listener.incoming().flatten() {
                    if *hold.lock().expect("hold") {
                        held.lock().expect("held").push(stream);
                    }
                }
            });
        }
        let queue = Arc::new(RedisRunQueue::with_key(
            format!("redis://127.0.0.1:{port}/1"),
            "gol:test:probe",
        ));
        assert!(queue.ping().is_err(), "a refused connect fails");
        let failed = Instant::now();
        *hold.lock().expect("hold") = true;
        // Inside the window a call fails at once, without a connect that
        // would wait out REDIS_TIMEOUT on a Redis that never answers.
        let started = Instant::now();
        assert!(queue.ping().is_err());
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "waited {:?} inside the fail-fast window",
            started.elapsed()
        );
        std::thread::sleep(
            (REDIS_TIMEOUT + Duration::from_millis(100)).saturating_sub(failed.elapsed()),
        );
        let handles: Vec<_> = (0..3)
            .map(|_| {
                let queue = queue.clone();
                std::thread::spawn(move || {
                    let started = Instant::now();
                    let answer = queue.ping();
                    (answer.is_err(), started.elapsed())
                })
            })
            .collect();
        let calls: Vec<(bool, Duration)> = handles
            .into_iter()
            .map(|handle| handle.join().expect("caller"))
            .collect();
        assert!(calls.iter().all(|(failed, _)| *failed), "{calls:?}");
        let waited = calls
            .iter()
            .filter(|(_, took)| *took >= REDIS_TIMEOUT - Duration::from_millis(500))
            .count();
        let fast = calls
            .iter()
            .filter(|(_, took)| *took < Duration::from_secs(1))
            .count();
        assert_eq!((waited, fast), (1, 2), "{calls:?}");
    }
}
