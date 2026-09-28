//! Queue workers and the reaper (C4): threads in the server process, started
//! when `GOL_REDIS_URL` is set (owner decision 1A).
use std::collections::BTreeMap;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::Arc;

use harness::{Memory, RunMemory};
use protocol::{Event, RunId};

use crate::http::harness_events;
use crate::inference::dispatch_events;
use crate::queue::{QueueTiming, RedisRunQueue};
use crate::store::{is_terminal, Append, RunStore, StoredRun};

/// Takes runs off the queue and runs them to their end.
pub struct Worker {
    queue: RedisRunQueue,
    store: Arc<dyn RunStore>,
    memory: Arc<dyn Memory>,
    jev_base_url: String,
    timing: QueueTiming,
}

/// A run this worker claimed, with the token its lease holds.
pub struct Claim<'a> {
    worker: &'a Worker,
    run_id: RunId,
    token: String,
}

impl Worker {
    pub fn new(
        queue: RedisRunQueue,
        store: Arc<dyn RunStore>,
        memory: Arc<dyn Memory>,
        jev_base_url: impl Into<String>,
        timing: QueueTiming,
    ) -> Self {
        Self {
            queue,
            store,
            memory,
            jev_base_url: jev_base_url.into(),
            timing,
        }
    }

    /// Claims the oldest queued run, with its lease, or nothing when the
    /// queue is empty.
    pub fn claim(&self) -> Result<Option<Claim<'_>>, String> {
        let token = uuid::Uuid::new_v4().to_string();
        Ok(self
            .queue
            .claim(&token, self.timing.lease)?
            .map(|run_id| Claim {
                worker: self,
                run_id,
                token,
            }))
    }

    /// Claims one run and takes it to its end: runs it unless its log has
    /// already ended (owner decision 3A), records what the harness did, and
    /// only then acknowledges it. `None` when the queue was empty. A failure
    /// before the acknowledgement leaves the claim to expire, and the reaper
    /// hands the run back.
    pub fn work_one(&self) -> Result<Option<RunId>, String> {
        let Some(claim) = self.claim()? else {
            return Ok(None);
        };
        if let Some(stored) = claim.prepare()? {
            let events = claim.execute(&stored);
            claim.record(events)?;
        }
        claim.ack()?;
        Ok(Some(claim.run_id))
    }

    /// Works the queue for as long as the process runs.
    pub fn work_forever(&self) {
        loop {
            match self.work_one() {
                Ok(Some(_)) => {}
                Ok(None) => std::thread::sleep(self.timing.idle_wait),
                Err(error) => {
                    eprintln!("gol: queue worker: {error}");
                    std::thread::sleep(self.timing.idle_wait);
                }
            }
        }
    }
}

impl Claim<'_> {
    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    /// The run to execute, or `None` when there is nothing to run: its log
    /// already ended, or it is not stored.
    pub fn prepare(&self) -> Result<Option<StoredRun>, String> {
        let stored = self
            .worker
            .store
            .run(self.run_id)
            .map_err(|error| error.to_string())?;
        Ok(stored.filter(|run| !run.events.iter().any(|event| is_terminal(&event.payload))))
    }

    /// Runs the harness with Jev, renewing the lease every heartbeat, and
    /// returns what to record: scheduled, provisioning and starting, then the
    /// harness's events, which end the run (with `RunFailed` if the harness
    /// could not finish).
    pub fn execute(&self, stored: &StoredRun) -> Vec<Event> {
        let worker = self.worker;
        std::thread::scope(|scope| {
            let (stop, stopped) = mpsc::channel::<()>();
            scope.spawn(move || self.heartbeat(&stopped));
            let mut events = dispatch_events(&stored.spec);
            let memory = RunMemory::new(worker.memory.as_ref());
            let (harness, _outcome) = harness_events(&worker.jev_base_url, &stored.spec, &memory);
            events.extend(harness);
            drop(stop);
            events
        })
    }

    /// Appends `events`. The store refuses them once the log is terminal, so
    /// two workers on one run leave one terminal event.
    pub fn record(&self, events: Vec<Event>) -> Result<Append, String> {
        self.worker
            .store
            .append_events(self.run_id, events)
            .map_err(|error| error.to_string())
    }

    /// Acknowledges the run: off the processing list, and its lease released
    /// if this claim still holds it. Call it only once the run's log is
    /// terminal.
    pub fn ack(&self) -> Result<(), String> {
        self.worker.queue.ack(self.run_id, &self.token)
    }

    /// Renews the lease every heartbeat until `stopped` closes.
    fn heartbeat(&self, stopped: &mpsc::Receiver<()>) {
        let timing = self.worker.timing;
        while let Err(RecvTimeoutError::Timeout) = stopped.recv_timeout(timing.heartbeat) {
            match self
                .worker
                .queue
                .renew(self.run_id, &self.token, timing.lease)
            {
                Ok(true) => {}
                // Another worker has the run now; the store keeps one terminal
                // event whichever records first.
                Ok(false) => eprintln!("gol: queue worker: lost the lease on run {}", self.run_id),
                Err(error) => eprintln!("gol: queue worker: renew run {}: {error}", self.run_id),
            }
        }
    }
}

/// Hands back runs whose lease ran out, every `reap_every`, for as long as
/// the process runs.
pub fn reap_forever(queue: &RedisRunQueue, timing: QueueTiming) {
    loop {
        if let Err(error) = queue.reap() {
            eprintln!("gol: queue reaper: {error}");
        }
        std::thread::sleep(timing.reap_every);
    }
}

/// The queue the server runs, from its environment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueSettings {
    pub redis_url: String,
    pub workers: usize,
}

/// `GOL_REDIS_URL` (unset or empty: no queue) with `GOL_WORKERS` worker
/// threads, 2 by default (owner decision 1A for C4).
pub fn queue_from_env(env: &BTreeMap<String, String>) -> Result<Option<QueueSettings>, String> {
    let Some(redis_url) = env.get("GOL_REDIS_URL").filter(|url| !url.is_empty()) else {
        return Ok(None);
    };
    let workers = match env.get("GOL_WORKERS").filter(|count| !count.is_empty()) {
        None => 2,
        Some(count) => count
            .parse::<usize>()
            .ok()
            .filter(|count| *count > 0)
            .ok_or_else(|| format!("GOL_WORKERS must be a positive count, not {count:?}"))?,
    };
    Ok(Some(QueueSettings {
        redis_url: redis_url.clone(),
        workers,
    }))
}

/// Starts `settings.workers` worker threads and one reaper, which run for as
/// long as the process does.
pub fn start_queue(
    settings: &QueueSettings,
    store: Arc<dyn RunStore>,
    memory: Arc<dyn Memory>,
    jev_base_url: &str,
) {
    let timing = QueueTiming::default();
    for _ in 0..settings.workers {
        let worker = Worker::new(
            RedisRunQueue::open(&settings.redis_url),
            store.clone(),
            memory.clone(),
            jev_base_url,
            timing,
        );
        std::thread::spawn(move || worker.work_forever());
    }
    let reaper = RedisRunQueue::open(&settings.redis_url);
    std::thread::spawn(move || reap_forever(&reaper, timing));
}
