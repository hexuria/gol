//! Park and wake in the run queue (Phase 2.3, decision 34A). A run that
//! waits on an ask is parked: off processing, into the parked set with the
//! ask it waits on, its lease and its start count gone, so it holds no
//! worker and no waiting counts toward max_deliveries. Only the claim that
//! holds the lease can park it. A wake puts a parked run back on the runs
//! list, once. Needs Redis, as `queue_worker.rs` does.
use std::time::Duration;

use protocol::{MessageId, RunId};
use server::RedisRunQueue;

const REDIS_URL: &str = "redis://127.0.0.1/";

fn queue() -> RedisRunQueue {
    RedisRunQueue::with_key(REDIS_URL, format!("gol:test:{}", RunId::new()))
}

const LEASE: Duration = Duration::from_secs(30);

#[test]
fn a_parked_run_holds_no_lease_and_survives_the_reaper() {
    let queue = queue();
    let run = RunId::new();
    let ask = MessageId::new();
    queue.push(run).expect("push");
    assert_eq!(queue.claim("t1", LEASE).expect("claim"), Some(run));
    assert_eq!(queue.start(run), Ok(1));
    assert_eq!(queue.park(run, "t1", ask), Ok(true));
    assert_eq!(queue.processing().expect("processing"), []);
    assert_eq!(queue.queued().expect("queued"), []);
    assert_eq!(queue.parked().expect("parked"), [(run, ask)]);
    // Waiting does not count toward max_deliveries.
    assert_eq!(queue.starts(run), Ok(0));
    // No lease is left: renewing it fails, and the reaper finds nothing.
    assert_eq!(queue.renew(run, "t1", LEASE), Ok(false));
    assert_eq!(queue.reap().expect("reap"), []);
    assert_eq!(queue.parked().expect("parked"), [(run, ask)]);
}

// Only the claim that holds the lease parks: after the lease passed to
// another worker, the stale claim leaves the run with its new holder.
#[test]
fn a_claim_without_the_lease_does_not_park() {
    let queue = queue();
    let run = RunId::new();
    queue.push(run).expect("push");
    assert_eq!(queue.claim("t1", LEASE).expect("claim"), Some(run));
    assert_eq!(queue.park(run, "someone-else", MessageId::new()), Ok(false));
    assert_eq!(queue.processing().expect("processing"), [run]);
    assert_eq!(queue.parked().expect("parked"), []);
}

#[test]
fn a_wake_queues_a_parked_run_once() {
    let queue = queue();
    let run = RunId::new();
    queue.push(run).expect("push");
    queue.claim("t1", LEASE).expect("claim");
    queue.park(run, "t1", MessageId::new()).expect("park");
    assert_eq!(queue.wake(run), Ok(true));
    assert_eq!(queue.wake(run), Ok(false));
    assert_eq!(queue.queued().expect("queued"), [run]);
    assert_eq!(queue.parked().expect("parked"), []);
    // A run that is not parked is not woken onto the list.
    let other = RunId::new();
    assert_eq!(queue.wake(other), Ok(false));
    assert_eq!(queue.queued().expect("queued"), [run]);
}
