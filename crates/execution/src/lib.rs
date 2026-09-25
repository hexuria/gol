#![forbid(unsafe_code)]
use std::thread::ThreadId;

use harness::{
    run_to_completion, DeciderError, Driver, EchoTool, InMemory, ScriptedDecider, UnavailableModel,
};
use protocol::{Event, ExecutionPlacement, RunSpec};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecError {
    WrongPlacement(ExecutionPlacement),
    Decider(String),
    WorkerStopped,
}

pub struct PlacedRun {
    pub events: Vec<Event>,
    pub worker_thread: ThreadId,
}

pub fn run_reverse(spec: RunSpec, decider: ScriptedDecider) -> Result<PlacedRun, ExecError> {
    run_on_worker(spec, ExecutionPlacement::Reverse, decider)
}

pub fn run_box(spec: RunSpec, decider: ScriptedDecider) -> Result<PlacedRun, ExecError> {
    run_on_worker(spec, ExecutionPlacement::Box, decider)
}

fn run_on_worker(
    spec: RunSpec,
    placement: ExecutionPlacement,
    mut decider: ScriptedDecider,
) -> Result<PlacedRun, ExecError> {
    if spec.placement != placement {
        return Err(ExecError::WrongPlacement(spec.placement));
    }
    std::thread::spawn(move || {
        let worker_thread = std::thread::current().id();
        let mut driver = Driver::boot(spec).expect("placement boots");
        let echo = EchoTool;
        let tools: [&dyn harness::Tool; 1] = [&echo];
        run_to_completion(
            &mut driver,
            &mut decider,
            &tools,
            &UnavailableModel,
            &mut InMemory::default(),
        )
        .map_err(|error: DeciderError| ExecError::Decider(error.message))?;
        Ok(PlacedRun {
            events: driver.events().to_vec(),
            worker_thread,
        })
    })
    .join()
    .unwrap_or(Err(ExecError::WorkerStopped))
}
