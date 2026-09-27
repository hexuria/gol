use crate::driver::History;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EffectId {
    pub workflow: WorkflowRunId,
    pub path: Path,
    pub sequence: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkflowRunId(pub &'static str);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Path {
    Unrecorded,
    Zero,
    Nonzero,
}

/// Which `on_counter` arm a counter output selects. An output other than
/// "0", even one that is not a number, is `Nonzero`.
pub(crate) fn path(counter: Option<&str>) -> Path {
    match counter {
        None => Path::Unrecorded,
        Some("0") => Path::Zero,
        Some(_) => Path::Nonzero,
    }
}

pub fn effect_id(workflow: WorkflowRunId, history: &History, sequence: u32) -> EffectId {
    EffectId {
        workflow,
        path: path(history.counter()),
        sequence,
    }
}
