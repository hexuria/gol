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

pub fn effect_id(workflow: WorkflowRunId, history: &History, sequence: u32) -> EffectId {
    let path = match history.counter {
        None => Path::Unrecorded,
        Some(0) => Path::Zero,
        Some(_) => Path::Nonzero,
    };
    EffectId {
        workflow,
        path,
        sequence,
    }
}
