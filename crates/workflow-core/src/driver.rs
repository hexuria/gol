use crate::step::WorkflowStep;

pub trait WorkflowDriver {
    fn evaluate(&self, ctx: &WorkflowContext, history: &History) -> WorkflowStep;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkflowContext;

/// What a workflow run has recorded so far, oldest first.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct History {
    pub records: Vec<Record>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Record {
    Tool { name: String, output: String },
    AgentSpawned { agent: String },
}

impl Record {
    /// The record a `counter` tool call leaves: its value in decimal.
    pub fn counter(value: i64) -> Self {
        Record::Tool {
            name: "counter".to_string(),
            output: value.to_string(),
        }
    }
}

impl History {
    pub fn new(records: Vec<Record>) -> Self {
        Self { records }
    }

    /// The output of the last `counter` tool record, if there is one.
    pub fn counter(&self) -> Option<&str> {
        self.records.iter().rev().find_map(|record| match record {
            Record::Tool { name, output } if name == "counter" => Some(output.as_str()),
            _ => None,
        })
    }
}
