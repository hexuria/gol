use rhai::{Engine, OptimizationLevel};
use runtime_tokio::Journal;
use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;
use workflow_core::{CounterBranch, History, WorkflowContext, WorkflowDriver, WorkflowStep};

struct Slot {
    step: Option<WorkflowStep>,
    journal: Option<Journal>,
}

pub fn run(source: &str) -> WorkflowStep {
    let slot = Rc::new(RefCell::new(Slot {
        step: None,
        journal: None,
    }));
    let mut engine = Engine::new();
    engine.set_optimization_level(OptimizationLevel::None);

    {
        let slot = Rc::clone(&slot);
        engine.register_fn("evaluate", move || {
            let step = CounterBranch.evaluate(&WorkflowContext, &History { counter: None });
            slot.borrow_mut().step = Some(step);
        });
    }
    {
        let slot = Rc::clone(&slot);
        engine.register_fn("open", move |path: String| {
            let opened = Journal::open(Path::new(&path)).expect("open");
            slot.borrow_mut().journal = Some(opened);
        });
    }
    {
        let slot = Rc::clone(&slot);
        engine.register_fn("commit", move || {
            slot.borrow_mut()
                .journal
                .as_mut()
                .expect("open")
                .commit(&[])
                .expect("commit");
        });
    }

    engine.run(source).expect("script");
    let step = slot.borrow_mut().step.take().expect("evaluate");
    step
}

#[cfg(test)]
mod tests {
    use super::run;
    use workflow_core::{ToolSpec, WaitCondition, WorkflowCommand};

    #[test]
    fn script_reaches_evaluate() {
        let source = include_str!("../counter.rhai");
        assert_eq!(source, "evaluate();\n");
        let step = run(source);
        assert_eq!(
            step.commands,
            [WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" })]
        );
        assert_eq!(step.wait, WaitCondition::None);

        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../workflow-core");
        let files = [
            "Cargo.toml",
            "src/lib.rs",
            "src/driver.rs",
            "src/step.rs",
            "src/program.rs",
            "src/id.rs",
        ];
        let forbidden = ["rhai", "Engine", "AST", "Scope", "Dynamic", "Map", "FnPtr"];
        for file in files {
            let text = std::fs::read_to_string(root.join(file)).unwrap();
            for word in forbidden {
                assert!(!text.contains(word), "{file} contains {word}");
            }
        }
    }
}
