use rhai::{Engine, OptimizationLevel};
use runtime_tokio::Journal;
use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;
use workflow_core::{CounterBranch, History, WorkflowContext, WorkflowDriver, WorkflowStep};

struct Slot {
    step: Option<WorkflowStep>,
    journal: Option<Journal>,
    path: Option<String>,
}

pub fn run(source: &str) -> WorkflowStep {
    let slot = Rc::new(RefCell::new(Slot {
        step: None,
        journal: None,
        path: None,
    }));
    let mut engine = Engine::new();
    engine.set_optimization_level(OptimizationLevel::None);

    {
        let slot = Rc::clone(&slot);
        engine.register_fn("evaluate", move || {
            let path = slot.borrow().path.clone();
            let history = history_from_journal(path.as_deref());
            let step = CounterBranch.evaluate(&WorkflowContext, &history);
            slot.borrow_mut().step = Some(step);
        });
    }
    {
        let slot = Rc::clone(&slot);
        engine.register_fn("open", move |path: String| {
            let opened = Journal::open(Path::new(&path)).expect("open");
            let mut slot = slot.borrow_mut();
            slot.path = Some(path);
            slot.journal = Some(opened);
        });
    }
    {
        let slot = Rc::clone(&slot);
        engine.register_fn("commit", move || {
            slot.borrow_mut()
                .journal
                .as_mut()
                .expect("open")
                .commit(&0i64.to_le_bytes())
                .expect("commit");
        });
    }

    engine.run(source).expect("script");
    let step = slot.borrow_mut().step.take().expect("evaluate");
    step
}

fn history_from_journal(path: Option<&str>) -> History {
    let Some(path) = path else {
        return History { counter: None };
    };
    let bytes = std::fs::read(path).expect("read");
    match bytes.len() {
        0 => History { counter: None },
        8 => {
            let array: [u8; 8] = bytes.try_into().expect("journal length");
            History {
                counter: Some(i64::from_le_bytes(array)),
            }
        }
        _ => panic!("journal length"),
    }
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

        let dir = std::env::temp_dir().join(format!("gol-rhai-{}-commit", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("log");
        let script = format!(
            "open(\"{}\");\nevaluate();\ncommit();\nevaluate();\n",
            path.display()
        );
        let committed = run(&script);
        assert_eq!(committed.commands, [WorkflowCommand::Complete]);
        assert_eq!(committed.wait, WaitCondition::None);
        assert_eq!(std::fs::read(&path).unwrap(), 0i64.to_le_bytes());

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
