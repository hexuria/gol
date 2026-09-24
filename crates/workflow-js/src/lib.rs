use boa_engine::{js_string, Context, JsResult, JsValue, NativeFunction, Source};
use runtime_tokio::Journal;
use std::path::Path;
use workflow_core::{CounterBranch, History, WorkflowContext, WorkflowDriver, WorkflowStep};

struct Slot {
    step: Option<WorkflowStep>,
    journal: Option<Journal>,
    path: Option<String>,
}

pub fn run(source: &str) -> WorkflowStep {
    let mut context = Context::default();
    context.insert_data(Slot {
        step: None,
        journal: None,
        path: None,
    });
    context
        .register_global_callable(
            js_string!("evaluate"),
            0,
            NativeFunction::from_fn_ptr(evaluate),
        )
        .expect("evaluate");
    context
        .register_global_callable(js_string!("open"), 1, NativeFunction::from_fn_ptr(open))
        .expect("open");
    context
        .register_global_callable(js_string!("commit"), 0, NativeFunction::from_fn_ptr(commit))
        .expect("commit");

    context.eval(Source::from_bytes(source)).expect("script");
    context
        .remove_data::<Slot>()
        .expect("evaluate")
        .step
        .expect("evaluate")
}

fn evaluate(_this: &JsValue, _args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = context.get_data::<Slot>().expect("evaluate").path.clone();
    let history = history_from_journal(path.as_deref());
    let mut slot = context.remove_data::<Slot>().expect("evaluate");
    slot.step = Some(CounterBranch.evaluate(&WorkflowContext, &history));
    context.insert_data(*slot);
    Ok(JsValue::undefined())
}

fn open(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let path = args
        .first()
        .and_then(JsValue::as_string)
        .expect("open")
        .to_std_string()
        .expect("open");
    let opened = Journal::open(Path::new(&path)).expect("open");
    let mut slot = context.remove_data::<Slot>().expect("open");
    slot.path = Some(path);
    slot.journal = Some(opened);
    context.insert_data(*slot);
    Ok(JsValue::undefined())
}

fn commit(_this: &JsValue, _args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let mut slot = context.remove_data::<Slot>().expect("open");
    slot.journal
        .as_mut()
        .expect("open")
        .commit(&0i64.to_le_bytes())
        .expect("commit");
    context.insert_data(*slot);
    Ok(JsValue::undefined())
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
        let source = include_str!("../counter.js");
        assert_eq!(source, "evaluate();\n");
        let step = run(source);
        assert_eq!(
            step.commands,
            [WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" })]
        );
        assert_eq!(step.wait, WaitCondition::None);

        let dir = std::env::temp_dir().join(format!("gol-js-{}-commit", std::process::id()));
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
        let forbidden = [
            "boa_engine",
            "boa_ast",
            "boa_gc",
            "boa_interner",
            "boa_macros",
            "boa_parser",
            "boa_string",
            "js_string",
            "JsValue",
            "JsString",
            "JsResult",
            "NativeFunction",
            "Source",
            "JsObject",
            "FunctionObjectBuilder",
            "HostDefined",
        ];
        for file in files {
            let text = std::fs::read_to_string(root.join(file)).unwrap();
            for word in forbidden {
                assert!(!text.contains(word), "{file} contains {word}");
            }
        }
    }
}
