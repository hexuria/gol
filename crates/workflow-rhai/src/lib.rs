use rhai::{Engine, EvalAltResult, OptimizationLevel, Position};
use std::cell::RefCell;
use std::rc::Rc;
use workflow_core::{Decision, ToolName, WorkflowProgram};

#[derive(Debug)]
pub enum FrontendError {
    Script(String),
    InvalidProgram(String),
}

impl std::fmt::Display for FrontendError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrontendError::Script(message) | FrontendError::InvalidProgram(message) => {
                formatter.write_str(message)
            }
        }
    }
}

impl std::error::Error for FrontendError {}

struct Builder {
    stack: Vec<Decision>,
    fault: Option<String>,
}

pub fn compile(source: &str) -> Result<WorkflowProgram, FrontendError> {
    let builder = Rc::new(RefCell::new(Builder {
        stack: Vec::new(),
        fault: None,
    }));
    let mut engine = Engine::new();
    engine.set_optimization_level(OptimizationLevel::None);
    register(&mut engine, &builder);

    let evaluated = engine.run(source);
    let built = builder.borrow();
    if let Some(message) = built.fault.clone() {
        return Err(FrontendError::InvalidProgram(message));
    }
    if let Err(error) = evaluated {
        return Err(FrontendError::Script(error.to_string()));
    }
    match built.stack.as_slice() {
        [decision] => Ok(WorkflowProgram {
            root: decision.clone(),
        }),
        _ => Err(FrontendError::InvalidProgram(
            "script must record one decision".to_string(),
        )),
    }
}

fn register(engine: &mut Engine, builder: &Rc<RefCell<Builder>>) {
    {
        let builder = Rc::clone(builder);
        engine.register_fn(
            "on_counter",
            move |_: (), _: (), _: ()| -> Result<(), Box<EvalAltResult>> {
                let mut built = builder.borrow_mut();
                let Some(other) = built.stack.pop() else {
                    return fault(&mut built, "missing decision");
                };
                let Some(zero) = built.stack.pop() else {
                    return fault(&mut built, "missing decision");
                };
                let Some(missing) = built.stack.pop() else {
                    return fault(&mut built, "missing decision");
                };
                built.stack.push(Decision::OnCounter {
                    missing: Box::new(missing),
                    zero: Box::new(zero),
                    other: Box::new(other),
                });
                Ok(())
            },
        );
    }
    {
        let builder = Rc::clone(builder);
        engine.register_fn(
            "tool",
            move |name: String| -> Result<(), Box<EvalAltResult>> {
                if name != "counter" {
                    return fault(&mut builder.borrow_mut(), "unknown tool");
                }
                builder
                    .borrow_mut()
                    .stack
                    .push(Decision::Tool(ToolName::Counter));
                Ok(())
            },
        );
    }
    {
        let builder = Rc::clone(builder);
        engine.register_fn("complete", move || {
            builder.borrow_mut().stack.push(Decision::Complete);
        });
    }
    {
        let builder = Rc::clone(builder);
        engine.register_fn("fail", move || {
            builder.borrow_mut().stack.push(Decision::Fail);
        });
    }
}

fn fault(builder: &mut Builder, message: &str) -> Result<(), Box<EvalAltResult>> {
    builder.fault = Some(message.to_string());
    Err(EvalAltResult::ErrorRuntime(message.into(), Position::NONE).into())
}

#[cfg(test)]
mod tests {
    use super::compile;
    use workflow_core::{
        evaluate_program, Decision, History, ToolName, ToolSpec, WaitCondition, WorkflowCommand,
    };

    #[test]
    fn compiles_the_counter_program() {
        let source = include_str!("../counter.rhai");
        assert_eq!(
            source,
            "on_counter(tool(\"counter\"), complete(), fail());\n"
        );
        let program = compile(source).unwrap();
        assert_eq!(
            program.root,
            Decision::OnCounter {
                missing: Box::new(Decision::Tool(ToolName::Counter)),
                zero: Box::new(Decision::Complete),
                other: Box::new(Decision::Fail),
            }
        );

        let unrecorded = evaluate_program(&program, &History { counter: None });
        assert_eq!(
            unrecorded.commands,
            [WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" })]
        );
        assert_eq!(unrecorded.wait, WaitCondition::None);

        let zero = evaluate_program(&program, &History { counter: Some(0) });
        assert_eq!(zero.commands, [WorkflowCommand::Complete]);
        assert_eq!(zero.wait, WaitCondition::None);

        let other = evaluate_program(&program, &History { counter: Some(1) });
        assert_eq!(other.commands, [WorkflowCommand::Fail]);
        assert_eq!(other.wait, WaitCondition::None);

        assert_eq!(
            compile("let x = 1;\n").unwrap_err().to_string(),
            "script must record one decision"
        );
        assert_eq!(
            compile("tool(\"nope\");\n").unwrap_err().to_string(),
            "unknown tool"
        );
    }
}
