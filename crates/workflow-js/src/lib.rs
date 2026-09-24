use boa_engine::{
    js_string, Context, JsError, JsNativeError, JsResult, JsValue, NativeFunction, Source,
};
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
    let mut context = Context::default();
    context.insert_data(Builder {
        stack: Vec::new(),
        fault: None,
    });
    register(&mut context, "onCounter", 3, on_counter);
    register(&mut context, "tool", 1, tool);
    register(&mut context, "complete", 0, complete);
    register(&mut context, "fail", 0, fail);

    let evaluated = context.eval(Source::from_bytes(source));
    let builder = context
        .remove_data::<Builder>()
        .ok_or_else(|| FrontendError::InvalidProgram("compiler state missing".to_string()))?;
    if let Some(message) = builder.fault {
        return Err(FrontendError::InvalidProgram(message));
    }
    if let Err(error) = evaluated {
        return Err(FrontendError::Script(error.to_string()));
    }
    match builder.stack.as_slice() {
        [decision] => Ok(WorkflowProgram {
            root: decision.clone(),
        }),
        _ => Err(FrontendError::InvalidProgram(
            "script must record one decision".to_string(),
        )),
    }
}

fn register(
    context: &mut Context,
    name: &str,
    length: usize,
    body: fn(&JsValue, &[JsValue], &mut Context) -> JsResult<JsValue>,
) {
    context
        .register_global_callable(js_string!(name), length, NativeFunction::from_fn_ptr(body))
        .unwrap_or_else(|_| panic!("register {name}"));
}

fn on_counter(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    if args.len() != 3 {
        return fault(context, "onCounter takes three decisions");
    }
    let other = pop(context)?;
    let zero = pop(context)?;
    let missing = pop(context)?;
    push(
        context,
        Decision::OnCounter {
            missing: Box::new(missing),
            zero: Box::new(zero),
            other: Box::new(other),
        },
    )
}

fn tool(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let Some(name) = args
        .first()
        .and_then(JsValue::as_string)
        .and_then(|value| value.to_std_string().ok())
    else {
        return fault(context, "tool name must be a string");
    };
    if name != "counter" {
        return fault(context, "unknown tool");
    }
    push(context, Decision::Tool(ToolName::Counter))
}

fn complete(_this: &JsValue, _args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    push(context, Decision::Complete)
}

fn fail(_this: &JsValue, _args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    push(context, Decision::Fail)
}

fn push(context: &mut Context, decision: Decision) -> JsResult<JsValue> {
    let mut builder = take(context)?;
    builder.stack.push(decision);
    context.insert_data(builder);
    Ok(JsValue::undefined())
}

fn pop(context: &mut Context) -> JsResult<Decision> {
    let mut builder = take(context)?;
    let decision = builder.stack.pop();
    context.insert_data(builder);
    decision.ok_or_else(|| native("missing decision"))
}

fn fault(context: &mut Context, message: &str) -> JsResult<JsValue> {
    if let Some(mut builder) = context.remove_data::<Builder>() {
        builder.fault = Some(message.to_string());
        context.insert_data(*builder);
    }
    Err(native(message))
}

fn take(context: &mut Context) -> JsResult<Builder> {
    context
        .remove_data::<Builder>()
        .map(|builder| *builder)
        .ok_or_else(|| native("compiler state missing"))
}

fn native(message: &str) -> JsError {
    JsNativeError::typ()
        .with_message(message.to_string())
        .into()
}

#[cfg(test)]
mod tests {
    use super::compile;
    use workflow_core::{
        evaluate_program, Decision, History, ToolName, ToolSpec, WaitCondition, WorkflowCommand,
    };

    #[test]
    fn compiles_the_counter_program() {
        let source = include_str!("../counter.js");
        assert_eq!(
            source,
            "onCounter(tool(\"counter\"), complete(), fail());\n"
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
