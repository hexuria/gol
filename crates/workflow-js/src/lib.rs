#![forbid(unsafe_code)]
use boa_engine::{
    js_string, Context, JsError, JsNativeError, JsObject, JsResult, JsValue, NativeFunction, Source,
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

/// Every decision the script made, keyed by the object handed back to it. A
/// script cannot forge one: lookup is by object identity, not by shape.
struct Builder {
    nodes: Vec<Node>,
    fault: Option<String>,
}

struct Node {
    handle: JsObject,
    decision: Decision,
    used: bool,
}

// Scripts run at compile time, so every one is bounded: an endless loop or a
// runaway recursion is a script error, never a hung compiler.
const MAX_LOOP_ITERATIONS: u64 = 100_000;
const MAX_RECURSION: usize = 64;

pub fn compile(source: &str) -> Result<WorkflowProgram, FrontendError> {
    let mut context = Context::default();
    let limits = context.runtime_limits_mut();
    limits.set_loop_iteration_limit(MAX_LOOP_ITERATIONS);
    limits.set_recursion_limit(MAX_RECURSION);
    context.insert_data(Builder {
        nodes: Vec::new(),
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
    // The program is the one decision no other decision consumed.
    let mut roots = builder.nodes.iter().filter(|node| !node.used);
    match (roots.next(), roots.next()) {
        (Some(root), None) => Ok(WorkflowProgram {
            root: root.decision.clone(),
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
    let [missing, zero, other] = args else {
        return fault(context, "onCounter takes three decisions");
    };
    let mut builder = take(context)?;
    let branches = [missing, zero, other].map(|arg| builder.consume(arg));
    let [Some(missing), Some(zero), Some(other)] = branches else {
        context.insert_data(builder);
        return fault(context, "onCounter takes three decisions");
    };
    context.insert_data(builder);
    make(
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
    make(context, Decision::Tool(ToolName::Counter))
}

fn complete(_this: &JsValue, _args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    make(context, Decision::Complete)
}

fn fail(_this: &JsValue, _args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    make(context, Decision::Fail)
}

impl Builder {
    /// Marks the decision behind `value` as used and returns it, or `None`
    /// when `value` is not an object this compiler handed out.
    fn consume(&mut self, value: &JsValue) -> Option<Decision> {
        let object = value.as_object()?;
        let node = self
            .nodes
            .iter_mut()
            .find(|node| JsObject::equals(&node.handle, &object))?;
        node.used = true;
        Some(node.decision.clone())
    }
}

fn make(context: &mut Context, decision: Decision) -> JsResult<JsValue> {
    let handle = JsObject::with_null_proto();
    let mut builder = take(context)?;
    builder.nodes.push(Node {
        handle: handle.clone(),
        decision,
        used: false,
    });
    context.insert_data(builder);
    Ok(handle.into())
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
