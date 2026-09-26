#![forbid(unsafe_code)]
use boa_engine::object::builtins::JsArray;
use boa_engine::{
    js_string, Context, JsError, JsNativeError, JsObject, JsResult, JsValue, NativeFunction, Source,
};
use workflow_core::{Decision, WorkflowProgram};

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

/// `decision` is `None` once another decision consumed it, so each handle is
/// used at most once and the program is built by moving subtrees, never by
/// copying them.
struct Node {
    handle: JsObject,
    decision: Option<Decision>,
}

// Scripts run at compile time. Boa bounds loop iterations per call frame and
// recursion depth; the decision cap bounds what a script can build. Boa has no
// instruction budget, so a loop spread across calls, a native built-in or a
// backtracking regex is not bounded here: a caller compiling untrusted JS must
// run it under its own time limit.
const MAX_LOOP_ITERATIONS: u64 = 100_000;
const MAX_RECURSION: usize = 64;
const MAX_DECISIONS: usize = 1024;
const ON_COUNTER_ARITY: &str = "onCounter takes exactly three decisions";
// A tool or agent name is checked against the catalog when the workflow is
// registered; here it only has to be a short, non-empty string.
const MAX_NAME_BYTES: usize = 128;
const TOOL_NAME: &str = "tool name must be 1 to 128 bytes";
const AGENT_NAME: &str = "agent name must be 1 to 128 bytes";
const SEQ_EMPTY: &str = "seq takes at least one decision";
const SEQ_ITEMS: &str = "seq takes an array of decisions";

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
    register(&mut context, "tool", 2, tool);
    register(&mut context, "spawnAgent", 2, spawn_agent);
    register(&mut context, "seq", 1, seq);
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
    let mut roots = builder.nodes.into_iter().filter_map(|node| node.decision);
    match (roots.next(), roots.next()) {
        (Some(root), None) => Ok(WorkflowProgram { root }),
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
        return fault(context, ON_COUNTER_ARITY);
    };
    let mut builder = take(context)?;
    let branches = [missing, zero, other].map(|arg| builder.consume(arg, ON_COUNTER_ARITY));
    context.insert_data(builder);
    let [missing, zero, other] = match branches {
        [Ok(missing), Ok(zero), Ok(other)] => [missing, zero, other],
        [Err(message), _, _] | [_, Err(message), _] | [_, _, Err(message)] => {
            return fault(context, message);
        }
    };
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
    let (name, input) = match args {
        [name] => (name, None),
        [name, input] => (name, Some(input)),
        _ => return fault(context, "tool takes a name and an optional input"),
    };
    let Some(name) = string(name) else {
        return fault(context, "tool name must be a string");
    };
    let input = match input.map(string) {
        None => String::new(),
        Some(Some(input)) => input,
        Some(None) => return fault(context, "tool input must be a string"),
    };
    if !valid_name(&name) {
        return fault(context, TOOL_NAME);
    }
    make(context, Decision::Tool { name, input })
}

fn spawn_agent(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let [agent, input] = args else {
        return fault(context, "spawnAgent takes an agent and an input");
    };
    let (Some(agent), Some(input)) = (string(agent), string(input)) else {
        return fault(context, "spawnAgent takes two strings");
    };
    if !valid_name(&agent) {
        return fault(context, AGENT_NAME);
    }
    make(context, Decision::SpawnAgent { agent, input })
}

fn seq(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    let [items] = args else {
        return fault(context, SEQ_ITEMS);
    };
    let Some(array) = items
        .as_object()
        .and_then(|object| JsArray::from_object(object).ok())
    else {
        return fault(context, SEQ_ITEMS);
    };
    // Reading an element can run script code (a getter), so every element is
    // read before the builder leaves the context.
    let length = array.length(context)?;
    if length == 0 {
        return fault(context, SEQ_EMPTY);
    }
    if length > MAX_DECISIONS as u64 {
        return fault(context, "too many decisions");
    }
    let mut items = Vec::new();
    for index in 0..length {
        items.push(array.at(index as i64, context)?);
    }
    let mut builder = take(context)?;
    let decisions: Result<Vec<_>, _> = items
        .iter()
        .map(|item| builder.consume(item, SEQ_ITEMS))
        .collect();
    context.insert_data(builder);
    match decisions {
        Ok(decisions) => make(context, Decision::Seq(decisions)),
        Err(message) => fault(context, message),
    }
}

fn string(value: &JsValue) -> Option<String> {
    value
        .as_string()
        .and_then(|value| value.to_std_string().ok())
}

fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= MAX_NAME_BYTES
}

fn complete(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    if !args.is_empty() {
        return fault(context, "complete takes no arguments");
    }
    make(context, Decision::Complete)
}

fn fail(_this: &JsValue, args: &[JsValue], context: &mut Context) -> JsResult<JsValue> {
    if !args.is_empty() {
        return fault(context, "fail takes no arguments");
    }
    make(context, Decision::Fail)
}

impl Builder {
    /// Moves the decision behind `value` out of the builder. Fails with
    /// `not_decision` when `value` is not an object this compiler handed out,
    /// or with "decision used twice" when it was used before.
    fn consume(
        &mut self,
        value: &JsValue,
        not_decision: &'static str,
    ) -> Result<Decision, &'static str> {
        let node = value
            .as_object()
            .and_then(|object| {
                self.nodes
                    .iter_mut()
                    .find(|node| JsObject::equals(&node.handle, &object))
            })
            .ok_or(not_decision)?;
        node.decision.take().ok_or("decision used twice")
    }
}

fn make(context: &mut Context, decision: Decision) -> JsResult<JsValue> {
    let handle = JsObject::with_null_proto();
    let mut builder = take(context)?;
    if builder.nodes.len() >= MAX_DECISIONS {
        context.insert_data(builder);
        return fault(context, "too many decisions");
    }
    builder.nodes.push(Node {
        handle: handle.clone(),
        decision: Some(decision),
    });
    context.insert_data(builder);
    Ok(handle.into())
}

/// Records the first fault; a later one does not replace it.
fn fault(context: &mut Context, message: &str) -> JsResult<JsValue> {
    if let Some(mut builder) = context.remove_data::<Builder>() {
        builder.fault.get_or_insert_with(|| message.to_string());
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
        evaluate_program, Decision, History, Record, ToolSpec, WaitCondition, WorkflowCommand,
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
                missing: Box::new(Decision::Tool {
                    name: "counter".to_string(),
                    input: String::new(),
                }),
                zero: Box::new(Decision::Complete),
                other: Box::new(Decision::Fail),
            }
        );

        let unrecorded = evaluate_program(&program, &History::default());
        assert_eq!(
            unrecorded.commands,
            [WorkflowCommand::ExecuteTool(ToolSpec::new("counter", ""))]
        );
        assert_eq!(unrecorded.wait, WaitCondition::None);

        let zero = evaluate_program(&program, &History::new(vec![Record::counter(0)]));
        assert_eq!(zero.commands, [WorkflowCommand::Complete]);
        assert_eq!(zero.wait, WaitCondition::None);

        let other = evaluate_program(&program, &History::new(vec![Record::counter(1)]));
        assert_eq!(other.commands, [WorkflowCommand::Fail]);
        assert_eq!(other.wait, WaitCondition::None);

        assert_eq!(
            compile("let x = 1;\n").unwrap_err().to_string(),
            "script must record one decision"
        );
        // Any short, non-empty tool name compiles; the catalog check comes later.
        assert_eq!(
            compile("tool(\"nope\");\n").unwrap().root,
            Decision::Tool {
                name: "nope".to_string(),
                input: String::new(),
            }
        );
        assert_eq!(
            compile("tool(\"\");\n").unwrap_err().to_string(),
            "tool name must be 1 to 128 bytes"
        );
    }
}
