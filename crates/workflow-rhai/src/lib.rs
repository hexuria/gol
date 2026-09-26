#![forbid(unsafe_code)]
use rhai::packages::{ArithmeticPackage, BasicIteratorPackage, LogicPackage, Package};
use rhai::{Array, Dynamic, Engine, EvalAltResult, OptimizationLevel, Position};
use std::cell::RefCell;
use std::rc::Rc;
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

/// A decision the script holds. It indexes the builder, so a script cannot
/// forge one: only the builtins make them.
#[derive(Clone, Copy)]
struct Node(usize);

/// Every decision the script made. A slot is `None` once another decision
/// consumed it, so each handle is used at most once and the program is built
/// by moving subtrees, never by copying them.
struct Builder {
    nodes: Vec<Option<Decision>>,
    fault: Option<String>,
}

impl Builder {
    fn make(&mut self, decision: Decision) -> Result<Node, Box<EvalAltResult>> {
        if self.nodes.len() >= MAX_DECISIONS {
            return fault(self, "too many decisions");
        }
        self.nodes.push(Some(decision));
        Ok(Node(self.nodes.len() - 1))
    }

    fn take(&mut self, node: Node) -> Result<Decision, Box<EvalAltResult>> {
        match self.nodes[node.0].take() {
            Some(decision) => Ok(decision),
            None => fault(self, "decision used twice"),
        }
    }
}

// Scripts run at compile time, so every one is bounded. Operations, call
// levels and expression depth bound the time a script can run; the string,
// collection and decision caps bound what it can build.
const MAX_OPERATIONS: u64 = 100_000;
const MAX_CALL_LEVELS: usize = 32;
const MAX_EXPR_DEPTH: usize = 64;
const MAX_FUNCTION_EXPR_DEPTH: usize = 32;
const MAX_STRING_SIZE: usize = 64 * 1024;
const MAX_COLLECTION_SIZE: usize = 1024;
const MAX_DECISIONS: usize = 1024;
// A tool or agent name is checked against the catalog when the workflow is
// registered; here it only has to be a short, non-empty string.
const MAX_NAME_BYTES: usize = 128;

pub fn compile(source: &str) -> Result<WorkflowProgram, FrontendError> {
    let builder = Rc::new(RefCell::new(Builder {
        nodes: Vec::new(),
        fault: None,
    }));
    let mut engine = language();
    engine
        .set_optimization_level(OptimizationLevel::None)
        .set_max_operations(MAX_OPERATIONS)
        .set_max_call_levels(MAX_CALL_LEVELS)
        .set_max_expr_depths(MAX_EXPR_DEPTH, MAX_FUNCTION_EXPR_DEPTH)
        .set_max_string_size(MAX_STRING_SIZE)
        .set_max_array_size(MAX_COLLECTION_SIZE)
        .set_max_map_size(MAX_COLLECTION_SIZE);
    register(&mut engine, &builder);

    let evaluated = engine.run(source);
    let mut built = builder.borrow_mut();
    if let Some(message) = built.fault.take() {
        return Err(FrontendError::InvalidProgram(message));
    }
    if let Err(error) = evaluated {
        return Err(FrontendError::Script(error.to_string()));
    }
    // The program is the one decision no other decision consumed.
    let mut roots = built.nodes.iter_mut().filter_map(Option::take);
    match (roots.next(), roots.next()) {
        (Some(root), None) => Ok(WorkflowProgram { root }),
        _ => Err(FrontendError::InvalidProgram(
            "script must record one decision".to_string(),
        )),
    }
}

const BUILTINS: [&str; 6] = [
    "on_counter",
    "tool",
    "spawn_agent",
    "seq",
    "complete",
    "fail",
];

/// The language a workflow script is written in: numbers, comparisons,
/// ranges, variables, control flow, script functions and the builtins below.
/// It leaves out Rhai's standard library and function pointers. Array, map
/// and string methods take callbacks that drop their errors (a sort comparator
/// that misuses a builtin would compile), `sleep` stalls without spending
/// operations, and `eval` compiles code the budget never sees at parse time.
fn language() -> Engine {
    let mut engine = Engine::new_raw();
    for package in [
        ArithmeticPackage::new().as_shared_module(),
        LogicPackage::new().as_shared_module(),
        BasicIteratorPackage::new().as_shared_module(),
    ] {
        engine.register_global_module(package);
    }
    for keyword in ["Fn", "call", "curry", "eval"] {
        engine.disable_symbol(keyword);
    }
    engine
}

fn register(engine: &mut Engine, builder: &Rc<RefCell<Builder>>) {
    engine.register_type_with_name::<Node>("Decision");
    {
        // A call to a builtin that no overload takes is a malformed program,
        // as in JS. The fault is recorded where the call fails, so a script
        // that catches the error still fails to compile. Rhai also runs this
        // hook when a matching overload fails (a fault of its own, or the
        // operation budget running out at that call), so only arguments no
        // overload accepts count as misuse.
        let builder = Rc::clone(builder);
        #[allow(deprecated)] // rhai marks this API volatile, not deprecated.
        engine.on_missing_function(move |name, args, _, _| {
            if BUILTINS.contains(&name) && !has_overload(name, args) {
                let message = format!("{name} called with the wrong arguments");
                let _ = fault::<()>(&mut builder.borrow_mut(), &message);
            }
            Ok(None)
        });
    }
    {
        let builder = Rc::clone(builder);
        engine.register_fn(
            "on_counter",
            move |missing: Node, zero: Node, other: Node| -> Result<Node, Box<EvalAltResult>> {
                let mut built = builder.borrow_mut();
                let decision = Decision::OnCounter {
                    missing: Box::new(built.take(missing)?),
                    zero: Box::new(built.take(zero)?),
                    other: Box::new(built.take(other)?),
                };
                built.make(decision)
            },
        );
    }
    {
        let builder = Rc::clone(builder);
        engine.register_fn("tool", move |name: &str| {
            make_tool(&mut builder.borrow_mut(), name, "")
        });
    }
    {
        let builder = Rc::clone(builder);
        engine.register_fn("tool", move |name: &str, input: &str| {
            make_tool(&mut builder.borrow_mut(), name, input)
        });
    }
    {
        let builder = Rc::clone(builder);
        engine.register_fn(
            "spawn_agent",
            move |agent: &str, input: &str| -> Result<Node, Box<EvalAltResult>> {
                let mut built = builder.borrow_mut();
                if !valid_name(agent) {
                    return fault(&mut built, AGENT_NAME);
                }
                built.make(Decision::SpawnAgent {
                    agent: agent.to_string(),
                    input: input.to_string(),
                })
            },
        );
    }
    {
        let builder = Rc::clone(builder);
        engine.register_fn(
            "seq",
            move |items: Array| -> Result<Node, Box<EvalAltResult>> {
                let mut built = builder.borrow_mut();
                if items.is_empty() {
                    return fault(&mut built, SEQ_EMPTY);
                }
                let mut decisions = Vec::with_capacity(items.len());
                for item in items {
                    let Some(node) = item.try_cast::<Node>() else {
                        return fault(&mut built, SEQ_ITEMS);
                    };
                    decisions.push(built.take(node)?);
                }
                built.make(Decision::Seq(decisions))
            },
        );
    }
    {
        let builder = Rc::clone(builder);
        engine.register_fn("complete", move || {
            builder.borrow_mut().make(Decision::Complete)
        });
    }
    {
        let builder = Rc::clone(builder);
        engine.register_fn("fail", move || builder.borrow_mut().make(Decision::Fail));
    }
}

const TOOL_NAME: &str = "tool name must be 1 to 128 bytes";
const AGENT_NAME: &str = "agent name must be 1 to 128 bytes";
const SEQ_EMPTY: &str = "seq takes at least one decision";
const SEQ_ITEMS: &str = "seq takes an array of decisions";

fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= MAX_NAME_BYTES
}

fn make_tool(built: &mut Builder, name: &str, input: &str) -> Result<Node, Box<EvalAltResult>> {
    if !valid_name(name) {
        return fault(built, TOOL_NAME);
    }
    built.make(Decision::Tool {
        name: name.to_string(),
        input: input.to_string(),
    })
}

/// Whether one of the overloads registered above takes these arguments.
fn has_overload(name: &str, args: &[&mut Dynamic]) -> bool {
    match (name, args) {
        ("on_counter", [missing, zero, other]) => {
            missing.is::<Node>() && zero.is::<Node>() && other.is::<Node>()
        }
        ("tool", [name]) => name.is_string(),
        ("tool" | "spawn_agent", [name, input]) => name.is_string() && input.is_string(),
        ("seq", [items]) => items.is_array(),
        ("complete" | "fail", []) => true,
        _ => false,
    }
}

/// Records the first fault; a later one does not replace it.
fn fault<T>(builder: &mut Builder, message: &str) -> Result<T, Box<EvalAltResult>> {
    builder.fault.get_or_insert_with(|| message.to_string());
    Err(EvalAltResult::ErrorRuntime(message.into(), Position::NONE).into())
}

#[cfg(test)]
mod tests {
    use super::compile;
    use workflow_core::{
        evaluate_program, Decision, History, Record, ToolSpec, WaitCondition, WorkflowCommand,
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
