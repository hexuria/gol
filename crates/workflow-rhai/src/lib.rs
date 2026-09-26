#![forbid(unsafe_code)]
use rhai::packages::{ArithmeticPackage, BasicIteratorPackage, LogicPackage, Package};
use rhai::{Dynamic, Engine, EvalAltResult, OptimizationLevel, Position};
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

/// A decision the script holds. It indexes the builder, so a script cannot
/// forge one: only `tool`, `complete`, `fail` and `on_counter` make them.
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

const BUILTINS: [&str; 4] = ["on_counter", "tool", "complete", "fail"];

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
        engine.register_fn(
            "tool",
            move |name: &str| -> Result<Node, Box<EvalAltResult>> {
                let mut built = builder.borrow_mut();
                if name != "counter" {
                    return fault(&mut built, "unknown tool");
                }
                built.make(Decision::Tool(ToolName::Counter))
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

/// Whether one of the overloads registered above takes these arguments.
fn has_overload(name: &str, args: &[&mut Dynamic]) -> bool {
    match (name, args) {
        ("on_counter", [missing, zero, other]) => {
            missing.is::<Node>() && zero.is::<Node>() && other.is::<Node>()
        }
        ("tool", [name]) => name.is_string(),
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
