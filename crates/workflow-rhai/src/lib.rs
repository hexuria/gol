#![forbid(unsafe_code)]
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

/// A decision the script holds. It indexes the builder, so a script cannot
/// forge one: only `tool`, `complete`, `fail` and `on_counter` make them.
#[derive(Clone, Copy)]
struct Node(usize);

struct Builder {
    nodes: Vec<Decision>,
    used: Vec<bool>,
    fault: Option<String>,
}

impl Builder {
    fn make(&mut self, decision: Decision) -> Node {
        self.nodes.push(decision);
        self.used.push(false);
        Node(self.nodes.len() - 1)
    }

    fn take(&mut self, node: Node) -> Decision {
        self.used[node.0] = true;
        self.nodes[node.0].clone()
    }
}

// Scripts run at compile time, so every one is bounded: an endless loop or a
// deep expression is a script error, never a hung compiler.
const MAX_OPERATIONS: u64 = 100_000;
const MAX_CALL_LEVELS: usize = 32;
const MAX_EXPR_DEPTH: usize = 64;
const MAX_FUNCTION_EXPR_DEPTH: usize = 32;
const MAX_STRING_SIZE: usize = 64 * 1024;
const MAX_COLLECTION_SIZE: usize = 1024;

pub fn compile(source: &str) -> Result<WorkflowProgram, FrontendError> {
    let builder = Rc::new(RefCell::new(Builder {
        nodes: Vec::new(),
        used: Vec::new(),
        fault: None,
    }));
    let mut engine = Engine::new();
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
    let built = builder.borrow();
    if let Some(message) = built.fault.clone() {
        return Err(FrontendError::InvalidProgram(message));
    }
    if let Err(error) = evaluated {
        return Err(FrontendError::Script(error.to_string()));
    }
    // The program is the one decision no other decision consumed.
    let mut roots = built
        .used
        .iter()
        .zip(&built.nodes)
        .filter(|(used, _)| !**used)
        .map(|(_, decision)| decision);
    match (roots.next(), roots.next()) {
        (Some(root), None) => Ok(WorkflowProgram { root: root.clone() }),
        _ => Err(FrontendError::InvalidProgram(
            "script must record one decision".to_string(),
        )),
    }
}

fn register(engine: &mut Engine, builder: &Rc<RefCell<Builder>>) {
    engine.register_type_with_name::<Node>("Decision");
    {
        let builder = Rc::clone(builder);
        engine.register_fn(
            "on_counter",
            move |missing: Node, zero: Node, other: Node| {
                let mut built = builder.borrow_mut();
                let decision = Decision::OnCounter {
                    missing: Box::new(built.take(missing)),
                    zero: Box::new(built.take(zero)),
                    other: Box::new(built.take(other)),
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
                Ok(built.make(Decision::Tool(ToolName::Counter)))
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

fn fault<T>(builder: &mut Builder, message: &str) -> Result<T, Box<EvalAltResult>> {
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
