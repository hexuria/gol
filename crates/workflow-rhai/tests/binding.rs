use workflow_core::counter_program;
use workflow_rhai::{compile, FrontendError};

#[test]
fn on_counter_takes_its_branches_from_its_arguments() {
    let program = compile(
        "let missing = tool(\"counter\");\nlet zero = complete();\nlet other = fail();\non_counter(missing, zero, other);\n",
    )
    .unwrap();
    assert_eq!(program, counter_program());
}

#[test]
fn decisions_bound_before_the_call_keep_their_argument_position() {
    let program =
        compile("let a = complete();\nlet b = fail();\non_counter(tool(\"counter\"), a, b);\n")
            .unwrap();
    assert_eq!(program, counter_program());
}

#[test]
fn on_counter_rejects_values_that_are_not_decisions() {
    assert!(compile("on_counter(1, 2, 3);\n").is_err());
}

#[test]
fn two_unused_decisions_are_not_one_program() {
    assert_eq!(
        compile("complete();\nfail();\n").unwrap_err().to_string(),
        "script must record one decision"
    );
}

// A loop ten times longer than the operation budget must stop at the budget.
// It is finite, so a compiler without the budget returns instead of hanging.
#[test]
fn a_loop_past_the_budget_is_a_script_error() {
    let outcome = compile("let i = 0;\nwhile i < 1_000_000 { i += 1; }\ncomplete();\n");
    assert!(
        matches!(outcome, Err(FrontendError::Script(_))),
        "{outcome:?}"
    );
}

#[test]
fn deep_recursion_is_a_script_error() {
    let outcome = compile("fn f(n) { if n > 0 { f(n - 1) } }\nf(1_000);\ncomplete();\n");
    assert!(
        matches!(outcome, Err(FrontendError::Script(_))),
        "{outcome:?}"
    );
}
