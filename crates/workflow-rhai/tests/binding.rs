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
    assert!(matches!(
        compile("on_counter(1, 2, 3);\n"),
        Err(FrontendError::InvalidProgram(_))
    ));
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

// A handle is used once. Reusing one would clone its subtree on every use, so
// a short loop could build an exponentially large program.
#[test]
fn a_decision_used_twice_is_rejected() {
    assert!(matches!(
        compile("let d = complete();\non_counter(tool(\"counter\"), d, d);\n"),
        Err(FrontendError::InvalidProgram(message)) if message == "decision used twice"
    ));
}

#[test]
fn a_doubling_loop_is_rejected() {
    assert!(matches!(
        compile("let d = complete();\nfor i in 0..8 { d = on_counter(d, d, d); }\nd;\n"),
        Err(FrontendError::InvalidProgram(_))
    ));
}

#[test]
fn a_program_past_the_decision_cap_is_rejected() {
    assert!(matches!(
        compile("let d = complete();\nfor i in 0..400 { d = on_counter(d, fail(), fail()); }\nd;\n"),
        Err(FrontendError::InvalidProgram(message)) if message == "too many decisions"
    ));
}

#[test]
fn a_long_chain_under_the_cap_compiles() {
    let program = compile(
        "let d = complete();\nfor i in 0..100 { d = on_counter(d, fail(), fail()); }\nd;\n",
    )
    .unwrap();
    let mut depth = 0;
    let mut node = &program.root;
    while let workflow_core::Decision::OnCounter { missing, .. } = node {
        depth += 1;
        node = missing;
    }
    assert_eq!(depth, 100);
}

// The same misuse is the same error kind in Rhai and JS.
#[test]
fn misuse_is_an_invalid_program() {
    for source in [
        "tool(1);\n",
        "on_counter(1, 2, 3);\n",
        "on_counter(complete(), fail());\n",
        "on_counter(tool(\"counter\"), complete(), fail(), fail());\n",
    ] {
        assert!(
            matches!(compile(source), Err(FrontendError::InvalidProgram(_))),
            "{source}: {:?}",
            compile(source)
        );
    }
}
