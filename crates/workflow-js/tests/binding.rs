use workflow_core::counter_program;
use workflow_js::{compile, FrontendError};

#[test]
fn on_counter_takes_its_branches_from_its_arguments() {
    let program = compile(
        "const missing = tool(\"counter\");\nconst zero = complete();\nconst other = fail();\nonCounter(missing, zero, other);\n",
    )
    .unwrap();
    assert_eq!(program, counter_program());
}

#[test]
fn decisions_bound_before_the_call_keep_their_argument_position() {
    let program =
        compile("const a = complete();\nconst b = fail();\nonCounter(tool(\"counter\"), a, b);\n")
            .unwrap();
    assert_eq!(program, counter_program());
}

#[test]
fn on_counter_rejects_values_that_are_not_decisions() {
    assert!(matches!(
        compile("onCounter(1, 2, 3);\n"),
        Err(FrontendError::InvalidProgram(_))
    ));
}

#[test]
fn a_forged_decision_object_is_rejected() {
    assert!(matches!(
        compile("const d = complete();\nonCounter(tool(\"counter\"), d, {});\n"),
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
    let outcome = compile("let i = 0;\nwhile (i < 1000000) { i++; }\ncomplete();\n");
    assert!(
        matches!(outcome, Err(FrontendError::Script(_))),
        "{outcome:?}"
    );
}

#[test]
fn deep_recursion_is_a_script_error() {
    let outcome =
        compile("function f(n) { return n > 0 ? f(n - 1) : 0; }\nf(1000);\ncomplete();\n");
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
        compile("const d = complete();\nonCounter(tool(\"counter\"), d, d);\n"),
        Err(FrontendError::InvalidProgram(message)) if message == "decision used twice"
    ));
}

#[test]
fn a_doubling_loop_is_rejected() {
    assert!(matches!(
        compile("let d = complete();\nfor (let i = 0; i < 8; i++) { d = onCounter(d, d, d); }\n"),
        Err(FrontendError::InvalidProgram(message)) if message == "decision used twice"
    ));
}

#[test]
fn a_program_past_the_decision_cap_is_rejected() {
    assert!(matches!(
        compile("let d = complete();\nfor (let i = 0; i < 400; i++) { d = onCounter(d, fail(), fail()); }\n"),
        Err(FrontendError::InvalidProgram(message)) if message == "too many decisions"
    ));
}

#[test]
fn a_long_chain_under_the_cap_compiles() {
    let program = compile(
        "let d = complete();\nfor (let i = 0; i < 100; i++) { d = onCounter(d, fail(), fail()); }\n",
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
        "onCounter(1, 2, 3);\n",
        "onCounter(complete(), fail());\n",
        "onCounter(tool(\"counter\"), complete(), fail(), fail());\n",
        "onCounter(tool(\"counter\"), complete(), fail(), fail(), fail());\n",
        "tool(\"counter\", 1);\n",
        "complete(1);\n",
        "fail(1);\n",
    ] {
        assert!(
            matches!(compile(source), Err(FrontendError::InvalidProgram(_))),
            "{source}: {:?}",
            compile(source)
        );
    }
}

// complete() plus 341 links of three decisions each is exactly the cap.
#[test]
fn the_decision_cap_is_exactly_1024() {
    let at_cap = "let d = complete();\nfor (let i = 0; i < 341; i++) { d = onCounter(d, fail(), fail()); }\n";
    assert!(compile(at_cap).is_ok(), "{:?}", compile(at_cap));
    let past_cap = "let d = complete();\nfor (let i = 0; i < 341; i++) { d = onCounter(d, fail(), fail()); }\nfail();\n";
    assert!(matches!(
        compile(past_cap),
        Err(FrontendError::InvalidProgram(message)) if message == "too many decisions"
    ));
}

// Catching a misuse must not turn it into a valid program: a fault stays set
// even when the script catches the exception it raised.
#[test]
fn a_caught_misuse_is_still_rejected() {
    for source in [
        "try { tool(1); } catch (e) {}\ncomplete();\n",
        "try { onCounter(1, 2, 3); } catch (e) {}\ncomplete();\n",
        "try { tool(\"counter\", 1); } catch (e) {}\ncomplete();\n",
        "try { complete(1); } catch (e) {}\ncomplete();\n",
        "[1, 2].sort((a, b) => { try { tool(1); } catch (e) {} return 0; });\ncomplete();\n",
    ] {
        assert!(
            matches!(compile(source), Err(FrontendError::InvalidProgram(_))),
            "{source}: {:?}",
            compile(source)
        );
    }
}

// The first fault is the one reported.
#[test]
fn the_first_fault_is_reported() {
    assert!(matches!(
        compile("try { tool(1); } catch (e) {}\ntool(\"nope\");\n"),
        Err(FrontendError::InvalidProgram(message)) if message == "tool name must be a string"
    ));
}
