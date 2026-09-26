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
        Err(FrontendError::InvalidProgram(message)) if message == "decision used twice"
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
        "on_counter(tool(\"counter\"), complete(), fail(), fail(), fail());\n",
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
    let at_cap =
        "let d = complete();\nfor i in 0..341 { d = on_counter(d, fail(), fail()); }\nd;\n";
    assert!(compile(at_cap).is_ok(), "{:?}", compile(at_cap));
    let past_cap = "let d = complete();\nfor i in 0..341 { d = on_counter(d, fail(), fail()); }\nfail();\nd;\n";
    assert!(matches!(
        compile(past_cap),
        Err(FrontendError::InvalidProgram(message)) if message == "too many decisions"
    ));
}

// Catching a misuse must not turn it into a valid program: the fault stays
// set, as in JS.
#[test]
fn a_caught_misuse_is_still_rejected() {
    for source in [
        "try { tool(1); } catch {}\ncomplete();\n",
        "try { on_counter(1, 2, 3); } catch {}\ncomplete();\n",
        "try { tool(\"counter\", 1); } catch {}\ncomplete();\n",
        "try { complete(1); } catch {}\ncomplete();\n",
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
        compile("try { tool(1); } catch {}\ntool(\"nope\");\n"),
        Err(FrontendError::InvalidProgram(message)) if message == "tool called with the wrong arguments"
    ));
}

// A workflow script has no function pointers, callbacks, eval or sleep: they
// are how a script could swallow a misuse (a sort or for_each callback drops
// its error) or stall compilation without spending operations.
#[test]
fn the_language_has_no_callbacks_eval_or_sleep() {
    for source in [
        "try { [1].for_each(Fn(\"tool\").curry(1)); } catch {}\ncomplete();\n",
        "[1, 2].sort(|a, b| { tool(1); 0 });\ncomplete();\n",
        "[1, 1].dedup(|a, b| complete(1));\ncomplete();\n",
        "let f = |x| x;\nf.call(1);\ncomplete();\n",
        "eval(\"complete()\");\n",
        "sleep(1);\ncomplete();\n",
    ] {
        assert!(
            matches!(compile(source), Err(FrontendError::Script(_))),
            "{source}: {:?}",
            compile(source)
        );
    }
}

// With this count the operation budget runs out exactly at the complete()
// call on line 4. That is the budget, not a misuse of complete(). The count
// was found by compiling the script for every loop bound near the budget
// (100_000 operations, about 6 per iteration) and taking the one whose error
// points at line 4. It depends on rhai's operation accounting, which is why
// rhai is pinned exactly; after a rhai upgrade, find it again the same way.
#[test]
fn the_budget_running_out_at_a_builtin_is_not_a_misuse() {
    let outcome = compile("let i = 0;\nwhile i < 16665 { i += 1; }\n1;\ncomplete();\n");
    assert!(
        matches!(&outcome, Err(FrontendError::Script(message)) if message.contains("Too many operations") && message.contains("line 4")),
        "{outcome:?}"
    );
}
