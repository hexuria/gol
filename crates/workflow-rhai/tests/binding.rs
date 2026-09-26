use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use workflow_core::counter_program;
use workflow_rhai::{compile, FrontendError};

fn compile_within(source: &'static str, limit: Duration) -> Option<Result<(), FrontendError>> {
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let _ = sender.send(compile(source).map(|_| ()));
    });
    receiver.recv_timeout(limit).ok()
}

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

#[test]
fn an_endless_loop_is_rejected_instead_of_hanging() {
    let outcome = compile_within("loop {}\n", Duration::from_secs(5));
    assert!(
        matches!(outcome, Some(Err(FrontendError::Script(_)))),
        "{outcome:?}"
    );
}

#[test]
fn unbounded_recursion_is_rejected() {
    let outcome = compile_within("fn f(n) { f(n + 1) }\nf(0);\n", Duration::from_secs(5));
    assert!(
        matches!(outcome, Some(Err(FrontendError::Script(_)))),
        "{outcome:?}"
    );
}
