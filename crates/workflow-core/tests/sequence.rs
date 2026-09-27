use workflow_core::{
    counter_program, effect_id, evaluate_program, AgentSpec, Decision, History, Path, Record,
    ToolSpec, WaitCondition, WorkflowCommand, WorkflowProgram, WorkflowRunId,
};

fn tool(name: &str, input: &str) -> Decision {
    Decision::Tool {
        name: name.to_string(),
        input: input.to_string(),
    }
}

fn spawn_agent(agent: &str, input: &str) -> Decision {
    Decision::SpawnAgent {
        agent: agent.to_string(),
        input: input.to_string(),
    }
}

fn tool_record(name: &str, output: &str) -> Record {
    Record::Tool {
        name: name.to_string(),
        output: output.to_string(),
    }
}

fn spawned(agent: &str) -> Record {
    Record::AgentSpawned {
        agent: agent.to_string(),
    }
}

fn next(program: &WorkflowProgram, records: Vec<Record>) -> WorkflowCommand {
    let step = evaluate_program(program, &History::new(records));
    assert_eq!(step.wait, WaitCondition::None);
    let [command] = <[WorkflowCommand; 1]>::try_from(step.commands).expect("one command");
    command
}

fn search_then_helper() -> WorkflowProgram {
    WorkflowProgram {
        root: Decision::Seq(vec![
            tool("search", "q"),
            spawn_agent("helper", "go"),
            Decision::Complete,
        ]),
    }
}

#[test]
fn seq_returns_the_first_decision_without_a_record() {
    let program = search_then_helper();
    assert_eq!(
        next(&program, vec![]),
        WorkflowCommand::ExecuteTool(ToolSpec::new("search", "q"))
    );
    assert_eq!(
        next(&program, vec![tool_record("search", "found")]),
        WorkflowCommand::SpawnAgent(AgentSpec::new("helper", "go"))
    );
    assert_eq!(
        next(
            &program,
            vec![tool_record("search", "found"), spawned("helper")]
        ),
        WorkflowCommand::Complete
    );
}

#[test]
fn a_record_that_does_not_match_its_decision_fails() {
    let program = search_then_helper();
    // A spawn recorded where the tool call belongs.
    assert_eq!(
        next(&program, vec![spawned("helper")]),
        WorkflowCommand::Fail
    );
    // A tool recorded under another name.
    assert_eq!(
        next(&program, vec![tool_record("other", "x")]),
        WorkflowCommand::Fail
    );
    // A spawn of another agent.
    assert_eq!(
        next(
            &program,
            vec![tool_record("search", "found"), spawned("other")]
        ),
        WorkflowCommand::Fail
    );
    // A tool record where the spawn belongs.
    assert_eq!(
        next(
            &program,
            vec![tool_record("search", "found"), tool_record("helper", "x")]
        ),
        WorkflowCommand::Fail
    );
}

#[test]
fn running_past_the_end_fails() {
    let program = WorkflowProgram {
        root: Decision::Seq(vec![tool("search", "q"), spawn_agent("helper", "go")]),
    };
    assert_eq!(
        next(
            &program,
            vec![tool_record("search", "found"), spawned("helper")]
        ),
        WorkflowCommand::Fail
    );
    let single = WorkflowProgram {
        root: tool("search", "q"),
    };
    assert_eq!(
        next(&single, vec![tool_record("search", "found")]),
        WorkflowCommand::Fail
    );
}

#[test]
fn nested_seqs_walk_the_records_in_order() {
    let program = WorkflowProgram {
        root: Decision::Seq(vec![
            Decision::Seq(vec![tool("a", "1"), tool("b", "2")]),
            Decision::Seq(vec![tool("c", "3")]),
            Decision::Complete,
        ]),
    };
    assert_eq!(
        next(&program, vec![tool_record("a", "")]),
        WorkflowCommand::ExecuteTool(ToolSpec::new("b", "2"))
    );
    assert_eq!(
        next(&program, vec![tool_record("a", ""), tool_record("b", "")]),
        WorkflowCommand::ExecuteTool(ToolSpec::new("c", "3"))
    );
    assert_eq!(
        next(
            &program,
            vec![
                tool_record("a", ""),
                tool_record("b", ""),
                tool_record("c", "")
            ]
        ),
        WorkflowCommand::Complete
    );
}

#[test]
fn counter_is_the_output_of_the_last_counter_record() {
    assert_eq!(History::default().counter(), None);
    assert_eq!(
        History::new(vec![tool_record("search", "0")]).counter(),
        None
    );
    assert_eq!(
        History::new(vec![
            Record::counter(5),
            tool_record("search", "x"),
            Record::counter(0),
            spawned("helper"),
        ])
        .counter(),
        Some("0")
    );
    assert_eq!(Record::counter(-7), tool_record("counter", "-7"));
}

#[test]
fn a_counter_output_other_than_zero_takes_the_other_arm() {
    let program = counter_program();
    let run = WorkflowRunId("counter");
    for (output, command, path) in [
        ("0", WorkflowCommand::Complete, Path::Zero),
        ("1", WorkflowCommand::Fail, Path::Nonzero),
        ("x", WorkflowCommand::Fail, Path::Nonzero),
        ("", WorkflowCommand::Fail, Path::Nonzero),
        ("00", WorkflowCommand::Fail, Path::Nonzero),
        ("-0", WorkflowCommand::Fail, Path::Nonzero),
    ] {
        let records = vec![tool_record("counter", output)];
        assert_eq!(next(&program, records.clone()), command, "{output:?}");
        assert_eq!(
            effect_id(run, &History::new(records), 0).path,
            path,
            "{output:?}"
        );
    }
    assert_eq!(
        next(&program, vec![]),
        WorkflowCommand::ExecuteTool(ToolSpec::new("counter", ""))
    );
    assert_eq!(
        effect_id(run, &History::default(), 0).path,
        Path::Unrecorded
    );
}

#[test]
fn on_counter_reads_the_counter_recorded_before_it() {
    let program = WorkflowProgram {
        root: Decision::Seq(vec![
            tool("counter", ""),
            Decision::OnCounter {
                missing: Box::new(Decision::Fail),
                zero: Box::new(spawn_agent("helper", "zero")),
                other: Box::new(Decision::Fail),
            },
            Decision::Complete,
        ]),
    };
    assert_eq!(
        next(&program, vec![]),
        WorkflowCommand::ExecuteTool(ToolSpec::new("counter", ""))
    );
    assert_eq!(
        next(&program, vec![Record::counter(0)]),
        WorkflowCommand::SpawnAgent(AgentSpec::new("helper", "zero"))
    );
    assert_eq!(
        next(&program, vec![Record::counter(0), spawned("helper")]),
        WorkflowCommand::Complete
    );
    assert_eq!(
        next(&program, vec![Record::counter(3)]),
        WorkflowCommand::Fail
    );
}

#[test]
fn each_recorded_spawn_moves_the_cursor_one_record() {
    let program = WorkflowProgram {
        root: Decision::Seq(vec![
            spawn_agent("first", "1"),
            spawn_agent("second", "2"),
            tool("search", "q"),
            Decision::Complete,
        ]),
    };
    assert_eq!(
        next(&program, vec![spawned("first")]),
        WorkflowCommand::SpawnAgent(AgentSpec::new("second", "2"))
    );
    assert_eq!(
        next(&program, vec![spawned("first"), spawned("second")]),
        WorkflowCommand::ExecuteTool(ToolSpec::new("search", "q"))
    );
    assert_eq!(
        next(
            &program,
            vec![
                spawned("first"),
                spawned("second"),
                tool_record("search", "")
            ]
        ),
        WorkflowCommand::Complete
    );
}

// The zero and other arms run after the counter the missing arm recorded, so
// work in an arm after the counter call runs.
#[test]
fn an_arm_runs_after_the_counter_its_missing_arm_recorded() {
    let program = WorkflowProgram {
        root: Decision::OnCounter {
            missing: Box::new(tool("counter", "")),
            zero: Box::new(Decision::Seq(vec![tool("search", "q"), Decision::Complete])),
            other: Box::new(Decision::Fail),
        },
    };
    assert_eq!(
        next(&program, vec![]),
        WorkflowCommand::ExecuteTool(ToolSpec::new("counter", ""))
    );
    assert_eq!(
        next(&program, vec![Record::counter(0)]),
        WorkflowCommand::ExecuteTool(ToolSpec::new("search", "q"))
    );
    assert_eq!(
        next(
            &program,
            vec![Record::counter(0), tool_record("search", "")]
        ),
        WorkflowCommand::Complete
    );
    assert_eq!(
        next(&program, vec![Record::counter(4)]),
        WorkflowCommand::Fail
    );
}

// A counter recorded after an on_counter does not change the arm it took.
#[test]
fn a_later_counter_does_not_change_an_earlier_on_counter() {
    let program = WorkflowProgram {
        root: Decision::Seq(vec![
            Decision::OnCounter {
                missing: Box::new(tool("a", "")),
                zero: Box::new(tool("b", "")),
                other: Box::new(tool("b", "")),
            },
            tool("counter", ""),
            Decision::Complete,
        ]),
    };
    assert_eq!(
        next(&program, vec![]),
        WorkflowCommand::ExecuteTool(ToolSpec::new("a", ""))
    );
    assert_eq!(
        next(&program, vec![tool_record("a", "")]),
        WorkflowCommand::ExecuteTool(ToolSpec::new("counter", ""))
    );
    assert_eq!(
        next(&program, vec![tool_record("a", ""), Record::counter(0)]),
        WorkflowCommand::Complete
    );
}

// Every v1 Bend line, on every history with at most one counter record it can
// produce, means what it meant before records: no counter selects the missing
// arm, "0" the zero arm, any other value the other arm. Only a line whose
// missing arm executes can record a counter. A line whose zero or other arm
// also executes can record a second one; that history is walked by the cursor
// rule instead (v1_lines_that_call_the_counter_twice_follow_the_cursor).
#[test]
fn v1_counter_programs_keep_their_meaning_on_reachable_histories() {
    let arms = [
        (
            tool("counter", ""),
            WorkflowCommand::ExecuteTool(ToolSpec::new("counter", "")),
        ),
        (Decision::Complete, WorkflowCommand::Complete),
        (Decision::Fail, WorkflowCommand::Fail),
    ];
    for (missing, on_missing) in &arms {
        for (zero, on_zero) in &arms {
            for (other, on_other) in &arms {
                let program = WorkflowProgram {
                    root: Decision::OnCounter {
                        missing: Box::new(missing.clone()),
                        zero: Box::new(zero.clone()),
                        other: Box::new(other.clone()),
                    },
                };
                assert_eq!(&next(&program, vec![]), on_missing);
                if matches!(missing, Decision::Tool { .. }) {
                    assert_eq!(&next(&program, vec![Record::counter(0)]), on_zero);
                    assert_eq!(&next(&program, vec![Record::counter(1)]), on_other);
                    assert_eq!(&next(&program, vec![Record::counter(-1)]), on_other);
                }
            }
        }
    }
}

// `v1 on_counter execute execute complete` on two counter records: the missing
// arm consumes the first, the zero arm the second, and the program has no
// decision left, so it fails. Before records, the latest counter chose the
// other arm and completed; Bend's v1 evaluation still does.
#[test]
fn v1_lines_that_call_the_counter_twice_follow_the_cursor() {
    let program = WorkflowProgram {
        root: Decision::OnCounter {
            missing: Box::new(tool("counter", "")),
            zero: Box::new(tool("counter", "")),
            other: Box::new(Decision::Complete),
        },
    };
    assert_eq!(
        next(&program, vec![Record::counter(0)]),
        WorkflowCommand::ExecuteTool(ToolSpec::new("counter", ""))
    );
    assert_eq!(
        next(&program, vec![Record::counter(0), Record::counter(1)]),
        WorkflowCommand::Fail
    );
}
