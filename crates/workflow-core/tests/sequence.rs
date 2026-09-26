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
fn on_counter_inside_seq_reads_the_whole_history() {
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
