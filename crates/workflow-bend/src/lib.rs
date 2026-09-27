#![deny(unsafe_code)]
mod boundary;

use std::path::Path;

use workflow_core::{
    evaluate_program, Decision, History, WorkflowContext, WorkflowDriver, WorkflowProgram,
    WorkflowStep,
};

use boundary::{
    bend_program, check_version, failure_text, run_sandboxed, stage, staged_dir, Limits,
};

#[derive(Debug)]
pub enum FrontendError {
    Setup(String),
    Version(String),
    Proof(String),
    Encoding(String),
    Timeout,
    OutputLimit,
}

impl std::fmt::Display for FrontendError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrontendError::Setup(message)
            | FrontendError::Version(message)
            | FrontendError::Proof(message)
            | FrontendError::Encoding(message) => formatter.write_str(message),
            FrontendError::Timeout => formatter.write_str("bend process timed out"),
            FrontendError::OutputLimit => formatter.write_str("bend output exceeded the limit"),
        }
    }
}

impl std::error::Error for FrontendError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BendDriver {
    program: WorkflowProgram,
}

impl BendDriver {
    pub fn new(program: WorkflowProgram) -> Self {
        Self { program }
    }

    pub fn program(&self) -> &WorkflowProgram {
        &self.program
    }
}

impl WorkflowDriver for BendDriver {
    fn evaluate(&self, _ctx: &WorkflowContext, history: &History) -> WorkflowStep {
        evaluate_program(&self.program, history)
    }
}

pub fn compile(source: &Path) -> Result<WorkflowProgram, FrontendError> {
    let bend = bend_program()?;
    let staged = stage(source)?;
    let dir = staged_dir(&staged);
    check_version(&bend, dir)?;
    let proof = run_sandboxed(
        &bend,
        &["PROOF.bend", "--check-only"],
        dir,
        Limits::default(),
    )?;
    if !proof.status.success() {
        return Err(FrontendError::Proof(failure_text(&proof)));
    }
    if proof.stdout != "All terms check.\n" || !proof.stderr.is_empty() {
        return Err(FrontendError::Proof(
            "proof check did not report All terms check.".to_string(),
        ));
    }
    let encoded = run_sandboxed(&bend, &["workflow.bend"], dir, Limits::default())?;
    if !encoded.status.success() {
        return Err(FrontendError::Encoding(failure_text(&encoded)));
    }
    if !encoded.stderr.is_empty() {
        return Err(FrontendError::Encoding(encoded.stderr.trim().to_string()));
    }
    parse_encoding(&encoded.stdout)
}

/// The most tokens a v2 line may have. It bounds the program's nesting, and so
/// the depth `evaluate_program` recurses to.
const MAX_TOKENS: usize = 2048;
const MAX_NAME_BYTES: usize = 128;
const MAX_INPUT_BYTES: usize = 64 * 1024;

/// A value on the v2 decoder's stack: a decision, or the items of a sequence
/// still being built (`end` starts one, each `seq` puts one item in front).
enum Item {
    Decision(Decision),
    Items(Vec<Decision>),
}

impl Item {
    fn into_decision(self) -> Decision {
        match self {
            Item::Decision(decision) => decision,
            Item::Items(items) => Decision::Seq(items),
        }
    }
}

/// Parses Bend's v2 line: `v2`, then the program in postfix. Leaves are
/// `tool.<name>.<input>` and `spawn.<agent>.<input>`, with each text as
/// lowercase hex of its UTF-8, and `complete` and `fail`. `on_counter` takes
/// the three decisions before it. `end` is an empty sequence, and `seq` puts
/// the decision before a sequence in front of it. The line must leave exactly
/// one value.
fn parse_encoding(stdout: &str) -> Result<WorkflowProgram, FrontendError> {
    let Some(line) = stdout.strip_suffix('\n') else {
        return Err(encoding("bend encoding is missing its trailing newline"));
    };
    if line.contains('\n') {
        return Err(encoding("bend encoding must be one line"));
    }
    let Some(inner) = line
        .strip_prefix('"')
        .and_then(|text| text.strip_suffix('"'))
    else {
        return Err(encoding("bend encoding must be a quoted token line"));
    };
    if !inner
        .bytes()
        .all(|byte| byte.is_ascii_graphic() || byte == b' ')
        || inner.contains(['\\', '"'])
    {
        return Err(encoding("bend encoding has an unsupported character"));
    }
    let mut tokens = inner.split(' ');
    if tokens.next() != Some("v2") {
        return Err(encoding("expected a v2 token line"));
    }
    let mut stack: Vec<Item> = Vec::new();
    for (count, token) in tokens.enumerate() {
        if count >= MAX_TOKENS {
            return Err(encoding("bend encoding has too many tokens"));
        }
        let item = match token {
            "complete" => Item::Decision(Decision::Complete),
            "fail" => Item::Decision(Decision::Fail),
            "end" => Item::Items(Vec::new()),
            "seq" => {
                let Some(Item::Items(mut items)) = stack.pop() else {
                    return Err(encoding("seq must follow a sequence"));
                };
                let head = pop_decision(&mut stack, "seq")?;
                items.insert(0, head);
                Item::Items(items)
            }
            "on_counter" => {
                let other = pop_decision(&mut stack, "on_counter")?;
                let zero = pop_decision(&mut stack, "on_counter")?;
                let missing = pop_decision(&mut stack, "on_counter")?;
                Item::Decision(Decision::OnCounter {
                    missing: Box::new(missing),
                    zero: Box::new(zero),
                    other: Box::new(other),
                })
            }
            _ => {
                if let Some(texts) = token.strip_prefix("tool.") {
                    let (name, input) = texts_of(texts)?;
                    Item::Decision(Decision::Tool { name, input })
                } else if let Some(texts) = token.strip_prefix("spawn.") {
                    let (agent, input) = texts_of(texts)?;
                    Item::Decision(Decision::SpawnAgent { agent, input })
                } else {
                    return Err(FrontendError::Encoding(format!("unknown token {token}")));
                }
            }
        };
        stack.push(item);
    }
    match (stack.pop(), stack.is_empty()) {
        (Some(root), true) => Ok(WorkflowProgram {
            root: root.into_decision(),
        }),
        _ => Err(encoding("bend encoding must leave exactly one program")),
    }
}

fn encoding(message: &str) -> FrontendError {
    FrontendError::Encoding(message.to_string())
}

fn pop_decision(stack: &mut Vec<Item>, token: &str) -> Result<Decision, FrontendError> {
    stack
        .pop()
        .map(Item::into_decision)
        .ok_or_else(|| FrontendError::Encoding(format!("{token} is missing a decision")))
}

/// `<name>.<input>`: a name of 1 to 128 bytes and an input of at most 65536,
/// the limits the Rhai and JS frontends apply.
fn texts_of(texts: &str) -> Result<(String, String), FrontendError> {
    let Some((name, input)) = texts.split_once('.') else {
        return Err(encoding("a tool or spawn token needs a name and an input"));
    };
    let (name, input) = (unhex(name)?, unhex(input)?);
    if name.is_empty() || name.len() > MAX_NAME_BYTES || input.len() > MAX_INPUT_BYTES {
        return Err(encoding("a tool or spawn name or input is out of bounds"));
    }
    Ok((name, input))
}

/// Lowercase hex of UTF-8 text.
fn unhex(hex: &str) -> Result<String, FrontendError> {
    let digit = |byte: u8| match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    };
    if !hex.len().is_multiple_of(2) {
        return Err(encoding("hex text must have an even length"));
    }
    let bytes = hex
        .as_bytes()
        .chunks(2)
        .map(|pair| Some(digit(pair[0])? << 4 | digit(pair[1])?))
        .collect::<Option<Vec<u8>>>()
        .ok_or_else(|| encoding("hex text must be lowercase hex digits"))?;
    String::from_utf8(bytes).map_err(|_| encoding("hex text must be UTF-8"))
}

/// The v2 line a program encodes to, as Bend prints it: the reference
/// encoder the parser is tested against.
#[cfg(test)]
pub(crate) fn encode_line(program: &WorkflowProgram) -> String {
    fn hex(text: &str) -> String {
        text.bytes().map(|byte| format!("{byte:02x}")).collect()
    }
    fn sequence(list: &[Decision], out: &mut Vec<String>) {
        match list.split_first() {
            None => out.push("end".to_string()),
            Some((head, rest)) => {
                decision(head, out);
                sequence(rest, out);
                out.push("seq".to_string());
            }
        }
    }
    fn decision(decision_: &Decision, out: &mut Vec<String>) {
        match decision_ {
            Decision::Tool { name, input } => {
                out.push(format!("tool.{}.{}", hex(name), hex(input)))
            }
            Decision::SpawnAgent { agent, input } => {
                out.push(format!("spawn.{}.{}", hex(agent), hex(input)))
            }
            Decision::Complete => out.push("complete".to_string()),
            Decision::Fail => out.push("fail".to_string()),
            Decision::OnCounter {
                missing,
                zero,
                other,
            } => {
                decision(missing, out);
                decision(zero, out);
                decision(other, out);
                out.push("on_counter".to_string());
            }
            Decision::Seq(list) => sequence(list, out),
        }
    }
    let mut out = vec!["v2".to_string()];
    decision(&program.root, &mut out);
    format!("\"{}\"\n", out.join(" "))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use harness_core::transition;
    use workflow_core::{
        counter_program, evaluate_program, Decision, History, Record, ToolSpec, WaitCondition,
        WorkflowCommand, WorkflowContext, WorkflowProgram, WorkflowStep,
    };

    use super::{compile, parse_encoding, BendDriver};
    use crate::boundary::{bend_program, run_sandboxed, stage, staged_dir, Limits, STAGED_FILES};

    fn experiment() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../experiments/bend")
    }

    fn histories() -> [Option<i64>; 6] {
        [
            None,
            Some(0),
            Some(1),
            Some(-1),
            Some(i64::MAX),
            Some(i64::MIN),
        ]
    }

    fn expected(counter: Option<i64>) -> WorkflowStep {
        let command = match counter {
            None => WorkflowCommand::ExecuteTool(ToolSpec::new("counter", "")),
            Some(0) => WorkflowCommand::Complete,
            Some(_) => WorkflowCommand::Fail,
        };
        WorkflowStep {
            commands: vec![command],
            wait: WaitCondition::None,
        }
    }

    fn scratch() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("gol-bend-test-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn copy_experiment(dir: &Path) {
        for name in STAGED_FILES {
            fs::copy(experiment().join(name), dir.join(name)).unwrap();
        }
    }

    /// The reference program `experiments/bend/workflow.bend` emits.
    fn search_program() -> WorkflowProgram {
        WorkflowProgram {
            root: Decision::Seq(vec![
                tool("search", "q"),
                Decision::OnCounter {
                    missing: Box::new(tool("counter", "")),
                    zero: Box::new(Decision::Seq(vec![
                        spawn_agent("helper", "zero"),
                        Decision::Complete,
                    ])),
                    other: Box::new(Decision::Fail),
                },
            ]),
        }
    }

    #[test]
    fn histories_agree_across_rust_rhai_js_and_bend() {
        // Bend emits the reference program; Rhai and JS build the same one.
        let bend = compile(&experiment()).unwrap();
        assert_eq!(bend, search_program());
        // BendDriver hands the loaded program to harness_core::transition.
        let driver = BendDriver::new(bend);
        for history in alphabet_histories() {
            let (_, commands) = transition(&driver, &WorkflowContext, &history);
            assert_eq!(
                commands,
                evaluate_program(&search_program(), &history).commands
            );
        }
        assert_eq!(
            workflow_rhai::compile(include_str!("../../workflow-rhai/search.rhai")).unwrap(),
            search_program()
        );
        assert_eq!(
            workflow_js::compile(include_str!("../../workflow-js/search.js")).unwrap(),
            search_program()
        );

        // The counter program across Rust, Rhai and JS.
        let rhai =
            workflow_rhai::compile(include_str!("../../workflow-rhai/counter.rhai")).unwrap();
        let js = workflow_js::compile(include_str!("../../workflow-js/counter.js")).unwrap();
        // The same program with its branches bound to names first: a frontend
        // must take each branch from its argument, not from evaluation order.
        let rhai_bound = workflow_rhai::compile(
            "let a = complete();\nlet b = fail();\non_counter(tool(\"counter\"), a, b);\n",
        )
        .unwrap();
        let js_bound = workflow_js::compile(
            "const a = complete();\nconst b = fail();\nonCounter(tool(\"counter\"), a, b);\n",
        )
        .unwrap();
        for counter in histories() {
            let history = match counter {
                None => History::default(),
                Some(value) => History::new(vec![Record::counter(value)]),
            };
            let step = expected(counter);
            assert_eq!(evaluate_program(&counter_program(), &history), step);
            assert_eq!(evaluate_program(&rhai, &history), step);
            assert_eq!(evaluate_program(&js, &history), step);
            assert_eq!(evaluate_program(&rhai_bound, &history), step);
            assert_eq!(evaluate_program(&js_bound, &history), step);
        }

        // More tool, spawn and sequence programs. Rhai and JS must build the
        // same program as Rust; evaluate_program is shared, so equal programs
        // evaluate alike, and crates/workflow-core/tests/sequence.rs checks
        // what they evaluate to.
        for (rust, rhai, js) in new_construct_programs() {
            assert_eq!(workflow_rhai::compile(rhai).unwrap(), rust);
            assert_eq!(workflow_js::compile(js).unwrap(), rust);
        }
    }

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

    fn new_construct_programs() -> [(WorkflowProgram, &'static str, &'static str); 3] {
        [
            (
                WorkflowProgram {
                    root: Decision::Seq(vec![
                        tool("search", "q"),
                        spawn_agent("helper", "go"),
                        Decision::Complete,
                    ]),
                },
                "seq([tool(\"search\", \"q\"), spawn_agent(\"helper\", \"go\"), complete()]);\n",
                "seq([tool(\"search\", \"q\"), spawnAgent(\"helper\", \"go\"), complete()]);\n",
            ),
            (
                WorkflowProgram {
                    root: Decision::Seq(vec![
                        tool("counter", ""),
                        Decision::OnCounter {
                            missing: Box::new(Decision::Fail),
                            zero: Box::new(Decision::Seq(vec![
                                spawn_agent("helper", "zero"),
                                Decision::Complete,
                            ])),
                            other: Box::new(Decision::Fail),
                        },
                    ]),
                },
                "seq([tool(\"counter\"), on_counter(fail(), seq([spawn_agent(\"helper\", \"zero\"), complete()]), fail())]);\n",
                "seq([tool(\"counter\"), onCounter(fail(), seq([spawnAgent(\"helper\", \"zero\"), complete()]), fail())]);\n",
            ),
            (
                WorkflowProgram {
                    root: Decision::OnCounter {
                        missing: Box::new(Decision::Seq(vec![
                            tool("counter", "reset"),
                            tool("search", "q"),
                        ])),
                        zero: Box::new(Decision::Complete),
                        other: Box::new(Decision::Seq(vec![
                            spawn_agent("helper", "retry"),
                            Decision::Fail,
                        ])),
                    },
                },
                "on_counter(seq([tool(\"counter\", \"reset\"), tool(\"search\", \"q\")]), complete(), seq([spawn_agent(\"helper\", \"retry\"), fail()]));\n",
                "onCounter(seq([tool(\"counter\", \"reset\"), tool(\"search\", \"q\")]), complete(), seq([spawnAgent(\"helper\", \"retry\"), fail()]));\n",
            ),
        ]
    }

    /// The eight records `W.record` in workflow.bend names, in `GolKind`
    /// order. "00" and "count" catch a walk that compares text by prefix.
    fn alphabet() -> [Record; 8] {
        let tool = |name: &str, output: &str| Record::Tool {
            name: name.to_string(),
            output: output.to_string(),
        };
        let spawned = |agent: &str| Record::AgentSpawned {
            agent: agent.to_string(),
        };
        [
            tool("counter", "0"),
            tool("counter", "1"),
            tool("counter", "00"),
            tool("count", "0"),
            tool("search", "found"),
            tool("other", "x"),
            spawned("helper"),
            spawned("other"),
        ]
    }

    /// Every history of length 0 to 3 over the alphabet, in evals.bend's
    /// order: length first; the first record varies slowest.
    fn alphabet_histories() -> Vec<History> {
        let mut all = Vec::new();
        let mut level: Vec<Vec<Record>> = vec![Vec::new()];
        all.extend(level.clone());
        for _ in 0..3 {
            let mut next = Vec::new();
            for record in alphabet() {
                for history in &level {
                    let mut longer = vec![record.clone()];
                    longer.extend(history.iter().cloned());
                    next.push(longer);
                }
            }
            all.extend(next.iter().cloned());
            level = next;
        }
        all.into_iter().map(History::new).collect()
    }

    /// The code evals.bend prints for a command.
    fn code(command: &WorkflowCommand) -> String {
        let hex = |text: &str| -> String { text.bytes().map(|b| format!("{b:02x}")).collect() };
        match command {
            WorkflowCommand::ExecuteTool(tool) => {
                format!("tool.{}.{}", hex(&tool.name), hex(&tool.input))
            }
            WorkflowCommand::SpawnAgent(spawn) => {
                format!("spawn.{}.{}", hex(&spawn.agent), hex(&spawn.input))
            }
            WorkflowCommand::Complete => "c".to_string(),
            WorkflowCommand::Fail => "f".to_string(),
        }
    }

    /// The programs evals.bend evaluates, in its order, as Rust builds them.
    fn eval_programs() -> [WorkflowProgram; 5] {
        let program = |root| WorkflowProgram { root };
        [
            search_program(),
            counter_program(),
            // A counter before on_counter's cursor, behind a newer record.
            program(Decision::Seq(vec![
                tool("counter", ""),
                tool("search", "q"),
                Decision::OnCounter {
                    missing: Box::new(Decision::Fail),
                    zero: Box::new(Decision::Complete),
                    other: Box::new(Decision::Fail),
                },
            ])),
            // A missing arm that records no counter; the sequence goes on.
            program(Decision::Seq(vec![
                Decision::OnCounter {
                    missing: Box::new(tool("search", "q")),
                    zero: Box::new(Decision::Fail),
                    other: Box::new(Decision::Fail),
                },
                spawn_agent("helper", "zero"),
            ])),
            // A missing arm that records the counter and more.
            program(Decision::OnCounter {
                missing: Box::new(Decision::Seq(vec![
                    tool("counter", ""),
                    tool("search", "q"),
                ])),
                zero: Box::new(Decision::Seq(vec![
                    spawn_agent("helper", "zero"),
                    Decision::Complete,
                ])),
                other: Box::new(Decision::Fail),
            }),
        ]
    }

    // Bend evaluates five programs with its own cursor walk (evals.bend). On
    // every history of length 0 to 3 over the eight records, its command
    // must be the one Rust's evaluate_program returns. The programs reach
    // every frame of Bend's walk: a counter before on_counter's cursor, a
    // missing arm with and without a counter, and both arms.
    #[test]
    fn bend_agrees_with_rust() {
        let bend = bend_program().unwrap();
        let staged = stage(&experiment()).unwrap();
        let output = run_sandboxed(
            &bend,
            &["evals.bend"],
            staged_dir(&staged),
            // evals.bend normalizes 2925 walks. Under emulation and a
            // parallel test run that can pass compile's 30 s, so this
            // test-only run gets longer; compile's own limit is unchanged.
            Limits {
                timeout: Duration::from_secs(120),
                ..Limits::default()
            },
        )
        .unwrap();
        assert!(
            output.status.success() && output.stderr.is_empty(),
            "{output:?}"
        );
        let line = output
            .stdout
            .strip_prefix("\"e2 ")
            .and_then(|rest| rest.strip_suffix("\"\n"))
            .expect("an e2 line");
        let codes: Vec<&str> = line.split(' ').collect();
        let histories = alphabet_histories();
        assert_eq!(histories.len(), 585);
        assert_eq!(codes.len(), 5 * 585);
        let mut codes = codes.into_iter();
        for program in eval_programs() {
            for history in &histories {
                let step = evaluate_program(&program, history);
                assert_eq!(step.commands.len(), 1);
                assert_eq!(
                    code(&step.commands[0]),
                    codes.next().unwrap(),
                    "{program:?} on {history:?}"
                );
            }
        }
    }

    fn line(tokens: &str) -> String {
        format!("\"v2 {tokens}\"\n")
    }

    fn hex(text: &str) -> String {
        text.bytes().map(|byte| format!("{byte:02x}")).collect()
    }

    // Every limit and rejection of the v2 parser, at its boundary.
    #[test]
    fn parse_encoding_limits() {
        let reject = |tokens: &str| parse_encoding(&line(tokens)).unwrap_err().to_string();
        let tool_line = |name: &str, input: &str| format!("tool.{}.{}", hex(name), hex(input));

        // Names are 1 to 128 bytes; inputs at most 65536.
        assert!(parse_encoding(&line(&tool_line(&"a".repeat(128), ""))).is_ok());
        let bounds = "a tool or spawn name or input is out of bounds";
        assert_eq!(reject(&tool_line(&"a".repeat(129), "")), bounds);
        assert_eq!(reject(&tool_line("", "x")), bounds);
        assert_eq!(reject(&format!("spawn..{}", hex("x"))), bounds);
        assert!(parse_encoding(&line(&tool_line("a", &"x".repeat(65536)))).is_ok());
        assert_eq!(reject(&tool_line("a", &"x".repeat(65537))), bounds);

        // Hex is lowercase, even-length and UTF-8.
        assert_eq!(reject("tool.4A."), "hex text must be lowercase hex digits");
        assert_eq!(reject("tool.6."), "hex text must have an even length");
        assert_eq!(reject("tool.ff."), "hex text must be UTF-8");
        assert_eq!(
            reject("tool.61"),
            "a tool or spawn token needs a name and an input"
        );

        // Structure.
        assert_eq!(reject("complete fail seq"), "seq must follow a sequence");
        assert_eq!(reject("end seq"), "seq is missing a decision");
        assert_eq!(
            reject("complete complete on_counter"),
            "on_counter is missing a decision"
        );
        assert_eq!(
            reject("complete fail"),
            "bend encoding must leave exactly one program"
        );
        assert_eq!(
            parse_encoding("\"v2\"\n").unwrap_err().to_string(),
            "bend encoding must leave exactly one program"
        );
        assert_eq!(reject("execute"), "unknown token execute");
        assert_eq!(
            parse_encoding(&line("end")).unwrap().root,
            Decision::Seq(Vec::new())
        );

        // At most 2048 tokens after `v2`: on_counter(complete, complete,
        // seq of k completes) is 2k + 4 tokens.
        let tokens = |k: usize| {
            let mut out = vec!["complete"; 2];
            out.extend(std::iter::repeat_n("complete", k));
            out.push("end");
            out.extend(std::iter::repeat_n("seq", k));
            out.push("on_counter");
            out.join(" ")
        };
        assert_eq!(tokens(1022).split(' ').count(), 2048);
        assert!(parse_encoding(&line(&tokens(1022))).is_ok());
        let over = format!("{} complete", tokens(1022));
        assert_eq!(reject(&over), "bend encoding has too many tokens");
    }

    // v2 replaced v1: no v1 line parses, including the old counter line.
    #[test]
    fn bend_v1_line_rejected() {
        for missing in ["execute", "complete", "fail"] {
            for zero in ["execute", "complete", "fail"] {
                for other in ["execute", "complete", "fail"] {
                    let line = format!("\"v1 on_counter {missing} {zero} {other}\"\n");
                    assert_eq!(
                        parse_encoding(&line).unwrap_err().to_string(),
                        "expected a v2 token line",
                        "{line}"
                    );
                }
            }
        }
    }

    #[test]
    fn the_reference_line_parses_to_the_reference_program() {
        let line = "\"v2 tool.736561726368.71 tool.636f756e746572. spawn.68656c706572.7a65726f complete end seq seq fail on_counter end seq seq\"\n";
        assert_eq!(parse_encoding(line).unwrap(), search_program());
        assert_eq!(super::encode_line(&search_program()), line);
    }

    #[test]
    fn open_proof_is_rejected() {
        let dir = scratch();
        copy_experiment(&dir);
        fs::write(
            dir.join("PROOF.bend"),
            "import Base\nimport ./LAWS.bend as Laws\n",
        )
        .unwrap();
        let error = compile(&dir).unwrap_err();
        assert!(error.to_string().contains("TODO"), "{}", error);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn carriage_return_cannot_hide_an_io_main() {
        let dir = scratch();
        copy_experiment(&dir);
        let marker = dir.join("owned");
        let workflow = fs::read_to_string(dir.join("workflow.bend")).unwrap();
        let head = workflow.rsplit_once("def main() -> String:\n").unwrap().0;
        let mut source = head.to_string();
        source.push_str("def helper() -> String:\n  \"x\ndef main() -> String:\ny\"\r");
        source.push_str(
            "def main() -> IO(Unit):\n  do IO<Unit>:\n    file : File <- IO.try(File, File.open(\"",
        );
        source.push_str(&marker.display().to_string());
        source.push_str("\", \"w\"))\n    wrote : File & Result<&1, &1, U32 & String, Unit> <- File.write(file, \"owned\")\n    IO.print(\"\\\"v1 on_counter execute complete fail\\\"\")\n");
        assert!(source.contains('\r'));
        assert!(
            !source.lines().any(|line| line == "def main() -> IO(Unit):"),
            "the IO main must stay hidden from a newline split"
        );
        fs::write(dir.join("workflow.bend"), &source).unwrap();
        let error = compile(&dir).unwrap_err();
        assert!(
            error.to_string().contains("main must return String"),
            "{error}"
        );
        assert!(!marker.exists(), "compile ran the hidden IO main");
        let _ = fs::remove_dir_all(dir);
    }

    // An IO main in either file whose main is run is refused before bend
    // starts.
    #[test]
    fn io_main_is_not_executed() {
        for file in ["workflow.bend", "evals.bend"] {
            let dir = scratch();
            copy_experiment(&dir);
            fs::write(
                dir.join(file),
                "import Base\ndef main() -> IO(Unit):\n  do IO<Unit>:\n    IO.print(\"EXECUTED\")\n",
            )
            .unwrap();
            let error = compile(&dir).unwrap_err();
            assert_eq!(
                error.to_string(),
                format!("{file} main must return String; IO is not executed")
            );
            let _ = fs::remove_dir_all(dir);
        }
    }

    #[test]
    fn encoding_must_match_the_versioned_line() {
        let dir = scratch();
        copy_experiment(&dir);
        let workflow = fs::read_to_string(dir.join("workflow.bend")).unwrap();
        let replaced = workflow.replace(
            "def main() -> String:\n  encode_line(search_program())\n",
            "def main() -> String:\n  \"nope\"\n",
        );
        assert_ne!(replaced, workflow);
        fs::write(dir.join("workflow.bend"), replaced).unwrap();
        let error = compile(&dir).unwrap_err();
        assert_eq!(error.to_string(), "expected a v2 token line");
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn timeout_kills_the_process_group() {
        let dir = scratch();
        let error = run_sandboxed(
            Path::new("/bin/sleep"),
            &["30"],
            &dir,
            Limits {
                timeout: Duration::from_millis(200),
                stdout: 64,
                stderr: 64,
            },
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "bend process timed out");
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn output_limit_kills_a_flood() {
        let dir = scratch();
        let error = run_sandboxed(
            Path::new("/usr/bin/python3"),
            &["-c", "print('x' * 100000)"],
            &dir,
            Limits {
                timeout: Duration::from_secs(5),
                stdout: 128,
                stderr: 128,
            },
        )
        .unwrap_err();
        assert_eq!(error.to_string(), "bend output exceeded the limit");
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn version_constant_is_the_pinned_release() {
        assert_eq!(crate::boundary::BEND_VERSION, "bend 2.0.28");
    }
}
