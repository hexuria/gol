mod boundary;

use std::path::Path;

use workflow_core::{
    evaluate_program, Decision, History, ToolName, WorkflowContext, WorkflowDriver,
    WorkflowProgram, WorkflowStep,
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
    let encoded = run_sandboxed(&bend, &["counter.bend"], dir, Limits::default())?;
    if !encoded.status.success() {
        return Err(FrontendError::Encoding(failure_text(&encoded)));
    }
    if !encoded.stderr.is_empty() {
        return Err(FrontendError::Encoding(encoded.stderr.trim().to_string()));
    }
    parse_encoding(&encoded.stdout)
}

fn parse_encoding(stdout: &str) -> Result<WorkflowProgram, FrontendError> {
    let Some(line) = stdout.strip_suffix('\n') else {
        return Err(FrontendError::Encoding(
            "bend encoding is missing its trailing newline".to_string(),
        ));
    };
    if line.contains('\n') {
        return Err(FrontendError::Encoding(
            "bend encoding must be one line".to_string(),
        ));
    }
    let Some(inner) = line
        .strip_prefix('"')
        .and_then(|text| text.strip_suffix('"'))
    else {
        return Err(FrontendError::Encoding(
            "bend encoding must be a quoted token line".to_string(),
        ));
    };
    if inner
        .chars()
        .any(|ch| ch == '\\' || ch == '"' || (ch.is_whitespace() && ch != ' '))
    {
        return Err(FrontendError::Encoding(
            "bend encoding has an unsupported character".to_string(),
        ));
    }
    let parts: Vec<&str> = inner.split(' ').collect();
    if parts.len() != 5 || parts[0] != "v1" || parts[1] != "on_counter" {
        return Err(FrontendError::Encoding(
            "expected v1 on_counter <missing> <zero> <other>".to_string(),
        ));
    }
    Ok(WorkflowProgram {
        root: Decision::OnCounter {
            missing: Box::new(parse_arm(parts[2])?),
            zero: Box::new(parse_arm(parts[3])?),
            other: Box::new(parse_arm(parts[4])?),
        },
    })
}

fn parse_arm(token: &str) -> Result<Decision, FrontendError> {
    match token {
        "execute" => Ok(Decision::Tool(ToolName::Counter)),
        "complete" => Ok(Decision::Complete),
        "fail" => Ok(Decision::Fail),
        _ => Err(FrontendError::Encoding(format!("unknown command {token}"))),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use harness_core::transition;
    use workflow_core::{
        counter_program, evaluate_program, History, ToolSpec, WaitCondition, WorkflowCommand,
        WorkflowContext, WorkflowStep,
    };

    use super::{compile, BendDriver};
    use crate::boundary::{run_sandboxed, Limits};

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
            None => WorkflowCommand::ExecuteTool(ToolSpec { name: "counter" }),
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
        for name in ["counter.bend", "LAWS.bend", "PROOF.bend"] {
            fs::copy(experiment().join(name), dir.join(name)).unwrap();
        }
    }

    #[test]
    fn histories_agree_across_rust_rhai_js_and_bend() {
        let bend = compile(&experiment()).unwrap();
        assert_eq!(bend, counter_program());
        let rhai =
            workflow_rhai::compile(include_str!("../../workflow-rhai/counter.rhai")).unwrap();
        let js = workflow_js::compile(include_str!("../../workflow-js/counter.js")).unwrap();
        let ctx = WorkflowContext;
        let driver = BendDriver::new(bend.clone());
        for counter in histories() {
            let history = History { counter };
            let step = expected(counter);
            assert_eq!(evaluate_program(&counter_program(), &history), step);
            assert_eq!(evaluate_program(&rhai, &history), step);
            assert_eq!(evaluate_program(&js, &history), step);
            assert_eq!(evaluate_program(&bend, &history), step);
            assert_eq!(
                transition(&driver, &ctx, &history),
                transition(&workflow_core::CounterBranch, &ctx, &history)
            );
        }
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
    fn io_main_is_not_executed() {
        let dir = scratch();
        copy_experiment(&dir);
        fs::write(
            dir.join("counter.bend"),
            "import Base\ndef main() -> IO(Unit):\n  do IO<Unit>:\n    IO.print(\"EXECUTED\")\n",
        )
        .unwrap();
        let error = compile(&dir).unwrap_err();
        assert_eq!(
            error.to_string(),
            "counter.bend main must return String; IO is not executed"
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn encoding_must_match_the_versioned_line() {
        let dir = scratch();
        copy_experiment(&dir);
        let counter = fs::read_to_string(dir.join("counter.bend")).unwrap();
        let replaced = counter.replace(
            "def main() -> String:\n  encode(counter_program())\n",
            "def main() -> String:\n  \"nope\"\n",
        );
        assert_ne!(replaced, counter);
        fs::write(dir.join("counter.bend"), replaced).unwrap();
        let error = compile(&dir).unwrap_err();
        assert!(error.to_string().contains("v1 on_counter"), "{}", error);
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
        assert_eq!(crate::boundary::BEND_VERSION, "bend 2.0.27");
    }
}
