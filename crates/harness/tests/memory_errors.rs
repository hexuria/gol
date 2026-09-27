//! A memory store that fails ends the run with RunFailed{Infrastructure}; the
//! memory effect is not recorded as done.
use harness::{
    run_to_completion, Driver, InMemory, Memory, ScriptedDecider, StoreError, UnavailableModel,
};
use protocol::{
    AgentId, Capability, CredentialSource, Effect, EventPayload, ExecutionPlacement, FailureClass,
    HarnessState, MemoryScope, ModelProvider, Owner, RunSpec, WorkModel,
};

struct Down;

impl Memory for Down {
    fn read(&self, _scope: MemoryScope, _key: &str) -> Result<Option<String>, StoreError> {
        Err(StoreError::new("connection refused"))
    }

    fn write(&mut self, _scope: MemoryScope, _key: &str, _value: &str) -> Result<(), StoreError> {
        Err(StoreError::new("connection refused"))
    }
}

fn spec() -> RunSpec {
    RunSpec::builder()
        .owner(Owner::new("https://issuer.test", "user-1", "tenant-1"))
        .agent(AgentId::new(), "1")
        .input("hello")
        .placement(ExecutionPlacement::Local)
        .work_model(WorkModel {
            provider: ModelProvider::OpenAI,
            model_name: "gpt-test".to_string(),
            credential: CredentialSource::PlatformGateway,
        })
        .capabilities(vec![
            Capability::new("memory.read"),
            Capability::new("memory.write"),
        ])
        .build()
}

fn run(effect: Effect, memory: &mut dyn Memory) -> Driver {
    let mut driver = Driver::boot(spec()).unwrap();
    let mut decider = ScriptedDecider::new([
        effect,
        Effect::Complete {
            outcome: "done".to_string(),
        },
    ]);
    run_to_completion(&mut driver, &mut decider, &[], &UnavailableModel, memory).unwrap();
    driver
}

fn read() -> Effect {
    Effect::MemoryRead {
        scope: MemoryScope::Run,
        key: "k".to_string(),
    }
}

fn write() -> Effect {
    Effect::MemoryWrite {
        scope: MemoryScope::Run,
        key: "k".to_string(),
        value: "v".to_string(),
    }
}

fn memory_events(driver: &Driver) -> usize {
    driver
        .events()
        .iter()
        .filter(|event| {
            matches!(
                event.payload,
                EventPayload::MemoryRead { .. } | EventPayload::MemoryWritten { .. }
            )
        })
        .count()
}

#[test]
fn a_memory_read_error_fails_the_run() {
    let driver = run(read(), &mut Down);
    assert_eq!(
        driver.state().harness,
        HarnessState::Failed {
            class: FailureClass::Infrastructure,
            message: "memory read: connection refused".to_string(),
        }
    );
    assert_eq!(memory_events(&driver), 0);
}

#[test]
fn a_memory_write_error_fails_the_run() {
    let driver = run(write(), &mut Down);
    assert_eq!(
        driver.state().harness,
        HarnessState::Failed {
            class: FailureClass::Infrastructure,
            message: "memory write: connection refused".to_string(),
        }
    );
    assert_eq!(memory_events(&driver), 0);
}

#[test]
fn a_working_memory_records_the_effect_and_completes() {
    let mut memory = InMemory::default();
    let driver = run(write(), &mut memory);
    assert_eq!(memory_events(&driver), 1);
    assert_eq!(
        driver.state().harness,
        HarnessState::Completed {
            outcome: "done".to_string()
        }
    );
    assert_eq!(
        memory.read(MemoryScope::Run, "k"),
        Ok(Some("v".to_string()))
    );
}
