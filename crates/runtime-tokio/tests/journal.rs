//! The framed journal (C5). Each record is one frame,
//! `[len u32][kind u8][payload][crc32]`, after a `Start` frame that names the
//! format. Opening a journal cuts off a torn tail (an incomplete last frame)
//! and refuses anything else it cannot read: a bad checksum, an unknown
//! kind, a file from before framing.
use std::cell::Cell;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use proptest::prelude::*;
use runtime_tokio::{frame, replay, start_frame, Journal};
use workflow_core::{
    counter_program, evaluate_program, AgentSpec, Decision, History, Record, ToolSpec,
    WorkflowCommand, WorkflowContext, WorkflowDriver, WorkflowProgram, WorkflowStep,
};

fn scratch(case: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("gol-journal-{}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join(case);
    let _ = fs::remove_file(&path);
    path
}

/// Commits `records` to a new journal at `path` and returns its bytes.
fn written(path: &Path, records: &[Record]) -> Vec<u8> {
    let mut journal = Journal::open(path).unwrap();
    for record in records {
        journal.commit(record).unwrap();
    }
    fs::read(path).unwrap()
}

fn spawned(agent: &str) -> Record {
    Record::AgentSpawned {
        agent: agent.to_string(),
    }
}

fn tool(name: &str, output: &str) -> Record {
    Record::Tool {
        name: name.to_string(),
        output: output.to_string(),
    }
}

#[test]
fn a_new_journal_starts_with_its_header_and_appends_whole_frames() {
    let path = scratch("new");
    let mut journal = Journal::open(&path).unwrap();
    assert_eq!(fs::read(&path).unwrap(), start_frame());
    assert_eq!(journal.history().unwrap(), History::default());
    journal.commit(&Record::counter(7)).unwrap();
    let mut expected = start_frame();
    expected.extend(frame(&Record::counter(7)).unwrap());
    assert_eq!(fs::read(&path).unwrap(), expected);
    assert_eq!(
        Journal::open(&path).unwrap().history().unwrap(),
        History::new(vec![Record::counter(7)])
    );
}

// An agent-spawned marker and a counter result of 0 were the same bytes
// (0i64) before framing, so a spawn read back as a counter of 0.
#[test]
fn start_marker_is_not_counter_zero() {
    let spawn = scratch("spawn");
    let zero = scratch("zero");
    written(&spawn, &[spawned("child")]);
    written(&zero, &[Record::counter(0)]);
    let spawn_history = Journal::open(&spawn).unwrap().history().unwrap();
    let zero_history = Journal::open(&zero).unwrap().history().unwrap();
    assert_eq!(spawn_history, History::new(vec![spawned("child")]));
    assert_eq!(spawn_history.counter(), None);
    assert_eq!(zero_history.counter(), Some("0"));
    assert_ne!(spawn_history, zero_history);
}

/// Spawns a child agent, then branches on a counter.
struct SpawnThenCount;

impl WorkflowDriver for SpawnThenCount {
    fn evaluate(&self, _ctx: &WorkflowContext, history: &History) -> WorkflowStep {
        let program = WorkflowProgram {
            root: Decision::Seq(vec![
                Decision::SpawnAgent {
                    agent: "child".to_string(),
                    input: String::new(),
                },
                counter_program().root,
            ]),
        };
        evaluate_program(&program, history)
    }
}

// The host marked a spawn with the bytes of a counter result of 0, so the
// spawn replayed as a counter record and the workflow failed at it.
#[test]
fn a_workflow_that_spawns_then_counts_completes() {
    let path = scratch("spawn-then-count");
    let mut journal = Journal::open(&path).unwrap();
    let calls = Cell::new(0);
    let mut stand_in = || {
        calls.set(calls.get() + 1);
        0
    };
    let mut commands = Vec::new();
    for _ in 0..3 {
        let (step, _) = replay(
            &SpawnThenCount,
            &WorkflowContext,
            &mut journal,
            &mut stand_in,
        )
        .unwrap();
        commands.push(step.commands);
    }
    assert_eq!(
        commands,
        [
            vec![WorkflowCommand::SpawnAgent(AgentSpec::new("child", ""))],
            vec![WorkflowCommand::ExecuteTool(ToolSpec::new("counter", ""))],
            vec![WorkflowCommand::Complete],
        ]
    );
    assert_eq!(calls.get(), 1);
    assert_eq!(
        journal.history().unwrap(),
        History::new(vec![spawned("child"), Record::counter(0)])
    );
}

/// Cuts a journal holding `records` to its first `cut` bytes, opens it, and
/// checks that it replays the longest prefix of whole records and that the
/// file then holds exactly their frames.
fn assert_cut_replays_to_a_whole_prefix(case: &str, records: &[Record], cut: usize) {
    let full = written(&scratch(&format!("{case}-full")), records);
    let path = scratch(&format!("{case}-cut"));
    fs::write(&path, &full[..cut]).unwrap();
    let journal = Journal::open(&path).unwrap();
    let mut end = start_frame().len();
    let mut whole = 0;
    for record in records {
        let next = end + frame(record).unwrap().len();
        if next > cut {
            break;
        }
        end = next;
        whole += 1;
    }
    assert_eq!(
        journal.history().unwrap(),
        History::new(records[..whole].to_vec()),
        "cut at {cut}"
    );
    // A torn header is rewritten whole; a torn record is cut off.
    assert_eq!(
        fs::read(&path).unwrap(),
        full[..end].to_vec(),
        "cut at {cut}"
    );
}

#[test]
fn every_truncation_prefix_replays_to_prior_or_full_state() {
    let records = [
        Record::counter(1),
        spawned("child"),
        tool("echo", "héllo wörld"),
        Record::counter(-2),
    ];
    for cut in 0..=journal_bytes(&records).len() {
        assert_cut_replays_to_a_whole_prefix("prefix", &records, cut);
    }
}

// A checksum that does not match, in a middle record or in a complete last
// record, is not a torn write: opening refuses the journal and leaves it
// untouched (owner decision 2A for C5).
#[test]
fn corrupt_crc_is_invalid_data() {
    let records = [Record::counter(1), tool("echo", "hi"), Record::counter(2)];
    let full = written(&scratch("crc-full"), &records);
    let start = start_frame().len();
    let first = frame(&records[0]).unwrap().len();
    let second = frame(&records[1]).unwrap().len();
    let middle_payload = start + first + 6;
    let last_payload = start + first + second + 6;
    for at in [middle_payload, last_payload, full.len() - 1] {
        let mut corrupt = full.clone();
        corrupt[at] ^= 0x40;
        let path = scratch("crc-corrupt");
        fs::write(&path, &corrupt).unwrap();
        let error = Journal::open(&path).map(|_| ()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData, "byte {at}");
        assert_eq!(fs::read(&path).unwrap(), corrupt, "byte {at}");
    }
}

// A journal from before framing (raw 8-byte counters) is refused, not read
// as a torn tail and truncated (owner decision 3A for C5).
#[test]
fn unframed_journal_is_refused() {
    for unframed in [
        7i64.to_le_bytes().to_vec(),
        [7i64.to_le_bytes(), 9i64.to_le_bytes()].concat(),
        b"not a journal".to_vec(),
    ] {
        let path = scratch("unframed");
        fs::write(&path, &unframed).unwrap();
        let error = Journal::open(&path).map(|_| ()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert_eq!(fs::read(&path).unwrap(), unframed);
    }
}

/// A frame with a correct checksum around any kind and payload.
fn raw_frame(kind: u8, payload: &[u8]) -> Vec<u8> {
    let len = u32::try_from(1 + payload.len()).unwrap();
    let mut bytes = len.to_le_bytes().to_vec();
    bytes.push(kind);
    bytes.extend_from_slice(payload);
    let crc = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    bytes
}

// The format byte for byte, against a frame built here: a length that
// counts the kind and payload, the kind, strings with their lengths in
// front, and a CRC-32 of all of it, little-endian throughout.
#[test]
fn frames_hold_length_kind_payload_and_checksum() {
    assert_eq!(start_frame(), raw_frame(0, b"gol-journal\x01\0\0\0"));
    assert_eq!(
        frame(&Record::counter(-7)).unwrap(),
        raw_frame(1, b"\x07\0\0\0counter\x02\0\0\0-7")
    );
    assert_eq!(
        frame(&spawned("child")).unwrap(),
        raw_frame(2, b"\x05\0\0\0child")
    );
}

// A record the reader does not know, a second header, a payload that does
// not decode, or a length out of bounds is refused, even with a good
// checksum.
#[test]
fn a_well_checksummed_record_it_cannot_read_is_invalid_data() {
    let empty = {
        let mut bytes = 0u32.to_le_bytes().to_vec();
        let crc = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&crc.to_le_bytes());
        bytes
    };
    let cases = [
        raw_frame(9, b""),
        start_frame()[..].to_vec(),
        empty,
        raw_frame(1, &[0xff, 0xff, 0xff, 0xff]),
        raw_frame(2, &[1, 0, 0, 0, 0xff]),
        raw_frame(2, &[0, 0, 0, 0, 0]),
        {
            let mut huge = (u32::MAX).to_le_bytes().to_vec();
            huge.extend_from_slice(&[1; 16]);
            huge
        },
    ];
    for case in cases {
        let path = scratch("unreadable");
        let mut bytes = start_frame();
        bytes.extend_from_slice(&case);
        fs::write(&path, &bytes).unwrap();
        let error = Journal::open(&path).map(|_| ()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData, "{case:?}");
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
}

fn record() -> impl Strategy<Value = Record> {
    prop_oneof![
        any::<i64>().prop_map(Record::counter),
        (".{0,12}", ".{0,24}").prop_map(|(name, output)| Record::Tool { name, output }),
        ".{0,12}".prop_map(|agent| Record::AgentSpawned { agent }),
    ]
}

/// The bytes of a journal holding `records`.
fn journal_bytes(records: &[Record]) -> Vec<u8> {
    let mut bytes = start_frame();
    for record in records {
        bytes.extend(frame(record).unwrap());
    }
    bytes
}

/// Opens a journal written as `bytes`. What it reads is a prefix of them,
/// exactly the frames of the records it replays; what it refuses is
/// `InvalidData` and stays as it was. Returns the records it read.
fn open_reads_a_prefix_or_refuses(case: &str, bytes: &[u8]) -> Option<Vec<Record>> {
    let path = scratch(case);
    fs::write(&path, bytes).unwrap();
    match Journal::open(&path) {
        Ok(journal) => {
            let kept = fs::read(&path).unwrap();
            assert!(bytes.starts_with(&kept));
            let records = journal.history().unwrap().records;
            assert_eq!(kept, journal_bytes(&records));
            Some(records)
        }
        Err(error) => {
            assert_eq!(error.kind(), ErrorKind::InvalidData);
            assert_eq!(fs::read(&path).unwrap(), bytes);
            None
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    // Any journal, cut anywhere, replays to the longest prefix of its whole
    // records.
    #[test]
    fn any_truncation_replays_to_a_whole_prefix(
        records in prop::collection::vec(record(), 0..6),
        cut in any::<prop::sample::Index>(),
    ) {
        let len = journal_bytes(&records).len();
        assert_cut_replays_to_a_whole_prefix("prop", &records, cut.index(len + 1));
    }

    // Whatever follows a header, opening never panics: it reads a prefix
    // or refuses the file.
    #[test]
    fn any_bytes_after_a_header_open_or_are_refused(
        tail in prop::collection::vec(any::<u8>(), 0..64),
    ) {
        let mut bytes = start_frame();
        bytes.extend_from_slice(&tail);
        open_reads_a_prefix_or_refuses("prop-bytes", &bytes);
    }

    // A changed byte is refused, or cuts the journal short where a changed
    // length runs past its end; it never reads as a record the journal did
    // not hold.
    #[test]
    fn a_changed_byte_never_reads_as_another_record(
        records in prop::collection::vec(record(), 1..5),
        at in any::<prop::sample::Index>(),
        change in 1..=u8::MAX,
    ) {
        let mut bytes = journal_bytes(&records);
        let header = start_frame().len();
        let at = header + at.index(bytes.len() - header);
        bytes[at] ^= change;
        if let Some(read) = open_reads_a_prefix_or_refuses("prop-changed", &bytes) {
            prop_assert!(read.len() < records.len());
            prop_assert_eq!(read.as_slice(), &records[..read.len()]);
        }
    }
}
