//! A run's journal: one file of framed records.
//!
//! A frame is `[len u32][kind u8][payload][crc32]`, little-endian. `len`
//! counts the kind byte and the payload; the CRC-32 covers the length, the
//! kind and the payload. The file starts with one `Start` frame that names
//! the format. Each commit appends one frame with one `write_all`, so a
//! process killed mid-commit leaves a torn tail: a strict prefix of its last
//! frame. Nothing is claimed past SIGKILL; the journal never calls fsync.
use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;

use workflow_core::{History, Record};

/// The `Start` payload: the format's name, then its version.
const MAGIC: &[u8] = b"gol-journal";
const VERSION: u32 = 1;

const START: u8 = 0;
const TOOL_RESULT: u8 = 1;
const AGENT_SPAWNED: u8 = 2;

/// The largest `len` a frame holds: 16 MiB of kind and payload.
const MAX_LEN: u32 = 16 << 20;

pub struct Journal {
    file: File,
}

impl Journal {
    /// Opens the journal at `path`, creating it with its `Start` frame if it
    /// is empty. A torn tail is cut off. Anything else that does not read as
    /// frames (a bad checksum, an unknown kind, a file from before framing)
    /// is `InvalidData`, and the file is left as it is.
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let whole = decode(&bytes)?.whole;
        if whole < bytes.len() {
            file.set_len(whole as u64)?;
        }
        let mut journal = Self { file };
        if whole == 0 {
            journal.append(&start_frame())?;
        }
        Ok(journal)
    }

    /// Appends `record` as one frame. A write that fails part way is rolled
    /// back, so the journal still ends on a whole frame.
    pub fn commit(&mut self, record: &Record) -> io::Result<()> {
        self.append(&frame(record)?)
    }

    /// The records committed so far, in order.
    pub fn history(&self) -> io::Result<History> {
        let mut file = &self.file;
        let mut bytes = Vec::new();
        file.seek(SeekFrom::Start(0))?;
        file.read_to_end(&mut bytes)?;
        Ok(History::new(decode(&bytes)?.records))
    }

    fn append(&mut self, frame: &[u8]) -> io::Result<()> {
        let end = self.file.metadata()?.len();
        if let Err(error) = self.file.write_all(frame) {
            if self.file.metadata()?.len() != end {
                self.file.set_len(end)?;
            }
            return Err(error);
        }
        Ok(())
    }
}

/// `record` as the frame a commit appends. A record longer than a frame
/// holds is `InvalidInput`.
pub fn frame(record: &Record) -> io::Result<Vec<u8>> {
    let mut payload = Vec::new();
    let kind = match record {
        Record::Tool { name, output } => {
            put_string(&mut payload, name)?;
            put_string(&mut payload, output)?;
            TOOL_RESULT
        }
        Record::AgentSpawned { agent } => {
            put_string(&mut payload, agent)?;
            AGENT_SPAWNED
        }
    };
    if 1 + payload.len() > MAX_LEN as usize {
        return Err(too_long());
    }
    Ok(framed(kind, &payload))
}

/// The `Start` frame every journal begins with.
pub fn start_frame() -> Vec<u8> {
    let mut payload = MAGIC.to_vec();
    payload.extend_from_slice(&VERSION.to_le_bytes());
    framed(START, &payload)
}

/// `kind` and `payload` as one frame; the caller has checked that they fit.
fn framed(kind: u8, payload: &[u8]) -> Vec<u8> {
    let len = (1 + payload.len()) as u32;
    let mut frame = len.to_le_bytes().to_vec();
    frame.push(kind);
    frame.extend_from_slice(payload);
    let crc = crc32fast::hash(&frame);
    frame.extend_from_slice(&crc.to_le_bytes());
    frame
}

fn put_string(payload: &mut Vec<u8>, text: &str) -> io::Result<()> {
    let len = u32::try_from(text.len()).map_err(|_| too_long())?;
    payload.extend_from_slice(&len.to_le_bytes());
    payload.extend_from_slice(text.as_bytes());
    Ok(())
}

/// A journal's records and the length of its whole frames, `Start`
/// included. Bytes past `whole` are a torn tail.
struct Decoded {
    records: Vec<Record>,
    whole: usize,
}

fn decode(bytes: &[u8]) -> io::Result<Decoded> {
    let start = start_frame();
    if bytes.len() < start.len() && start.starts_with(bytes) {
        // Empty, or a `Start` frame that was cut short.
        return Ok(Decoded {
            records: Vec::new(),
            whole: 0,
        });
    }
    if !bytes.starts_with(&start) {
        return Err(invalid("not a gol-journal v1 file"));
    }
    let mut records = Vec::new();
    let mut whole = start.len();
    while let Some((record, size)) = next_frame(&bytes[whole..])? {
        records.push(record);
        whole += size;
    }
    Ok(Decoded { records, whole })
}

/// The record in the first frame of `bytes` and the frame's size, or `None`
/// when `bytes` is empty or a strict prefix of a frame.
fn next_frame(bytes: &[u8]) -> io::Result<Option<(Record, usize)>> {
    let Some((len, _)) = take_u32(bytes) else {
        return Ok(None);
    };
    if len == 0 || len > MAX_LEN {
        return Err(invalid("record length"));
    }
    let Some((body, rest)) = bytes.split_at_checked(4 + len as usize) else {
        return Ok(None);
    };
    let Some((crc, _)) = take_u32(rest) else {
        return Ok(None);
    };
    if crc32fast::hash(body) != crc {
        return Err(invalid("record checksum"));
    }
    // `len` counts the kind byte, so the body holds it.
    let record = record(body[4], &body[5..])?;
    Ok(Some((record, body.len() + 4)))
}

fn record(kind: u8, payload: &[u8]) -> io::Result<Record> {
    let (record, rest) = match kind {
        TOOL_RESULT => {
            let (name, rest) = take_string(payload)?;
            let (output, rest) = take_string(rest)?;
            (Record::Tool { name, output }, rest)
        }
        AGENT_SPAWNED => {
            let (agent, rest) = take_string(payload)?;
            (Record::AgentSpawned { agent }, rest)
        }
        _ => return Err(invalid("record kind")),
    };
    if !rest.is_empty() {
        return Err(invalid("record payload"));
    }
    Ok(record)
}

fn take_string(bytes: &[u8]) -> io::Result<(String, &[u8])> {
    let (len, rest) = take_u32(bytes).ok_or_else(|| invalid("record payload"))?;
    let (text, rest) = rest
        .split_at_checked(len as usize)
        .ok_or_else(|| invalid("record payload"))?;
    let text = std::str::from_utf8(text).map_err(|_| invalid("record payload"))?;
    Ok((text.to_string(), rest))
}

fn take_u32(bytes: &[u8]) -> Option<(u32, &[u8])> {
    let (head, rest) = bytes.split_first_chunk::<4>()?;
    Some((u32::from_le_bytes(*head), rest))
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message)
}

fn too_long() -> io::Error {
    io::Error::new(ErrorKind::InvalidInput, "record longer than a frame holds")
}

#[cfg(test)]
mod tests {
    use super::{frame, Journal};
    use std::fs::{self, File};
    use std::path::PathBuf;
    use workflow_core::{History, Record};

    fn scratch(case: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("gol-journal-{}-{case}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("log");
        let _ = fs::remove_file(&path);
        path
    }

    #[test]
    fn held_bytes_are_not_a_hit_before_ok() {
        let path = scratch("held");
        let seed = Record::counter(1);
        let held = Record::counter(2);
        let held_frame = frame(&held).unwrap();
        let mut journal = Journal::open(&path).unwrap();
        journal.commit(&seed).unwrap();

        let before = fs::read(&path).unwrap();
        assert!(before
            .windows(held_frame.len())
            .all(|window| window != held_frame));
        assert_eq!(journal.history().unwrap(), History::new(vec![seed.clone()]));

        journal.commit(&held).unwrap();
        let after = fs::read(&path).unwrap();
        assert_eq!(after.len(), before.len() + held_frame.len());
        assert_eq!(&after[..before.len()], before);
        assert_eq!(&after[before.len()..], held_frame);
        assert_eq!(journal.history().unwrap(), History::new(vec![seed, held]));
    }

    #[test]
    fn failed_write_does_not_advance() {
        let dir = std::env::temp_dir().join(format!("gol-journal-{}-fail", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let before = fs::metadata(&dir).unwrap().len();
        let mut journal = Journal {
            file: File::open(&dir).unwrap(),
        };
        assert!(journal.commit(&Record::counter(1)).is_err());
        assert_eq!(fs::metadata(&dir).unwrap().len(), before);
    }

    // A record longer than a frame holds is refused before anything is
    // written, so it cannot leave a frame the reader would refuse.
    #[test]
    fn a_record_longer_than_a_frame_is_not_written() {
        let path = scratch("long");
        let mut journal = Journal::open(&path).unwrap();
        let before = fs::read(&path).unwrap();
        let long = Record::AgentSpawned {
            agent: "a".repeat(16 << 20),
        };
        let error = journal.commit(&long).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(fs::read(&path).unwrap(), before);
        let longest = Record::AgentSpawned {
            agent: "a".repeat((16 << 20) - 5),
        };
        journal.commit(&longest).unwrap();
        assert_eq!(
            Journal::open(&path).unwrap().history().unwrap(),
            History::new(vec![longest])
        );
        fs::remove_file(&path).unwrap();
    }
}
