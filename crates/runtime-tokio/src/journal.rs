use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

pub struct Journal {
    file: File,
}

impl Journal {
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { file })
    }

    pub fn commit(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        let end = self.file.metadata()?.len();
        if let Err(error) = self.file.write_all(bytes) {
            if self.file.metadata()?.len() != end {
                self.file.set_len(end)?;
            }
            return Err(error);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Journal;
    use std::fs::{self, File, OpenOptions};
    use std::io::Read;
    use std::path::Path;

    fn read_path(path: &Path) -> Vec<u8> {
        let mut file = OpenOptions::new().read(true).open(path).unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();
        buf
    }

    #[test]
    fn held_bytes_are_not_a_hit_before_ok() {
        let dir = std::env::temp_dir().join(format!("gol-journal-{}-held", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("log");
        let seed = b"seed";
        let held = b"held-result";
        assert!(!held.is_empty());
        fs::write(&path, seed).unwrap();

        let before = read_path(&path);
        assert_eq!(before, seed);
        assert!(before.windows(held.len()).all(|window| window != held));

        let mut journal = Journal::open(&path).unwrap();
        assert_eq!(read_path(&path), seed);

        journal.commit(held).unwrap();
        let after = read_path(&path);
        assert_eq!(after.len(), seed.len() + held.len());
        assert_eq!(&after[..seed.len()], seed);
        assert_eq!(&after[seed.len()..], held);
    }

    #[test]
    fn failed_write_does_not_advance() {
        let dir = std::env::temp_dir().join(format!("gol-journal-{}-fail", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let before = fs::metadata(&dir).unwrap().len();
        let mut journal = Journal {
            file: File::open(&dir).unwrap(),
        };
        assert!(journal.commit(b"held-result").is_err());
        assert_eq!(fs::metadata(&dir).unwrap().len(), before);
    }
}
