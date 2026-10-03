use bytes::{Buf, BufMut, BytesMut};
use ryme_error::{Result, RymeError};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const WAL_MAGIC: u32 = 0x52594D45;
pub const WAL_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub lsn: u64,
    pub commit_ts: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub struct Wal {
    dir: PathBuf,
    segment_size_bytes: u64,
    active_id: u64,
    active: File,
    next_lsn: u64,
}

impl Wal {
    pub fn open(dir: &Path, segment_size_bytes: u64) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let active_id = latest_segment_id(dir)?;
        let path = dir.join(segment_name(active_id));
        let mut active = OpenOptions::new().create(true).read(true).append(true).open(&path)?;
        let next_lsn = recover_next_lsn(&mut active)?;
        Ok(Self { dir: dir.to_path_buf(), segment_size_bytes, active_id, active, next_lsn })
    }

    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    pub fn append(&mut self, commit_ts: u64, payload: &[u8]) -> Result<WalRecord> {
        if payload.len() > u32::MAX as usize {
            return Err(RymeError::InvalidArgument(String::from("payload too large")));
        }
        let lsn = self.next_lsn;
        let frame = encode_frame(lsn, commit_ts, payload);
        let active_len = self.active.metadata()?.len();
        if active_len + frame.len() as u64 > self.segment_size_bytes {
            self.rotate()?;
        }
        self.active.write_all(&frame)?;
        self.next_lsn += 1;
        Ok(WalRecord { lsn, commit_ts, payload: payload.to_vec() })
    }

    pub fn sync(&mut self) -> Result<()> {
        self.active.sync_data()?;
        Ok(())
    }

    pub fn read_all(dir: &Path) -> Result<Vec<WalRecord>> {
        let mut ids = segment_ids(dir)?;
        ids.sort_unstable();
        let mut out = Vec::new();
        for id in ids {
            let path = dir.join(segment_name(id));
            let mut file = File::open(&path)?;
            let mut raw = Vec::new();
            file.read_to_end(&mut raw)?;
            let mut cursor = raw.as_slice();
            while !cursor.is_empty() {
                match decode_frame(cursor)? {
                    Some((record, consumed)) => {
                        cursor = &cursor[consumed..];
                        out.push(record);
                    }
                    None => break,
                }
            }
        }
        out.sort_by_key(|r| r.lsn);
        Ok(out)
    }

    pub fn truncate_below(&mut self, floor_commit_ts: u64) -> Result<usize> {
        let mut ids = segment_ids(&self.dir)?;
        ids.sort_unstable();
        let mut removed = 0;
        for id in ids {
            if id >= self.active_id {
                continue;
            }
            let path = self.dir.join(segment_name(id));
            let mut file = File::open(&path)?;
            let mut raw = Vec::new();
            file.read_to_end(&mut raw)?;
            let mut cursor = raw.as_slice();
            let mut keep = false;
            while !cursor.is_empty() {
                match decode_frame(cursor)? {
                    Some((record, consumed)) => {
                        cursor = &cursor[consumed..];
                        if record.commit_ts >= floor_commit_ts {
                            keep = true;
                            break;
                        }
                    }
                    None => break,
                }
            }
            if !keep {
                std::fs::remove_file(&path)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    fn rotate(&mut self) -> Result<()> {
        self.active.sync_data()?;
        self.active_id += 1;
        let path = self.dir.join(segment_name(self.active_id));
        self.active = OpenOptions::new().create(true).read(true).append(true).open(&path)?;
        Ok(())
    }
}

fn segment_name(id: u64) -> String {
    format!("seg-{id:020}.wal")
}

fn segment_ids(dir: &Path) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(ids),
        Err(e) => return Err(RymeError::from(e)),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(id) = parse_segment_name(&name) {
            ids.push(id);
        }
    }
    if ids.is_empty() {
        ids.push(0);
    }
    Ok(ids)
}

fn latest_segment_id(dir: &Path) -> Result<u64> {
    let mut ids = segment_ids(dir)?;
    ids.sort_unstable();
    Ok(ids.last().copied().unwrap_or(0))
}

fn parse_segment_name(name: &str) -> Option<u64> {
    let rest = name.strip_prefix("seg-")?.strip_suffix(".wal")?;
    rest.parse::<u64>().ok()
}

fn recover_next_lsn(active: &mut File) -> Result<u64> {
    active.seek(SeekFrom::Start(0))?;
    let mut raw = Vec::new();
    active.read_to_end(&mut raw)?;
    let mut cursor = raw.as_slice();
    let mut max: Option<u64> = None;
    while !cursor.is_empty() {
        match decode_frame(cursor)? {
            Some((record, consumed)) => {
                cursor = &cursor[consumed..];
                max = Some(record.lsn);
            }
            None => break,
        }
    }
    active.seek(SeekFrom::End(0))?;
    Ok(max.map(|v| v + 1).unwrap_or(0))
}

fn encode_frame(lsn: u64, commit_ts: u64, payload: &[u8]) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(20 + payload.len());
    buf.put_u32(WAL_MAGIC);
    buf.put_u16(WAL_VERSION);
    buf.put_u16(0);
    buf.put_u64(lsn);
    buf.put_u64(commit_ts);
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
    let crc = crc32fast::hash(&buf);
    buf.put_u32(crc);
    buf.freeze().to_vec()
}

fn decode_frame(input: &[u8]) -> Result<Option<(WalRecord, usize)>> {
    const HEADER: usize = 4 + 2 + 2 + 8 + 8 + 4;
    const TRAILER: usize = 4;
    if input.len() < HEADER + TRAILER {
        return Ok(None);
    }
    let mut cursor = &input[..HEADER];
    let magic = cursor.get_u32();
    let version = cursor.get_u16();
    let _reserved = cursor.get_u16();
    let lsn = cursor.get_u64();
    let commit_ts = cursor.get_u64();
    let len = cursor.get_u32() as usize;
    if magic != WAL_MAGIC || version != WAL_VERSION {
        return Err(RymeError::Corrupt(String::from("wal header")));
    }
    if input.len() < HEADER + len + TRAILER {
        return Ok(None);
    }
    let payload = input[HEADER..HEADER + len].to_vec();
    let stored = u32::from_be_bytes(
        input[HEADER + len..HEADER + len + TRAILER]
            .try_into()
            .map_err(|_| RymeError::Corrupt(String::from("wal crc")))?,
    );
    let computed = crc32fast::hash(&input[..HEADER + len]);
    if stored != computed {
        return Err(RymeError::Corrupt(String::from("wal checksum")));
    }
    Ok(Some((WalRecord { lsn, commit_ts, payload }, HEADER + len + TRAILER)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_recover() {
        let dir = std::env::temp_dir().join(format!("ryme-wal-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut wal = Wal::open(&dir, 1024 * 1024).unwrap();
        wal.append(7, b"hello").unwrap();
        wal.append(8, b"world").unwrap();
        wal.sync().unwrap();
        let records = Wal::read_all(&dir).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].payload, b"hello");
        assert_eq!(records[1].commit_ts, 8);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corruption_detected() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-wal-corrupt-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut wal = Wal::open(&dir, 1024 * 1024).unwrap();
        wal.append(1, b"good").unwrap();
        wal.append(2, b"also-good").unwrap();
        wal.sync().unwrap();
        drop(wal);
        let path = dir.join("seg-00000000000000000000.wal");
        let mut raw = std::fs::read(&path).unwrap();
        let last = raw.len() - 2;
        raw[last] ^= 0xff;
        std::fs::write(&path, &raw).unwrap();
        let result = Wal::read_all(&dir);
        assert!(matches!(result, Err(RymeError::Corrupt(_))));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn truncate_below_floor() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-wal-trunc-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut wal = Wal::open(&dir, 64).unwrap();
        for ts in 1..=8u64 {
            wal.append(ts, b"x").unwrap();
        }
        wal.sync().unwrap();
        assert!(Wal::read_all(&dir).unwrap().len() >= 8);
        let removed = wal.truncate_below(5).unwrap();
        assert!(removed >= 1);
        let records = Wal::read_all(&dir).unwrap();
        assert!(!records.is_empty());
        assert!(records.iter().all(|r| r.commit_ts >= 5));
        assert!(records.iter().any(|r| r.commit_ts == 8));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn torn_tail_recovers_prefix() {
        let dir =
            std::env::temp_dir().join(format!("ryme-wal-torn-{}-{}", std::process::id(), now_ms()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut wal = Wal::open(&dir, 1024 * 1024).unwrap();
        wal.append(1, b"first").unwrap();
        wal.append(2, b"second").unwrap();
        wal.sync().unwrap();
        drop(wal);
        let path = dir.join("seg-00000000000000000000.wal");
        let raw = std::fs::read(&path).unwrap();
        std::fs::write(&path, &raw[..raw.len() - 3]).unwrap();
        let records = Wal::read_all(&dir).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].payload, b"first");
        let reopened = Wal::open(&dir, 1024 * 1024).unwrap();
        assert_eq!(reopened.next_lsn(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn now_ms() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    }
}
