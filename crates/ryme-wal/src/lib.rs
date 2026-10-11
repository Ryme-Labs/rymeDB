use bytes::{Buf, BufMut, BytesMut};
use ryme_error::{Result, RymeError};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const WAL_MAGIC: u32 = 0x52594D45;
pub const WAL_VERSION: u16 = 1;
const REWRITE_DIR: &str = ".rewrite";
const REWRITE_MARKER: &str = ".rewrite-in-progress";

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
    poisoned: bool,
}

impl Wal {
    pub fn open(dir: &Path, segment_size_bytes: u64) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        recover_rewrite(dir)?;
        let active_id = latest_segment_id(dir)?;
        let path = dir.join(segment_name(active_id));
        let mut active = OpenOptions::new().create(true).read(true).append(true).open(&path)?;
        let next_lsn = recover_next_lsn(dir, active_id, &mut active)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            segment_size_bytes,
            active_id,
            active,
            next_lsn,
            poisoned: false,
        })
    }

    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    pub fn append(&mut self, commit_ts: u64, payload: &[u8]) -> Result<WalRecord> {
        if self.poisoned {
            return Err(RymeError::Unavailable(String::from(
                "wal append failed; reopen the wal before writing again",
            )));
        }
        if payload.len() > u32::MAX as usize {
            return Err(RymeError::InvalidArgument(String::from("payload too large")));
        }
        let lsn = self.next_lsn;
        let next_lsn = lsn
            .checked_add(1)
            .ok_or_else(|| RymeError::Corrupt(String::from("wal lsn overflow")))?;
        let frame = encode_frame(lsn, commit_ts, payload);
        let mut active_len = self.active.metadata()?.len();
        if active_len.saturating_add(frame.len() as u64) > self.segment_size_bytes {
            if let Err(error) = self.rotate() {
                self.poisoned = true;
                return Err(error);
            }
            active_len = 0;
        }
        let write_result =
            write_frame_with_rollback(&mut self.active, &frame, active_len, |file, frame| {
                file.write_all(frame)
            });
        if let Err(error) = write_result {
            self.poisoned = true;
            return Err(error.into());
        }
        self.next_lsn = next_lsn;
        Ok(WalRecord { lsn, commit_ts, payload: payload.to_vec() })
    }

    pub fn sync(&mut self) -> Result<()> {
        self.active.sync_data()?;
        Ok(())
    }

    pub fn read_all(dir: &Path) -> Result<Vec<WalRecord>> {
        let mut ids = segment_ids(dir)?;
        ids.sort_unstable();
        let latest_id = ids.last().copied();
        let mut out = Vec::new();
        let mut previous_lsn = None;
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
                        if previous_lsn.is_some_and(|previous| record.lsn <= previous) {
                            return Err(RymeError::Corrupt(String::from("wal lsn order")));
                        }
                        previous_lsn = Some(record.lsn);
                        out.push(record);
                    }
                    None if Some(id) == latest_id => break,
                    None => return Err(RymeError::Corrupt(String::from("truncated wal segment"))),
                }
            }
        }
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

    pub fn truncate_above(&mut self, ceiling_commit_ts: u64) -> Result<usize> {
        let records = Self::read_all(&self.dir)?;
        let kept: Vec<WalRecord> = records
            .iter()
            .filter(|record| record.commit_ts <= ceiling_commit_ts)
            .cloned()
            .collect();
        let removed = records.len().saturating_sub(kept.len());
        if removed == 0 {
            return Ok(0);
        }
        self.rewrite(&kept)?;
        Ok(removed)
    }

    pub fn replace_records(&mut self, records: &[WalRecord]) -> Result<()> {
        self.rewrite(records)
    }

    fn rewrite(&mut self, records: &[WalRecord]) -> Result<()> {
        if records.windows(2).any(|pair| pair[0].lsn >= pair[1].lsn) {
            return Err(RymeError::InvalidArgument(String::from("wal lsn order")));
        }
        let next_lsn = records
            .last()
            .map(|record| {
                record
                    .lsn
                    .checked_add(1)
                    .ok_or_else(|| RymeError::Corrupt(String::from("wal lsn overflow")))
            })
            .unwrap_or(Ok(0))?;
        let temporary = self.dir.join(REWRITE_DIR);
        if temporary.exists() {
            std::fs::remove_dir_all(&temporary)?;
        }
        std::fs::create_dir_all(&temporary)?;
        let mut id = 0u64;
        let mut path = temporary.join(segment_name(id));
        let mut file = File::create(&path)?;
        for record in records {
            let frame = encode_frame(record.lsn, record.commit_ts, &record.payload);
            let current_len = file.metadata()?.len();
            if current_len > 0 && current_len + frame.len() as u64 > self.segment_size_bytes {
                file.sync_data()?;
                id += 1;
                path = temporary.join(segment_name(id));
                file = File::create(&path)?;
            }
            file.write_all(&frame)?;
        }
        file.sync_data()?;
        drop(file);
        sync_directory(&temporary)?;
        create_rewrite_marker(&self.dir)?;
        if let Err(error) = install_rewrite(&self.dir) {
            self.poisoned = true;
            return Err(error);
        }

        self.active_id = latest_segment_id(&self.dir)?;
        let active_path = self.dir.join(segment_name(self.active_id));
        self.active = OpenOptions::new().create(true).read(true).append(true).open(active_path)?;
        self.next_lsn = next_lsn;
        self.poisoned = false;
        Ok(())
    }

    fn rotate(&mut self) -> Result<()> {
        self.active.sync_data()?;
        let next_id = self
            .active_id
            .checked_add(1)
            .ok_or_else(|| RymeError::Corrupt(String::from("wal segment id overflow")))?;
        let path = self.dir.join(segment_name(next_id));
        let next = OpenOptions::new().create(true).read(true).append(true).open(&path)?;
        sync_directory(&self.dir)?;
        self.active = next;
        self.active_id = next_id;
        Ok(())
    }
}

fn create_rewrite_marker(dir: &Path) -> Result<()> {
    let marker = dir.join(REWRITE_MARKER);
    let file = OpenOptions::new().write(true).create_new(true).open(marker)?;
    file.sync_all()?;
    sync_directory(dir)
}

fn recover_rewrite(dir: &Path) -> Result<()> {
    if dir.join(REWRITE_MARKER).exists() {
        install_rewrite(dir)?;
    }
    Ok(())
}

// Once the marker is durable, the staged generation is authoritative. Copying
// (rather than moving) makes installation idempotent if the process crashes.
fn install_rewrite(dir: &Path) -> Result<()> {
    let temporary = dir.join(REWRITE_DIR);
    let mut staged = Vec::new();
    for entry in std::fs::read_dir(&temporary)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(id) = parse_segment_name(&name) {
            staged.push((id, entry.path(), name));
        }
    }
    staged.sort_unstable_by_key(|(id, _, _)| *id);
    if staged.is_empty() {
        return Err(RymeError::Corrupt(String::from("empty WAL rewrite")));
    }

    let mut previous_lsn = None;
    for (_, path, name) in &staged {
        let raw = std::fs::read(path)?;
        let mut cursor = raw.as_slice();
        while !cursor.is_empty() {
            let Some((record, consumed)) = decode_frame(cursor)? else {
                return Err(RymeError::Corrupt(String::from("truncated rewrite segment")));
            };
            if previous_lsn.is_some_and(|previous| record.lsn <= previous) {
                return Err(RymeError::Corrupt(String::from("rewrite lsn order")));
            }
            previous_lsn = Some(record.lsn);
            cursor = &cursor[consumed..];
        }
        std::fs::copy(path, dir.join(name))?;
        File::open(dir.join(name))?.sync_all()?;
    }
    sync_directory(dir)?;

    for id in segment_ids(dir)? {
        if staged.binary_search_by_key(&id, |(staged_id, _, _)| *staged_id).is_err() {
            std::fs::remove_file(dir.join(segment_name(id)))?;
        }
    }
    sync_directory(dir)?;
    std::fs::remove_file(dir.join(REWRITE_MARKER))?;
    sync_directory(dir)?;
    std::fs::remove_dir_all(temporary)?;
    Ok(())
}

fn sync_directory(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

fn write_frame_with_rollback<F>(
    file: &mut File,
    frame: &[u8],
    offset: u64,
    mut write: F,
) -> std::io::Result<()>
where
    F: FnMut(&mut File, &[u8]) -> std::io::Result<()>,
{
    match write(file, frame) {
        Ok(()) => Ok(()),
        Err(write_error) => match file.set_len(offset).and_then(|()| file.sync_data()) {
            Ok(()) => Err(write_error),
            Err(rollback_error) => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("WAL append failed ({write_error}); rollback failed ({rollback_error})"),
            )),
        },
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

fn recover_next_lsn(dir: &Path, active_id: u64, active: &mut File) -> Result<u64> {
    let mut ids = segment_ids(dir)?;
    ids.sort_unstable();
    let latest_id = ids.last().copied();
    let mut previous_lsn = None;

    for id in ids {
        let mut raw = Vec::new();
        if id == active_id {
            active.seek(SeekFrom::Start(0))?;
            active.read_to_end(&mut raw)?;
        } else {
            File::open(dir.join(segment_name(id)))?.read_to_end(&mut raw)?;
        }
        let mut cursor = raw.as_slice();
        while !cursor.is_empty() {
            match decode_frame(cursor)? {
                Some((record, consumed)) => {
                    cursor = &cursor[consumed..];
                    if previous_lsn.is_some_and(|previous| record.lsn <= previous) {
                        return Err(RymeError::Corrupt(String::from("wal lsn order")));
                    }
                    previous_lsn = Some(record.lsn);
                }
                None if id == active_id && Some(id) == latest_id => {
                    let valid_len = raw.len() - cursor.len();
                    active.set_len(valid_len as u64)?;
                    active.sync_data()?;
                    break;
                }
                None => return Err(RymeError::Corrupt(String::from("truncated wal segment"))),
            }
        }
    }

    active.seek(SeekFrom::End(0))?;
    previous_lsn
        .map(|lsn| {
            lsn.checked_add(1).ok_or_else(|| RymeError::Corrupt(String::from("wal lsn overflow")))
        })
        .unwrap_or(Ok(0))
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
    fn recovery_rejects_out_of_order_lsns() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-wal-lsn-order-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut raw = encode_frame(2, 2, b"later");
        raw.extend_from_slice(&encode_frame(1, 1, b"earlier"));
        std::fs::write(dir.join(segment_name(0)), raw).unwrap();

        assert!(matches!(Wal::read_all(&dir), Err(RymeError::Corrupt(_))));
        assert!(matches!(Wal::open(&dir, 1024), Err(RymeError::Corrupt(_))));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn recovery_rejects_out_of_order_lsns_across_segments() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-wal-segment-lsn-order-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(segment_name(0)), encode_frame(2, 2, b"later")).unwrap();
        std::fs::write(dir.join(segment_name(1)), encode_frame(1, 1, b"earlier")).unwrap();

        assert!(matches!(Wal::open(&dir, 1024), Err(RymeError::Corrupt(_))));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn replacement_rejects_out_of_order_lsns_without_changing_wal() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-wal-replace-lsn-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut wal = Wal::open(&dir, 1024).unwrap();
        wal.append(1, b"existing").unwrap();
        wal.sync().unwrap();
        let invalid = [
            WalRecord { lsn: 2, commit_ts: 2, payload: b"later".to_vec() },
            WalRecord { lsn: 1, commit_ts: 1, payload: b"earlier".to_vec() },
        ];

        assert!(matches!(wal.replace_records(&invalid), Err(RymeError::InvalidArgument(_))));
        assert_eq!(Wal::read_all(&dir).unwrap()[0].payload, b"existing");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn replacement_rejects_lsn_overflow_without_changing_wal() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-wal-replace-overflow-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut wal = Wal::open(&dir, 1024).unwrap();
        wal.append(1, b"existing").unwrap();
        wal.sync().unwrap();
        let invalid = [WalRecord { lsn: u64::MAX, commit_ts: 2, payload: b"overflow".to_vec() }];

        assert!(matches!(wal.replace_records(&invalid), Err(RymeError::Corrupt(_))));
        assert_eq!(Wal::read_all(&dir).unwrap()[0].payload, b"existing");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn recovery_uses_previous_segment_when_active_segment_is_empty() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-wal-empty-active-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut wal = Wal::open(&dir, 1024).unwrap();
        wal.append(1, b"first").unwrap();
        wal.sync().unwrap();
        wal.rotate().unwrap();
        drop(wal);

        let mut reopened = Wal::open(&dir, 1024).unwrap();
        assert_eq!(reopened.next_lsn(), 1);
        reopened.append(2, b"second").unwrap();
        reopened.sync().unwrap();
        let records = Wal::read_all(&dir).unwrap();
        assert_eq!(records.iter().map(|record| record.lsn).collect::<Vec<_>>(), vec![0, 1]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rotation_rejects_segment_id_overflow_without_changing_active_segment() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-wal-segment-overflow-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut wal = Wal::open(&dir, 1024).unwrap();
        let original_id = wal.active_id;
        wal.active_id = u64::MAX;
        assert!(matches!(wal.rotate(), Err(RymeError::Corrupt(_))));
        assert_eq!(wal.active_id, u64::MAX);
        assert_eq!(wal.active.metadata().unwrap().len(), 0);
        wal.active_id = original_id;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn failed_partial_frame_write_rolls_back_to_record_boundary() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-wal-partial-write-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(segment_name(0));
        let mut file = File::create(&path).unwrap();
        file.write_all(b"valid-prefix").unwrap();
        file.sync_data().unwrap();
        let offset = file.metadata().unwrap().len();
        let result =
            write_frame_with_rollback(&mut file, b"partial-frame", offset, |file, frame| {
                file.write_all(&frame[..5])?;
                Err(std::io::Error::new(std::io::ErrorKind::Other, "injected write failure"))
            });

        assert!(result.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"valid-prefix");
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
    fn truncate_above_floor_rewrites_wal() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-wal-trunc-above-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut wal = Wal::open(&dir, 64).unwrap();
        for ts in 1..=8u64 {
            wal.append(ts, b"x").unwrap();
        }
        wal.sync().unwrap();
        assert_eq!(wal.truncate_above(4).unwrap(), 4);
        let records = Wal::read_all(&dir).unwrap();
        assert_eq!(records.len(), 4);
        assert!(records.iter().all(|record| record.commit_ts <= 4));
        assert_eq!(wal.next_lsn(), 4);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_finishes_interrupted_rewrite_installation() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-wal-rewrite-recovery-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut wal = Wal::open(&dir, 64).unwrap();
        for ts in 1..=8u64 {
            wal.append(ts, b"old-generation").unwrap();
        }
        wal.sync().unwrap();
        let replacement =
            [WalRecord { lsn: 0, commit_ts: 99, payload: b"new-generation".to_vec() }];
        let staging = dir.join(REWRITE_DIR);
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::write(staging.join(segment_name(0)), encode_frame(0, 99, b"new-generation"))
            .unwrap();
        sync_directory(&staging).unwrap();
        create_rewrite_marker(&dir).unwrap();
        // Simulate a crash after old segments were removed but before the stage
        // was installed. Recovery must be safe to repeat after any partial copy.
        for id in segment_ids(&dir).unwrap() {
            std::fs::remove_file(dir.join(segment_name(id))).unwrap();
        }
        drop(wal);

        let mut recovered = Wal::open(&dir, 64).unwrap();
        assert_eq!(Wal::read_all(&dir).unwrap(), replacement);
        assert_eq!(recovered.next_lsn(), 1);
        recovered.append(100, b"after-recovery").unwrap();
        assert_eq!(Wal::read_all(&dir).unwrap().len(), 2);
        assert!(!dir.join(REWRITE_MARKER).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn torn_tail_is_removed_before_new_appends() {
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
        let mut reopened = Wal::open(&dir, 1024 * 1024).unwrap();
        assert_eq!(reopened.next_lsn(), 1);
        reopened.append(3, b"after-recovery").unwrap();
        reopened.sync().unwrap();
        drop(reopened);
        let records = Wal::read_all(&dir).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].payload, b"first");
        assert_eq!(records[1].payload, b"after-recovery");
        assert_eq!(records[1].lsn, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn torn_non_active_segment_is_corruption() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-wal-torn-sealed-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut wal = Wal::open(&dir, 64).unwrap();
        wal.append(1, b"first").unwrap();
        wal.append(2, b"second").unwrap();
        wal.sync().unwrap();
        drop(wal);

        let sealed = dir.join("seg-00000000000000000000.wal");
        let raw = std::fs::read(&sealed).unwrap();
        std::fs::write(&sealed, &raw[..raw.len() - 1]).unwrap();
        assert!(matches!(Wal::read_all(&dir), Err(RymeError::Corrupt(_))));
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn now_ms() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    }
}
