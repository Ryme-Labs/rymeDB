use bytes::{Buf, BufMut, BytesMut};
use ryme_error::{Result, RymeError};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const WAL_MAGIC: u32 = 0x52594D45;
pub const WAL_VERSION: u16 = 1;
pub const MAX_WAL_SEGMENT_BYTES: u64 = 256 * 1024 * 1024;
const WAL_FRAME_HEADER_BYTES: usize = 4 + 2 + 2 + 8 + 8 + 4;
const WAL_FRAME_TRAILER_BYTES: usize = 4;
const WAL_FRAME_OVERHEAD_BYTES: usize = WAL_FRAME_HEADER_BYTES + WAL_FRAME_TRAILER_BYTES;
const MAX_WAL_PAYLOAD_BYTES: usize = MAX_WAL_SEGMENT_BYTES as usize - WAL_FRAME_OVERHEAD_BYTES;
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
    active_len: u64,
    next_lsn: u64,
    poisoned: bool,
}

impl Wal {
    pub fn open(dir: &Path, segment_size_bytes: u64) -> Result<Self> {
        if segment_size_bytes == 0 {
            return Err(RymeError::InvalidArgument(String::from(
                "wal segment size must be positive",
            )));
        }
        if segment_size_bytes > MAX_WAL_SEGMENT_BYTES {
            return Err(RymeError::Overload(String::from("wal segment")));
        }
        std::fs::create_dir_all(dir)?;
        recover_rewrite(dir)?;
        let active_id = latest_segment_id(dir)?;
        let path = dir.join(segment_name(active_id));
        let mut active = OpenOptions::new().create(true).read(true).append(true).open(&path)?;
        let next_lsn = recover_next_lsn(dir, active_id, &mut active)?;
        let active_len = active.metadata()?.len();
        Ok(Self {
            dir: dir.to_path_buf(),
            segment_size_bytes,
            active_id,
            active,
            active_len,
            next_lsn,
            poisoned: false,
        })
    }

    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    fn ensure_usable(&self) -> Result<()> {
        if self.poisoned {
            return Err(RymeError::Unavailable(String::from(
                "wal is in a failed state; reopen it before further operations",
            )));
        }
        Ok(())
    }

    pub fn append(&mut self, commit_ts: u64, payload: &[u8]) -> Result<WalRecord> {
        let lsn = self.append_lsn(commit_ts, payload)?;
        Ok(WalRecord { lsn, commit_ts, payload: payload.to_vec() })
    }

    /// Append a record and return its log sequence number without copying the
    /// payload into a `WalRecord` that the caller may not need.
    pub fn append_lsn(&mut self, commit_ts: u64, payload: &[u8]) -> Result<u64> {
        self.ensure_usable()?;
        let lsn = self.next_lsn;
        let next_lsn = lsn
            .checked_add(1)
            .ok_or_else(|| RymeError::Corrupt(String::from("wal lsn overflow")))?;
        let frame = encode_frame(lsn, commit_ts, payload)?;
        if self.active_len.saturating_add(frame.len() as u64) > self.segment_size_bytes {
            if let Err(error) = self.rotate() {
                self.poisoned = true;
                return Err(error);
            }
        }
        let write_result =
            write_frame_with_rollback(&mut self.active, &frame, self.active_len, |file, frame| {
                file.write_all(frame)
            });
        if let Err(error) = write_result {
            self.poisoned = true;
            return Err(error.into());
        }
        self.active_len = self.active_len.saturating_add(frame.len() as u64);
        self.next_lsn = next_lsn;
        Ok(lsn)
    }

    pub fn sync(&mut self) -> Result<()> {
        self.ensure_usable()?;
        if let Err(error) = self.active.sync_data() {
            self.poisoned = true;
            return Err(error.into());
        }
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
            let raw = read_segment_file(&path)?;
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
        self.ensure_usable()?;
        let mut ids = segment_ids(&self.dir)?;
        ids.sort_unstable();
        let mut obsolete = Vec::new();
        for id in ids {
            if id >= self.active_id {
                continue;
            }
            let path = self.dir.join(segment_name(id));
            let raw = read_segment_file(&path)?;
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
                    None => return Err(RymeError::Corrupt(String::from("truncated wal segment"))),
                }
            }
            if !keep {
                obsolete.push(path);
            }
        }

        let mut removed = 0;
        let removal_result = (|| -> Result<()> {
            for path in obsolete {
                std::fs::remove_file(path)?;
                removed += 1;
            }
            Ok(())
        })();
        if removed > 0 {
            sync_directory(&self.dir)?;
        }
        removal_result?;
        Ok(removed)
    }

    pub fn truncate_above(&mut self, ceiling_commit_ts: u64) -> Result<usize> {
        self.ensure_usable()?;
        let mut ids = segment_ids(&self.dir)?;
        ids.sort_unstable();
        let latest_id = ids.last().copied();

        // Avoid staging and rewriting the full WAL when this cutoff removes
        // nothing. On the rewrite path, processing one source segment at a
        // time keeps memory use independent of total WAL history.
        let mut previous_lsn = None;
        let mut removed = 0usize;
        for source_id in ids.iter().copied() {
            let raw = read_segment_file(&self.dir.join(segment_name(source_id)))?;
            let mut cursor = raw.as_slice();
            while !cursor.is_empty() {
                let (record, consumed) = match decode_frame(cursor)? {
                    Some(frame) => frame,
                    None if Some(source_id) == latest_id => break,
                    None => return Err(RymeError::Corrupt(String::from("truncated wal segment"))),
                };
                cursor = &cursor[consumed..];
                if previous_lsn.is_some_and(|previous| record.lsn <= previous) {
                    return Err(RymeError::Corrupt(String::from("wal lsn order")));
                }
                previous_lsn = Some(record.lsn);
                if record.commit_ts > ceiling_commit_ts {
                    removed = removed.saturating_add(1);
                }
            }
        }
        if removed == 0 {
            return Ok(0);
        }

        let temporary = self.dir.join(REWRITE_DIR);
        if temporary.exists() {
            std::fs::remove_dir_all(&temporary)?;
        }
        std::fs::create_dir_all(&temporary)?;

        let mut id = 0u64;
        let mut path = temporary.join(segment_name(id));
        let mut file = File::create(&path)?;
        let mut segment_len = 0u64;
        let mut previous_lsn = None;
        let mut last_kept_lsn = None;

        for source_id in ids {
            let raw = read_segment_file(&self.dir.join(segment_name(source_id)))?;
            let mut cursor = raw.as_slice();
            while !cursor.is_empty() {
                let (record, consumed) = match decode_frame(cursor)? {
                    Some(frame) => frame,
                    None if Some(source_id) == latest_id => break,
                    None => return Err(RymeError::Corrupt(String::from("truncated wal segment"))),
                };
                cursor = &cursor[consumed..];
                if previous_lsn.is_some_and(|previous| record.lsn <= previous) {
                    return Err(RymeError::Corrupt(String::from("wal lsn order")));
                }
                previous_lsn = Some(record.lsn);

                if record.commit_ts > ceiling_commit_ts {
                    continue;
                }

                let frame = encode_frame(record.lsn, record.commit_ts, &record.payload)?;
                if segment_len > 0
                    && segment_len.saturating_add(frame.len() as u64) > self.segment_size_bytes
                {
                    file.sync_data()?;
                    id = id.checked_add(1).ok_or_else(|| {
                        RymeError::Corrupt(String::from("wal segment id overflow"))
                    })?;
                    path = temporary.join(segment_name(id));
                    file = File::create(&path)?;
                    segment_len = 0;
                }
                file.write_all(&frame)?;
                segment_len = segment_len.saturating_add(frame.len() as u64);
                last_kept_lsn = Some(record.lsn);
            }
        }

        file.sync_data()?;
        drop(file);
        sync_directory(&temporary)?;
        let next_lsn = last_kept_lsn
            .map(|lsn| {
                lsn.checked_add(1)
                    .ok_or_else(|| RymeError::Corrupt(String::from("wal lsn overflow")))
            })
            .unwrap_or(Ok(0))?;
        // The staged generation is authoritative after the marker is durable;
        // stop appends through the old handle if installation fails.
        self.poisoned = true;
        create_rewrite_marker(&self.dir)?;
        install_rewrite(&self.dir)?;
        self.active_id = latest_segment_id(&self.dir)?;
        let active_path = self.dir.join(segment_name(self.active_id));
        self.active = OpenOptions::new().create(true).read(true).append(true).open(active_path)?;
        self.active_len = self.active.metadata()?.len();
        self.next_lsn = next_lsn;
        self.poisoned = false;
        Ok(removed)
    }

    pub fn replace_records(&mut self, records: &[WalRecord]) -> Result<()> {
        self.rewrite(records)
    }

    fn rewrite(&mut self, records: &[WalRecord]) -> Result<()> {
        self.ensure_usable()?;
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
        for record in records {
            validate_payload_len(record.payload.len())?;
        }
        if temporary.exists() {
            std::fs::remove_dir_all(&temporary)?;
        }
        std::fs::create_dir_all(&temporary)?;
        let mut id = 0u64;
        let mut path = temporary.join(segment_name(id));
        let mut file = File::create(&path)?;
        for record in records {
            let frame = encode_frame(record.lsn, record.commit_ts, &record.payload)?;
            let current_len = file.metadata()?.len();
            if current_len > 0 && current_len + frame.len() as u64 > self.segment_size_bytes {
                file.sync_data()?;
                id = id
                    .checked_add(1)
                    .ok_or_else(|| RymeError::Corrupt(String::from("wal segment id overflow")))?;
                path = temporary.join(segment_name(id));
                file = File::create(&path)?;
            }
            file.write_all(&frame)?;
        }
        file.sync_data()?;
        drop(file);
        sync_directory(&temporary)?;
        // From marker publication onward, recovery may choose the staged
        // generation over this handle's current WAL files. Do not let the
        // live writer append through the old handle if any rewrite step fails.
        self.poisoned = true;
        create_rewrite_marker(&self.dir)?;
        install_rewrite(&self.dir)?;

        self.active_id = latest_segment_id(&self.dir)?;
        let active_path = self.dir.join(segment_name(self.active_id));
        self.active = OpenOptions::new().create(true).read(true).append(true).open(active_path)?;
        self.active_len = self.active.metadata()?.len();
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
        self.active_len = 0;
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
        let raw = read_segment_file(path)?;
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
            Err(rollback_error) => Err(std::io::Error::other(format!(
                "WAL append failed ({write_error}); rollback failed ({rollback_error})"
            ))),
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

fn read_segment_file(path: &Path) -> Result<Vec<u8>> {
    let file = File::open(path)?;
    let length = file.metadata()?.len();
    if length > MAX_WAL_SEGMENT_BYTES {
        return Err(RymeError::Overload(String::from("wal segment")));
    }
    let mut raw = Vec::with_capacity(length as usize);
    file.take(MAX_WAL_SEGMENT_BYTES + 1).read_to_end(&mut raw)?;
    if raw.len() as u64 > MAX_WAL_SEGMENT_BYTES {
        return Err(RymeError::Overload(String::from("wal segment")));
    }
    Ok(raw)
}

fn recover_next_lsn(dir: &Path, active_id: u64, active: &mut File) -> Result<u64> {
    let mut ids = segment_ids(dir)?;
    ids.sort_unstable();
    let latest_id = ids.last().copied();
    let mut previous_lsn = None;

    for id in ids {
        let raw = if id == active_id {
            let length = active.metadata()?.len();
            if length > MAX_WAL_SEGMENT_BYTES {
                return Err(RymeError::Overload(String::from("wal segment")));
            }
            let mut contents = Vec::with_capacity(length as usize);
            active.seek(SeekFrom::Start(0))?;
            active.take(MAX_WAL_SEGMENT_BYTES + 1).read_to_end(&mut contents)?;
            if contents.len() as u64 > MAX_WAL_SEGMENT_BYTES {
                return Err(RymeError::Overload(String::from("wal segment")));
            }
            contents
        } else {
            read_segment_file(&dir.join(segment_name(id)))?
        };
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

fn encode_frame(lsn: u64, commit_ts: u64, payload: &[u8]) -> Result<Vec<u8>> {
    validate_payload_len(payload.len())?;
    let mut buf = BytesMut::with_capacity(WAL_FRAME_OVERHEAD_BYTES + payload.len());
    buf.put_u32(WAL_MAGIC);
    buf.put_u16(WAL_VERSION);
    buf.put_u16(0);
    buf.put_u64(lsn);
    buf.put_u64(commit_ts);
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);
    let crc = crc32fast::hash(&buf);
    buf.put_u32(crc);
    Ok(buf.freeze().to_vec())
}

fn validate_payload_len(payload_len: usize) -> Result<()> {
    if payload_len > MAX_WAL_PAYLOAD_BYTES {
        return Err(RymeError::InvalidArgument(String::from("payload too large")));
    }
    Ok(())
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
    let reserved = cursor.get_u16();
    let lsn = cursor.get_u64();
    let commit_ts = cursor.get_u64();
    let len = cursor.get_u32() as usize;
    if magic != WAL_MAGIC || version != WAL_VERSION || reserved != 0 {
        return Err(RymeError::Corrupt(String::from("wal header")));
    }
    if len > MAX_WAL_PAYLOAD_BYTES {
        return Err(RymeError::Corrupt(String::from("wal payload length")));
    }
    let frame_len = HEADER
        .checked_add(len)
        .and_then(|length| length.checked_add(TRAILER))
        .ok_or_else(|| RymeError::Corrupt(String::from("wal frame length")))?;
    if input.len() < frame_len {
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
    Ok(Some((WalRecord { lsn, commit_ts, payload }, frame_len)))
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
    fn append_lsn_writes_payload_without_returning_a_copy() {
        let dir = std::env::temp_dir().join(format!("ryme-wal-lsn-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut wal = Wal::open(&dir, 1024 * 1024).unwrap();
        assert_eq!(wal.append_lsn(7, b"first").unwrap(), 0);
        let record = wal.append(8, b"second").unwrap();
        assert_eq!(record.lsn, 1);
        assert_eq!(record.payload, b"second");
        let records = Wal::read_all(&dir).unwrap();
        assert_eq!(records[0].payload, b"first");
        assert_eq!(records[1].payload, b"second");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_rejects_zero_segment_size() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-wal-zero-segment-size-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        assert!(matches!(Wal::open(&dir, 0), Err(RymeError::InvalidArgument(_))));
        assert!(!dir.exists());
    }

    #[test]
    fn open_rejects_oversized_segment_before_reading() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-wal-oversized-segment-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(segment_name(0));
        let file = File::create(&path).unwrap();
        file.set_len(MAX_WAL_SEGMENT_BYTES + 1).unwrap();

        assert!(matches!(
            Wal::open(&dir, 1024 * 1024),
            Err(RymeError::Overload(message)) if message == "wal segment"
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn payload_length_matches_wire_format() {
        assert!(validate_payload_len(MAX_WAL_PAYLOAD_BYTES).is_ok());
        assert!(matches!(
            validate_payload_len(MAX_WAL_PAYLOAD_BYTES + 1),
            Err(RymeError::InvalidArgument(message)) if message == "payload too large"
        ));
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
        let mut raw = encode_frame(2, 2, b"later").unwrap();
        raw.extend_from_slice(&encode_frame(1, 1, b"earlier").unwrap());
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
        std::fs::write(dir.join(segment_name(0)), encode_frame(2, 2, b"later").unwrap()).unwrap();
        std::fs::write(dir.join(segment_name(1)), encode_frame(1, 1, b"earlier").unwrap()).unwrap();

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
    fn retention_rejects_truncated_sealed_segment() {
        let dir = std::env::temp_dir().join(format!(
            "ryme-wal-truncated-sealed-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut wal = Wal::open(&dir, 64).unwrap();
        wal.append(1, b"first").unwrap();
        wal.append(2, b"second").unwrap();
        wal.sync().unwrap();

        let path = dir.join(segment_name(0));
        let raw = std::fs::read(&path).unwrap();
        std::fs::write(&path, &raw[..raw.len() - 1]).unwrap();
        assert!(matches!(wal.truncate_below(2), Err(RymeError::Corrupt(_))));
        assert!(path.exists());
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
        assert_eq!(wal.active_len, wal.active.metadata().unwrap().len());
        wal.sync().unwrap();
        wal.rotate().unwrap();
        assert_eq!(wal.active_len, 0);
        drop(wal);

        let mut reopened = Wal::open(&dir, 1024).unwrap();
        assert_eq!(reopened.next_lsn(), 1);
        assert_eq!(reopened.active_len, reopened.active.metadata().unwrap().len());
        reopened.append(2, b"second").unwrap();
        assert_eq!(reopened.active_len, reopened.active.metadata().unwrap().len());
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
                Err(std::io::Error::other("injected write failure"))
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
        std::fs::write(
            staging.join(segment_name(0)),
            encode_frame(0, 99, b"new-generation").unwrap(),
        )
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
