use crate::codec::encode_record_frame;
use crate::error::{invalid_data, invalid_input};
use crate::stats::StorageStatsCounters;
use crate::storage::{Record, sync_parent_dir};

use std::{
    ffi::OsStr,
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const SEGMENT_FORMAT_MAGIC: &[u8; 4] = b"SKR1";
const SEGMENT_FILE_HEADER_LEN: usize = 4;
pub(crate) const SEGMENT_FILE_HEADER_LEN_U64: u64 = SEGMENT_FILE_HEADER_LEN as u64;
const SEGMENT_FILE_SUFFIX: &str = ".segment";

#[derive(Debug)]
pub(crate) struct SegmentWriter {
    dir: PathBuf,
    active_segment: SegmentName,
    file: File,
    segment_len: u64,
    max_segment_len: u64,
}

impl SegmentWriter {
    pub(crate) fn open(
        dir: &Path,
        active_segment: SegmentName,
        max_segment_len: u64,
        stats: &mut StorageStatsCounters,
    ) -> io::Result<Self> {
        let segment_path = active_segment.path(dir);
        let file_existed = segment_path.exists();
        let mut file = open_active_segment_file(&segment_path)?;
        if !file_existed {
            sync_parent_dir(&segment_path)?;
        }
        let segment_len = file.seek(SeekFrom::End(0))?;
        stats.segment_opened(active_segment.id(), segment_len);

        Ok(Self {
            dir: dir.to_path_buf(),
            active_segment,
            file,
            segment_len,
            max_segment_len,
        })
    }

    pub(crate) fn append(
        &mut self,
        node_id: noraft::NodeId,
        record: &Record,
        stats: &mut StorageStatsCounters,
    ) -> io::Result<RecordPosition> {
        let frame = encode_record_frame(node_id, record)?;
        let written_bytes =
            u64::try_from(frame.len()).map_err(|_| invalid_input("record is too large"))?;
        if self.should_rotate(written_bytes) {
            self.rotate(stats)?;
        }
        let record_position = RecordPosition {
            segment: self.active_segment,
            offset: self.segment_len,
        };
        self.file.write_all(&frame)?;
        self.segment_len = self
            .segment_len
            .checked_add(written_bytes)
            .ok_or_else(|| invalid_data("segment length overflow"))?;
        stats.record_written(record.metric_kind(), written_bytes);
        stats.segment_written(self.segment_len);
        Ok(record_position)
    }

    pub(crate) fn sync(&mut self, stats: &mut StorageStatsCounters) -> io::Result<()> {
        if stats.as_ref().unsynced_records != 0 || stats.as_ref().unsynced_bytes != 0 {
            self.sync_current_segment(stats)?;
        }
        Ok(())
    }

    pub(crate) fn active_segment(&self) -> SegmentName {
        self.active_segment
    }

    fn should_rotate(&self, written_bytes: u64) -> bool {
        if self.segment_len <= SEGMENT_FILE_HEADER_LEN_U64 {
            return false;
        }
        self.segment_len
            .checked_add(written_bytes)
            .is_none_or(|len| self.max_segment_len < len)
    }

    fn rotate(&mut self, stats: &mut StorageStatsCounters) -> io::Result<()> {
        self.sync_current_segment(stats)?;
        let next_segment = self.active_segment.next_append()?;
        let segment_path = next_segment.path(&self.dir);
        let file = create_active_segment_file(&segment_path)?;
        sync_parent_dir(&segment_path)?;

        self.active_segment = next_segment;
        self.file = file;
        self.segment_len = SEGMENT_FILE_HEADER_LEN_U64;
        stats.segment_rotated(self.active_segment.id(), self.segment_len);
        Ok(())
    }

    fn sync_current_segment(&mut self, stats: &mut StorageStatsCounters) -> io::Result<()> {
        self.file.sync_data()?;
        stats.segment_synced();
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SegmentId(u64);

impl SegmentId {
    const FIRST: Self = Self(0);

    fn get(self) -> u64 {
        self.0
    }

    fn next(self) -> io::Result<Self> {
        let id = self
            .0
            .checked_add(1)
            .ok_or_else(|| invalid_data("segment ID overflow"))?;
        Ok(Self(id))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SegmentName {
    id: SegmentId,
}

impl SegmentName {
    fn first_append() -> Self {
        Self {
            id: SegmentId::FIRST,
        }
    }

    fn next_append(self) -> io::Result<Self> {
        Ok(Self {
            id: self.id.next()?,
        })
    }

    fn parse_file_name(file_name: &OsStr) -> Option<Self> {
        let file_name = file_name.to_str()?;
        let id = file_name
            .strip_prefix("append-")?
            .strip_suffix(SEGMENT_FILE_SUFFIX)?;
        if id.is_empty() || !id.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        if id.len() > 1 && id.starts_with('0') {
            return None;
        }
        let id = SegmentId(id.parse().ok()?);
        Some(Self { id })
    }

    pub(crate) fn parse_str(s: &str) -> Option<Self> {
        Self::parse_file_name(OsStr::new(s))
    }

    pub(crate) fn path(self, dir: &Path) -> PathBuf {
        dir.join(self.file_name())
    }

    pub(crate) fn file_name(self) -> String {
        format!("append-{}{}", self.id.get(), SEGMENT_FILE_SUFFIX)
    }

    pub(crate) fn id(self) -> u64 {
        self.id.get()
    }
}

pub(crate) fn select_active_append_segment(dir: &Path) -> io::Result<SegmentName> {
    Ok(discover_segment_paths(dir)?
        .into_iter()
        .map(|segment| segment.name)
        .max()
        .unwrap_or_else(SegmentName::first_append))
}

pub(crate) fn segment_file_exists(dir: &Path, name: SegmentName) -> io::Result<bool> {
    match std::fs::metadata(name.path(dir)) {
        Ok(metadata) => Ok(metadata.is_file()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

#[derive(Debug)]
pub(crate) struct SegmentPath {
    pub(crate) name: SegmentName,
    pub(crate) path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RecordPosition {
    pub(crate) segment: SegmentName,
    pub(crate) offset: u64,
}

pub(crate) fn discover_segment_paths(dir: &Path) -> io::Result<Vec<SegmentPath>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };

    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let Some(name) = SegmentName::parse_file_name(&entry.file_name()) else {
            continue;
        };
        paths.push(SegmentPath {
            name,
            path: entry.path(),
        });
    }
    paths.sort_by_key(|segment| segment.name);
    Ok(paths)
}

pub(crate) fn read_segment_header(
    file: &mut File,
    allow_partial: bool,
    recover_partial: bool,
    stats: Option<&mut StorageStatsCounters>,
) -> io::Result<bool> {
    file.seek(SeekFrom::Start(0))?;
    let file_len = file.metadata()?.len();
    if file_len == 0 {
        if allow_partial {
            return Ok(false);
        }
        return Err(invalid_data("missing segment header"));
    }
    if file_len < SEGMENT_FILE_HEADER_LEN_U64 {
        if allow_partial && recover_partial {
            if let Some(stats) = stats {
                stats.replay_truncated();
            }
            file.set_len(0)?;
            file.seek(SeekFrom::Start(0))?;
            return Ok(false);
        }
        if allow_partial {
            if let Some(stats) = stats {
                stats.replay_truncated();
            }
            return Ok(false);
        }
        return Err(invalid_data("partial segment header in inactive segment"));
    }

    let mut magic = [0; SEGMENT_FILE_HEADER_LEN];
    file.read_exact(&mut magic)?;
    if &magic != SEGMENT_FORMAT_MAGIC {
        return Err(invalid_data("unsupported segment format"));
    }
    Ok(true)
}

fn open_active_segment_file(path: &Path) -> io::Result<File> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;
    if file.metadata()?.len() == 0 {
        file.write_all(SEGMENT_FORMAT_MAGIC)?;
    }
    Ok(file)
}

fn create_active_segment_file(path: &Path) -> io::Result<File> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .append(true)
        .open(path)?;
    file.write_all(SEGMENT_FORMAT_MAGIC)?;
    Ok(file)
}
