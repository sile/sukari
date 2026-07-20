#![no_main]

use std::{
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use libfuzzer_sys::fuzz_target;
use sukari::{StorageEngine, SyncPolicy};

const SEGMENT_FILE_NAME: &str = "append-0.segment";
const NODE_REGISTRY_FILE_NAME: &str = "nodes.json";
const NODE_REGISTRY: &[u8] =
    br#"{"version":1,"nodes":{"1":{"startup":true,"metadata":{},"removed":false}}}"#;

#[derive(Debug)]
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Option<Self> {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);

        let path = std::env::temp_dir().join(format!(
            "sukari-fuzz-segment-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        match std::fs::remove_dir_all(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(_) => return None,
        }
        if std::fs::create_dir(&path).is_err() {
            return None;
        }
        Some(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fuzz_target!(|data: &[u8]| {
    exercise_segment(data);

    let mut body = Vec::new();
    body.extend_from_slice(&1u64.to_le_bytes());
    body.extend_from_slice(data);
    if let Some(segment) = segment_file_with_record(&body) {
        exercise_segment(&segment);
    }

    if let Some(segment) = segment_file_with_record(data) {
        exercise_segment(&segment);
    }
});

fn exercise_segment(segment: &[u8]) {
    let Some(dir) = TempDir::new() else {
        return;
    };
    if std::fs::write(dir.path().join(NODE_REGISTRY_FILE_NAME), NODE_REGISTRY).is_err() {
        return;
    }
    if std::fs::write(dir.path().join(SEGMENT_FILE_NAME), segment).is_err() {
        return;
    }

    let Ok(mut engine) = StorageEngine::new(dir.path(), SyncPolicy::UnsafeNoSync) else {
        return;
    };
    let _ = engine.load_all();
}

fn segment_file_with_record(body: &[u8]) -> Option<Vec<u8>> {
    let body_len = u32::try_from(body.len()).ok()?;
    let mut segment = Vec::new();
    segment.extend_from_slice(b"SKR1");
    segment.extend_from_slice(&body_len.to_le_bytes());
    segment.extend_from_slice(&crc32c(body).to_le_bytes());
    segment.extend_from_slice(body);
    Some(segment)
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            if crc & 1 == 0 {
                crc >>= 1;
            } else {
                crc = (crc >> 1) ^ 0x82F6_3B78;
            }
        }
    }
    !crc
}
