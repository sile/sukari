use crate::bytes::Bytes;
use crate::crc32c::{Crc32c, crc32c};
use crate::error::{invalid_data, invalid_input};
use crate::metrics::StorageMetricsCounters;
use crate::storage::{CommandPayload, LogAppend, NodeRecord, Record, Snapshot, SnapshotCheckpoint};

use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, Read},
};

const SEGMENT_RECORD_BASE_HEADER_LEN: usize = 4;
const SEGMENT_CHECKSUM_LEN: usize = 4;
const SEGMENT_RECORD_HEADER_LEN: usize = SEGMENT_RECORD_BASE_HEADER_LEN + SEGMENT_CHECKSUM_LEN;
const MAX_COUNT_ITEMS: u32 = 1_000_000;
const MAX_RECORD_BODY_LEN: u32 = 1024 * 1024 * 1024;

pub(crate) fn encode_record_frame(node_id: noraft::NodeId, record: &Record) -> io::Result<Vec<u8>> {
    let mut body = Encoder::new();
    encode_node_id(node_id, &mut body)?;
    encode_record(record, &mut body)?;
    let body = body.finish();

    let body_len = u32::try_from(body.len()).map_err(|_| invalid_input("record is too large"))?;
    if MAX_RECORD_BODY_LEN < body_len {
        return Err(invalid_input("record is too large"));
    }

    let mut frame = Vec::new();
    frame.extend_from_slice(&body_len.to_le_bytes());
    frame.extend_from_slice(&crc32c(&body).to_le_bytes());
    debug_assert_eq!(frame.len(), SEGMENT_RECORD_HEADER_LEN);
    frame.extend_from_slice(&body);
    Ok(frame)
}

pub(crate) fn read_record_body(
    file: &mut File,
    metrics: Option<&mut StorageMetricsCounters>,
) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0; SEGMENT_RECORD_BASE_HEADER_LEN];
    match file.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    let body_len = u32::from_le_bytes(header);
    if MAX_RECORD_BODY_LEN < body_len {
        return Err(invalid_data("segment record is too large"));
    }

    let mut checksum = [0; SEGMENT_CHECKSUM_LEN];
    match file.read_exact(&mut checksum) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let expected_checksum = u32::from_le_bytes(checksum);

    let mut body = Vec::new();
    let mut reader = file.take(u64::from(body_len));
    reader.read_to_end(&mut body)?;
    if body.len() != body_len as usize {
        return Ok(None);
    }

    let actual_checksum = crc32c(&body);
    if actual_checksum != expected_checksum {
        if let Some(metrics) = metrics {
            metrics.checksum_failed();
        }
        return Err(invalid_data("segment record checksum mismatch"));
    }
    Ok(Some(body))
}

pub(crate) fn scan_record_frame(
    file: &mut File,
    metrics: &mut StorageMetricsCounters,
) -> io::Result<Option<()>> {
    let mut header = [0; SEGMENT_RECORD_BASE_HEADER_LEN];
    match file.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    let body_len = u32::from_le_bytes(header);
    if MAX_RECORD_BODY_LEN < body_len {
        return Err(invalid_data("segment record is too large"));
    }

    let mut checksum = [0; SEGMENT_CHECKSUM_LEN];
    match file.read_exact(&mut checksum) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let expected_checksum = u32::from_le_bytes(checksum);

    let mut crc = Crc32c::new();
    let mut remaining = u64::from(body_len);
    let mut buffer = [0; 8192];
    while remaining != 0 {
        let read_len = remaining.min(buffer.len() as u64) as usize;
        match file.read_exact(&mut buffer[..read_len]) {
            Ok(()) => {
                crc.update(&buffer[..read_len]);
                remaining -= read_len as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }
    }

    if crc.value() != expected_checksum {
        metrics.checksum_failed();
        return Err(invalid_data("segment record checksum mismatch"));
    }

    Ok(Some(()))
}

pub(crate) fn frame_len_from_body(body: &[u8]) -> io::Result<u64> {
    u64::try_from(SEGMENT_RECORD_HEADER_LEN + body.len())
        .map_err(|_| invalid_data("segment frame length overflow"))
}

pub(crate) fn decode_node_record(bytes: &[u8]) -> io::Result<NodeRecord> {
    let mut decoder = Decoder::new(bytes);
    let node_id = decode_node_id(&mut decoder)?;
    let record = match decoder.get_u8()? {
        0 => Record::CurrentTerm(decode_term(&mut decoder)?),
        1 => Record::VotedFor(decode_optional_node_id(&mut decoder)?),
        2 => Record::Append(decode_log_append(&mut decoder)?),
        3 => Record::SnapshotCheckpoint(decode_snapshot_checkpoint(&mut decoder)?),
        _ => return Err(invalid_data("unknown segment record tag")),
    };
    decoder.finish()?;
    Ok(NodeRecord { node_id, record })
}

pub(crate) fn decode_record_node_id(bytes: &[u8]) -> io::Result<noraft::NodeId> {
    let mut decoder = Decoder::new(bytes);
    decode_node_id(&mut decoder)
}

fn encode_record(record: &Record, encoder: &mut Encoder) -> io::Result<()> {
    match record {
        Record::CurrentTerm(term) => {
            encoder.put_u8(0)?;
            encode_term(*term, encoder)?;
        }
        Record::VotedFor(voted_for) => {
            encoder.put_u8(1)?;
            encode_optional_node_id(*voted_for, encoder)?;
        }
        Record::Append(append) => {
            encoder.put_u8(2)?;
            encode_log_append(append, encoder)?;
        }
        Record::SnapshotCheckpoint(checkpoint) => {
            encoder.put_u8(3)?;
            encode_snapshot_checkpoint(checkpoint, encoder)?;
        }
    }
    Ok(())
}

fn encode_log_append(append: &LogAppend, encoder: &mut Encoder) -> io::Result<()> {
    append.validate()?;
    encode_log_entries(append.entries(), encoder)?;
    encode_count(
        append.command_payloads().len(),
        "too many command payloads",
        encoder,
    )?;
    for (index, payload) in append.command_payloads() {
        encode_log_index(*index, encoder)?;
        encoder.put_u8(payload.tag())?;
        encoder.put_bytes(payload.bytes().as_slice())?;
    }
    Ok(())
}

fn decode_log_append(decoder: &mut Decoder<'_>) -> io::Result<LogAppend> {
    let entries = decode_log_entries(decoder)?;
    let command_count = decode_count(decoder, "too many command payloads")?;

    let mut command_payloads = BTreeMap::new();
    for _ in 0..command_count {
        let index = decode_log_index(decoder)?;
        let tag = decoder.get_u8()?;
        let payload = CommandPayload::new(tag, Bytes::from(decoder.get_bytes()?));
        if command_payloads.insert(index, payload).is_some() {
            return Err(invalid_data("duplicate command payload index"));
        }
    }

    LogAppend::new(entries, command_payloads)
        .map_err(|_| invalid_data("invalid command payload mapping"))
}

fn encode_snapshot(snapshot: &Snapshot, encoder: &mut Encoder) -> io::Result<()> {
    encode_log_position(snapshot.last_included, encoder)?;
    encode_cluster_config(&snapshot.config, encoder)?;
    encoder.put_bytes(snapshot.data.as_slice())
}

fn decode_snapshot(decoder: &mut Decoder<'_>) -> io::Result<Snapshot> {
    Ok(Snapshot {
        last_included: decode_log_position(decoder)?,
        config: decode_cluster_config(decoder)?,
        data: Bytes::from(decoder.get_bytes()?),
    })
}

fn encode_snapshot_checkpoint(
    checkpoint: &SnapshotCheckpoint,
    encoder: &mut Encoder,
) -> io::Result<()> {
    checkpoint.validate()?;
    encode_term(checkpoint.current_term, encoder)?;
    encode_optional_node_id(checkpoint.voted_for, encoder)?;
    encode_snapshot(&checkpoint.snapshot, encoder)?;
    encode_log_append(&checkpoint.suffix, encoder)
}

fn decode_snapshot_checkpoint(decoder: &mut Decoder<'_>) -> io::Result<SnapshotCheckpoint> {
    let checkpoint = SnapshotCheckpoint {
        current_term: decode_term(decoder)?,
        voted_for: decode_optional_node_id(decoder)?,
        snapshot: decode_snapshot(decoder)?,
        suffix: decode_log_append(decoder)?,
    };
    checkpoint
        .validate()
        .map_err(|_| invalid_data("invalid snapshot checkpoint"))?;
    Ok(checkpoint)
}

fn encode_log_entries(entries: &noraft::LogEntries, encoder: &mut Encoder) -> io::Result<()> {
    encode_log_position(entries.prev_position(), encoder)?;
    encode_count(entries.len(), "too many log entries", encoder)?;
    for entry in entries.iter() {
        encode_log_entry(&entry, encoder)?;
    }
    Ok(())
}

fn decode_log_entries(decoder: &mut Decoder<'_>) -> io::Result<noraft::LogEntries> {
    let prev_position = decode_log_position(decoder)?;
    let len = decode_count(decoder, "too many log entries")?;

    if prev_position
        .index
        .checked_add(noraft::LogIndex::new(u64::from(len)))
        .is_none()
    {
        return Err(invalid_data("log entries overflow the log index space"));
    }

    let mut entries = noraft::LogEntries::new(prev_position);
    for _ in 0..len {
        entries.push(decode_log_entry(decoder)?);
    }
    Ok(entries)
}

fn encode_log_entry(entry: &noraft::LogEntry, encoder: &mut Encoder) -> io::Result<()> {
    match entry {
        noraft::LogEntry::Term(term) => {
            encoder.put_u8(0)?;
            encode_term(*term, encoder)?;
        }
        noraft::LogEntry::ClusterConfig(config) => {
            encoder.put_u8(1)?;
            encode_cluster_config(config, encoder)?;
        }
        noraft::LogEntry::Command => encoder.put_u8(2)?,
    }
    Ok(())
}

fn decode_log_entry(decoder: &mut Decoder<'_>) -> io::Result<noraft::LogEntry> {
    match decoder.get_u8()? {
        0 => Ok(noraft::LogEntry::Term(decode_term(decoder)?)),
        1 => Ok(noraft::LogEntry::ClusterConfig(decode_cluster_config(
            decoder,
        )?)),
        2 => Ok(noraft::LogEntry::Command),
        _ => Err(invalid_data("unknown log entry tag")),
    }
}

fn encode_cluster_config(config: &noraft::ClusterConfig, encoder: &mut Encoder) -> io::Result<()> {
    encode_node_id_set(&config.voters, encoder)?;
    encode_node_id_set(&config.new_voters, encoder)?;
    encode_node_id_set(&config.non_voters, encoder)
}

fn decode_cluster_config(decoder: &mut Decoder<'_>) -> io::Result<noraft::ClusterConfig> {
    Ok(noraft::ClusterConfig {
        voters: decode_node_id_set(decoder)?,
        new_voters: decode_node_id_set(decoder)?,
        non_voters: decode_node_id_set(decoder)?,
    })
}

fn encode_node_id_set(
    nodes: &std::collections::BTreeSet<noraft::NodeId>,
    encoder: &mut Encoder,
) -> io::Result<()> {
    encode_count(nodes.len(), "too many node IDs", encoder)?;
    for node in nodes {
        encode_node_id(*node, encoder)?;
    }
    Ok(())
}

fn decode_node_id_set(
    decoder: &mut Decoder<'_>,
) -> io::Result<std::collections::BTreeSet<noraft::NodeId>> {
    let len = decode_count(decoder, "too many node IDs")?;

    let mut nodes = std::collections::BTreeSet::new();
    for _ in 0..len {
        if !nodes.insert(decode_node_id(decoder)?) {
            return Err(invalid_data("duplicate node ID"));
        }
    }
    Ok(nodes)
}

fn encode_optional_node_id(
    node_id: Option<noraft::NodeId>,
    encoder: &mut Encoder,
) -> io::Result<()> {
    match node_id {
        Some(node_id) => {
            encoder.put_u8(1)?;
            encode_node_id(node_id, encoder)?;
        }
        None => encoder.put_u8(0)?,
    }
    Ok(())
}

fn decode_optional_node_id(decoder: &mut Decoder<'_>) -> io::Result<Option<noraft::NodeId>> {
    match decoder.get_u8()? {
        0 => Ok(None),
        1 => Ok(Some(decode_node_id(decoder)?)),
        _ => Err(invalid_data("invalid optional node ID tag")),
    }
}

fn encode_log_position(position: noraft::LogPosition, encoder: &mut Encoder) -> io::Result<()> {
    encode_term(position.term, encoder)?;
    encode_log_index(position.index, encoder)
}

fn decode_log_position(decoder: &mut Decoder<'_>) -> io::Result<noraft::LogPosition> {
    Ok(noraft::LogPosition::new(
        decode_term(decoder)?,
        decode_log_index(decoder)?,
    ))
}

fn encode_term(term: noraft::Term, encoder: &mut Encoder) -> io::Result<()> {
    encoder.put_u64(term.get())
}

fn decode_term(decoder: &mut Decoder<'_>) -> io::Result<noraft::Term> {
    Ok(noraft::Term::new(decoder.get_u64()?))
}

fn encode_node_id(node_id: noraft::NodeId, encoder: &mut Encoder) -> io::Result<()> {
    encoder.put_u64(node_id.get())
}

fn decode_node_id(decoder: &mut Decoder<'_>) -> io::Result<noraft::NodeId> {
    Ok(noraft::NodeId::new(decoder.get_u64()?))
}

fn encode_log_index(index: noraft::LogIndex, encoder: &mut Encoder) -> io::Result<()> {
    encoder.put_u64(index.get())
}

fn decode_log_index(decoder: &mut Decoder<'_>) -> io::Result<noraft::LogIndex> {
    Ok(noraft::LogIndex::new(decoder.get_u64()?))
}

fn encode_count(value: usize, error: &'static str, encoder: &mut Encoder) -> io::Result<()> {
    let value = u32::try_from(value).map_err(|_| invalid_input(error))?;
    if MAX_COUNT_ITEMS < value {
        return Err(invalid_input(error));
    }
    encoder.put_u32(value)
}

fn decode_count(decoder: &mut Decoder<'_>, error: &'static str) -> io::Result<u32> {
    let value = decoder.get_u32()?;
    if MAX_COUNT_ITEMS < value {
        return Err(invalid_data(error));
    }
    Ok(value)
}

#[derive(Debug)]
struct Encoder {
    bytes: Vec<u8>,
    len: u32,
}

impl Encoder {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            len: 0,
        }
    }

    fn put_u8(&mut self, value: u8) -> io::Result<()> {
        self.put_slice(&[value])
    }

    fn put_u32(&mut self, value: u32) -> io::Result<()> {
        self.put_slice(&value.to_le_bytes())
    }

    fn put_u64(&mut self, value: u64) -> io::Result<()> {
        self.put_slice(&value.to_le_bytes())
    }

    fn put_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        let len =
            u32::try_from(bytes.len()).map_err(|_| invalid_input("byte slice is too large"))?;
        if MAX_RECORD_BODY_LEN < len {
            return Err(invalid_input("byte slice is too large"));
        }
        let additional = 4usize
            .checked_add(bytes.len())
            .ok_or_else(|| invalid_input("record is too large"))?;
        let new_len = self.check_append_len(additional)?;
        self.bytes.extend_from_slice(&len.to_le_bytes());
        self.bytes.extend_from_slice(bytes);
        self.len = new_len;
        Ok(())
    }

    fn put_slice(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.len = self.check_append_len(bytes.len())?;
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn check_append_len(&self, additional: usize) -> io::Result<u32> {
        let additional =
            u32::try_from(additional).map_err(|_| invalid_input("record is too large"))?;
        let len = self
            .len
            .checked_add(additional)
            .ok_or_else(|| invalid_input("record is too large"))?;
        if MAX_RECORD_BODY_LEN < len {
            return Err(invalid_input("record is too large"));
        }
        Ok(len)
    }

    fn finish(self) -> Vec<u8> {
        debug_assert_eq!(self.bytes.len(), self.len as usize);
        self.bytes
    }
}

#[derive(Debug)]
struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn get_u8(&mut self) -> io::Result<u8> {
        let bytes = self.take(1)?;
        Ok(bytes[0])
    }

    fn get_u32(&mut self) -> io::Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes(
            bytes.try_into().expect("u32 encoding should be four bytes"),
        ))
    }

    fn get_u64(&mut self) -> io::Result<u64> {
        let bytes = self.take(8)?;
        Ok(u64::from_le_bytes(
            bytes
                .try_into()
                .expect("u64 encoding should be eight bytes"),
        ))
    }

    fn get_bytes(&mut self) -> io::Result<&'a [u8]> {
        let len = self.get_u32()?;
        if MAX_RECORD_BODY_LEN < len {
            return Err(invalid_data("byte slice is too large"));
        }
        let len = usize::try_from(len).map_err(|_| invalid_data("byte slice is too large"))?;
        self.take(len)
    }

    fn take(&mut self, len: usize) -> io::Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(len)
            .ok_or_else(|| invalid_data("record offset overflow"))?;
        if self.bytes.len() < end {
            return Err(invalid_data("record is too short"));
        }
        let slice = &self.bytes[self.position..end];
        self.position = end;
        Ok(slice)
    }

    fn finish(&self) -> io::Result<()> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(invalid_data("record has trailing bytes"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoder_rejects_record_body_limit_before_integer_write() {
        let mut encoder = encoder_with_len_for_test(MAX_RECORD_BODY_LEN);

        let err = encoder
            .put_u8(0)
            .expect_err("integer write should exceed the record body limit");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(encoder.bytes.is_empty());
        assert_eq!(encoder.len, MAX_RECORD_BODY_LEN);
    }

    #[test]
    fn encoder_rejects_record_body_limit_before_byte_write() {
        let mut encoder = encoder_with_len_for_test(MAX_RECORD_BODY_LEN - 3);

        let err = encoder
            .put_bytes(&[])
            .expect_err("byte length prefix should exceed the record body limit");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(encoder.bytes.is_empty());
        assert_eq!(encoder.len, MAX_RECORD_BODY_LEN - 3);
    }

    fn encoder_with_len_for_test(len: u32) -> Encoder {
        Encoder {
            bytes: Vec::new(),
            len,
        }
    }
}
