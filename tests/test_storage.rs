use sukari::{
    bytes::Bytes,
    storage::{LogAppend, StorageState},
};

use std::{collections::BTreeMap, io};

#[test]
fn log_append_requires_command_payloads() {
    let entries =
        noraft::LogEntries::from_iter(noraft::LogPosition::ZERO, [noraft::LogEntry::Command]);

    let err = LogAppend::new(entries, BTreeMap::new())
        .expect_err("command entries should require payloads");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn log_append_rejects_payloads_without_command_entries() {
    let entries = noraft::LogEntries::from_iter(
        noraft::LogPosition::ZERO,
        [noraft::LogEntry::Term(noraft::Term::new(1))],
    );
    let mut commands = BTreeMap::new();
    commands.insert(noraft::LogIndex::new(1), Bytes::from(b"command".as_slice()));

    let err =
        LogAppend::new(entries, commands).expect_err("term entries should not accept payloads");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn storage_state_applies_log_suffix_replacement() {
    let mut state = StorageState::default();
    let mut commands = BTreeMap::new();
    commands.insert(noraft::LogIndex::new(2), Bytes::from(b"old-2".as_slice()));
    commands.insert(noraft::LogIndex::new(3), Bytes::from(b"old-3".as_slice()));

    let initial_entries = noraft::LogEntries::from_iter(
        noraft::LogPosition::ZERO,
        [
            noraft::LogEntry::Term(noraft::Term::new(1)),
            noraft::LogEntry::Command,
            noraft::LogEntry::Command,
        ],
    );
    let initial_append = LogAppend::new(initial_entries, commands)
        .expect("initial append should have matching command payloads");
    state
        .apply_append(&initial_append)
        .expect("initial append should apply");

    let mut replacement_commands = BTreeMap::new();
    replacement_commands.insert(noraft::LogIndex::new(2), Bytes::from(b"new-2".as_slice()));
    let replacement_entries =
        noraft::LogEntries::from_iter(position(1, 1), [noraft::LogEntry::Command]);
    let replacement_append = LogAppend::new(replacement_entries, replacement_commands)
        .expect("replacement append should have matching command payloads");
    state
        .apply_append(&replacement_append)
        .expect("replacement append should apply");

    assert_eq!(state.log.entries().last_position(), position(1, 2));
    assert_eq!(state.commands.len(), 1);
    assert_eq!(
        state
            .commands
            .get(&noraft::LogIndex::new(2))
            .expect("replacement payload should exist")
            .as_slice(),
        b"new-2"
    );
}

fn position(term: u64, index: u64) -> noraft::LogPosition {
    noraft::LogPosition {
        term: noraft::Term::new(term),
        index: noraft::LogIndex::new(index),
    }
}
