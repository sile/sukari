use sukari::{OperationKindMetrics, RecordKindMetrics, RejectedOperationMetrics, StorageMetrics};

#[test]
fn record_kind_metrics_displays_json() {
    let metrics = record_kind_metrics(1, 2, 3, 4);

    assert_eq!(
        metrics.to_string(),
        r#"{"current_term":1,"voted_for":2,"log_append":3,"snapshot_checkpoint":4}"#
    );
    assert_eq!(
        nojson::json(|f| f.value(metrics)).to_string(),
        metrics.to_string()
    );
}

#[test]
fn operation_kind_metrics_displays_json() {
    let metrics = operation_kind_metrics(1, 2, 3, 4, 5, 6);

    assert_eq!(
        metrics.to_string(),
        r#"{"load":1,"save_current_term":2,"save_voted_for":3,"append_entries":4,"save_snapshot":5,"remove_node":6}"#
    );
    assert_eq!(
        nojson::json(|f| f.value(metrics)).to_string(),
        metrics.to_string()
    );
}

#[test]
fn rejected_operation_metrics_displays_json() {
    let metrics = RejectedOperationMetrics {
        unknown_nodes: operation_kind_metrics(1, 2, 3, 4, 5, 6),
        removed_nodes: operation_kind_metrics(7, 8, 9, 10, 11, 12),
    };

    assert_eq!(
        metrics.to_string(),
        concat!(
            r#"{"unknown_nodes":{"load":1,"save_current_term":2,"#,
            r#""save_voted_for":3,"append_entries":4,"save_snapshot":5,"remove_node":6},"#,
            r#""removed_nodes":{"load":7,"save_current_term":8,"#,
            r#""save_voted_for":9,"append_entries":10,"save_snapshot":11,"remove_node":12}}"#,
        )
    );
    assert_eq!(
        nojson::json(|f| f.value(&metrics)).to_string(),
        metrics.to_string()
    );
}

#[test]
fn storage_metrics_displays_json() {
    let metrics = StorageMetrics {
        records_written: record_kind_metrics(1, 2, 3, 4),
        bytes_written: record_kind_metrics(5, 6, 7, 8),
        records_replayed: record_kind_metrics(9, 10, 11, 12),
        bytes_replayed: record_kind_metrics(13, 14, 15, 16),
        segment_rotations: 17,
        syncs: 18,
        durable_syncs: 19,
        replay_truncations: 20,
        checksum_failures: 21,
        nodes_created: 22,
        nodes_removed: 23,
        rejected_operations: RejectedOperationMetrics {
            unknown_nodes: operation_kind_metrics(24, 25, 26, 27, 28, 29),
            removed_nodes: operation_kind_metrics(30, 31, 32, 33, 34, 35),
        },
        snapshot_checkpoints_saved: 36,
        gc_runs: 37,
        gc_segments_deleted: 38,
        active_nodes: 39,
        removed_nodes: 40,
        checkpoint_index_nodes: 41,
        active_append_segment_id: 42,
        active_append_segment_len_bytes: 43,
        unsynced_records: 44,
        unsynced_bytes: 45,
    };

    assert_eq!(
        metrics.to_string(),
        concat!(
            r#"{"records_written":{"current_term":1,"voted_for":2,"log_append":3,"snapshot_checkpoint":4},"#,
            r#""bytes_written":{"current_term":5,"voted_for":6,"log_append":7,"snapshot_checkpoint":8},"#,
            r#""records_replayed":{"current_term":9,"voted_for":10,"log_append":11,"snapshot_checkpoint":12},"#,
            r#""bytes_replayed":{"current_term":13,"voted_for":14,"log_append":15,"snapshot_checkpoint":16},"#,
            r#""segment_rotations":17,"syncs":18,"durable_syncs":19,"replay_truncations":20,"#,
            r#""checksum_failures":21,"nodes_created":22,"nodes_removed":23,"#,
            r#""rejected_operations":{"unknown_nodes":{"load":24,"save_current_term":25,"#,
            r#""save_voted_for":26,"append_entries":27,"save_snapshot":28,"remove_node":29},"#,
            r#""removed_nodes":{"load":30,"save_current_term":31,"save_voted_for":32,"#,
            r#""append_entries":33,"save_snapshot":34,"remove_node":35}},"#,
            r#""snapshot_checkpoints_saved":36,"gc_runs":37,"gc_segments_deleted":38,"#,
            r#""active_nodes":39,"removed_nodes":40,"checkpoint_index_nodes":41,"#,
            r#""active_append_segment_id":42,"active_append_segment_len_bytes":43,"#,
            r#""unsynced_records":44,"unsynced_bytes":45}"#,
        )
    );
    assert_eq!(
        nojson::json(|f| f.value(&metrics)).to_string(),
        metrics.to_string()
    );
}

fn record_kind_metrics(
    current_term: u64,
    voted_for: u64,
    log_append: u64,
    snapshot_checkpoint: u64,
) -> RecordKindMetrics {
    RecordKindMetrics {
        current_term,
        voted_for,
        log_append,
        snapshot_checkpoint,
    }
}

fn operation_kind_metrics(
    load: u64,
    save_current_term: u64,
    save_voted_for: u64,
    append_entries: u64,
    save_snapshot: u64,
    remove_node: u64,
) -> OperationKindMetrics {
    OperationKindMetrics {
        load,
        save_current_term,
        save_voted_for,
        append_entries,
        save_snapshot,
        remove_node,
    }
}
