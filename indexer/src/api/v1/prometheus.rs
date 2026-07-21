use indexer_actors::metrics::IndexerMetricsSnapshot;
use std::fmt::Write;

pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Render a metrics snapshot using the Prometheus 0.0.4 text exposition format.
///
/// Block hashes are intentionally omitted: emitting a new hash as a label for
/// every block would create an unbounded stream of short-lived time series.
pub fn render(snapshot: &IndexerMetricsSnapshot) -> String {
    let mut output = String::with_capacity(8192);

    macro_rules! metric {
        ($name:literal, $help:literal, $kind:literal, $value:expr) => {{
            writeln!(output, concat!("# HELP ", $name, " ", $help)).unwrap();
            writeln!(output, concat!("# TYPE ", $name, " ", $kind)).unwrap();
            writeln!(output, concat!($name, " {}"), $value).unwrap();
        }};
    }

    metric!(
        "kasia_indexer_handshakes_by_sender",
        "Current number of indexed handshakes in the sender index.",
        "gauge",
        snapshot.handshakes_by_sender
    );
    metric!(
        "kasia_indexer_unique_handshakes_by_receiver",
        "Current number of indexed unique handshakes in the receiver index.",
        "gauge",
        snapshot.uniq_handshakes_by_receiver
    );
    metric!(
        "kasia_indexer_payments_by_sender",
        "Current number of indexed payments in the sender index.",
        "gauge",
        snapshot.payments_by_sender
    );
    metric!(
        "kasia_indexer_unique_payments_by_receiver",
        "Current number of indexed unique payments in the receiver index.",
        "gauge",
        snapshot.uniq_payments_by_receiver
    );
    metric!(
        "kasia_indexer_contextual_messages",
        "Current number of indexed contextual messages.",
        "gauge",
        snapshot.contextual_messages
    );
    metric!(
        "kasia_indexer_blocks_processed_total",
        "Total number of blocks processed since the metrics state was initialized.",
        "counter",
        snapshot.blocks_processed
    );
    metric!(
        "kasia_indexer_unknown_sender_entries",
        "Current number of unresolved sender entries.",
        "gauge",
        snapshot.unknown_sender_entries
    );
    metric!(
        "kasia_indexer_resolved_senders_total",
        "Total number of sender entries resolved since the metrics state was initialized.",
        "counter",
        snapshot.resolved_senders
    );
    metric!(
        "kasia_indexer_pruned_blocks_total",
        "Total number of blocks pruned since the metrics state was initialized.",
        "counter",
        snapshot.pruned_blocks
    );
    metric!(
        "kasia_indexer_push_registered_devices",
        "Current number of registered push notification devices.",
        "gauge",
        snapshot.push_registered_devices
    );
    metric!(
        "kasia_indexer_push_register_calls_total",
        "Total number of push registration calls.",
        "counter",
        snapshot.push_register_calls_total
    );
    metric!(
        "kasia_indexer_push_update_calls_total",
        "Total number of push registration update calls.",
        "counter",
        snapshot.push_update_calls_total
    );
    metric!(
        "kasia_indexer_push_unregister_calls_total",
        "Total number of push unregistration calls.",
        "counter",
        snapshot.push_unregister_calls_total
    );
    metric!(
        "kasia_indexer_push_fast_path_skips_total",
        "Total number of push events skipped by the fast path.",
        "counter",
        snapshot.push_fast_path_skips_total
    );
    metric!(
        "kasia_indexer_push_events_total",
        "Total number of push notification events processed.",
        "counter",
        snapshot.push_events_total
    );
    metric!(
        "kasia_indexer_push_tokens_looked_up_total",
        "Total number of push tokens looked up.",
        "counter",
        snapshot.push_tokens_looked_up_total
    );
    metric!(
        "kasia_indexer_push_filtered_alias_total",
        "Total number of push deliveries filtered as alias recipients.",
        "counter",
        snapshot.push_filtered_alias_total
    );
    metric!(
        "kasia_indexer_push_filtered_primary_total",
        "Total number of push deliveries filtered as primary recipients.",
        "counter",
        snapshot.push_filtered_primary_total
    );
    metric!(
        "kasia_indexer_push_dedup_dropped_total",
        "Total number of duplicate push deliveries dropped.",
        "counter",
        snapshot.push_dedup_dropped_total
    );
    metric!(
        "kasia_indexer_push_sent_total",
        "Total number of push notifications sent successfully.",
        "counter",
        snapshot.push_sent_ok_total
    );
    metric!(
        "kasia_indexer_push_send_errors_total",
        "Total number of push notification send failures.",
        "counter",
        snapshot.push_send_failed_total
    );
    metric!(
        "kasia_indexer_push_unregistered_removed_total",
        "Total number of unregistered push tokens removed.",
        "counter",
        snapshot.push_unregistered_removed_total
    );
    metric!(
        "kasia_indexer_push_invalid_tokens_total",
        "Total number of invalid push tokens observed.",
        "counter",
        snapshot.push_invalid_token_total
    );
    metric!(
        "kasia_indexer_db_read_operations_total",
        "Total number of measured database read operations.",
        "counter",
        snapshot.db_read_ops_total
    );
    metric!(
        "kasia_indexer_db_write_operations_total",
        "Total number of measured database write operations.",
        "counter",
        snapshot.db_write_ops_total
    );
    metric!(
        "kasia_indexer_db_read_duration_seconds_total",
        "Cumulative time spent in measured database reads in seconds.",
        "counter",
        snapshot.db_read_time_ms_total as f64 / 1000.0
    );
    metric!(
        "kasia_indexer_db_write_duration_seconds_total",
        "Cumulative time spent in measured database writes in seconds.",
        "counter",
        snapshot.db_write_time_ms_total as f64 / 1000.0
    );
    metric!(
        "kasia_indexer_db_commit_conflicts_total",
        "Total number of database commit conflicts.",
        "counter",
        snapshot.db_commit_conflicts_total
    );
    metric!(
        "kasia_indexer_db_errors_total",
        "Total number of measured database errors.",
        "counter",
        snapshot.db_errors_total
    );

    output
}

#[cfg(test)]
mod tests {
    use super::{CONTENT_TYPE, render};
    use indexer_actors::metrics::IndexerMetricsSnapshot;

    #[test]
    fn renders_prometheus_text_without_block_hash_labels() {
        let output = render(&IndexerMetricsSnapshot {
            blocks_processed: 42,
            push_registered_devices: 3,
            push_send_failed_total: 2,
            db_read_time_ms_total: 1_500,
            ..Default::default()
        });

        assert_eq!(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8");
        assert!(output.contains("# TYPE kasia_indexer_blocks_processed_total counter\n"));
        assert!(output.contains("kasia_indexer_blocks_processed_total 42\n"));
        assert!(output.contains("kasia_indexer_push_registered_devices 3\n"));
        assert!(output.contains("kasia_indexer_push_send_errors_total 2\n"));
        assert!(output.contains("kasia_indexer_db_read_duration_seconds_total 1.5\n"));
        assert!(!output.contains("latest_block"));
        assert!(!output.contains("latest_accepting_block"));
        assert!(output.ends_with('\n'));
    }
}
