use crate::util::ToHex64;
use arc_swap::ArcSwap;
use fstr::FStr;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "serde")]
use serde::Serialize;
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

/// A snapshot of the indexer metrics.
/// This structure contains a copy of all metric counters as simple u64 values.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct IndexerMetricsSnapshot {
    /// Number of handshakes indexed by sender
    pub handshakes_by_sender: u64,
    /// Number of handshakes indexed by receiver
    pub uniq_handshakes_by_receiver: u64,
    /// Number of payments indexed by sender
    pub payments_by_sender: u64,
    /// Number of payments indexed by receiver
    pub uniq_payments_by_receiver: u64,
    /// Number of contextual messages indexed
    pub contextual_messages: u64,
    /// Number of blocks processed
    pub blocks_processed: u64,
    /// Latest block hash processed
    #[cfg_attr(feature = "utoipa", schema(value_type = String, format = "hex"))]
    pub latest_block: FStr<64>,
    /// Latest accepting block hash
    #[cfg_attr(feature = "utoipa", schema(value_type = String, format = "hex"))]
    pub latest_accepting_block: FStr<64>,
    /// Number of unknown sender entries
    pub unknown_sender_entries: u64,
    pub resolved_senders: u64,
    pub pruned_blocks: u64,
    pub push_registered_devices: u64,
    pub push_register_calls_total: u64,
    pub push_update_calls_total: u64,
    pub push_unregister_calls_total: u64,
    pub push_fast_path_skips_total: u64,
    pub push_events_total: u64,
    pub push_tokens_looked_up_total: u64,
    pub push_filtered_alias_total: u64,
    pub push_filtered_primary_total: u64,
    pub push_dedup_dropped_total: u64,
    pub push_sent_ok_total: u64,
    pub push_send_failed_total: u64,
    pub push_unregistered_removed_total: u64,
    pub push_invalid_token_total: u64,
    pub db_read_ops_total: u64,
    pub db_write_ops_total: u64,
    pub db_read_time_ms_total: u64,
    pub db_write_time_ms_total: u64,
    pub db_commit_conflicts_total: u64,
    pub db_errors_total: u64,
}

impl Display for IndexerMetricsSnapshot {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Indexer Metrics Snapshot:")?;
        writeln!(f, "  Handshakes by sender: {}", self.handshakes_by_sender)?;
        writeln!(
            f,
            "  Handshakes by receiver: {}",
            self.uniq_handshakes_by_receiver
        )?;
        writeln!(f, "  Payments by sender: {}", self.payments_by_sender)?;
        writeln!(
            f,
            "  Payments by receiver: {}",
            self.uniq_payments_by_receiver
        )?;
        writeln!(f, "  Contextual messages: {}", self.contextual_messages)?;
        writeln!(f, "  Blocks processed: {}", self.blocks_processed)?;
        writeln!(f, "  Latest block: {}", self.latest_block)?;
        writeln!(
            f,
            "  Latest accepting block: {}",
            self.latest_accepting_block
        )?;
        writeln!(
            f,
            "  Unknown sender entries: {}",
            self.unknown_sender_entries
        )?;
        writeln!(f, "  Resolved senders: {}", self.resolved_senders)
    }
}

/// Metrics structure containing atomic counters for all partition statistics
#[derive(Debug)]
pub struct IndexerMetrics {
    /// Number of handshakes indexed by sender
    pub handshakes_by_sender: AtomicU64,
    /// Number of handshakes indexed by receiver
    pub uniq_handshakes_by_receiver: AtomicU64,
    /// Number of payments indexed by sender
    pub payments_by_sender: AtomicU64,
    /// Number of payments indexed by receiver
    pub uniq_payments_by_receiver: AtomicU64,
    /// Number of contextual messages indexed
    pub contextual_messages: AtomicU64,
    /// Number of blocks processed
    pub blocks_processed: AtomicU64,
    /// Latest block hash processed
    pub latest_block: ArcSwap<FStr<64>>,
    /// Latest accepting block hash
    pub latest_accepting_block: ArcSwap<FStr<64>>,
    /// Number of unknown sender entries
    pub unknown_sender_entries: AtomicU64,
    pub resolved_sender: AtomicU64,
    pub pruned_blocks: AtomicU64,
    pub push_registered_devices: AtomicU64,
    pub push_register_calls_total: AtomicU64,
    pub push_update_calls_total: AtomicU64,
    pub push_unregister_calls_total: AtomicU64,
    pub push_fast_path_skips_total: AtomicU64,
    pub push_events_total: AtomicU64,
    pub push_tokens_looked_up_total: AtomicU64,
    pub push_filtered_alias_total: AtomicU64,
    pub push_filtered_primary_total: AtomicU64,
    pub push_dedup_dropped_total: AtomicU64,
    pub push_sent_ok_total: AtomicU64,
    pub push_send_failed_total: AtomicU64,
    pub push_unregistered_removed_total: AtomicU64,
    pub push_invalid_token_total: AtomicU64,
    pub db_read_ops_total: AtomicU64,
    pub db_write_ops_total: AtomicU64,
    pub db_read_time_ms_total: AtomicU64,
    pub db_write_time_ms_total: AtomicU64,
    pub db_commit_conflicts_total: AtomicU64,
    pub db_errors_total: AtomicU64,
}

impl IndexerMetrics {
    /// Create a new metrics instance with all counters initialized to zero
    pub fn new() -> Self {
        Self {
            handshakes_by_sender: AtomicU64::new(0),
            uniq_handshakes_by_receiver: AtomicU64::new(0),
            payments_by_sender: AtomicU64::new(0),
            uniq_payments_by_receiver: AtomicU64::new(0),
            contextual_messages: AtomicU64::new(0),
            blocks_processed: AtomicU64::new(0),
            latest_block: ArcSwap::new(Arc::new(Default::default())),
            latest_accepting_block: ArcSwap::new(Arc::new(Default::default())),
            unknown_sender_entries: AtomicU64::new(0),
            resolved_sender: Default::default(),
            pruned_blocks: Default::default(),
            push_registered_devices: AtomicU64::new(0),
            push_register_calls_total: AtomicU64::new(0),
            push_update_calls_total: AtomicU64::new(0),
            push_unregister_calls_total: AtomicU64::new(0),
            push_fast_path_skips_total: AtomicU64::new(0),
            push_events_total: AtomicU64::new(0),
            push_tokens_looked_up_total: AtomicU64::new(0),
            push_filtered_alias_total: AtomicU64::new(0),
            push_filtered_primary_total: AtomicU64::new(0),
            push_dedup_dropped_total: AtomicU64::new(0),
            push_sent_ok_total: AtomicU64::new(0),
            push_send_failed_total: AtomicU64::new(0),
            push_unregistered_removed_total: AtomicU64::new(0),
            push_invalid_token_total: AtomicU64::new(0),
            db_read_ops_total: AtomicU64::new(0),
            db_write_ops_total: AtomicU64::new(0),
            db_read_time_ms_total: AtomicU64::new(0),
            db_write_time_ms_total: AtomicU64::new(0),
            db_commit_conflicts_total: AtomicU64::new(0),
            db_errors_total: AtomicU64::new(0),
        }
    }

    /// Create a new metrics instance from a snapshot
    pub fn from_snapshot(snapshot: IndexerMetricsSnapshot) -> Self {
        Self {
            handshakes_by_sender: AtomicU64::new(snapshot.handshakes_by_sender),
            uniq_handshakes_by_receiver: AtomicU64::new(snapshot.uniq_handshakes_by_receiver),
            payments_by_sender: AtomicU64::new(snapshot.payments_by_sender),
            uniq_payments_by_receiver: AtomicU64::new(snapshot.uniq_payments_by_receiver),
            contextual_messages: AtomicU64::new(snapshot.contextual_messages),
            blocks_processed: AtomicU64::new(snapshot.blocks_processed),
            latest_block: ArcSwap::new(Arc::new(snapshot.latest_block)),
            latest_accepting_block: ArcSwap::new(Arc::new(snapshot.latest_accepting_block)),
            unknown_sender_entries: AtomicU64::new(snapshot.unknown_sender_entries),
            resolved_sender: AtomicU64::new(snapshot.resolved_senders),
            pruned_blocks: AtomicU64::new(snapshot.pruned_blocks),
            push_registered_devices: AtomicU64::new(snapshot.push_registered_devices),
            push_register_calls_total: AtomicU64::new(snapshot.push_register_calls_total),
            push_update_calls_total: AtomicU64::new(snapshot.push_update_calls_total),
            push_unregister_calls_total: AtomicU64::new(snapshot.push_unregister_calls_total),
            push_fast_path_skips_total: AtomicU64::new(snapshot.push_fast_path_skips_total),
            push_events_total: AtomicU64::new(snapshot.push_events_total),
            push_tokens_looked_up_total: AtomicU64::new(snapshot.push_tokens_looked_up_total),
            push_filtered_alias_total: AtomicU64::new(snapshot.push_filtered_alias_total),
            push_filtered_primary_total: AtomicU64::new(snapshot.push_filtered_primary_total),
            push_dedup_dropped_total: AtomicU64::new(snapshot.push_dedup_dropped_total),
            push_sent_ok_total: AtomicU64::new(snapshot.push_sent_ok_total),
            push_send_failed_total: AtomicU64::new(snapshot.push_send_failed_total),
            push_unregistered_removed_total: AtomicU64::new(
                snapshot.push_unregistered_removed_total,
            ),
            push_invalid_token_total: AtomicU64::new(snapshot.push_invalid_token_total),
            db_read_ops_total: AtomicU64::new(snapshot.db_read_ops_total),
            db_write_ops_total: AtomicU64::new(snapshot.db_write_ops_total),
            db_read_time_ms_total: AtomicU64::new(snapshot.db_read_time_ms_total),
            db_write_time_ms_total: AtomicU64::new(snapshot.db_write_time_ms_total),
            db_commit_conflicts_total: AtomicU64::new(snapshot.db_commit_conflicts_total),
            db_errors_total: AtomicU64::new(snapshot.db_errors_total),
        }
    }

    /// Create a snapshot of the current metrics
    pub fn snapshot(&self) -> IndexerMetricsSnapshot {
        IndexerMetricsSnapshot {
            handshakes_by_sender: self.handshakes_by_sender.load(Ordering::Relaxed),
            uniq_handshakes_by_receiver: self.uniq_handshakes_by_receiver.load(Ordering::Relaxed),
            payments_by_sender: self.payments_by_sender.load(Ordering::Relaxed),
            uniq_payments_by_receiver: self.uniq_payments_by_receiver.load(Ordering::Relaxed),
            contextual_messages: self.contextual_messages.load(Ordering::Relaxed),
            blocks_processed: self.blocks_processed.load(Ordering::Relaxed),
            latest_block: *self.latest_block.load().as_ref(),
            latest_accepting_block: *self.latest_accepting_block.load().as_ref(),
            unknown_sender_entries: self.unknown_sender_entries.load(Ordering::Relaxed),
            resolved_senders: self.resolved_sender.load(Ordering::Relaxed),
            pruned_blocks: self.pruned_blocks.load(Ordering::Relaxed),
            push_registered_devices: self.push_registered_devices.load(Ordering::Relaxed),
            push_register_calls_total: self.push_register_calls_total.load(Ordering::Relaxed),
            push_update_calls_total: self.push_update_calls_total.load(Ordering::Relaxed),
            push_unregister_calls_total: self.push_unregister_calls_total.load(Ordering::Relaxed),
            push_fast_path_skips_total: self.push_fast_path_skips_total.load(Ordering::Relaxed),
            push_events_total: self.push_events_total.load(Ordering::Relaxed),
            push_tokens_looked_up_total: self.push_tokens_looked_up_total.load(Ordering::Relaxed),
            push_filtered_alias_total: self.push_filtered_alias_total.load(Ordering::Relaxed),
            push_filtered_primary_total: self.push_filtered_primary_total.load(Ordering::Relaxed),
            push_dedup_dropped_total: self.push_dedup_dropped_total.load(Ordering::Relaxed),
            push_sent_ok_total: self.push_sent_ok_total.load(Ordering::Relaxed),
            push_send_failed_total: self.push_send_failed_total.load(Ordering::Relaxed),
            push_unregistered_removed_total: self
                .push_unregistered_removed_total
                .load(Ordering::Relaxed),
            push_invalid_token_total: self.push_invalid_token_total.load(Ordering::Relaxed),
            db_read_ops_total: self.db_read_ops_total.load(Ordering::Relaxed),
            db_write_ops_total: self.db_write_ops_total.load(Ordering::Relaxed),
            db_read_time_ms_total: self.db_read_time_ms_total.load(Ordering::Relaxed),
            db_write_time_ms_total: self.db_write_time_ms_total.load(Ordering::Relaxed),
            db_commit_conflicts_total: self.db_commit_conflicts_total.load(Ordering::Relaxed),
            db_errors_total: self.db_errors_total.load(Ordering::Relaxed),
        }
    }

    /// Update handshakes by sender count
    pub fn set_handshakes_by_sender(&self, count: u64) {
        self.handshakes_by_sender.store(count, Ordering::Relaxed);
    }

    /// Update handshakes by receiver count
    pub fn set_handshakes_by_receiver(&self, count: u64) {
        self.uniq_handshakes_by_receiver
            .store(count, Ordering::Relaxed);
    }

    /// Update payments by sender count
    pub fn set_payments_by_sender(&self, count: u64) {
        self.payments_by_sender.store(count, Ordering::Relaxed);
    }

    /// Update payments by receiver count
    pub fn set_payments_by_receiver(&self, count: u64) {
        self.uniq_payments_by_receiver
            .store(count, Ordering::Relaxed);
    }

    /// Increment blocks processed count by 1
    pub fn increment_blocks_processed(&self) {
        self.blocks_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Set latest block hash
    pub fn set_latest_block(&self, hash: [u8; 32]) {
        self.latest_block.store(Arc::new(hash.to_hex_64()));
    }

    /// Set latest accepting block hash
    pub fn set_latest_accepting_block(&self, hash: [u8; 32]) {
        self.latest_accepting_block
            .store(Arc::new(hash.to_hex_64()));
    }

    /// Get current handshakes by sender count
    pub fn get_handshakes_by_sender(&self) -> u64 {
        self.handshakes_by_sender.load(Ordering::Relaxed)
    }

    /// Get current handshakes by receiver count
    pub fn get_handshakes_by_receiver(&self) -> u64 {
        self.uniq_handshakes_by_receiver.load(Ordering::Relaxed)
    }

    /// Get current payments by sender count
    pub fn get_payments_by_sender(&self) -> u64 {
        self.payments_by_sender.load(Ordering::Relaxed)
    }

    /// Get current payments by receiver count
    pub fn get_payments_by_receiver(&self) -> u64 {
        self.uniq_payments_by_receiver.load(Ordering::Relaxed)
    }

    /// Get current contextual messages count
    pub fn get_contextual_messages(&self) -> u64 {
        self.contextual_messages.load(Ordering::Relaxed)
    }

    /// Get current blocks processed count
    pub fn get_blocks_processed(&self) -> u64 {
        self.blocks_processed.load(Ordering::Relaxed)
    }

    pub fn increment_pruned_blocks(&self, pruned_blocks: u64) {
        self.pruned_blocks
            .fetch_add(pruned_blocks, Ordering::Relaxed);
    }

    pub fn set_contextual_messages(&self, count: u64) {
        self.contextual_messages.store(count, Ordering::Relaxed);
    }

    pub fn set_push_registered_devices(&self, count: u64) {
        self.push_registered_devices.store(count, Ordering::Relaxed);
    }

    pub fn increment_push_registered_devices(&self, count: u64) {
        self.push_registered_devices
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn decrement_push_registered_devices(&self, count: u64) {
        let _ = self.push_registered_devices.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(count)),
        );
    }

    pub fn increment_push_register_calls_total(&self) {
        self.push_register_calls_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_push_update_calls_total(&self) {
        self.push_update_calls_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_push_unregister_calls_total(&self) {
        self.push_unregister_calls_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_push_fast_path_skips_total(&self) {
        self.push_fast_path_skips_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_push_events_total(&self) {
        self.push_events_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_push_tokens_looked_up_total(&self, count: u64) {
        self.push_tokens_looked_up_total
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_push_filtered_alias_total(&self) {
        self.push_filtered_alias_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_push_filtered_primary_total(&self) {
        self.push_filtered_primary_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_push_dedup_dropped_total(&self) {
        self.push_dedup_dropped_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_push_sent_ok_total(&self) {
        self.push_sent_ok_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_push_send_failed_total(&self) {
        self.push_send_failed_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_push_unregistered_removed_total(&self) {
        self.push_unregistered_removed_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_push_invalid_token_total(&self) {
        self.push_invalid_token_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_db_read_ops_total(&self, count: u64) {
        self.db_read_ops_total.fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_db_write_ops_total(&self, count: u64) {
        self.db_write_ops_total.fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_db_read_time_ms_total(&self, ms: u64) {
        self.db_read_time_ms_total.fetch_add(ms, Ordering::Relaxed);
    }

    pub fn increment_db_write_time_ms_total(&self, ms: u64) {
        self.db_write_time_ms_total.fetch_add(ms, Ordering::Relaxed);
    }

    pub fn increment_db_commit_conflicts_total(&self) {
        self.db_commit_conflicts_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_db_errors_total(&self) {
        self.db_errors_total.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for IndexerMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared metrics instance wrapped in Arc for use across multiple workers
pub type SharedMetrics = Arc<IndexerMetrics>;

/// Create a new shared metrics instance
pub fn create_shared_metrics() -> SharedMetrics {
    Arc::new(IndexerMetrics::new())
}

/// Create a new shared metrics instance from a snapshot
pub fn create_shared_metrics_from_snapshot(snapshot: IndexerMetricsSnapshot) -> SharedMetrics {
    Arc::new(IndexerMetrics::from_snapshot(snapshot))
}
