use arc_swap::ArcSwap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::util::ToHex64;

#[cfg(feature = "serde")]
use serde::Serialize;
#[cfg(feature = "utoipa")]
use utoipa::{ToSchema, schema};

/// A snapshot of the indexer metrics.
/// This structure contains a copy of all metric counters as simple u64 values.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", serde_with::serde_as, derive(Serialize))]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
pub struct IndexerMetricsSnapshot {
    /// Number of handshakes indexed by sender
    pub handshakes_by_sender: u64,
    /// Number of handshakes indexed by receiver
    pub handshakes_by_receiver: u64,
    /// Number of payments indexed by sender
    pub payments_by_sender: u64,
    /// Number of payments indexed by receiver
    pub payments_by_receiver: u64,
    /// Number of contextual messages indexed
    pub contextual_messages: u64,
    /// Number of blocks processed
    pub blocks_processed: u64,
    /// Latest block hash processed
    #[cfg_attr(feature = "utoipa", schema(value_type = String, format = "hex"))]
    #[cfg_attr(feature = "serde", serde_as(as = "serde_with::hex::Hex"))]
    pub latest_block: [u8; 32],
    /// Latest accepting block hash
    #[cfg_attr(feature = "utoipa", schema(value_type = String, format = "hex"))]
    #[cfg_attr(feature = "serde", serde_as(as = "serde_with::hex::Hex"))]
    pub latest_accepting_block: [u8; 32],
    /// Number of unknown sender entries
    pub unknown_sender_entries: u64,
    pub resolved_senders: u64,
    pub pruned_blocks: u64,
}

impl Display for IndexerMetricsSnapshot {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Indexer Metrics Snapshot:")?;
        writeln!(f, "  Handshakes by sender: {}", self.handshakes_by_sender)?;
        writeln!(
            f,
            "  Handshakes by receiver: {}",
            self.handshakes_by_receiver
        )?;
        writeln!(f, "  Payments by sender: {}", self.payments_by_sender)?;
        writeln!(f, "  Payments by receiver: {}", self.payments_by_receiver)?;
        writeln!(f, "  Contextual messages: {}", self.contextual_messages)?;
        writeln!(f, "  Blocks processed: {}", self.blocks_processed)?;
        writeln!(f, "  Latest block: {}", self.latest_block.to_hex_64())?;
        writeln!(
            f,
            "  Latest accepting block: {}",
            self.latest_accepting_block.to_hex_64()
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
    pub handshakes_by_receiver: AtomicU64,
    /// Number of payments indexed by sender
    pub payments_by_sender: AtomicU64,
    /// Number of payments indexed by receiver
    pub payments_by_receiver: AtomicU64,
    /// Number of contextual messages indexed
    pub contextual_messages: AtomicU64,
    /// Number of blocks processed
    pub blocks_processed: AtomicU64,
    /// Latest block hash processed
    pub latest_block: ArcSwap<[u8; 32]>,
    /// Latest accepting block hash
    pub latest_accepting_block: ArcSwap<[u8; 32]>,
    /// Number of unknown sender entries
    pub unknown_sender_entries: AtomicU64,
    pub resolved_sender: AtomicU64,
    pub pruned_blocks: AtomicU64,
}

impl IndexerMetrics {
    /// Create a new metrics instance with all counters initialized to zero
    pub fn new() -> Self {
        Self {
            handshakes_by_sender: AtomicU64::new(0),
            handshakes_by_receiver: AtomicU64::new(0),
            payments_by_sender: AtomicU64::new(0),
            payments_by_receiver: AtomicU64::new(0),
            contextual_messages: AtomicU64::new(0),
            blocks_processed: AtomicU64::new(0),
            latest_block: ArcSwap::new(Arc::new(Default::default())),
            latest_accepting_block: ArcSwap::new(Arc::new(Default::default())),
            unknown_sender_entries: AtomicU64::new(0),
            resolved_sender: Default::default(),
            pruned_blocks: Default::default(),
        }
    }

    /// Create a new metrics instance from a snapshot
    pub fn from_snapshot(snapshot: IndexerMetricsSnapshot) -> Self {
        Self {
            handshakes_by_sender: AtomicU64::new(snapshot.handshakes_by_sender),
            handshakes_by_receiver: AtomicU64::new(snapshot.handshakes_by_receiver),
            payments_by_sender: AtomicU64::new(snapshot.payments_by_sender),
            payments_by_receiver: AtomicU64::new(snapshot.payments_by_receiver),
            contextual_messages: AtomicU64::new(snapshot.contextual_messages),
            blocks_processed: AtomicU64::new(snapshot.blocks_processed),
            latest_block: ArcSwap::new(Arc::new(snapshot.latest_block)),
            latest_accepting_block: ArcSwap::new(Arc::new(snapshot.latest_accepting_block)),
            unknown_sender_entries: AtomicU64::new(snapshot.unknown_sender_entries),
            resolved_sender: AtomicU64::new(snapshot.resolved_senders),
            pruned_blocks: AtomicU64::new(snapshot.pruned_blocks),
        }
    }

    /// Create a snapshot of the current metrics
    pub fn snapshot(&self) -> IndexerMetricsSnapshot {
        IndexerMetricsSnapshot {
            handshakes_by_sender: self.handshakes_by_sender.load(Ordering::Relaxed),
            handshakes_by_receiver: self.handshakes_by_receiver.load(Ordering::Relaxed),
            payments_by_sender: self.payments_by_sender.load(Ordering::Relaxed),
            payments_by_receiver: self.payments_by_receiver.load(Ordering::Relaxed),
            contextual_messages: self.contextual_messages.load(Ordering::Relaxed),
            blocks_processed: self.blocks_processed.load(Ordering::Relaxed),
            latest_block: *self.latest_block.load().as_ref(),
            latest_accepting_block: *self.latest_accepting_block.load().as_ref(),
            unknown_sender_entries: self.unknown_sender_entries.load(Ordering::Relaxed),
            resolved_senders: self.resolved_sender.load(Ordering::Relaxed),
            pruned_blocks: self.pruned_blocks.load(Ordering::Relaxed),
        }
    }

    /// Update handshakes by sender count
    pub fn set_handshakes_by_sender(&self, count: u64) {
        self.handshakes_by_sender.store(count, Ordering::Relaxed);
    }

    /// Update handshakes by receiver count
    pub fn set_handshakes_by_receiver(&self, count: u64) {
        self.handshakes_by_receiver.store(count, Ordering::Relaxed);
    }

    /// Update payments by sender count
    pub fn set_payments_by_sender(&self, count: u64) {
        self.payments_by_sender.store(count, Ordering::Relaxed);
    }

    /// Update payments by receiver count
    pub fn set_payments_by_receiver(&self, count: u64) {
        self.payments_by_receiver.store(count, Ordering::Relaxed);
    }

    /// Update contextual messages count
    pub fn increment_contextual_messages_count(&self) {
        self.contextual_messages.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment blocks processed count by 1
    pub fn increment_blocks_processed(&self) {
        self.blocks_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Set latest block hash
    pub fn set_latest_block(&self, hash: [u8; 32]) {
        self.latest_block.store(Arc::new(hash));
    }

    /// Set latest accepting block hash
    pub fn set_latest_accepting_block(&self, hash: [u8; 32]) {
        self.latest_accepting_block.store(Arc::new(hash));
    }

    /// Get current handshakes by sender count
    pub fn get_handshakes_by_sender(&self) -> u64 {
        self.handshakes_by_sender.load(Ordering::Relaxed)
    }

    /// Get current handshakes by receiver count
    pub fn get_handshakes_by_receiver(&self) -> u64 {
        self.handshakes_by_receiver.load(Ordering::Relaxed)
    }

    /// Get current payments by sender count
    pub fn get_payments_by_sender(&self) -> u64 {
        self.payments_by_sender.load(Ordering::Relaxed)
    }

    /// Get current payments by receiver count
    pub fn get_payments_by_receiver(&self) -> u64 {
        self.payments_by_receiver.load(Ordering::Relaxed)
    }

    /// Get current contextual messages count
    pub fn get_contextual_messages(&self) -> u64 {
        self.contextual_messages.load(Ordering::Relaxed)
    }

    /// Get current blocks processed count
    pub fn get_blocks_processed(&self) -> u64 {
        self.blocks_processed.load(Ordering::Relaxed)
    }

    /// Get latest block hash
    pub fn get_latest_block(&self) -> [u8; 32] {
        *self.latest_block.load().as_ref()
    }

    /// Get latest accepting block hash
    pub fn get_latest_accepting_block(&self) -> [u8; 32] {
        *self.latest_accepting_block.load().as_ref()
    }
    pub fn increment_pruned_blocks(&self, pruned_blocks: u64) {
        self.pruned_blocks
            .fetch_add(pruned_blocks, Ordering::Relaxed);
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
