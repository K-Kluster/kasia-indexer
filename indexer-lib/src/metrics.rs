use arc_swap::ArcSwap;
use kaspa_rpc_core::RpcHash;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "serde")]
use serde::Serialize;
#[cfg(feature = "utoipa")]
use utoipa::{ToSchema, schema};

/// A snapshot of the indexer metrics.
/// This structure contains a copy of all metric counters as simple u64 values.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(Serialize))]
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
    /// Number of self stash indexed
    pub self_stashes: u64,
    /// Number of blocks processed
    pub blocks_processed: u64,
    /// Latest block hash processed
    #[cfg_attr(feature = "utoipa", schema(value_type = String, format = "hex"))]
    pub latest_block: RpcHash,
    /// Latest accepting block hash
    #[cfg_attr(feature = "utoipa", schema(value_type = String, format = "hex"))]
    pub latest_accepting_block: RpcHash,
    /// Number of unknown DAA entries
    pub unknown_daa_entries: u64,
    /// Number of unknown sender entries
    pub unknown_sender_entries: u64,
    /// Number of unknown transaction entries
    pub unknown_tx_entries: u64,
    pub resolved_daa: u64,
    pub resolved_senders: u64,
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
        writeln!(f, "  Self Stashes: {}", self.self_stashes)?;
        writeln!(f, "  Blocks processed: {}", self.blocks_processed)?;
        writeln!(f, "  Latest block: {}", self.latest_block)?;
        writeln!(
            f,
            "  Latest accepting block: {}",
            self.latest_accepting_block
        )?;
        writeln!(f, "  Unknown DAA entries: {}", self.unknown_daa_entries)?;
        writeln!(
            f,
            "  Unknown sender entries: {}",
            self.unknown_sender_entries
        )?;
        writeln!(f, "  Unknown tx entries: {}", self.unknown_tx_entries)?;
        writeln!(f, "  Resolved DAA entries: {}", self.resolved_daa)?;
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
    /// Number of self stash indexed
    pub self_stashes: AtomicU64,
    /// Number of blocks processed
    pub blocks_processed: AtomicU64,
    /// Latest block hash processed
    pub latest_block: ArcSwap<RpcHash>,
    /// Latest accepting block hash
    pub latest_accepting_block: ArcSwap<RpcHash>,
    /// Number of unknown DAA entries
    pub unknown_daa_entries: AtomicU64,
    /// Number of unknown sender entries
    pub unknown_sender_entries: AtomicU64,
    /// Number of unknown transaction entries
    pub unknown_tx_entries: AtomicU64,
    pub resolved_daa: AtomicU64,
    pub resolved_sender: AtomicU64,
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
            self_stashes: AtomicU64::new(0),
            blocks_processed: AtomicU64::new(0),
            latest_block: ArcSwap::new(Arc::new(RpcHash::default())),
            latest_accepting_block: ArcSwap::new(Arc::new(RpcHash::default())),
            unknown_daa_entries: AtomicU64::new(0),
            unknown_sender_entries: AtomicU64::new(0),
            unknown_tx_entries: AtomicU64::new(0),
            resolved_daa: Default::default(),
            resolved_sender: Default::default(),
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
            self_stashes: AtomicU64::new(snapshot.self_stashes),
            blocks_processed: AtomicU64::new(snapshot.blocks_processed),
            latest_block: ArcSwap::new(Arc::new(snapshot.latest_block)),
            latest_accepting_block: ArcSwap::new(Arc::new(snapshot.latest_accepting_block)),
            unknown_daa_entries: AtomicU64::new(snapshot.unknown_daa_entries),
            unknown_sender_entries: AtomicU64::new(snapshot.unknown_sender_entries),
            unknown_tx_entries: AtomicU64::new(snapshot.unknown_tx_entries),
            resolved_daa: AtomicU64::new(snapshot.resolved_daa),
            resolved_sender: AtomicU64::new(snapshot.resolved_senders),
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
            self_stashes: self.self_stashes.load(Ordering::Relaxed),
            blocks_processed: self.blocks_processed.load(Ordering::Relaxed),
            latest_block: *self.latest_block.load().as_ref(),
            latest_accepting_block: *self.latest_accepting_block.load().as_ref(),
            unknown_daa_entries: self.unknown_daa_entries.load(Ordering::Relaxed),
            unknown_sender_entries: self.unknown_sender_entries.load(Ordering::Relaxed),
            unknown_tx_entries: self.unknown_tx_entries.load(Ordering::Relaxed),
            resolved_daa: self.resolved_daa.load(Ordering::Relaxed),
            resolved_senders: self.resolved_sender.load(Ordering::Relaxed),
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

    /// Update self stash count
    pub fn increment_self_stash_count(&self) {
        self.self_stashes.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment blocks processed count by 1
    pub fn increment_blocks_processed(&self) {
        self.blocks_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Set latest block hash
    pub fn set_latest_block(&self, hash: RpcHash) {
        self.latest_block.store(Arc::new(hash));
    }

    /// Set latest accepting block hash
    pub fn set_latest_accepting_block(&self, hash: RpcHash) {
        self.latest_accepting_block.store(Arc::new(hash));
    }

    /// Set unknown DAA entries count
    pub fn set_unknown_daa_entries(&self, count: u64) {
        self.unknown_daa_entries.store(count, Ordering::Relaxed);
    }

    /// Set unknown sender entries count
    pub fn set_unknown_sender_entries(&self, count: u64) {
        self.unknown_sender_entries.store(count, Ordering::Relaxed);
    }

    /// Set unknown tx entries count
    pub fn set_unknown_tx_entries(&self, count: u64) {
        self.unknown_tx_entries.store(count, Ordering::Relaxed);
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

    /// Get current self stash count
    pub fn get_self_stash(&self) -> u64 {
        self.self_stashes.load(Ordering::Relaxed)
    }

    /// Get current blocks processed count
    pub fn get_blocks_processed(&self) -> u64 {
        self.blocks_processed.load(Ordering::Relaxed)
    }

    /// Get latest block hash
    pub fn get_latest_block(&self) -> RpcHash {
        *self.latest_block.load().as_ref()
    }

    /// Get latest accepting block hash
    pub fn get_latest_accepting_block(&self) -> RpcHash {
        *self.latest_accepting_block.load().as_ref()
    }

    /// Increment daa resolved count by 1
    pub fn increment_daa_resolved(&self) {
        self.resolved_daa.fetch_add(1, Ordering::Relaxed);
    }

    // Increment daa resolved count by 1
    pub fn increment_senders_resolved(&self) {
        self.resolved_sender.fetch_add(1, Ordering::Relaxed);
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
