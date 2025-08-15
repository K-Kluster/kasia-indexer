//! Database partitions for Kaspa BlockDAG indexer.
//!
//! Organized by functional purpose:
//! - `headers`: Block header data and gap tracking
//! - `messages`: Protocol message storage (handshakes, payments, contextual)
//! - `processing`: Transaction processing state and resolution workflows

// Functional module groups
pub mod headers;
pub mod messages;
pub mod processing;

// Standalone modules
pub mod metadata;
pub mod resolution_keys;
pub mod util;

/// Database partition identifiers.
///
/// Each partition stores specific data with optimized key-value layouts
/// for efficient querying. All data is stored using bytemuck-compatible
/// types for zero-copy casting with fjall's efficient Arc<[u8]> clones.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PartitionId {
    // Message partitions
    HandshakeBySender,
    HandshakeByReceiver,
    TxIdToHandshake,

    // Transaction acceptance tracking
    AcceptanceToTxId,
    TxIdToAcceptance,

    // Transaction processing state
    SkipTx,
    UnknownTx,
    BlockCompactHeader,
    UnknownAcceptingDaa,
    PendingSenderResolution,

    // System and gap tracking
    Metadata,
    BlockGaps,
    AcceptingBlockGaps, // Unused but kept for ID consistency
    ContextualMessageBySender,

    // Payment tracking
    PaymentBySender,
    PaymentByReceiver,
    TxIdToPayment,

    // Self Stash
    SelfStashByOwner,
    TxIdToSelfStash,
}

use fjall::UserValue;
use std::marker::PhantomData;

/// Byte view wrapper for transaction ID arrays, optimized for fjall storage.
///
/// This structure wraps fjall's Arc<[u8]> data with type-safe access to arrays
/// of 32-byte transaction IDs. Designed for zero-copy casting with bytemuck
/// and efficient fjall clones. The "Like" prefix indicates it provides the same
/// interface as the non-prefixed version but operates on byte slices from fjall.
pub struct TxIdsSliceView<T: AsRef<[u8]> = UserValue> {
    bytes: T,
    _phantom: PhantomData<[u8; 32]>,
}

impl<T: AsRef<[u8]>> TxIdsSliceView<T> {
    pub fn new(bytes: T) -> Self {
        assert_eq!(
            bytes.as_ref().len() % 32,
            0,
            "Bytes length must be multiple of 32 for transaction IDs"
        );
        Self {
            bytes,
            _phantom: PhantomData,
        }
    }

    /// Returns the transaction IDs as a slice of 32-byte arrays.
    pub fn as_tx_ids(&self) -> &[[u8; 32]] {
        self.bytes.as_ref().as_chunks::<32>().0
    }

    /// Consumes the wrapper and returns the underlying byte data.
    pub fn into_inner(self) -> T {
        self.bytes
    }

    /// Returns the number of transaction IDs in the collection.
    pub fn len(&self) -> usize {
        self.bytes.as_ref().len() / 32
    }

    /// Returns true if the collection contains no transaction IDs.
    pub fn is_empty(&self) -> bool {
        self.bytes.as_ref().is_empty()
    }
}

// Keep the old name as an alias for backward compatibility during transition
pub type LikeTxIds<T = UserValue> = TxIdsSliceView<T>;
