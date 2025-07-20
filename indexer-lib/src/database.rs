// includes both handshake and direct message
pub mod handshake;

pub mod acceptance;

pub mod skip_tx;

pub mod unknown_tx;

pub mod block_compact_header;

pub mod unknown_accepting_daa;

pub mod accepting_block_gaps;
pub mod block_gaps;
pub mod contextual_message_by_sender;
pub mod metadata;
pub mod payment;
pub mod pending_sender_resolution;
pub mod resolution_keys;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PartitionId {
    HandshakeBySender,
    HandshakeByReceiver,
    TxIdToHandshake,

    AcceptanceToTxId,
    TxIdToAcceptance,

    SkipTx,
    UnknownTx,
    BlockCompactHeader,
    UnknownAcceptingDaa,
    PendingSenderResolution,

    Metadata,
    BlockGaps,
    AcceptingBlockGaps,
    ContextualMessageBySender,

    PaymentBySender,
    PaymentByReceiver,
    TxIdToPayment,
}

pub mod util;

use fjall::UserValue;
use std::marker::PhantomData;

/// Shared wrapper for transaction ID arrays - reused across multiple partitions
pub struct LikeTxIds<T: AsRef<[u8]> = UserValue> {
    bts: T,
    phantom_data: PhantomData<[u8; 32]>,
}

impl<T: AsRef<[u8]>> LikeTxIds<T> {
    pub fn new(bts: T) -> Self {
        assert_eq!(bts.as_ref().len() % 32, 0);
        Self {
            bts,
            phantom_data: PhantomData,
        }
    }

    pub fn as_tx_ids(&self) -> &[[u8; 32]] {
        self.bts.as_ref().as_chunks::<32>().0
    }

    pub fn inner(self) -> T {
        self.bts
    }

    pub fn len(&self) -> usize {
        self.bts.as_ref().len() / 32
    }

    pub fn is_empty(&self) -> bool {
        self.bts.as_ref().is_empty()
    }
}
