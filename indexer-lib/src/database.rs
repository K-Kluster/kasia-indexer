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

    None = 255, // Special variant for resolved entries with empty value
}

mod util;
