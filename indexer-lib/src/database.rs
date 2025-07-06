// includes both handshake and direct message
pub mod handshake;

pub mod acceptance;

pub mod skip_tx;

pub mod unknown_tx;

pub mod accepting_block_scores;

pub mod pending_sender_resolution;

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
    AcceptingBlockScores,
    UnknownAcceptingBlockDaa,
}

mod util;
