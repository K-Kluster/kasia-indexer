// includes both handshake and direct message
pub mod handshake;

pub mod acceptance;

pub mod skip_tx;

pub mod unknown_tx;

pub mod block_daa_scores;

pub mod unknown_accepting_daa;

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
    BlockDaaScores,
    UnknownAcceptingDaa,
    PendingSenderResolution,
}

mod util;
