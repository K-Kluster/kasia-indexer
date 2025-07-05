// includes both handshake and direct message
pub mod handshake;

pub mod acceptance;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PartitionId {
    HandshakeBySender,
    HandshakeByReceiver,
    TxIdToHandshake,

    AcceptanceToTxId,
    TxIdToAcceptance,

    AcceptanceTrigger,
}
