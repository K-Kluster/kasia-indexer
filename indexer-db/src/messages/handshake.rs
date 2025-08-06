use crate::{AddressPayload, SharedImmutable};
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use std::fmt::Debug;
use zerocopy::big_endian::U64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

#[derive(Clone)]
pub struct HandshakeBySenderPartition(fjall::TxPartition);

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, KnownLayout, IntoBytes, FromBytes, Unaligned, Immutable,
)]
#[repr(C)]
pub struct HandshakeKeyBySender {
    pub sender: AddressPayload, // allows retrieving every tx with unknown sender
    pub block_time: U64,
    pub block_hash: [u8; 32],
    pub receiver: AddressPayload,
    pub version: u8,
    pub tx_id: [u8; 32],
}

impl HandshakeBySenderPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> anyhow::Result<Self> {
        Ok(Self(keyspace.open_partition(
            "handshake_by_sender",
            PartitionCreateOptions::default(),
        )?))
    }

    pub fn approximate_len(&self) -> usize {
        self.0.approximate_len()
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = anyhow::Result<SharedImmutable<HandshakeKeyBySender>>> {
        self.0
            .inner()
            .keys()
            .map(|r| r.map_err(anyhow::Error::from).map(SharedImmutable::new))
    }

    pub fn insert_wtx(
        &self,
        wtx: &mut WriteTransaction,
        key: &HandshakeKeyBySender,
    ) -> anyhow::Result<()> {
        wtx.insert(&self.0, key.as_bytes(), []);
        Ok(())
    }

    pub fn iter_by_sender_from_block_time_rtx(
        &self,
        rtx: &ReadTransaction,
        sender: AddressPayload,
        block_time: u64,
    ) -> impl Iterator<Item = anyhow::Result<SharedImmutable<HandshakeKeyBySender>>> {
        let mut from = [0u8; 34 + 8];
        from[..34].copy_from_slice(sender.as_bytes());
        from[34..].copy_from_slice(&block_time.to_be_bytes());
        let mut to = [255u8; 34 + 8];
        to[..34].copy_from_slice(sender.as_bytes());
        rtx.range(&self.0, from..=to).map(|r| {
            r.map(|(k, _v)| SharedImmutable::new(k))
                .map_err(anyhow::Error::from)
        })
    }
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, KnownLayout, IntoBytes, FromBytes, Immutable, Unaligned,
)]
#[repr(C)]
pub struct HandshakeKeyByReceiver {
    pub receiver: AddressPayload,
    pub block_time: U64,
    pub block_hash: [u8; 32],
    pub version: u8,
    pub tx_id: [u8; 32],
}

#[derive(Clone)]
pub struct HandshakeByReceiverPartition(fjall::TxPartition);

impl HandshakeByReceiverPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> anyhow::Result<Self> {
        Ok(Self(keyspace.open_partition(
            "handshake_by_receiver",
            PartitionCreateOptions::default(),
        )?))
    }

    pub fn insert_wtx(
        &self,
        wtx: &mut WriteTransaction,
        key: &HandshakeKeyByReceiver,
        sender: Option<AddressPayload>,
    ) -> anyhow::Result<()> {
        let sender = sender.unwrap_or_default();
        wtx.update_fetch(&self.0, key.as_bytes(), |old| match old {
            None => Some(sender.as_bytes().into()),
            Some(old) => {
                let old_sender = AddressPayload::try_ref_from_bytes(old.as_bytes()).unwrap();
                if old_sender != &AddressPayload::default() {
                    Some(old.clone())
                } else {
                    Some(sender.as_bytes().into())
                }
            }
        })?;
        Ok(())
    }

    pub fn iter_by_receiver_from_block_time_rtx(
        &self,
        rtx: &ReadTransaction,
        receiver: &AddressPayload,
        block_time: u64,
    ) -> impl Iterator<
        Item = anyhow::Result<(
            SharedImmutable<HandshakeKeyByReceiver>,
            SharedImmutable<AddressPayload>,
        )>,
    > {
        let mut from = [0u8; 34 + 8];
        from[..34].copy_from_slice(receiver.as_bytes());
        from[34..].copy_from_slice(&block_time.to_be_bytes());
        let mut to = [255u8; 34 + 8];
        to[..34].copy_from_slice(receiver.as_bytes());
        rtx.range(&self.0, from..=to).map(|r| {
            r.map(|(k, v)| (SharedImmutable::new(k), SharedImmutable::new(v)))
                .map_err(anyhow::Error::from)
        })
    }
}

#[derive(Clone)]
pub struct TxIdToHandshakePartition(fjall::TxPartition);

impl TxIdToHandshakePartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> anyhow::Result<Self> {
        Ok(Self(keyspace.open_partition(
            "tx-id-to-handshake",
            PartitionCreateOptions::default(),
        )?))
    }

    pub fn len(&self) -> anyhow::Result<usize> {
        Ok(self.0.inner().len()?)
    }

    pub fn is_empty(&self) -> anyhow::Result<bool> {
        Ok(self.0.inner().is_empty()?)
    }

    pub fn insert(&self, tx_id: &[u8; 32], sealed_hex: &[u8]) -> anyhow::Result<()> {
        self.0.insert(tx_id, sealed_hex)?;
        Ok(())
    }

    pub fn insert_wtx(&self, wtx: &mut WriteTransaction, tx_id: &[u8; 32], sealed_hex: &[u8]) {
        wtx.insert(&self.0, tx_id, sealed_hex);
    }

    pub fn approximate_len(&self) -> usize {
        self.0.approximate_len()
    }

    pub fn get_rtx(
        &self,
        rtx: &ReadTransaction,
        tx_id: &[u8; 32],
    ) -> anyhow::Result<Option<impl AsRef<[u8]>>> {
        rtx.get(&self.0, tx_id).map_err(anyhow::Error::from)
    }
}
