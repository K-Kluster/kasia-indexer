use crate::{AddressPayload, SharedImmutable};
use anyhow::Result;
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use std::fmt::Debug;
use zerocopy::big_endian::U64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

/// Partition for storing contextual messages by sender and alias
/// Key: sender + alias + block_time + block_hash + version + tx_id
/// Value: sealed_hex data
/// Note: sender can be zeros (when not resolved yet)
/// Designed for REST API prefix search by sender, alias, time
#[derive(Clone)]
pub struct ContextualMessageBySenderPartition(fjall::TxPartition);

#[repr(C)]
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, IntoBytes, FromBytes, Immutable, Unaligned, KnownLayout,
)]
pub struct ContextualMessageBySenderKey {
    pub sender: AddressPayload, // zeros when not resolved yet
    pub alias: [u8; 16],        // alias for prefix search, zero-padded
    pub block_time: U64,        // u64 BE for chronological ordering
    pub block_hash: [u8; 32],   // block hash for uniqueness
    pub receiver: AddressPayload,
    pub version: u8,     // message version
    pub tx_id: [u8; 32], // transaction id
}

impl ContextualMessageBySenderPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(keyspace.open_partition(
            "contextual_message_by_sender",
            PartitionCreateOptions::default(),
        )?))
    }

    pub fn len(&self) -> Result<usize> {
        Ok(self.0.inner().len()?)
    }

    pub fn approximate_len(&self) -> usize {
        self.0.approximate_len()
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.0.inner().is_empty()?)
    }

    /// Insert a contextual message
    pub fn insert(&self, wtx: &mut WriteTransaction, key: &ContextualMessageBySenderKey) {
        wtx.insert(&self.0, key.as_bytes(), [])
    }

    /// Get all contextual messages for a sender (prefix search)
    pub fn get_by_sender_prefix(
        &self,
        rtx: &ReadTransaction,
        sender: &AddressPayload,
    ) -> impl DoubleEndedIterator<Item = Result<SharedImmutable<ContextualMessageBySenderKey>>> + '_
    {
        let prefix = sender.as_bytes();
        rtx.prefix(&self.0, prefix).map(|item| {
            let (key_bytes, _value_bytes) = item?;
            Ok(SharedImmutable::new(key_bytes))
        })
    }
    /// Get all contextual messages (for admin/debug purposes)
    pub fn get_all(
        &self,
        rtx: &ReadTransaction,
    ) -> impl DoubleEndedIterator<Item = Result<SharedImmutable<ContextualMessageBySenderKey>>> + '_
    {
        rtx.iter(&self.0).map(|item| {
            let (key_bytes, _value_bytes) = item?;
            Ok(SharedImmutable::new(key_bytes))
        })
    }

    /// Get contextual messages by sender and alias from a specific block time (for pagination)
    pub fn get_by_sender_alias_from_block_time(
        &self,
        rtx: &ReadTransaction,
        sender: &AddressPayload,
        alias: &[u8; 16],
        from_block_time: u64,
    ) -> impl DoubleEndedIterator<Item = Result<SharedImmutable<ContextualMessageBySenderKey>>> + '_
    {
        // Create range start: sender (34 bytes) + alias (16 bytes) + block_time (8 bytes)
        let mut range_start = [0u8; 58]; // 34 + 16 + 8
        range_start[..34].copy_from_slice(sender.as_bytes());
        range_start[34..50].copy_from_slice(alias);
        range_start[50..58].copy_from_slice(&from_block_time.to_be_bytes());

        // Create range start: sender (34 bytes) + alias (16 bytes) + block_time (8 bytes)
        let mut range_end = [0xFF; 58]; // 34 + 16 + 8
        range_end[..34].copy_from_slice(sender.as_bytes());
        range_end[34..50].copy_from_slice(alias);

        rtx.range(&self.0, range_start..=range_end).map(|item| {
            let (key_bytes, _value_bytes) = item?;
            Ok(SharedImmutable::new(key_bytes))
        })
    }
}

#[derive(Clone)]
pub struct TxIdToContextualMessagePartition(fjall::TxPartition);

impl TxIdToContextualMessagePartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(keyspace.open_partition(
            "tx-id-to-contextual-message",
            PartitionCreateOptions::default(),
        )?))
    }

    pub fn len(&self) -> Result<usize> {
        Ok(self.0.inner().len()?)
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.0.inner().is_empty()?)
    }

    pub fn insert(&self, tx_id: &[u8; 32], sealed_hex: &[u8]) -> Result<()> {
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
    ) -> Result<Option<SharedImmutable<[u8]>>> {
        rtx.get(&self.0, tx_id)
            .map(|bts| bts.map(SharedImmutable::new))
            .map_err(anyhow::Error::from)
    }
}
