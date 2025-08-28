use crate::{AddressPayload, SharedImmutable};
use anyhow::Result;
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use std::fmt::Debug;
use std::mem::offset_of;
use zerocopy::big_endian::U64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

pub const SCOPE_LEN: usize = 255;

#[repr(transparent)]
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Unaligned, Immutable, KnownLayout, IntoBytes, FromBytes,
)]
pub struct SelfStashScope([u8; SCOPE_LEN]);
impl From<&[u8]> for SelfStashScope {
    fn from(s: &[u8]) -> Self {
        let mut b = [0u8; SCOPE_LEN];
        let n = core::cmp::min(SCOPE_LEN, s.len());
        b[..n].copy_from_slice(&s[..n]);
        SelfStashScope(b)
    }
}

/// owner (34) + scope (255) + block_time (8) + block_hash (32) + version (1) + tx_id (32)
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Immutable, KnownLayout, IntoBytes, FromBytes, Unaligned,
)]
#[repr(C)]
pub struct SelfStashKeyByOwner {
    pub owner: AddressPayload,
    pub scope: SelfStashScope,
    pub block_time: U64,
    pub block_hash: [u8; 32],
    pub version: u8,
    pub tx_id: [u8; 32],
}

#[derive(Clone)]
pub struct SelfStashByOwnerPartition(fjall::TxPartition);

impl SelfStashByOwnerPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> anyhow::Result<Self> {
        Ok(Self(keyspace.open_partition(
            "self_stash_by_owner",
            PartitionCreateOptions::default(),
        )?))
    }

    pub fn insert_wtx(&self, wtx: &mut WriteTransaction, key: &SelfStashKeyByOwner) {
        wtx.insert(&self.0, key.as_bytes(), []);
    }

    pub fn iter(
        &self,
    ) -> impl DoubleEndedIterator<Item = Result<SharedImmutable<SelfStashKeyByOwner>>> {
        self.0.inner().iter().map(|r| {
            r.map_err(anyhow::Error::from)
                .map(|(k, _v)| SharedImmutable::new(k))
        })
    }

    pub fn iter_by_owner_and_scope_from_block_time_rtx(
        &self,
        rtx: &ReadTransaction,
        scope: &[u8],
        owner: AddressPayload,
        block_time: u64,
    ) -> impl DoubleEndedIterator<Item = Result<SharedImmutable<SelfStashKeyByOwner>>> + '_ {
        // layout prefix: owner (34) + scope (255) + block_time (8)
        const OWNER_LEN: usize =
            offset_of!(SelfStashKeyByOwner, owner) + size_of::<AddressPayload>(); // 34
        const PREFIX_LEN: usize = offset_of!(SelfStashKeyByOwner, block_time) + size_of::<U64>();

        let scope_bytes = SelfStashScope::from(scope);

        // start: owner + scope + from block_time
        let mut range_start = [0u8; PREFIX_LEN];
        range_start[..OWNER_LEN].copy_from_slice(owner.as_bytes());
        range_start[OWNER_LEN..OWNER_LEN + SCOPE_LEN].copy_from_slice(scope_bytes.as_bytes());
        range_start[OWNER_LEN + SCOPE_LEN..PREFIX_LEN].copy_from_slice(&block_time.to_be_bytes());

        // end: owner + same scope + max time (0xFF...)
        let mut range_end = [0xFFu8; PREFIX_LEN];
        range_end[..OWNER_LEN].copy_from_slice(owner.as_bytes());
        range_end[OWNER_LEN..OWNER_LEN + SCOPE_LEN].copy_from_slice(scope_bytes.as_bytes());

        rtx.range(&self.0, range_start..=range_end).map(|item| {
            let (key_bytes, _value_bytes) = item?;
            Ok(SharedImmutable::new(key_bytes))
        })
    }
}

#[derive(Clone)]
pub struct TxIdToSelfStashPartition(fjall::TxPartition);

impl TxIdToSelfStashPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(keyspace.open_partition(
            "tx-id-to-self-stash",
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
