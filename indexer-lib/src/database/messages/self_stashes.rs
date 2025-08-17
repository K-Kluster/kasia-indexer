use std::{
    fmt::{Debug, Formatter},
    marker::PhantomData,
    ops::Deref,
};

use anyhow::{Result, anyhow};
use bytemuck::{AnyBitPattern, NoUninit};
use fjall::{PartitionCreateOptions, ReadTransaction, UserKey, WriteTransaction};

use crate::database::messages::AddressPayload;

/// owner (34) + scope (255) + block_time (8) + block_hash (32) + version (1) + tx_id (32)
#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
#[repr(C)]
pub struct SelfStashKeyByOwner {
    pub owner: AddressPayload,
    pub scope: [u8; 255],
    pub block_time: [u8; 8], // be
    pub block_hash: [u8; 32],
    pub version: u8,
    pub tx_id: [u8; 32],
}

#[repr(transparent)]
#[derive(Clone, PartialEq, Eq)]
pub struct LikeSelfStashByOwnerKey<T: AsRef<[u8]>> {
    bts: T,
    phantom_data: PhantomData<SelfStashKeyByOwner>,
}

impl<T: AsRef<[u8]>> Debug for LikeSelfStashByOwnerKey<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

impl<T: AsRef<[u8]>> LikeSelfStashByOwnerKey<T> {
    pub fn new(bts: T) -> Self {
        Self {
            bts,
            phantom_data: PhantomData,
        }
    }
}

impl<T: AsRef<[u8]>> Deref for LikeSelfStashByOwnerKey<T> {
    type Target = SelfStashKeyByOwner;

    fn deref(&self) -> &Self::Target {
        bytemuck::from_bytes(self.bts.as_ref())
    }
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

    pub fn insert_wtx(&self, wtx: &mut WriteTransaction, key: &SelfStashKeyByOwner, data: &[u8]) {
        wtx.insert(&self.0, bytemuck::bytes_of(key), data);
    }

    /// error if the key doesn't exists
    /// the key can change after this operation as owner is part of it
    pub fn update_owner(
        &self,
        wtx: &mut WriteTransaction,
        old_key: &SelfStashKeyByOwner,
        owner: &AddressPayload,
    ) -> anyhow::Result<()> {
        // exist guard
        let old_key_as_bytes = bytemuck::bytes_of(old_key);
        let data = self.0.inner().get(old_key_as_bytes)?.ok_or(anyhow!(
            "Cannot find an existing self-stash key while updating the owner"
        ))?;

        // create new key
        let new_key = {
            let mut old_cloned = old_key.clone();
            old_cloned.owner = owner.clone();

            old_cloned
        };

        wtx.insert(&self.0, bytemuck::bytes_of(&new_key), data);
        wtx.remove(&self.0, old_key_as_bytes);

        Ok(())
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = anyhow::Result<(SelfStashKeyByOwner, AddressPayload)>> {
        self.0.inner().iter().map(|r| {
            r.map_err(anyhow::Error::from).map(|(k, v)| {
                (
                    *bytemuck::from_bytes(k.as_ref()),
                    *bytemuck::from_bytes(v.as_ref()),
                )
            })
        })
    }

    pub fn iter_by_owner(
        &self,
        rtx: &ReadTransaction,
        owner: AddressPayload,
        block_time: u64,
    ) -> impl DoubleEndedIterator<Item = Result<(LikeSelfStashByOwnerKey<UserKey>, impl AsRef<[u8]>)>> + '_
    {
        // layout prefix: owner (34) + scope (255) + block_time (8)
        const OWNER_LEN: usize = core::mem::size_of::<AddressPayload>(); // 34
        const SCOPE_LEN: usize = 255;
        const TIME_LEN: usize = 8;
        const PREFIX_LEN: usize = OWNER_LEN + SCOPE_LEN + TIME_LEN;

        // start: owner + (scope or zeros) + from block_time
        let mut range_start = [0u8; PREFIX_LEN];
        range_start[..OWNER_LEN].copy_from_slice(bytemuck::bytes_of(&owner));
        range_start[OWNER_LEN + SCOPE_LEN..PREFIX_LEN].copy_from_slice(&block_time.to_be_bytes());

        // end: owner + (same scope if provided, otherwise 0xFF for all scopes) + max time (0xFF...)
        let mut range_end = [0xFFu8; PREFIX_LEN];
        range_end[..OWNER_LEN].copy_from_slice(bytemuck::bytes_of(&owner));

        rtx.range(&self.0, range_start..=range_end).map(|item| {
            let (key_bytes, value_bytes) = item?;
            Ok((LikeSelfStashByOwnerKey::new(key_bytes), value_bytes))
        })
    }

    pub fn iter_by_owner_and_scope_from_block_time_rtx(
        &self,
        rtx: &ReadTransaction,
        scope: Option<&[u8]>,
        owner: AddressPayload,
        block_time: u64,
    ) -> impl DoubleEndedIterator<Item = Result<(LikeSelfStashByOwnerKey<UserKey>, impl AsRef<[u8]>)>> + '_
    {
        // layout prefix: owner (34) + scope (255) + block_time (8)
        const OWNER_LEN: usize = core::mem::size_of::<AddressPayload>(); // 34
        const SCOPE_LEN: usize = 255;
        const TIME_LEN: usize = 8;
        const PREFIX_LEN: usize = OWNER_LEN + SCOPE_LEN + TIME_LEN;

        // build the fixed scope bytes if provided, zero-padded to 255
        let mut scope_bytes = [0u8; SCOPE_LEN];
        if let Some(s) = scope {
            let copy_len = core::cmp::min(SCOPE_LEN, s.len());
            scope_bytes[..copy_len].copy_from_slice(&s[..copy_len]);
        }

        // start: owner + (scope or zeros) + from block_time
        let mut range_start = [0u8; PREFIX_LEN];
        range_start[..OWNER_LEN].copy_from_slice(bytemuck::bytes_of(&owner));
        if scope.is_some() {
            range_start[OWNER_LEN..OWNER_LEN + SCOPE_LEN].copy_from_slice(&scope_bytes);
        } // else keep zeros to start from the first scope
        range_start[OWNER_LEN + SCOPE_LEN..PREFIX_LEN].copy_from_slice(&block_time.to_be_bytes());

        // end: owner + (same scope if provided, otherwise 0xFF for all scopes) + max time (0xFF...)
        let mut range_end = [0xFFu8; PREFIX_LEN];
        range_end[..OWNER_LEN].copy_from_slice(bytemuck::bytes_of(&owner));
        if scope.is_some() {
            // exact same scope to constrain results to this scope only
            range_end[OWNER_LEN..OWNER_LEN + SCOPE_LEN].copy_from_slice(&scope_bytes);
        }

        rtx.range(&self.0, range_start..=range_end).map(|item| {
            let (key_bytes, value_bytes) = item?;
            Ok((LikeSelfStashByOwnerKey::new(key_bytes), value_bytes))
        })
    }

    pub fn approximate_len(&self) -> usize {
        self.0.approximate_len()
    }
}
