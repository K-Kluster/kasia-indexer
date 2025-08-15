use std::marker::PhantomData;

use anyhow::{Result, bail};
use bytemuck::{AnyBitPattern, NoUninit};
use fjall::{PartitionCreateOptions, ReadTransaction, UserKey, UserValue, WriteTransaction};

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
pub struct LikeSelfStashData<T: AsRef<[u8]>> {
    bts: T,
    phantom_data: PhantomData<[u8]>,
}

impl<T: AsRef<[u8]>> LikeSelfStashData<T> {
    pub fn new(bts: T) -> anyhow::Result<Self> {
        if bts.as_ref().len() < 255 + 34 {
            bail!(
                "SelfStash data must be at least 34 + 255 bytes (scope), got {}",
                bts.as_ref().len()
            );
        }
        Ok(Self {
            bts,
            phantom_data: PhantomData,
        })
    }

    pub fn scope(&self) -> &[u8] {
        let scope_bytes = self.bts.as_ref()[33..33 + 255]
            .try_into()
            .expect("scope bytes should be exactly 255 bytes");
        scope_bytes
    }

    pub fn owner(&self) -> &[u8] {
        self.bts.as_ref()[..33]
            .try_into()
            .expect("scope bytes should be exactly 255 bytes")
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

    pub fn insert(
        &self,
        key: &SelfStashKeyByOwner,
        sender: Option<AddressPayload>,
    ) -> anyhow::Result<()> {
        let sender = sender.unwrap_or_default();
        self.0
            .insert(bytemuck::bytes_of(key), bytemuck::bytes_of(&sender))?;
        Ok(())
    }

    pub fn insert_wtx(
        &self,
        wtx: &mut WriteTransaction,
        key: &SelfStashKeyByOwner,
        sender: Option<AddressPayload>,
    ) {
        let sender = sender.unwrap_or_default();
        wtx.insert(
            &self.0,
            bytemuck::bytes_of(key),
            bytemuck::bytes_of(&sender),
        );
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

    pub fn iter_by_owner_and_scope_from_block_time_rtx(
        &self,
        rtx: &ReadTransaction,
        scope: Option<&[u8]>,
        owner: AddressPayload,
        block_time: u64,
    ) -> impl DoubleEndedIterator<Item = Result<(LikeSelfStashData<UserKey>, impl AsRef<[u8]>)>> + '_
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
            Ok((LikeSelfStashData::new(key_bytes)?, value_bytes))
        })
    }

    pub fn approximate_len(&self) -> usize {
        self.0.approximate_len()
    }
}

#[derive(Clone)]
pub struct TxIdToSelfStashPartition(fjall::TxPartition);

impl TxIdToSelfStashPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> anyhow::Result<Self> {
        Ok(Self(keyspace.open_partition(
            "tx_id_to_self_stash",
            PartitionCreateOptions::default(),
        )?))
    }
    pub fn approximate_len(&self) -> usize {
        self.0.approximate_len()
    }

    pub fn insert(&self, tx_id: &[u8], scope: &[u8], sealed_hex: &[u8]) -> anyhow::Result<()> {
        if tx_id.len() != 32 {
            bail!("Transaction ID must be 32 bytes, got {}", tx_id.len());
        }

        let mut fixed_scope = [0u8; 255];
        let len = scope.len().min(255); // cap at 255
        fixed_scope[..len].copy_from_slice(&scope[..len]);

        // Value: fixed_scope (255 bytes) + sealed_hex (remaining bytes)
        let mut value_bytes = Vec::with_capacity(fixed_scope.len() + sealed_hex.len());
        value_bytes.extend_from_slice(&fixed_scope);
        value_bytes.extend_from_slice(sealed_hex);

        self.0.insert(tx_id, value_bytes)?;
        Ok(())
    }

    pub fn insert_wtx(
        &self,
        wtx: &mut WriteTransaction,
        tx_id: &[u8],
        scope: Option<&[u8]>,
        sealed_hex: &[u8],
    ) -> anyhow::Result<()> {
        if tx_id.len() != 32 {
            bail!("Transaction ID must be 32 bytes, got {}", tx_id.len());
        }

        // pad with zeros
        let mut fixed_scope = [0u8; 255];
        match scope {
            Some(scope) => {
                let scope_length = scope.len().min(255);
                fixed_scope[..scope_length].copy_from_slice(&scope[..scope_length]); // cap at 255,
            }
            None => (),
        };

        // Value: fixed_scope (255 bytes) + sealed_hex (remaining bytes)
        let mut value_bytes = Vec::with_capacity(fixed_scope.len() + sealed_hex.len());
        value_bytes.extend_from_slice(&fixed_scope);
        value_bytes.extend_from_slice(sealed_hex);

        wtx.insert(&self.0, tx_id, value_bytes);
        Ok(())
    }

    pub fn get(&self, tx_id: &[u8]) -> anyhow::Result<Option<([u8; 255], Vec<u8>)>> {
        if tx_id.len() != 32 {
            bail!("Transaction ID must be 32 bytes, got {}", tx_id.len());
        }

        if let Some(value_bytes) = self.0.get(tx_id)? {
            if value_bytes.len() >= 255 {
                // Split at position 255: first 255 bytes = scope, rest = sealed_hex
                let scope_bytes: [u8; 255] = value_bytes[..255]
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("Failed to parse scope bytes"))?;

                let sealed_hex = value_bytes[8..].to_vec();
                Ok(Some((scope_bytes, sealed_hex)))
            } else {
                bail!(
                    "Invalid value length in tx_id_to_self_stash partition: expected at least 255 bytes, got {}",
                    value_bytes.len()
                )
            }
        } else {
            Ok(None)
        }
    }

    pub fn get_rtx(
        &self,
        rtx: &ReadTransaction,
        tx_id: &[u8],
    ) -> anyhow::Result<Option<LikeSelfStashData<UserValue>>> {
        if tx_id.len() != 32 {
            bail!("Transaction ID must be 32 bytes, got {}", tx_id.len());
        }

        if let Some(value_bytes) = rtx.get(&self.0, tx_id)? {
            let self_stash_data = LikeSelfStashData::new(value_bytes)?;
            Ok(Some(self_stash_data))
        } else {
            Ok(None)
        }
    }
}
