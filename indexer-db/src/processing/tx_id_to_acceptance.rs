use crate::{AddressPayload, PartitionId, SharedImmutable};
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use std::ops::Deref;
use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

#[derive(Clone)]
pub struct TxIDToAcceptancePartition(fjall::TxPartition);

#[repr(C)]
#[derive(Clone, Copy, Debug, Immutable, IntoBytes, FromBytes, Unaligned, KnownLayout)]
pub struct AcceptanceKey {
    pub tx_id: [u8; 32],
    pub receiver: AddressPayload,
}

#[repr(C)]
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Immutable,
    IntoBytes,
    FromBytes,
    Unaligned,
    KnownLayout,
    Eq,
    PartialEq,
)]
pub struct AcceptanceValueHeader {
    pub accepting_block_hash: [u8; 32],
    pub accepting_daa: zerocopy::U64<zerocopy::LE>,
}

#[repr(C)]
#[derive(Debug, Immutable, Unaligned, KnownLayout, FromBytes)]
pub struct AcceptanceValueHeaderWithSuffix {
    pub header: AcceptanceValueHeader,
    pub suffix: [u8],
}

impl Deref for AcceptanceValueHeaderWithSuffix {
    type Target = AcceptanceValueHeader;

    fn deref(&self) -> &Self::Target {
        &self.header
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry<'a> {
    pub partition_id: PartitionId,
    pub action: Action,
    pub key: &'a [u8],
}

#[repr(u8)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, TryFromBytes, IntoBytes, Unaligned, Immutable, KnownLayout,
)]
pub enum Action {
    UpdateValueSender,
    InsertByKeySender,
}

pub struct InsertionEntry<'a> {
    pub partition_id: PartitionId,
    pub action: Action,
    pub partition_key: &'a dyn Fn() -> &'a [u8],
}

impl TxIDToAcceptancePartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> anyhow::Result<Self> {
        Ok(Self(keyspace.open_partition(
            "tx_id_to_acceptance",
            PartitionCreateOptions::default(),
        )?))
    }

    pub fn insert_wtx<'a>(
        &self,
        wtx: &mut WriteTransaction,
        k: &AcceptanceKey,
        entries: &[InsertionEntry<'a>],
    ) -> anyhow::Result<()> {
        let entries_size: usize = entries
            .iter()
            .map(|entry| (entry.partition_key)().len() + 2)
            .sum();
        wtx.fetch_update(&self.0, k.as_bytes(), |old_value| match old_value {
            None => {
                let mut v = Vec::with_capacity(size_of::<AcceptanceValueHeader>() + entries_size);
                v.extend_from_slice(AcceptanceValueHeader::new_zeroed().as_bytes());
                entries.iter().for_each(|entry| {
                    v.extend_from_slice(entry.partition_id.as_bytes());
                    v.extend_from_slice(entry.action.as_bytes());
                    v.extend_from_slice((entry.partition_key)());
                });
                Some(v.into())
            }
            Some(old) => {
                let mut v = Vec::with_capacity(old.len() + entries_size);
                v.extend_from_slice(old.as_ref());
                entries.iter().for_each(|entry| {
                    v.extend_from_slice(entry.partition_id.as_bytes());
                    v.extend_from_slice(entry.action.as_bytes());
                    v.extend_from_slice((entry.partition_key)());
                });
                Some(v.into())
            }
        })?;
        Ok(())
    }

    pub fn remove_wtx(&self, wtx: &mut WriteTransaction, k: &AcceptanceKey) {
        wtx.remove(&self.0, k.as_bytes());
    }

    pub fn key_by_tx_id_rtx(
        &self,
        rtx: &ReadTransaction,
        tx_id: &[u8; 32],
    ) -> anyhow::Result<Option<SharedImmutable<AcceptanceKey>>> {
        let mut iter = rtx.prefix(&self.0, tx_id);
        let r = iter.next().transpose()?;
        assert!(iter.next().is_none()); // we should not have more than one entry
        Ok(r.map(|(k, _)| SharedImmutable::new(k)))
    }

    pub fn key_by_tx_id(
        &self,
        tx_id: &[u8; 32],
    ) -> anyhow::Result<Option<SharedImmutable<AcceptanceKey>>> {
        let mut iter = self.0.inner().prefix(tx_id);
        let r = iter.next().transpose()?;
        assert!(iter.next().is_none()); // we should not have more than one entry
        Ok(r.map(|(k, _)| SharedImmutable::new(k)))
    }

    pub fn update_acceptance_wtx(
        &self,
        wtx: &mut WriteTransaction,
        k: &AcceptanceKey,
        accepting_block_hash: [u8; 32],
        acceptance_daa: u64,
    ) -> anyhow::Result<LookupOutput> {
        let old_value = wtx.fetch_update(&self.0, k.as_bytes(), |old_value| {
            let old_value = old_value?;
            let mut v = Vec::with_capacity(old_value.len());
            let h = AcceptanceValueHeader {
                accepting_block_hash,
                accepting_daa: acceptance_daa.into(),
            };
            v.extend_from_slice(h.as_bytes());
            v.extend_from_slice(old_value[size_of::<AcceptanceValueHeader>()..].as_ref());
            Some(v.into())
        })?;
        Ok(match old_value {
            None => LookupOutput::KeyDoesNotExist,
            Some(value) if value.len() > size_of::<AcceptanceValueHeader>() => {
                LookupOutput::KeysExistsWithEntries
            }
            Some(_) => LookupOutput::KeyExistsNoEntries,
        })
    }

    pub fn resolve_entries_wtx(
        &self,
        wtx: &mut WriteTransaction,
        k: &AcceptanceKey,
        key_len_fn: impl Fn(PartitionId) -> usize,
        entry_cb_fn: impl for<'a, 'b> Fn(&'a mut WriteTransaction, Entry<'b>) -> anyhow::Result<()>,
    ) -> anyhow::Result<KeyExists> {
        let old_value = wtx.fetch_update(&self.0, k.as_bytes(), |old_value| {
            let old_value = old_value?;
            let new_value = &old_value.as_ref()[..size_of::<AcceptanceValueHeader>()];
            Some(new_value.into())
        })?;

        let Some(old_value) = old_value else {
            return Ok(false);
        };
        let mut entries = &old_value[size_of::<AcceptanceValueHeader>()..];
        while !entries.is_empty() {
            let partition_id = PartitionId::try_read_from_bytes(&entries[..1])
                .map_err(|_| anyhow::anyhow!("Invalid partition id: {}", entries[0]))?;
            let key_len = key_len_fn(partition_id);
            let action = Action::try_read_from_bytes(&entries[1..2])
                .map_err(|_| anyhow::anyhow!("Invalid action: {}", entries[1]))?;
            let entry = Entry {
                partition_id,
                action,
                key: &entries[2..2 + key_len],
            };
            entry_cb_fn(wtx, entry)?;
            entries = &entries[2 + key_len..];
        }
        Ok(true)
    }

    pub fn acceptance_by_tx_id_rtx(
        &self,
        rtx: &ReadTransaction,
        tx_id: &[u8; 32],
    ) -> anyhow::Result<Option<SharedImmutable<AcceptanceValueHeaderWithSuffix>>> {
        let mut iter = rtx.prefix(&self.0, tx_id);
        let kv = iter.next().transpose()?;
        let r = match kv {
            None => Ok(None),
            Some((_k, v)) => {
                let r = SharedImmutable::<AcceptanceValueHeaderWithSuffix>::new(v);
                if r.header == AcceptanceValueHeader::new_zeroed() {
                    Ok(None)
                } else {
                    Ok(Some(r))
                }
            }
        };
        assert!(iter.next().is_none()); // we should not have more than one entry
        r
    }
}

type KeyExists = bool;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupOutput {
    KeyDoesNotExist,
    KeyExistsNoEntries,
    KeysExistsWithEntries,
}
