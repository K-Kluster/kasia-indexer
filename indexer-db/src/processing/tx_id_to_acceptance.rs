use crate::{AddressPayload, PartitionId, SharedImmutable};
use anyhow::bail;
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use tracing::warn;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

#[derive(Clone)]
pub struct TxIDToAcceptancePartition(fjall::TxPartition);

#[repr(C)]
#[derive(Clone, Copy, Debug, Immutable, IntoBytes, FromBytes, Unaligned, KnownLayout)]
pub struct AcceptanceKey {
    pub tx_id: [u8; 32],
    pub receiver: AddressPayload,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Immutable, IntoBytes, FromBytes, Unaligned, KnownLayout)]
pub struct AcceptanceValueResolved {
    zero_byte: u8,
    pub accepting_block_hash: [u8; 32],
    pub accepting_daa: zerocopy::U64<zerocopy::LE>,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnresolvedEntry<'a> {
    pub partition_id: PartitionId,
    pub action: Action,
    pub key: &'a [u8],
}

#[repr(u8)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, TryFromBytes, IntoBytes, Unaligned, Immutable, KnownLayout,
)]
pub enum Action {
    ReplaceByKeySender,
    UpdateValueSender,
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
        let value_size: usize = entries
            .iter()
            .map(|entry| (entry.partition_key)().len() + 2)
            .sum();
        let mut v = Vec::with_capacity(1 + value_size);
        v.push(1);
        entries.iter().for_each(|entry| {
            v.extend_from_slice(entry.partition_id.as_bytes());
            v.extend_from_slice(entry.action.as_bytes());
            v.extend_from_slice((entry.partition_key)());
        });
        wtx.insert(&self.0, k.as_bytes(), &v);

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

    pub fn resolve_wtx(
        &mut self,
        wtx: &mut WriteTransaction,
        k: &AcceptanceKey,
        accepting_block_hash: [u8; 32],
        acceptance_daa: u64,
        key_len_fn: impl Fn(PartitionId) -> usize,
        resolve_fn: impl for<'a, 'b> Fn(
            &'a mut WriteTransaction,
            UnresolvedEntry<'b>,
        ) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let mut value = None;
        wtx.fetch_update(&self.0, k.as_bytes(), |old_value| {
            value = old_value.cloned();
            Some(
                AcceptanceValueResolved {
                    zero_byte: 0,
                    accepting_block_hash,
                    accepting_daa: acceptance_daa.into(),
                }
                .as_bytes()
                .into(),
            )
        })?;
        let value = value.unwrap();
        let value = value.as_bytes();
        if value.first() == Some(&0u8) {
            warn!("Value already resolved: tx_id={:?}", k.tx_id);
            return Ok(());
        }
        if value.first() != Some(&1u8) {
            bail!("Invalid value: tx_id={:?}, expected 0 or 1", k.tx_id);
        }
        let mut entries = &value[1..];
        while !entries.is_empty() {
            let partition_id = PartitionId::try_read_from_bytes(&entries[..1])
                .map_err(|_| anyhow::anyhow!("Invalid partition id: {}", entries[0]))?;
            let key_len = key_len_fn(partition_id);
            let action = Action::try_read_from_bytes(&entries[1..2])
                .map_err(|_| anyhow::anyhow!("Invalid action: {}", entries[1]))?;
            let entry = UnresolvedEntry {
                partition_id,
                action,
                key: &entries[2..2 + key_len],
            };
            resolve_fn(wtx, entry)?;
            entries = &entries[2 + key_len..];
        }

        Ok(())
    }

    pub fn acceptance_by_tx_id_rtx(
        &self,
        rtx: &ReadTransaction,
        tx_id: &[u8; 32],
    ) -> anyhow::Result<Option<SharedImmutable<AcceptanceValueResolved>>> {
        let mut iter = rtx.prefix(&self.0, tx_id);
        let kv = iter.next().transpose()?;
        let r = match kv {
            None => Ok(None),
            Some((_k, v)) if v[0] == 1 => Ok(None),
            Some((_k, v)) if v[0] == 0 => Ok(Some(SharedImmutable::new(v))),
            Some(_) => bail!("Invalid value"),
        };
        assert!(iter.next().is_none()); // we should not have more than one entry
        r
    }
}
