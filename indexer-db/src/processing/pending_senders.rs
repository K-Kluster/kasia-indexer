use crate::SharedImmutable;
use fjall::{PartitionCreateOptions, WriteTransaction};
use zerocopy::big_endian::U64;
use zerocopy::{FromBytes, Immutable, IntoBytes, Unaligned};

#[derive(Clone)]
pub struct PendingSenderResolutionPartition(fjall::TxPartition);

#[repr(C)]
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Unaligned, Immutable, IntoBytes, FromBytes,
)]
pub struct PendingResolutionKey {
    pub accepting_daa_score: U64,
    pub tx_id: [u8; 32],
}

impl PendingSenderResolutionPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> anyhow::Result<Self> {
        Ok(Self(keyspace.open_partition(
            "pending_sender_resolution",
            PartitionCreateOptions::default().block_size(64 * 1024),
        )?))
    }
    pub fn len(&self) -> anyhow::Result<usize> {
        Ok(self.0.inner().len()?)
    }

    pub fn is_empty(&self) -> anyhow::Result<bool> {
        Ok(self.0.inner().is_empty()?)
    }

    pub fn insert_wtx(&self, wtx: &mut WriteTransaction, key: &PendingResolutionKey) {
        wtx.insert(&self.0, key.as_bytes(), [])
    }
    pub fn remove_wtx(&self, wtx: &mut WriteTransaction, key: &PendingResolutionKey) {
        wtx.remove(&self.0, key.as_bytes())
    }

    pub fn remove(&self, key: &PendingResolutionKey) -> anyhow::Result<()> {
        Ok(self.0.inner().remove(key.as_bytes())?)
    }
    pub fn get_all_pending(
        &self,
    ) -> impl DoubleEndedIterator<Item = anyhow::Result<SharedImmutable<PendingResolutionKey>>> + '_
    {
        self.0
            .inner()
            .iter()
            .map(|r| r.map_err(Into::into).map(|(k, _)| SharedImmutable::new(k)))
    }
}
