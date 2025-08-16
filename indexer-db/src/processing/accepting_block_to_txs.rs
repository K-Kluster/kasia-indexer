use crate::SharedImmutable;
use fjall::{KvSeparationOptions, PartitionCreateOptions, WriteTransaction};

#[derive(Clone)]
pub struct AcceptingBlockToTxIDPartition(fjall::TxPartition);

impl AcceptingBlockToTxIDPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> anyhow::Result<Self> {
        Ok(Self(keyspace.open_partition(
            "accepting_block_to_tx_id",
            PartitionCreateOptions::default().with_kv_separation(KvSeparationOptions::default()),
        )?))
    }

    pub fn insert_wtx(
        &self,
        wtx: &mut WriteTransaction,
        accepted_by_block_hash: &[u8; 32],
        tx_ids: &[[u8; 32]],
    ) {
        wtx.insert(&self.0, accepted_by_block_hash, tx_ids.as_flattened());
    }

    pub fn remove_wtx(
        &self,
        wtx: &mut WriteTransaction,
        accepted_by_block_hash: &[u8; 32],
    ) -> anyhow::Result<Option<SharedImmutable<[[u8; 32]]>>> {
        let old = wtx.fetch_update(&self.0, accepted_by_block_hash, |_old| None)?;
        Ok(old.map(SharedImmutable::new))
    }

    pub fn remove(&self, accepted_by_block_hash: &[u8; 32]) -> anyhow::Result<()> {
        Ok(self.0.inner().remove(accepted_by_block_hash)?)
    }
}
