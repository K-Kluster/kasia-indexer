use anyhow::Result;
use arrayref::array_ref;
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};

// todo use blockhash type after rewriting rpc client
#[derive(Debug, Copy, Clone)]
pub struct BlockGap {
    pub from: [u8; 32],
    pub to: [u8; 32],
}
#[derive(Clone)]
pub struct BlockGapsPartition(fjall::TxPartition);
impl BlockGapsPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(keyspace.open_partition(
            "block_gaps",
            PartitionCreateOptions::default().block_size(64 * 1024),
        )?))
    }

    /// Add a block gap that needs to be filled
    pub fn add_gap_wtx(&self, wtx: &mut WriteTransaction, BlockGap { from, to }: BlockGap) {
        wtx.insert(&self.0, from, to);
    }

    /// Add a block gap that needs to be filled
    pub fn add_gap(&self, BlockGap { from, to }: BlockGap) -> Result<()> {
        self.0.insert(from, to)?;
        Ok(())
    }

    /// Remove a gap (when it's been filled)
    pub fn remove_gap_wtx(&self, wtx: &mut WriteTransaction, from: &[u8; 32]) {
        wtx.remove(&self.0, from);
    }

    /// Remove a gap (when it's been filled)
    pub fn remove_gap(&self, from: &[u8; 32]) -> Result<()> {
        Ok(self.0.remove(from)?)
    }

    pub fn update_gap_wtx(&self, wtx: &mut WriteTransaction, BlockGap { from, to }: BlockGap) {
        wtx.insert(&self.0, from, to)
    }

    /// Get all block gaps that need to be filled
    pub fn get_all_gaps_rtx(
        &self,
        rtx: &ReadTransaction,
    ) -> impl DoubleEndedIterator<Item = Result<BlockGap>> + '_ {
        rtx.iter(&self.0).map(|item| {
            let (key, value) = item?;
            if key.len() == 32 && value.len() == 32 {
                Ok(BlockGap {
                    from: *array_ref![key, 0, 32],
                    to: *array_ref![value, 0, 32],
                })
            } else {
                Err(anyhow::anyhow!(
                    "Invalid key and value lengths in block_gaps partition"
                ))
            }
        })
    }

    pub fn get_all_gaps(&self) -> impl DoubleEndedIterator<Item = Result<BlockGap>> + '_ {
        self.0.inner().iter().map(|item| {
            let (key, value) = item?;
            if key.len() == 32 && value.len() == 32 {
                Ok(BlockGap {
                    from: *array_ref![key, 0, 32],
                    to: *array_ref![value, 0, 32],
                })
            } else {
                Err(anyhow::anyhow!(
                    "Invalid key and value lengths in block_gaps partition"
                ))
            }
        })
    }
}
