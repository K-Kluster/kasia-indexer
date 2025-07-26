use crate::historical_syncer::Cursor;
use anyhow::Result;
use bytemuck::{AnyBitPattern, NoUninit};
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use kaspa_math::Uint192;
use kaspa_rpc_core::RpcHash;

#[derive(Clone)]
pub struct BlockGapsPartition(fjall::TxPartition);

#[repr(C)]
#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlockGapKey {
    pub from_daa_score: [u8; 8],
    pub from_blue_work: [u8; 24],
    pub from_block_hash: [u8; 32],
    pub to_blue_work: [u8; 24],
    pub to_block_hash: [u8; 32],
    pub to_daa_score: [u8; 8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockGap {
    pub from_daa_score: u64,
    pub from_blue_work: Uint192,
    pub from_block_hash: RpcHash,
    pub to_blue_work: Uint192,
    pub to_block_hash: RpcHash,
    pub to_daa_score: u64,
}

impl BlockGap {
    pub fn from_cursors(
        Cursor {
            daa_score: from_daa_score,
            blue_work: from_blue_work,
            hash: from_block_hash,
        }: Cursor,
        Cursor {
            daa_score: to_daa_score,
            blue_work: to_blue_work,
            hash: to_block_hash,
        }: Cursor,
    ) -> Self {
        Self {
            from_daa_score,
            from_blue_work,
            from_block_hash,
            to_blue_work,
            to_block_hash,
            to_daa_score,
        }
    }
}

impl BlockGapsPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(keyspace.open_partition(
            "block_gaps",
            PartitionCreateOptions::default().block_size(64 * 1024),
        )?))
    }

    /// Add a block gap that needs to be filled
    pub fn add_gap_wtx(&self, wtx: &mut WriteTransaction, gap: BlockGap) {
        let key = BlockGapKey {
            from_daa_score: gap.from_daa_score.to_be_bytes(),
            from_blue_work: gap.from_blue_work.to_be_bytes(),
            from_block_hash: *gap.from_block_hash.as_ref(),
            to_blue_work: gap.to_blue_work.to_be_bytes(),
            to_block_hash: gap.to_block_hash.as_bytes(),
            to_daa_score: gap.to_daa_score.to_be_bytes(),
        };
        wtx.insert(&self.0, bytemuck::bytes_of(&key), []);
    }

    /// Add a block gap that needs to be filled
    pub fn add_gap(&self, gap: BlockGap) -> Result<()> {
        let key = BlockGapKey {
            from_daa_score: gap.from_daa_score.to_be_bytes(),
            from_blue_work: gap.from_blue_work.to_be_bytes(),
            from_block_hash: *gap.from_block_hash.as_ref(),
            to_blue_work: gap.to_blue_work.to_be_bytes(),
            to_block_hash: gap.to_block_hash.as_bytes(),
            to_daa_score: gap.to_daa_score.to_be_bytes(),
        };
        self.0.insert(bytemuck::bytes_of(&key), [])?;
        Ok(())
    }

    /// Remove a gap (when it's been filled)
    pub fn remove_gap_wtx(&self, wtx: &mut WriteTransaction, gap: BlockGap) {
        let key = BlockGapKey {
            from_daa_score: gap.from_daa_score.to_be_bytes(),
            from_blue_work: gap.from_blue_work.to_be_bytes(),
            from_block_hash: *gap.from_block_hash.as_ref(),
            to_blue_work: gap.to_blue_work.to_be_bytes(),
            to_block_hash: gap.to_block_hash.as_bytes(),
            to_daa_score: gap.to_daa_score.to_be_bytes(),
        };
        wtx.remove(&self.0, bytemuck::bytes_of(&key));
    }

    /// Remove a gap (when it's been filled)
    pub fn remove_gap(&self, gap: BlockGap) -> Result<()> {
        let key = BlockGapKey {
            from_daa_score: gap.from_daa_score.to_be_bytes(),
            from_blue_work: gap.from_blue_work.to_be_bytes(),
            from_block_hash: *gap.from_block_hash.as_ref(),
            to_blue_work: gap.to_blue_work.to_be_bytes(),
            to_block_hash: gap.to_block_hash.as_bytes(),
            to_daa_score: gap.to_daa_score.to_be_bytes(),
        };
        Ok(self.0.remove(bytemuck::bytes_of(&key))?)
    }

    /// Get all block gaps that need to be filled
    pub fn get_all_gaps_rtx(
        &self,
        rtx: &ReadTransaction,
    ) -> impl DoubleEndedIterator<Item = Result<BlockGap>> + '_ {
        rtx.iter(&self.0).map(|item| {
            let (key_bytes, _) = item?;
            if key_bytes.len() == 128 {
                // 8 + 24 + 32 + 24 + 32 +8
                let key: BlockGapKey = *bytemuck::from_bytes(&key_bytes);

                Ok(BlockGap {
                    from_daa_score: u64::from_be_bytes(key.from_daa_score),
                    from_blue_work: Uint192::from_be_bytes(key.from_blue_work),
                    from_block_hash: RpcHash::from_slice(&key.from_block_hash),
                    to_blue_work: Uint192::from_be_bytes(key.to_blue_work),
                    to_block_hash: RpcHash::from_slice(&key.to_block_hash),
                    to_daa_score: u64::from_be_bytes(key.to_daa_score),
                })
            } else {
                Err(anyhow::anyhow!(
                    "Invalid key length in block_gaps partition"
                ))
            }
        })
    }

    /// Get all block gaps that need to be filled
    pub fn get_all_gaps(&self) -> impl DoubleEndedIterator<Item = Result<BlockGap>> + '_ {
        self.0.inner().iter().map(|item| {
            let (key_bytes, _) = item?;
            if key_bytes.len() == 128 {
                // 8 + 24 + 32 + 24 + 32 +8
                let key: BlockGapKey = *bytemuck::from_bytes(&key_bytes);

                Ok(BlockGap {
                    from_daa_score: u64::from_be_bytes(key.from_daa_score),
                    from_blue_work: Uint192::from_be_bytes(key.from_blue_work),
                    from_block_hash: RpcHash::from_slice(&key.from_block_hash),
                    to_blue_work: Uint192::from_be_bytes(key.to_blue_work),
                    to_block_hash: RpcHash::from_slice(&key.to_block_hash),
                    to_daa_score: u64::from_be_bytes(key.to_daa_score),
                })
            } else {
                Err(anyhow::anyhow!(
                    "Invalid key length in block_gaps partition"
                ))
            }
        })
    }

    /// Add multiple gaps in batch
    pub fn add_gaps_batch<'a, I>(&self, wtx: &mut WriteTransaction, gaps: I)
    where
        I: Iterator<Item = &'a BlockGap>,
    {
        for gap in gaps {
            self.add_gap_wtx(wtx, gap.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_gap_key_serialization() {
        let key = BlockGapKey {
            from_daa_score: [0u8; 8],
            from_blue_work: [1u8; 24],
            from_block_hash: [2u8; 32],
            to_blue_work: [3u8; 24],
            to_block_hash: [4u8; 32],
            to_daa_score: [5; 8],
        };

        let bytes = bytemuck::bytes_of(&key);
        assert_eq!(bytes.len(), 128); // 8 + 24 + 32 + 24 + 32 + 8

        let deserialized: BlockGapKey = *bytemuck::from_bytes(bytes);
        assert_eq!(deserialized, key);
    }

    #[test]
    fn test_blue_work_conversion() {
        let blue_work = Uint192::from_be_bytes([1u8; 24]);
        let bytes = blue_work.to_be_bytes();
        assert_eq!(bytes.len(), 24);

        let converted_back = Uint192::from_be_bytes(bytes);
        assert_eq!(blue_work, converted_back);
    }
}
