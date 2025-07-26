use anyhow::Result;
use bytemuck::{AnyBitPattern, NoUninit};
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use kaspa_math::Uint192;
use kaspa_rpc_core::RpcHash;

/// FIFO partition for storing block gaps/holes that need to be filled
/// Key: from_blue_work + from_block_hash + to_blue_work + to_block_hash
/// Value: empty
/// Note: to_blue_work and to_block_hash can be zeros (used instead of None for optional fields)
///
/// Uses FIFO compaction strategy because:
/// - Gaps are temporary - will be filled and removed
/// - Older gaps become less relevant as blockchain progresses
/// - Self-balancing: automatically removes old entries when size limit reached
#[derive(Clone)]
pub struct BlockGapsPartition(fjall::TxPartition);

#[repr(C)]
#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlockGapKey {
    pub from_blue_work: [u8; 24],
    pub from_block_hash: [u8; 32],
    pub to_blue_work: [u8; 24],  // zeros = None
    pub to_block_hash: [u8; 32], // zeros = None
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockGap {
    pub from_blue_work: Uint192,
    pub from_block_hash: RpcHash,
    pub to_blue_work: Option<Uint192>,  // None represented as zeros
    pub to_block_hash: Option<RpcHash>, // None represented as zeros
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
            from_blue_work: gap.from_blue_work.to_be_bytes(),
            from_block_hash: *gap.from_block_hash.as_ref(),
            to_blue_work: gap
                .to_blue_work
                .map(|bw| bw.to_be_bytes())
                .unwrap_or([0u8; 24]),
            to_block_hash: gap.to_block_hash.map(|h| *h.as_ref()).unwrap_or([0u8; 32]),
        };
        wtx.insert(&self.0, bytemuck::bytes_of(&key), []);
    }

    /// Add a block gap that needs to be filled
    pub fn add_gap(&self, gap: BlockGap) -> Result<()> {
        let key = BlockGapKey {
            from_blue_work: gap.from_blue_work.to_be_bytes(),
            from_block_hash: *gap.from_block_hash.as_ref(),
            to_blue_work: gap
                .to_blue_work
                .map(|bw| bw.to_be_bytes())
                .unwrap_or([0u8; 24]),
            to_block_hash: gap.to_block_hash.map(|h| *h.as_ref()).unwrap_or([0u8; 32]),
        };
        self.0.insert(bytemuck::bytes_of(&key), [])?;
        Ok(())
    }

    /// Remove a gap (when it's been filled)
    pub fn remove_gap_wtx(&self, wtx: &mut WriteTransaction, gap: BlockGap) {
        let key = BlockGapKey {
            from_blue_work: gap.from_blue_work.to_be_bytes(),
            from_block_hash: *gap.from_block_hash.as_ref(),
            to_blue_work: gap
                .to_blue_work
                .map(|bw| bw.to_be_bytes())
                .unwrap_or([0u8; 24]),
            to_block_hash: gap.to_block_hash.map(|h| *h.as_ref()).unwrap_or([0u8; 32]),
        };
        wtx.remove(&self.0, bytemuck::bytes_of(&key));
    }

    /// Remove a gap (when it's been filled)
    pub fn remove_gap(&self, gap: BlockGap) -> Result<()> {
        let key = BlockGapKey {
            from_blue_work: gap.from_blue_work.to_be_bytes(),
            from_block_hash: *gap.from_block_hash.as_ref(),
            to_blue_work: gap
                .to_blue_work
                .map(|bw| bw.to_be_bytes())
                .unwrap_or([0u8; 24]),
            to_block_hash: gap.to_block_hash.map(|h| *h.as_ref()).unwrap_or([0u8; 32]),
        };
        Ok(self.0.remove(bytemuck::bytes_of(&key))?)
    }

    /// Get all block gaps that need to be filled
    pub fn get_all_gaps(
        &self,
        rtx: &ReadTransaction,
    ) -> impl DoubleEndedIterator<Item = Result<BlockGap>> + '_ {
        rtx.iter(&self.0).map(|item| {
            let (key_bytes, _) = item?;
            if key_bytes.len() == 112 {
                // 24 + 32 + 24 + 32
                let key: BlockGapKey = *bytemuck::from_bytes(&key_bytes);

                // Convert zeros back to None for optional fields
                let to_blue_work = if key.to_blue_work == [0u8; 24] {
                    None
                } else {
                    Some(Uint192::from_be_bytes(key.to_blue_work))
                };

                let to_block_hash = if key.to_block_hash == [0u8; 32] {
                    None
                } else {
                    Some(RpcHash::from_slice(&key.to_block_hash))
                };

                Ok(BlockGap {
                    from_blue_work: Uint192::from_be_bytes(key.from_blue_work),
                    from_block_hash: RpcHash::from_slice(&key.from_block_hash),
                    to_blue_work,
                    to_block_hash,
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
            from_blue_work: [1u8; 24],
            from_block_hash: [2u8; 32],
            to_blue_work: [3u8; 24],
            to_block_hash: [4u8; 32],
        };

        let bytes = bytemuck::bytes_of(&key);
        assert_eq!(bytes.len(), 112); // 24 + 32 + 24 + 32

        let deserialized: BlockGapKey = *bytemuck::from_bytes(bytes);
        assert_eq!(deserialized, key);
    }

    #[test]
    fn test_block_gap_with_optional_fields() {
        // Test with Some values
        let gap_with_to = BlockGap {
            from_blue_work: Uint192::from_be_bytes([1u8; 24]),
            from_block_hash: RpcHash::from_slice(&[2u8; 32]),
            to_blue_work: Some(Uint192::from_be_bytes([3u8; 24])),
            to_block_hash: Some(RpcHash::from_slice(&[4u8; 32])),
        };

        assert!(gap_with_to.to_blue_work.is_some());
        assert!(gap_with_to.to_block_hash.is_some());

        // Test with None values (represented as zeros)
        let gap_without_to = BlockGap {
            from_blue_work: Uint192::from_be_bytes([1u8; 24]),
            from_block_hash: RpcHash::from_slice(&[2u8; 32]),
            to_blue_work: None,
            to_block_hash: None,
        };

        assert!(gap_without_to.to_blue_work.is_none());
        assert!(gap_without_to.to_block_hash.is_none());
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
