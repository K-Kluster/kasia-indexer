use anyhow::Result;
use bytemuck::{AnyBitPattern, NoUninit};
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use kaspa_math::Uint192;
use kaspa_rpc_core::RpcHash;

/// FIFO partition for storing accepting block gaps/holes that need to be filled
/// Key: from_blue_work + from_block_hash + to_blue_work + to_block_hash
/// Value: empty
/// Note: to_blue_work and to_block_hash can be zeros (used instead of None for optional fields)
/// Note: These are specifically for accepting blocks (selected blocks in GHOSTDAG)
///
/// Uses FIFO compaction strategy because:
/// - Gaps are temporary - will be filled and removed
/// - Older gaps become less relevant as blockchain progresses
/// - Self-balancing: automatically removes old entries when size limit reached
#[derive(Clone)]
pub struct AcceptingBlockGapsPartition(fjall::TxPartition);

#[repr(C)]
#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq, PartialOrd, Ord)]
pub struct AcceptingBlockGapKey {
    pub from_blue_work: [u8; 24],
    pub from_block_hash: [u8; 32],
    pub to_blue_work: [u8; 24],  // zeros = None
    pub to_block_hash: [u8; 32], // zeros = None
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptingBlockGap {
    pub from_blue_work: Uint192,
    pub from_block_hash: RpcHash,
    pub to_blue_work: Option<Uint192>,  // None represented as zeros
    pub to_block_hash: Option<RpcHash>, // None represented as zeros
}

impl AcceptingBlockGapsPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(
            keyspace.open_partition(
                "accepting_block_gaps",
                PartitionCreateOptions::default()
                    .block_size(64 * 1024)
                    .compaction_strategy(fjall::compaction::Strategy::Fifo(
                        fjall::compaction::Fifo {
                            limit: 128 * 1024 * 1024,
                            ttl_seconds: None,
                        },
                    )),
            )?,
        ))
    }

    /// Add an accepting block gap that needs to be filled
    pub fn add_gap(&self, wtx: &mut WriteTransaction, gap: AcceptingBlockGap) -> Result<()> {
        let key = AcceptingBlockGapKey {
            from_blue_work: gap.from_blue_work.to_be_bytes(),
            from_block_hash: *gap.from_block_hash.as_ref(),
            to_blue_work: gap
                .to_blue_work
                .map(|bw| bw.to_be_bytes())
                .unwrap_or([0u8; 24]),
            to_block_hash: gap.to_block_hash.map(|h| *h.as_ref()).unwrap_or([0u8; 32]),
        };
        wtx.insert(&self.0, bytemuck::bytes_of(&key), []);
        Ok(())
    }

    /// Remove a gap (when it's been filled)
    pub fn remove_gap(&self, wtx: &mut WriteTransaction, gap: AcceptingBlockGap) -> Result<()> {
        let key = AcceptingBlockGapKey {
            from_blue_work: gap.from_blue_work.to_be_bytes(),
            from_block_hash: *gap.from_block_hash.as_ref(),
            to_blue_work: gap
                .to_blue_work
                .map(|bw| bw.to_be_bytes())
                .unwrap_or([0u8; 24]),
            to_block_hash: gap.to_block_hash.map(|h| *h.as_ref()).unwrap_or([0u8; 32]),
        };
        wtx.remove(&self.0, bytemuck::bytes_of(&key));
        Ok(())
    }

    /// Get all accepting block gaps that need to be filled
    pub fn get_all_gaps(
        &self,
        rtx: &ReadTransaction,
    ) -> impl DoubleEndedIterator<Item = Result<AcceptingBlockGap>> + '_ {
        rtx.iter(&self.0).map(|item| {
            let (key_bytes, _) = item?;
            if key_bytes.len() == 112 {
                // 24 + 32 + 24 + 32
                let key: AcceptingBlockGapKey = *bytemuck::from_bytes(&key_bytes);

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

                Ok(AcceptingBlockGap {
                    from_blue_work: Uint192::from_be_bytes(key.from_blue_work),
                    from_block_hash: RpcHash::from_slice(&key.from_block_hash),
                    to_blue_work,
                    to_block_hash,
                })
            } else {
                Err(anyhow::anyhow!(
                    "Invalid key length in accepting_block_gaps partition"
                ))
            }
        })
    }

    /// Add multiple gaps in batch
    pub fn add_gaps_batch<'a, I>(&self, wtx: &mut WriteTransaction, gaps: I) -> Result<()>
    where
        I: Iterator<Item = &'a AcceptingBlockGap>,
    {
        for gap in gaps {
            self.add_gap(wtx, gap.clone())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accepting_block_gap_key_serialization() {
        let key = AcceptingBlockGapKey {
            from_blue_work: [1u8; 24],
            from_block_hash: [2u8; 32],
            to_blue_work: [3u8; 24],
            to_block_hash: [4u8; 32],
        };

        let bytes = bytemuck::bytes_of(&key);
        assert_eq!(bytes.len(), 112); // 24 + 32 + 24 + 32

        let deserialized: AcceptingBlockGapKey = *bytemuck::from_bytes(bytes);
        assert_eq!(deserialized, key);
    }

    #[test]
    fn test_accepting_block_gap_with_optional_fields() {
        // Test with Some values
        let gap_with_to = AcceptingBlockGap {
            from_blue_work: Uint192::from_be_bytes([1u8; 24]),
            from_block_hash: RpcHash::from_slice(&[2u8; 32]),
            to_blue_work: Some(Uint192::from_be_bytes([3u8; 24])),
            to_block_hash: Some(RpcHash::from_slice(&[4u8; 32])),
        };

        assert!(gap_with_to.to_blue_work.is_some());
        assert!(gap_with_to.to_block_hash.is_some());

        // Test with None values (represented as zeros)
        let gap_without_to = AcceptingBlockGap {
            from_blue_work: Uint192::from_be_bytes([1u8; 24]),
            from_block_hash: RpcHash::from_slice(&[2u8; 32]),
            to_blue_work: None,
            to_block_hash: None,
        };

        assert!(gap_without_to.to_blue_work.is_none());
        assert!(gap_without_to.to_block_hash.is_none());
    }

    #[test]
    fn test_accepting_block_specificity() {
        // This partition is specifically for accepting blocks in GHOSTDAG
        let gap = AcceptingBlockGap {
            from_blue_work: Uint192::from_be_bytes([1u8; 24]),
            from_block_hash: RpcHash::from_slice(&[2u8; 32]),
            to_blue_work: None,
            to_block_hash: None,
        };

        // Accepting blocks are a subset of all blocks
        assert_eq!(gap.from_block_hash.as_bytes().len(), 32);
    }
}
