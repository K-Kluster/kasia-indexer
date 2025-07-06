use anyhow::Result;
use bytemuck::{AnyBitPattern, NoUninit};
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use kaspa_rpc_core::RpcHash;

/// Regular partition for storing accepting block hash to DAA score and blue score mapping
/// Note: These are accepting blocks (selected blocks in GHOSTDAG), not regular blocks
#[derive(Clone)]
pub struct AcceptingBlockScoresPartition(fjall::TxPartition);

#[repr(C)]
#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
pub struct AcceptingBlockScoresValue {
    pub daa_score: [u8; 8],  // be
    pub blue_score: [u8; 8], // be
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockScores {
    pub daa_score: u64,
    pub blue_score: u64,
}

impl AcceptingBlockScoresPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(keyspace.open_partition(
            "accepting_block_scores",
            PartitionCreateOptions::default(),
        )?))
    }

    /// Store DAA and blue scores for an accepting block
    pub fn insert_scores(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hash: [u8; 32],
        daa_score: u64,
        blue_score: u64,
    ) -> Result<()> {
        let scores = AcceptingBlockScoresValue {
            daa_score: daa_score.to_be_bytes(),
            blue_score: blue_score.to_be_bytes(),
        };
        wtx.insert(&self.0, accepting_block_hash, bytemuck::bytes_of(&scores));
        Ok(())
    }

    /// Get DAA and blue scores for an accepting block
    pub fn get_scores(
        &self,
        rtx: &ReadTransaction,
        accepting_block_hash: [u8; 32],
    ) -> Result<Option<BlockScores>> {
        Ok(rtx.get(&self.0, accepting_block_hash)?.map(|bytes| {
            let scores: AcceptingBlockScoresValue = *bytemuck::from_bytes(&bytes);
            let daa_score = u64::from_be_bytes(scores.daa_score);
            let blue_score = u64::from_be_bytes(scores.blue_score);
            BlockScores {
                daa_score,
                blue_score,
            }
        }))
    }

    /// Get DAA and blue scores for an accepting block (write transaction version)
    pub fn get_scores_wtx(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hash: [u8; 32],
    ) -> Result<Option<BlockScores>> {
        Ok(wtx.get(&self.0, accepting_block_hash)?.map(|bytes| { 
            let scores: AcceptingBlockScoresValue = *bytemuck::from_bytes(&bytes);
            let daa_score = u64::from_be_bytes(scores.daa_score);
            let blue_score = u64::from_be_bytes(scores.blue_score);
           BlockScores {
                daa_score,
                blue_score,
            }
        }))
    }

    /// Check if we have scores for an accepting block
    pub fn has_scores(
        &self,
        rtx: &ReadTransaction,
        accepting_block_hash: [u8; 32],
    ) -> Result<bool> {
        Ok(rtx.contains_key(&self.0, accepting_block_hash)?)
    }

    /// Remove scores for an accepting block (reorg case)
    pub fn remove_scores(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hash: [u8; 32],
    ) -> Result<()> {
        wtx.remove(&self.0, accepting_block_hash);
        Ok(())
    }
}

/// FIFO partition for storing accepting block hashes with unknown DAA scores
/// Note: These are accepting blocks (selected blocks in GHOSTDAG), not regular blocks
///
/// Uses FIFO compaction strategy because:
/// - Unknown DAA scores are temporary - will be resolved when RPC call completes
/// - Older entries become irrelevant as blockchain progresses
/// - Self-balancing: automatically removes old entries when size limit reached
/// - Prevents memory leaks from accumulating unresolved RPC requests
#[derive(Clone)]
pub struct UnknownAcceptingBlockDaaPartition(fjall::TxPartition);

impl UnknownAcceptingBlockDaaPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(
            keyspace.open_partition(
                "unknown_accepting_block_daa",
                PartitionCreateOptions::default()
                    .max_memtable_size(50 * 1024 * 1024)
                    .block_size(64 * 1024)
                    .compaction_strategy(fjall::compaction::Strategy::Fifo(
                        fjall::compaction::Fifo {
                            limit: 50 * 1024 * 1024,
                            ttl_seconds: None,
                        },
                    )),
            )?,
        ))
    }

    /// Mark an accepting block hash as needing DAA score resolution via RPC
    pub fn mark_unknown_daa(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hash: &RpcHash,
    ) -> Result<()> {
        wtx.insert(&self.0, accepting_block_hash.as_bytes(), []);
        Ok(())
    }

    /// Remove unknown DAA marking (when DAA score is resolved)
    pub fn remove_unknown_daa(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hash: RpcHash,
    ) -> Result<()> {
        wtx.remove(&self.0, accepting_block_hash.as_bytes());
        Ok(())
    }

    /// Check if an accepting block hash needs DAA score resolution
    pub fn needs_daa_resolution(
        &self,
        rtx: &ReadTransaction,
        accepting_block_hash: RpcHash,
    ) -> Result<bool> {
        Ok(rtx.contains_key(&self.0, accepting_block_hash)?)
    }

    /// Check if an accepting block hash needs DAA score resolution (write transaction version)
    pub fn needs_daa_resolution_wtx(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hash: RpcHash,
    ) -> Result<bool> {
        Ok(wtx.contains_key(&self.0, accepting_block_hash)?)
    }

    /// Get all accepting block hashes that need DAA score resolution
    pub fn get_all_unknown_accepting_blocks(&self, rtx: &ReadTransaction) -> impl DoubleEndedIterator<Item = Result<RpcHash>> {
       rtx.iter(&self.0).map(|r| {
            r.map_err(anyhow::Error::from).map(|(k, _)| {
                RpcHash::from_slice(k.as_ref())
            })
        })
    }

    /// Mark multiple accepting block hashes as needing DAA score resolution
    pub fn mark_unknown_daa_batch<'a, I>(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hashes: I,
    ) -> Result<()>
    where
        I: Iterator<Item = &'a RpcHash>,
    {
        for accepting_block_hash in accepting_block_hashes {
            self.mark_unknown_daa(wtx, accepting_block_hash)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accepting_block_scores_serialization() {
        let scores = AcceptingBlockScoresValue {
            daa_score: 12345u64.to_be_bytes(),
            blue_score: 67890u64.to_be_bytes(),
        };

        let bytes = bytemuck::bytes_of(&scores);
        assert_eq!(bytes.len(), 16);

        let deserialized: AcceptingBlockScoresValue = *bytemuck::from_bytes(bytes);
        assert_eq!(deserialized, scores);

        let daa = u64::from_be_bytes(deserialized.daa_score);
        let blue = u64::from_be_bytes(deserialized.blue_score);
        assert_eq!(daa, 12345);
        assert_eq!(blue, 67890);
    }

    #[test]
    fn test_accepting_block_hash_operations() {
        let accepting_block_hash1 = [1u8; 32];
        let accepting_block_hash2 = [2u8; 32];

        assert_eq!(accepting_block_hash1.len(), 32);
        assert_eq!(accepting_block_hash2.len(), 32);
        assert_ne!(accepting_block_hash1, accepting_block_hash2);
    }

    #[test]
    fn test_batch_operations() {
        let accepting_block_hashes = vec![[1u8; 32], [2u8; 32], [3u8; 32]];

        assert_eq!(accepting_block_hashes.len(), 3);

        // Test that we can iterate over references
        for accepting_block_hash in &accepting_block_hashes {
            assert_eq!(accepting_block_hash.len(), 32);
        }
    }
}
