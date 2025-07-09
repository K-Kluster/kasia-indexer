use anyhow::Result;
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use kaspa_rpc_core::RpcHash;

/// FIFO partition for storing accepting block hashes with unknown DAA scores
/// Note: These are accepting blocks (selected blocks in GHOSTDAG), not regular blocks
///
/// Uses FIFO compaction strategy because:
/// - Unknown DAA scores are temporary - will be resolved when RPC call completes
/// - Older entries become irrelevant as blockchain progresses
/// - Self-balancing: automatically removes old entries when size limit reached
#[derive(Clone)]
pub struct UnknownAcceptingDaaPartition(fjall::TxPartition);

impl UnknownAcceptingDaaPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(
            keyspace.open_partition(
                "unknown_accepting_daa",
                PartitionCreateOptions::default()
                    .max_memtable_size(64 * 1024 * 1024)
                    .block_size(64 * 1024)
                    .compaction_strategy(fjall::compaction::Strategy::Fifo(
                        fjall::compaction::Fifo {
                            limit: 64 * 1024 * 1024,
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
        Ok(rtx.contains_key(&self.0, accepting_block_hash.as_bytes())?)
    }

    /// Check if an accepting block hash needs DAA score resolution (write transaction version)
    pub fn needs_daa_resolution_wtx(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hash: RpcHash,
    ) -> Result<bool> {
        Ok(wtx.contains_key(&self.0, accepting_block_hash.as_bytes())?)
    }

    /// Get all accepting block hashes that need DAA score resolution
    pub fn get_all_unknown_accepting_blocks(
        &self,
        rtx: &ReadTransaction,
    ) -> impl DoubleEndedIterator<Item = Result<RpcHash>> + '_ {
        rtx.iter(&self.0).map(|r| {
            r.map_err(anyhow::Error::from)
                .map(|(k, _)| RpcHash::from_slice(k.as_ref()))
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
    fn test_rpc_hash_operations() {
        let accepting_block_hash = RpcHash::from_slice(&[1u8; 32]);
        assert_eq!(accepting_block_hash.as_bytes().len(), 32);
    }

    #[test]
    fn test_accepting_block_specificity() {
        // This partition is specifically for accepting blocks in GHOSTDAG
        let accepting_block_hash = RpcHash::from_slice(&[1u8; 32]);

        // Accepting blocks are a subset of all blocks
        assert_eq!(accepting_block_hash.as_bytes().len(), 32);
    }
}
