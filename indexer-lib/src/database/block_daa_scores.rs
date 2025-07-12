use anyhow::Result;
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use kaspa_rpc_core::RpcHash;

/// FIFO partition for storing block hash to DAA score mapping
/// Can store DAA scores for any block
/// Filled during block notification processing, queried during acceptance processing
///
/// Uses FIFO compaction strategy because:
/// - Block score mappings are temporary - older blocks become irrelevant
/// - Self-balancing: automatically removes old entries when size limit reached
#[derive(Clone)]
pub struct BlockDaaScoresPartition(fjall::TxPartition);

impl BlockDaaScoresPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(
            keyspace.open_partition(
                "block_daa_scores",
                PartitionCreateOptions::default()
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

    /// Store DAA score for a block
    pub fn insert_daa_score(
        &self,
        wtx: &mut WriteTransaction,
        block_hash: RpcHash,
        daa_score: u64,
    ) -> Result<()> {
        wtx.insert(&self.0, block_hash.as_bytes(), daa_score.to_be_bytes());
        Ok(())
    }

    /// Get DAA score for a block
    pub fn get_daa_score(&self, rtx: &ReadTransaction, block_hash: RpcHash) -> Result<Option<u64>> {
        if let Some(bytes) = rtx.get(&self.0, block_hash.as_bytes())? {
            if bytes.len() == 8 {
                let daa_bytes: [u8; 8] = bytes
                    .as_ref()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("Invalid DAA score length"))?;
                Ok(Some(u64::from_be_bytes(daa_bytes)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Get DAA score for a block (write transaction version)
    pub fn get_daa_score_wtx(
        &self,
        wtx: &mut WriteTransaction,
        block_hash: RpcHash,
    ) -> Result<Option<u64>> {
        if let Some(bytes) = wtx.get(&self.0, block_hash.as_bytes())? {
            if bytes.len() == 8 {
                let daa_bytes: [u8; 8] = bytes
                    .as_ref()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("Invalid DAA score length"))?;
                Ok(Some(u64::from_be_bytes(daa_bytes)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Check if we have DAA score for a block
    pub fn has_daa_score(&self, rtx: &ReadTransaction, block_hash: RpcHash) -> Result<bool> {
        Ok(rtx.contains_key(&self.0, block_hash.as_bytes())?)
    }

    /// Remove DAA score for a block (reorg case)
    pub fn remove_daa_score(&self, wtx: &mut WriteTransaction, block_hash: RpcHash) -> Result<()> {
        wtx.remove(&self.0, block_hash.as_bytes());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_daa_score_serialization() {
        let daa_score = 12345u64;
        let bytes = daa_score.to_be_bytes();
        assert_eq!(bytes.len(), 8);

        let deserialized = u64::from_be_bytes(bytes);
        assert_eq!(deserialized, daa_score);
    }

    #[test]
    fn test_rpc_hash_operations() {
        let block_hash = RpcHash::from_slice(&[1u8; 32]);
        assert_eq!(block_hash.as_bytes().len(), 32);
    }
}
