use anyhow::Result;
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use kaspa_rpc_core::RpcTransactionId;
use bytemuck::{AnyBitPattern, NoUninit};

/// FIFO partition for tracking transactions that need sender address/payload resolution
/// Key: accepting_daa_score + tx_id, Value: empty
/// Simple tracking - just marks what needs processing
/// 
/// Uses FIFO compaction strategy because:
/// - Pending resolutions are temporary - will be processed and removed
/// - Older entries become irrelevant as blockchain progresses
/// - Self-balancing: automatically removes old entries when size limit reached
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSenderResolution {
    pub accepting_daa_score: u64,
    pub tx_id: RpcTransactionId,
}

#[derive(Clone)]
pub struct PendingSenderResolutionPartition(fjall::TxPartition);

#[repr(C)]
#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq, PartialOrd, Ord)]
pub struct PendingResolutionKey {
    pub accepting_daa_score: [u8; 8], // be - for ordering
    pub tx_id: [u8; 32],
}

impl PendingSenderResolutionPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(
            keyspace.open_partition(
                "pending_sender_resolution",
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

    /// Mark a transaction as needing sender resolution
    pub fn mark_pending(
        &self,
        wtx: &mut WriteTransaction,
        accepting_daa_score: u64,
        tx_id: RpcTransactionId,
    ) -> Result<()> {
        let key = PendingResolutionKey {
            accepting_daa_score: accepting_daa_score.to_be_bytes(),
            tx_id: *tx_id.as_ref(),
        };
        wtx.insert(&self.0, bytemuck::bytes_of(&key), []);
        Ok(())
    }

    /// Remove pending marking (when resolution is complete)
    pub fn remove_pending(
        &self,
        wtx: &mut WriteTransaction,
        accepting_daa_score: u64,
        tx_id: RpcTransactionId,
    ) -> Result<()> {
        let key = PendingResolutionKey {
            accepting_daa_score: accepting_daa_score.to_be_bytes(),
            tx_id: *tx_id.as_ref(),
        };
        wtx.remove(&self.0, bytemuck::bytes_of(&key));
        Ok(())
    }

    /// Get all pending resolutions (for processing)
    pub fn get_all_pending(
        &self,
        rtx: &ReadTransaction,
    ) -> impl DoubleEndedIterator<Item = Result<PendingSenderResolution>> + '_ {
        rtx.iter(&self.0)
            .map(|item| {
                let (key_bytes, _) = item?;
                if key_bytes.len() == 40 {
                    let key: PendingResolutionKey = *bytemuck::from_bytes(&key_bytes);
                    let accepting_daa_score = u64::from_be_bytes(key.accepting_daa_score);
                    let tx_id = RpcTransactionId::from_slice(&key.tx_id);
                    Ok(PendingSenderResolution {
                        accepting_daa_score,
                        tx_id,
                    })
                } else {
                    Err(anyhow::anyhow!("Invalid key length in pending_sender_resolution partition"))
                }
            })
    }

    /// Mark multiple transactions as needing sender resolution
    pub fn mark_pending_batch<'a, I>(
        &self,
        wtx: &mut WriteTransaction,
        accepting_daa_score: u64,
        tx_ids: I,
    ) -> Result<()>
    where
        I: Iterator<Item = &'a RpcTransactionId>,
    {
        for tx_id in tx_ids {
            self.mark_pending(wtx, accepting_daa_score, *tx_id)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pending_resolution_key_serialization() {
        let key = PendingResolutionKey {
            accepting_daa_score: 12345u64.to_be_bytes(),
            tx_id: [1u8; 32],
        };
        
        let bytes = bytemuck::bytes_of(&key);
        assert_eq!(bytes.len(), 40);
        
        let deserialized: PendingResolutionKey = *bytemuck::from_bytes(bytes);
        assert_eq!(deserialized, key);
        
        let accepting_daa_score = u64::from_be_bytes(deserialized.accepting_daa_score);
        assert_eq!(accepting_daa_score, 12345);
    }

    #[test]
    fn test_key_ordering() {
        let key1 = PendingResolutionKey {
            accepting_daa_score: 100u64.to_be_bytes(),
            tx_id: [1u8; 32],
        };
        
        let key2 = PendingResolutionKey {
            accepting_daa_score: 200u64.to_be_bytes(),
            tx_id: [1u8; 32],
        };
        
        assert!(key1 < key2); // Lower DAA score comes first
    }

    #[test]
    fn test_pending_sender_resolution_struct() {
        let resolution = PendingSenderResolution {
            accepting_daa_score: 12345,
            tx_id: RpcTransactionId::from_slice(&[1u8; 32]),
        };
        
        assert_eq!(resolution.accepting_daa_score, 12345);
        assert_eq!(resolution.tx_id.as_bytes(), [1u8; 32]);
    }
}