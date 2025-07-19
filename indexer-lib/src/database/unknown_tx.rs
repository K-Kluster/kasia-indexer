use crate::database::util::TwoPointerIntersect;
use anyhow::{Result, bail};
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use kaspa_rpc_core::{RpcHash, RpcTransactionId};

/// FIFO partition for storing transactions with unknown acceptance status
/// Key: tx_id, Value: accepting_block_hash so we can search by transaction and get the block
///
/// Uses FIFO compaction strategy because:
/// - Unknown status is temporary - will be resolved when block/acceptance notifications arrive
/// - Older unknown transactions become irrelevant as blockchain progresses
/// - Self-balancing: automatically removes old entries when size limit reached
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownTxInfo {
    pub tx_id: RpcTransactionId,
    pub accepting_block_hash: RpcHash,
}

#[derive(Clone)]
pub struct UnknownTxPartition(fjall::TxPartition);

impl UnknownTxPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(
            keyspace.open_partition(
                "unknown_tx",
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

    /// Mark a transaction as having unknown acceptance status
    pub fn mark_unknown(
        &self,
        wtx: &mut WriteTransaction,
        tx_id: &[u8; 32],
        accepting_block_hash: RpcHash,
    ) -> Result<()> {
        let accepting_block_hash: &[u8; 32] = accepting_block_hash.as_ref();
        wtx.insert(&self.0, tx_id, accepting_block_hash);
        Ok(())
    }

    /// Get accepting block hash for an unknown transaction (if it exists)
    pub fn get_accepting_block_hash_rtx(
        &self,
        rtx: &ReadTransaction,
        tx_id: RpcTransactionId,
    ) -> Result<Option<RpcHash>> {
        rtx.get(&self.0, tx_id.as_bytes())?
            .map(|bytes| {
                if bytes.len() == 32 {
                    Ok(RpcHash::from_slice(&bytes))
                } else {
                    bail!("Invalid value length in unknown_tx partition")
                }
            })
            .transpose()
    }

    /// Get accepting block hash for an unknown transaction (if it exists)
    pub fn get_accepting_block_hash_wtx(
        &self,
        wtx: &mut WriteTransaction,
        tx_id: RpcTransactionId,
    ) -> Result<Option<RpcHash>> {
        wtx.get(&self.0, tx_id.as_bytes())?
            .map(|bytes| {
                if bytes.len() == 32 {
                    Ok(RpcHash::from_slice(&bytes))
                } else {
                    bail!("Invalid value length in unknown_tx partition")
                }
            })
            .transpose()
    }

    /// Check if a transaction has unknown acceptance status
    pub fn is_unknown(&self, rtx: &ReadTransaction, tx_id: RpcTransactionId) -> Result<bool> {
        Ok(rtx.get(&self.0, tx_id.as_bytes())?.is_some())
    }

    /// Check if a transaction has unknown acceptance status (write transaction version)
    pub fn is_unknown_wtx(
        &self,
        wtx: &mut WriteTransaction,
        tx_id: RpcTransactionId,
    ) -> Result<bool> {
        Ok(wtx.get(&self.0, tx_id.as_bytes())?.is_some())
    }

    /// Remove unknown marking for a transaction (when status becomes known)
    pub fn remove_unknown(
        &self,
        wtx: &mut WriteTransaction,
        tx_id: RpcTransactionId,
    ) -> Result<Option<RpcHash>> {
        Ok(wtx
            .fetch_update(&self.0, tx_id.as_bytes(), |_old| None)?
            .map(|b| RpcHash::try_from_slice(&b))
            .transpose()?)
    }

    // /// Check multiple transactions for unknown status
    // pub fn check_unknown_batch<'a, I>(
    //     &self,
    //     wtx: &mut WriteTransaction,
    //     tx_ids: I,
    // ) -> impl Iterator<Item = Result<(RpcTransactionId, RpcHash)>>
    // where
    //     I: Iterator<Item = &'a RpcTransactionId>, // lexicographically ordered
    // {
    //     let unknowns = wtx.iter(&self.0).map(|r| {
    //         r.map_err(anyhow::Error::from).and_then(|(k, v)| {
    //             if k.len() != 32 || v.len() != 32 {
    //                 bail!("Invalid key/value length in unknown_tx partition")
    //             }
    //             let tx_id = RpcTransactionId::from_slice(&k);
    //             let accepting_block_hash = RpcHash::from_slice(&v);
    //             Ok((tx_id, accepting_block_hash))
    //         })
    //     });
    //     unknowns.intersect_with(tx_ids)
    // }

    /// Get all unknown transactions with their accepting block hashes
    pub fn get_all_unknown(
        &self,
        rtx: &ReadTransaction,
    ) -> impl DoubleEndedIterator<Item = Result<UnknownTxInfo>> + '_ {
        rtx.iter(&self.0).map(|item| {
            let (key, value) = item?;
            if key.len() == 32 && value.len() == 32 {
                let tx_id = RpcTransactionId::from_slice(&key);
                let accepting_block_hash = RpcHash::from_slice(&value);
                Ok(UnknownTxInfo {
                    tx_id,
                    accepting_block_hash,
                })
            } else {
                Err(anyhow::anyhow!(
                    "Invalid key/value length in unknown_tx partition"
                ))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unknown_tx_operations() {
        let tx_id1 = [1u8; 32];
        let tx_id2 = [2u8; 32];

        // Test serialization - tx_id is just [u8; 32]
        assert_eq!(tx_id1.len(), 32);
        assert_eq!(tx_id2.len(), 32);

        // Test RpcTransactionId operations
        let rpc_tx_id = RpcTransactionId::from_slice(&tx_id1);
        assert_eq!(rpc_tx_id.as_bytes(), tx_id1);
    }

    #[test]
    fn test_batch_operations() {
        let tx_ids = vec![
            RpcTransactionId::from_slice(&[1u8; 32]),
            RpcTransactionId::from_slice(&[2u8; 32]),
            RpcTransactionId::from_slice(&[3u8; 32]),
        ];

        assert_eq!(tx_ids.len(), 3);

        // Test that we can iterate and extract bytes
        for tx_id in &tx_ids {
            let bytes = tx_id.as_bytes();
            assert_eq!(bytes.len(), 32);
        }
    }
}
