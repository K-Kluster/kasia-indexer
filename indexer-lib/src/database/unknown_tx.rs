use crate::database::LikeTxIds;
use anyhow::{Result, bail};
use fjall::{PartitionCreateOptions, ReadTransaction, UserValue, WriteTransaction};
use kaspa_rpc_core::{RpcHash, RpcTransactionId};

/// Explicit action to take when updating unknown transaction entries
#[derive(Debug, Clone)]
pub enum UnknownTxUpdateAction {
    /// Update with new transaction list
    Update(Vec<[u8; 32]>),
    /// Delete the entire entry for this accepting block hash
    Delete,
    /// Do nothing - keep the current state
    DoNothing,
}

/// FIFO partition for storing transactions with unknown acceptance status
/// Key: accepting_block_hash (32 bytes), Value: flattened array of tx_ids (32 bytes each)
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

    /// Insert unknown transactions for an accepting block hash (batch operation)
    /// Following the AcceptingBlockToTxIDPartition pattern
    pub fn insert_wtx(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hash: &RpcHash,
        tx_ids: &[[u8; 32]],
    ) {
        wtx.insert(
            &self.0,
            accepting_block_hash.as_bytes(),
            tx_ids.as_flattened(),
        );
    }

    /// Get all unknown transaction IDs for a specific accepting block hash
    pub fn get_by_accepting_block_hash(
        &self,
        rtx: &ReadTransaction,
        accepting_block_hash: &RpcHash,
    ) -> Result<Option<LikeTxIds<UserValue>>> {
        if let Some(value_bytes) = rtx.get(&self.0, accepting_block_hash.as_bytes())? {
            Ok(Some(LikeTxIds::new(value_bytes)))
        } else {
            Ok(None)
        }
    }

    /// Remove and return all unknown transaction IDs for a specific accepting block hash
    pub fn remove_by_accepting_block_hash(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hash: &RpcHash,
    ) -> Result<Option<LikeTxIds<UserValue>>> {
        let old = wtx.fetch_update(&self.0, accepting_block_hash.as_bytes(), |_old| None)?;
        Ok(old.map(LikeTxIds::new))
    }

    /// Update the transaction list for a specific accepting block hash using explicit action enum
    /// This allows atomic modifications like filtering out resolved transactions
    pub fn update_by_accepting_block_hash<F>(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hash: &RpcHash,
        mut update_fn: F,
    ) -> Result<Option<LikeTxIds<UserValue>>>
    where
        F: FnMut(&[[u8; 32]]) -> UnknownTxUpdateAction,
    {
        let old = wtx.fetch_update(&self.0, accepting_block_hash.as_bytes(), |current_value| {
            let value_bytes = current_value?;
            let like_tx_ids = LikeTxIds::new(value_bytes);
            let tx_ids = like_tx_ids.as_tx_ids();

            match update_fn(tx_ids) {
                UnknownTxUpdateAction::Update(updated_tx_ids) => {
                    if updated_tx_ids.is_empty() {
                        None // Remove the entire entry if no transactions remain
                    } else {
                        Some(updated_tx_ids.as_flattened().into())
                    }
                }
                UnknownTxUpdateAction::Delete => None,
                UnknownTxUpdateAction::DoNothing => Some(like_tx_ids.inner().clone()),
            }
        })?;
        Ok(old.map(LikeTxIds::new))
    }

    /// Check if an accepting block hash has any unknown transactions
    pub fn has_unknown_for_block(
        &self,
        rtx: &ReadTransaction,
        accepting_block_hash: &RpcHash,
    ) -> Result<bool> {
        Ok(rtx.get(&self.0, accepting_block_hash.as_bytes())?.is_some())
    }

    /// Get all unknown entries as (accepting_block_hash, LikeTxIds) pairs
    /// This is more efficient than flattening to individual transactions
    pub fn get_all_unknown(
        &self,
        rtx: &ReadTransaction,
    ) -> impl DoubleEndedIterator<Item = Result<(RpcHash, LikeTxIds<UserValue>)>> + '_ {
        rtx.iter(&self.0).map(|item| {
            let (key, value) = item?;
            if key.len() == 32 {
                let accepting_block_hash = RpcHash::from_slice(&key);
                let like_tx_ids = LikeTxIds::new(value);
                Ok((accepting_block_hash, like_tx_ids))
            } else {
                Err(anyhow::anyhow!(
                    "Invalid key length in unknown_tx partition: expected 32, got {}",
                    key.len()
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
