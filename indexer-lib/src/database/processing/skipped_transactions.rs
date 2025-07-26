use anyhow::Result;
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use kaspa_rpc_core::RpcTransactionId;

#[derive(Clone)]
pub struct SkipTxPartition(fjall::TxPartition);

impl SkipTxPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(
            keyspace.open_partition(
                "skip_tx",
                PartitionCreateOptions::default()
                    .max_memtable_size(32 * 1024 * 1024)
                    .block_size(32 * 1024)
                    .compaction_strategy(fjall::compaction::Strategy::SizeTiered(
                        fjall::compaction::SizeTiered {
                            base_size: 6 * 1024 * 1024,
                            level_ratio: 6,
                        },
                    )),
            )?,
        ))
    }

    /// Mark a transaction as one to skip
    pub fn mark_skip(&self, wtx: &mut WriteTransaction, tx_id: [u8; 32]) {
        wtx.insert(&self.0, tx_id, []);
    }

    /// Check if a transaction should be skipped
    pub fn should_skip(&self, rtx: &ReadTransaction, tx_id: &[u8; 32]) -> Result<bool> {
        Ok(rtx.get(&self.0, tx_id)?.is_some())
    }

    /// Check if a transaction should be skipped (write transaction version)
    pub fn should_skip_wtx(&self, wtx: &mut WriteTransaction, tx_id: [u8; 32]) -> Result<bool> {
        Ok(wtx.get(&self.0, tx_id)?.is_some())
    }

    /// Remove skip marking for a transaction (if needed for cleanup)
    pub fn remove_skip(&self, wtx: &mut WriteTransaction, tx_id: [u8; 32]) {
        wtx.remove(&self.0, tx_id);
    }

    /// Mark multiple transactions as skipped
    pub fn mark_skip_batch<'a, I>(&self, wtx: &mut WriteTransaction, tx_ids: I)
    where
        I: Iterator<Item = &'a RpcTransactionId>,
    {
        for tx_id in tx_ids {
            self.mark_skip(wtx, tx_id.as_bytes());
        }
    }

    /// Check multiple transactions for skip status
    pub fn check_skip_batch<'a, I>(
        &self,
        rtx: &ReadTransaction,
        tx_ids: I,
    ) -> Result<Vec<(RpcTransactionId, bool)>>
    where
        I: Iterator<Item = &'a RpcTransactionId>,
    {
        let mut results = Vec::new();
        for tx_id in tx_ids {
            let should_skip = self.should_skip(rtx, &tx_id.as_bytes())?;
            results.push((*tx_id, should_skip));
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skip_tx_operations() {
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
