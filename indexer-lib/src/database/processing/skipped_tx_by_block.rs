use crate::database::TxIdsSliceView;
use anyhow::Result;
use fjall::{
    KvSeparationOptions, PartitionCreateOptions, ReadTransaction, UserKey, WriteTransaction,
};

/// Partition for tracking skipped transactions organized by block DAA score.
///
/// **Key structure:** [daa_score (8 bytes BE)] + [block_hash (32 bytes)] = 40 bytes total
/// **Value:** Flattened array of skipped transaction IDs (32 bytes each)
///
/// **Pruning strategy:** Uses DAA score prefix for efficient range queries.
/// Since Kaspa has 10 blocks/sec × 300 txs/block = 3000 txs/sec, and we prune
/// at 2x finality depth (864K blocks ≈ 24 hours), this prevents unbounded growth
/// while maintaining all data needed for historical/real-time sync coordination.
///
/// **Methods:**
/// - `add_skip_for_block()`: Store skipped tx IDs for a block
/// - `get_skips_for_block()`: Retrieve skipped tx IDs by block
/// - `get_entries_to_prune()`: Range query for entries below DAA threshold
/// - `remove_by_raw_key()`: Remove entry by raw key (for pruning)
/// - `remove_block()`: Remove entry by DAA score + block hash
#[derive(Clone)]
pub struct SkipTxByBlockPartition(fjall::TxPartition);

impl SkipTxByBlockPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> fjall::Result<Self> {
        Ok(Self(
            keyspace.open_partition(
                "skip_tx_by_block",
                PartitionCreateOptions::default()
                    .max_memtable_size(32 * 1024 * 1024)
                    .block_size(64 * 1024)
                    .with_kv_separation(KvSeparationOptions::default())
                    .compaction_strategy(fjall::compaction::Strategy::SizeTiered(
                        fjall::compaction::SizeTiered {
                            base_size: 8 * 1024 * 1024,
                            level_ratio: 6,
                        },
                    )),
            )?,
        ))
    }

    /// Add skipped transaction IDs for a specific block.
    pub fn add_skip_for_block(
        &self,
        wtx: &mut WriteTransaction,
        daa_score: u64,
        block_hash: [u8; 32],
        tx_ids: &[[u8; 32]],
    ) {
        if tx_ids.is_empty() {
            return;
        }

        let mut key = [0u8; 40];
        key[0..8].copy_from_slice(&daa_score.to_be_bytes());
        key[8..40].copy_from_slice(&block_hash);

        wtx.insert(&self.0, key, tx_ids.as_flattened());
    }

    /// Get skipped transaction IDs for a specific block.
    pub fn get_skips_for_block(
        &self,
        rtx: &ReadTransaction,
        daa_score: u64,
        block_hash: &[u8; 32],
    ) -> Result<Option<TxIdsSliceView>> {
        let mut key = [0u8; 40];
        key[0..8].copy_from_slice(&daa_score.to_be_bytes());
        key[8..40].copy_from_slice(block_hash);

        Ok(rtx.get(&self.0, key)?.map(TxIdsSliceView::new))
    }

    /// Get all entries to prune (DAA score below threshold) using read transaction.
    /// Returns iterator of (key, tx_ids) pairs that should be pruned.
    pub fn get_entries_to_prune(
        &self,
        rtx: &ReadTransaction,
        prune_before_daa: [u8; 8],
    ) -> impl Iterator<Item = anyhow::Result<(UserKey, TxIdsSliceView)>> {
        rtx.range(&self.0, ..prune_before_daa).map(|result| {
            result
                .map_err(Into::into)
                .map(|(key, value)| (key, TxIdsSliceView::new(value)))
        })
    }
    /// Remove a specific block entry.
    pub fn remove_block(&self, wtx: &mut WriteTransaction, daa_score: u64, block_hash: [u8; 32]) {
        let mut key = [0u8; 40];
        key[0..8].copy_from_slice(&daa_score.to_be_bytes());
        key[8..40].copy_from_slice(&block_hash);
        wtx.remove(&self.0, key);
    }

    pub fn remove_by_raw_key(&self, wtx: &mut WriteTransaction, raw_key: UserKey) {
        wtx.remove(&self.0, raw_key);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_key_structure() {
        let daa_score = 123456u64;
        let block_hash = [0x42u8; 32];

        let mut expected_key = [0u8; 40];
        expected_key[0..8].copy_from_slice(&daa_score.to_be_bytes());
        expected_key[8..40].copy_from_slice(&block_hash);

        // Verify key format for DAA-based sorting
        assert_eq!(expected_key.len(), 40);
        assert_eq!(&expected_key[0..8], &daa_score.to_be_bytes());
        assert_eq!(&expected_key[8..40], &block_hash);
    }

    #[test]
    fn test_daa_score_ordering() {
        let low_daa = 100u64.to_be_bytes();
        let high_daa = 200u64.to_be_bytes();

        // Verify big-endian ordering works for range queries
        assert!(low_daa < high_daa);
    }
}
