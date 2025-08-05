use anyhow::{Result, bail};
use fjall::{PartitionCreateOptions, ReadTransaction};

/// Secondary index partition for compact headers, indexed by DAA score (u64 BE bytes) + block hash
/// Used for efficient pruning of old blocks with DAA score < threshold
/// Keys: [daa_score_be_bytes (8)] + [block_hash_bytes (32)]
/// Values: empty (data stored in primary partition)
#[derive(Clone)]
pub struct DaaIndexPartition(fjall::TxPartition);

impl DaaIndexPartition {
    const KEY_LEN: usize = 8 + 32;

    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(
            keyspace.open_partition(
                "daa_index_compact_header",
                PartitionCreateOptions::default()
                    .block_size(64 * 1024)
                    .compaction_strategy(fjall::compaction::Strategy::SizeTiered(
                        fjall::compaction::SizeTiered {
                            base_size: 4 * 1024 * 1024,
                            level_ratio: 6,
                        },
                    )),
            )?,
        ))
    }

    fn make_key(daa_score: u64, block_hash: &[u8; 32]) -> [u8; Self::KEY_LEN] {
        let mut key = [0u8; Self::KEY_LEN];
        key[0..8].copy_from_slice(&daa_score.to_be_bytes());
        key[8..].copy_from_slice(block_hash.as_ref());
        key
    }

    pub fn insert(&self, daa_score: u64, block_hash: &[u8; 32]) -> Result<()> {
        let key = Self::make_key(daa_score, block_hash);
        self.0.insert(key, [])?;
        Ok(())
    }

    pub fn delete(&self, daa_score: u64, block_hash: &[u8; 32]) -> Result<()> {
        let key = Self::make_key(daa_score, block_hash);
        self.0.remove(key)?;
        Ok(())
    }

    /// Returns an iterator over (daa_score, block_hash) where daa_score < max_daa.
    /// Assumes the underlying store iterates in sorted key order.
    pub fn iter_lt<'a>(
        &'a self,
        rtx: &'a ReadTransaction,
        max_daa: u64,
    ) -> impl Iterator<Item = Result<(u64, [u8; 32])>> + 'a {
        let max_prefix = max_daa.to_be_bytes();
        rtx.range(&self.0, ..max_prefix).map(move |res| {
            let (key, value) = res?;
            if key.len() != Self::KEY_LEN {
                bail!("Invalid key length: {}", key.len());
            }
            if !value.is_empty() {
                bail!("Unexpected non-empty value");
            }
            let daa_score = u64::from_be_bytes(key[0..8].try_into().unwrap());
            let block_hash: [u8; 32] = key[8..].try_into().unwrap();
            Ok((daa_score, block_hash))
        })
    }

    pub fn len(&self) -> Result<usize> {
        Ok(self.0.inner().len()?)
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.0.inner().is_empty()?)
    }

    pub fn latest_daa(&self) -> Result<Option<u64>> {
        let k = self.0.last_key_value()?;
        k.map(|(k, _v)| -> anyhow::Result<_> { Ok(u64::from_be_bytes(k.as_ref().try_into()?)) })
            .transpose()
    }
}
