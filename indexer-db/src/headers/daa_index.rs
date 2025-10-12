use crate::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};
use anyhow::{Result, bail};
use fjall::{PartitionCreateOptions, ReadTransaction};
use zerocopy::TryFromBytes;

/// Secondary index partition for compact headers, indexed by DAA score, blue work, and block hash
/// Used for efficient pruning of old blocks with DAA score < threshold
/// Keys: [daa_score_be_bytes (8)] + [blue_work_be_bytes (24)] + [block_hash_bytes (32)]
/// Values: empty (data stored in primary partition)
#[derive(Clone)]
pub struct DaaIndexPartition(pub(crate) fjall::TxPartition);

#[derive(Copy, Clone, Unaligned, Immutable, KnownLayout, IntoBytes, FromBytes)]
#[repr(C)]
pub struct DaaIndexKey {
    pub daa_score: zerocopy::U64<zerocopy::BigEndian>,
    pub blue_work: [u8; 24], // BE
    pub block_hash: [u8; 32],
}

impl DaaIndexPartition {
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

    fn make_key(daa_score: u64, block_hash: [u8; 32], blue_work_be: [u8; 24]) -> DaaIndexKey {
        DaaIndexKey {
            daa_score: daa_score.into(),
            blue_work: blue_work_be,
            block_hash,
        }
    }

    pub fn insert(
        &self,
        daa_score: u64,
        block_hash: [u8; 32],
        blue_work_be: [u8; 24],
    ) -> Result<()> {
        let key = Self::make_key(daa_score, block_hash, blue_work_be);
        self.0.insert(key.as_bytes(), [])?;
        Ok(())
    }

    pub fn delete(
        &self,
        daa_score: u64,
        block_hash: [u8; 32],
        blue_work_be: [u8; 24],
    ) -> Result<()> {
        let key = Self::make_key(daa_score, block_hash, blue_work_be);
        self.0.remove(key.as_bytes())?;
        Ok(())
    }

    /// Returns an iterator over (daa_score, blue_work, block_hash) where daa_score < max_daa.
    /// Keys are ordered first by DAA score, then by blue work (both in big-endian), then by block hash.
    pub fn iter_lt<'a>(
        &'a self,
        rtx: &'a ReadTransaction,
        max_daa: u64,
    ) -> impl DoubleEndedIterator<Item = Result<DaaIndexKey>> + 'a {
        let max_prefix = max_daa.to_be_bytes();
        rtx.range(&self.0, ..max_prefix).map(move |res| {
            let (key, value) = res?;
            if !value.is_empty() {
                bail!("Unexpected non-empty value");
            }
            DaaIndexKey::try_read_from_bytes(key.as_bytes())
                .map_err(|_| anyhow::anyhow!("db corrupted, failed to read daa index key"))
        })
    }

    pub fn len(&self) -> Result<usize> {
        Ok(self.0.inner().len()?)
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.0.inner().is_empty()?)
    }
}
