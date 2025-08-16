use anyhow::Result;
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use zerocopy::little_endian::U64;
use zerocopy::{FromBytes, Immutable, IntoBytes, TryFromBytes, Unaligned};

#[derive(Clone)]
pub struct BlockCompactHeaderPartition(fjall::TxPartition);

/// Compact header data containing blue work and DAA score
#[derive(Clone, Copy, Debug, PartialEq, Eq, Immutable, FromBytes, IntoBytes, Unaligned)]
#[repr(C)]
pub struct CompactHeader {
    pub blue_work: [u8; 24], // BlueWorkType serialized as 24 bytes LE
    pub daa_score: U64,      // u64 DAA score serialized as 8 bytes LE
}

impl BlockCompactHeaderPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(
            keyspace.open_partition(
                "block_compact_header",
                PartitionCreateOptions::default()
                    .block_size(64 * 1024)
                    .compaction_strategy(fjall::compaction::Strategy::SizeTiered(
                        fjall::compaction::SizeTiered {
                            base_size: 8 * 1024 * 1024,
                            level_ratio: 6,
                        },
                    )),
            )?,
        ))
    }

    pub fn insert_compact_header(
        &self,
        block_hash: &[u8; 32],
        blue_work: [u8; 24],
        daa_score: impl Into<U64>,
    ) -> Result<AlreadyExist> {
        let daa_score: U64 = daa_score.into();
        Ok(self
            .0
            .fetch_update(block_hash, move |_| {
                Some(
                    CompactHeader {
                        blue_work,
                        daa_score,
                    }
                    .as_bytes()
                    .into(),
                )
            })?
            .is_some())
    }

    pub fn get_compact_header_rtx(
        &self,
        rtx: &ReadTransaction,
        block_hash: &[u8; 32],
    ) -> Result<Option<CompactHeader>> {
        rtx.get(&self.0, block_hash.as_bytes())?
            .map(|bytes| {
                CompactHeader::try_read_from_bytes(bytes.as_bytes())
                    .map_err(|_| anyhow::anyhow!("Invalid CompactHeader length"))
            })
            .transpose()
    }

    pub fn get_compact_header_wtx(
        &self,
        wtx: &mut WriteTransaction,
        block_hash: &[u8; 32],
    ) -> Result<Option<CompactHeader>> {
        wtx.get(&self.0, block_hash.as_bytes())?
            .map(|bytes| {
                CompactHeader::try_read_from_bytes(bytes.as_bytes())
                    .map_err(|_| anyhow::anyhow!("Invalid CompactHeader length"))
            })
            .transpose()
    }

    pub fn get_compact_header(&self, block_hash: &[u8; 32]) -> Result<Option<CompactHeader>> {
        self.0
            .get(block_hash.as_bytes())?
            .map(|bytes| {
                CompactHeader::try_read_from_bytes(bytes.as_bytes())
                    .map_err(|_| anyhow::anyhow!("Invalid CompactHeader length"))
            })
            .transpose()
    }

    pub fn len(&self) -> Result<usize> {
        Ok(self.0.inner().len()?)
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.0.inner().is_empty()?)
    }

    pub fn remove(&self, block_hash: &[u8; 32]) -> Result<()> {
        self.0.remove(block_hash.as_bytes())?;
        Ok(())
    }
}

type AlreadyExist = bool;
