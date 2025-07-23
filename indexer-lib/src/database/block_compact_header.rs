use anyhow::{Result, bail};
use bytemuck::{AnyBitPattern, NoUninit};
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use kaspa_consensus_core::BlueWorkType;
use kaspa_rpc_core::RpcHash;

/// FIFO partition for storing block hash to compact header data (blue work + DAA score)
/// Can store both Blue Work and DAA score for any block
/// Filled during block notification processing, queried during acceptance processing
///
/// Uses FIFO compaction strategy because:
/// - Block header data is temporary - older blocks become irrelevant
/// - Self-balancing: automatically removes old entries when size limit reached
#[derive(Clone)]
pub struct BlockCompactHeaderPartition(fjall::TxPartition);

/// Compact header data containing blue work and DAA score
#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
#[repr(C)]
pub struct CompactHeader {
    pub blue_work: [u8; 24], // BlueWorkType serialized as 24 bytes LE
    pub daa_score: [u8; 8],  // u64 DAA score serialized as 8 bytes LE
}

impl BlockCompactHeaderPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(
            keyspace.open_partition(
                "block_compact_header",
                PartitionCreateOptions::default()
                    .block_size(64 * 1024)
                    .compaction_strategy(fjall::compaction::Strategy::Fifo(
                        fjall::compaction::Fifo {
                            limit: 128 * 1024 * 1024,
                            ttl_seconds: None,
                        },
                    )),
            )?,
        ))
    }

    pub fn insert_compact_header(
        &self,
        block_hash: &RpcHash,
        blue_work: BlueWorkType,
        daa_score: u64,
    ) -> Result<()> {
        let header = CompactHeader {
            blue_work: blue_work.to_le_bytes(),
            daa_score: daa_score.to_le_bytes(),
        };
        self.0
            .insert(block_hash.as_bytes(), bytemuck::bytes_of(&header))?;
        Ok(())
    }

    pub fn get_compact_header_rtx(
        &self,
        rtx: &ReadTransaction,
        block_hash: &RpcHash,
    ) -> Result<Option<CompactHeader>> {
        if let Some(bytes) = rtx.get(&self.0, block_hash.as_bytes())? {
            if bytes.len() == size_of::<CompactHeader>() {
                let header: CompactHeader = *bytemuck::from_bytes(&bytes);
                Ok(Some(header))
            } else {
                bail!("Invalid CompactHeader length")
            }
        } else {
            Ok(None)
        }
    }

    pub fn get_compact_header_wtx(
        &self,
        wtx: &mut WriteTransaction,
        block_hash: RpcHash,
    ) -> Result<Option<CompactHeader>> {
        if let Some(bytes) = wtx.get(&self.0, block_hash.as_bytes())? {
            if bytes.len() == size_of::<CompactHeader>() {
                let header: CompactHeader = *bytemuck::from_bytes(&bytes);
                Ok(Some(header))
            } else {
                bail!("Invalid CompactHeader length")
            }
        } else {
            Ok(None)
        }
    }

    pub fn get_blue_work_rtx(
        &self,
        rtx: &ReadTransaction,
        block_hash: &RpcHash,
    ) -> Result<Option<BlueWorkType>> {
        if let Some(header) = self.get_compact_header_rtx(rtx, block_hash)? {
            Ok(Some(BlueWorkType::from_le_bytes(header.blue_work)))
        } else {
            Ok(None)
        }
    }

    pub fn get_daa_score_rtx(
        &self,
        rtx: &ReadTransaction,
        block_hash: &RpcHash,
    ) -> Result<Option<u64>> {
        if let Some(header) = self.get_compact_header_rtx(rtx, block_hash)? {
            Ok(Some(u64::from_le_bytes(header.daa_score)))
        } else {
            Ok(None)
        }
    }

    pub fn get_compact_header(&self, block_hash: RpcHash) -> Result<Option<CompactHeader>> {
        if let Some(bytes) = self.0.get(block_hash.as_bytes())? {
            if bytes.len() == size_of::<CompactHeader>() {
                let header: CompactHeader = *bytemuck::from_bytes(&bytes);
                Ok(Some(header))
            } else {
                bail!("Invalid CompactHeader length")
            }
        } else {
            Ok(None)
        }
    }

    pub fn get_blue_work(&self, block_hash: RpcHash) -> Result<Option<BlueWorkType>> {
        if let Some(header) = self.get_compact_header(block_hash)? {
            Ok(Some(BlueWorkType::from_le_bytes(header.blue_work)))
        } else {
            Ok(None)
        }
    }

    pub fn get_daa_score(&self, block_hash: RpcHash) -> Result<Option<u64>> {
        if let Some(header) = self.get_compact_header(block_hash)? {
            Ok(Some(u64::from_le_bytes(header.daa_score)))
        } else {
            Ok(None)
        }
    }

    pub fn get_daa_score_wtx(&self, block_hash: RpcHash) -> Result<Option<u64>> {
        if let Some(header) = self.get_compact_header(block_hash)? {
            Ok(Some(u64::from_le_bytes(header.daa_score)))
        } else {
            Ok(None)
        }
    }

    pub fn len(&self) -> Result<usize> {
        Ok(self.0.inner().len()?)
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.0.inner().is_empty()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_hash_operations() {
        let block_hash = RpcHash::from_slice(&[1u8; 32]);
        assert_eq!(block_hash.as_bytes().len(), 32);
    }
}
