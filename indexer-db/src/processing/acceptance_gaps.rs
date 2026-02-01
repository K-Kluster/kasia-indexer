use anyhow::Result;
use arrayref::array_ref;
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};

#[derive(Debug, Copy, Clone)]
pub struct AcceptanceGap {
    pub from_daa: u64,
    pub to_daa: u64,
    pub from_block_hash: [u8; 32],
    pub to_block_hash: [u8; 32],
}

#[derive(Clone)]
pub struct AcceptanceGapsPartition(fjall::TxPartition);

impl AcceptanceGapsPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(keyspace.open_partition(
            "acceptance_gaps",
            PartitionCreateOptions::default().block_size(64 * 1024),
        )?))
    }

    fn key_bytes(to_daa: u64, to_block_hash: [u8; 32]) -> [u8; 40] {
        let mut key = [0u8; 40];
        key[..8].copy_from_slice(&to_daa.to_be_bytes());
        key[8..].copy_from_slice(&to_block_hash);
        key
    }

    fn value_bytes(from_daa: u64, from_block_hash: [u8; 32]) -> [u8; 40] {
        let mut value = [0u8; 40];
        value[..8].copy_from_slice(&from_daa.to_be_bytes());
        value[8..].copy_from_slice(&from_block_hash);
        value
    }

    pub fn add_gap_wtx(&self, wtx: &mut WriteTransaction, gap: AcceptanceGap) {
        let key = Self::key_bytes(gap.to_daa, gap.to_block_hash);
        let value = Self::value_bytes(gap.from_daa, gap.from_block_hash);
        wtx.insert(&self.0, key, value);
    }

    pub fn add_gap(&self, gap: AcceptanceGap) -> Result<()> {
        let key = Self::key_bytes(gap.to_daa, gap.to_block_hash);
        let value = Self::value_bytes(gap.from_daa, gap.from_block_hash);
        self.0.insert(key, value)?;
        Ok(())
    }

    pub fn remove_gap_wtx(
        &self,
        wtx: &mut WriteTransaction,
        to_daa: u64,
        to_block_hash: &[u8; 32],
    ) {
        let key = Self::key_bytes(to_daa, *to_block_hash);
        wtx.remove(&self.0, key);
    }

    pub fn get_all_gaps_rtx(
        &self,
        rtx: &ReadTransaction,
    ) -> impl DoubleEndedIterator<Item = Result<AcceptanceGap>> + '_ {
        rtx.iter(&self.0).map(|item| {
            let (key, value) = item?;
            if key.len() == 40 && value.len() == 40 {
                let to_daa = u64::from_be_bytes(*array_ref![key, 0, 8]);
                let to_block_hash = *array_ref![key, 8, 32];
                let from_daa = u64::from_be_bytes(*array_ref![value, 0, 8]);
                let from_block_hash = *array_ref![value, 8, 32];
                Ok(AcceptanceGap {
                    from_daa,
                    to_daa,
                    from_block_hash,
                    to_block_hash,
                })
            } else {
                Err(anyhow::anyhow!(
                    "Invalid key/value lengths in acceptance_gaps partition"
                ))
            }
        })
    }

    pub fn get_all_gaps(&self) -> impl DoubleEndedIterator<Item = Result<AcceptanceGap>> + '_ {
        self.0.inner().iter().map(|item| {
            let (key, value) = item?;
            if key.len() == 40 && value.len() == 40 {
                let to_daa = u64::from_be_bytes(*array_ref![key, 0, 8]);
                let to_block_hash = *array_ref![key, 8, 32];
                let from_daa = u64::from_be_bytes(*array_ref![value, 0, 8]);
                let from_block_hash = *array_ref![value, 8, 32];
                Ok(AcceptanceGap {
                    from_daa,
                    to_daa,
                    from_block_hash,
                    to_block_hash,
                })
            } else {
                Err(anyhow::anyhow!(
                    "Invalid key/value lengths in acceptance_gaps partition"
                ))
            }
        })
    }
}
