use anyhow::Result;
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use zerocopy::big_endian::U64 as U64_BE;
use zerocopy::little_endian::U64 as U64_LE;
use zerocopy::{FromBytes, Immutable, IntoBytes, TryFromBytes, Unaligned};

#[derive(Debug, Copy, Clone)]
pub struct AcceptanceGap {
    pub from_daa: u64,
    pub to_daa: u64,
    pub from_block_hash: [u8; 32],
    pub to_block_hash: [u8; 32],
}

#[derive(Clone, Copy, Debug, Immutable, FromBytes, IntoBytes, Unaligned)]
#[repr(C)]
struct AcceptanceGapKey {
    pub to_daa: U64_BE,
    pub to_block_hash: [u8; 32],
}

#[derive(Clone, Copy, Debug, Immutable, FromBytes, IntoBytes, Unaligned)]
#[repr(C)]
struct AcceptanceGapValue {
    pub from_daa: U64_LE,
    pub from_block_hash: [u8; 32],
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

    fn make_key(to_daa: u64, to_block_hash: [u8; 32]) -> AcceptanceGapKey {
        AcceptanceGapKey {
            to_daa: to_daa.into(),
            to_block_hash,
        }
    }

    fn make_value(from_daa: u64, from_block_hash: [u8; 32]) -> AcceptanceGapValue {
        AcceptanceGapValue {
            from_daa: from_daa.into(),
            from_block_hash,
        }
    }

    pub fn add_gap_wtx(&self, wtx: &mut WriteTransaction, gap: AcceptanceGap) {
        let key = Self::make_key(gap.to_daa, gap.to_block_hash);
        let value = Self::make_value(gap.from_daa, gap.from_block_hash);
        wtx.insert(&self.0, key.as_bytes(), value.as_bytes());
    }

    pub fn add_gap(&self, gap: AcceptanceGap) -> Result<()> {
        let key = Self::make_key(gap.to_daa, gap.to_block_hash);
        let value = Self::make_value(gap.from_daa, gap.from_block_hash);
        self.0.insert(key.as_bytes(), value.as_bytes())?;
        Ok(())
    }

    pub fn remove_gap_wtx(
        &self,
        wtx: &mut WriteTransaction,
        to_daa: u64,
        to_block_hash: &[u8; 32],
    ) {
        let key = Self::make_key(to_daa, *to_block_hash);
        wtx.remove(&self.0, key.as_bytes());
    }

    pub fn get_all_gaps_rtx(
        &self,
        rtx: &ReadTransaction,
    ) -> impl DoubleEndedIterator<Item = Result<AcceptanceGap>> + '_ {
        rtx.iter(&self.0).map(|item| {
            let (key, value) = item?;
            let key = AcceptanceGapKey::try_read_from_bytes(key.as_bytes())
                .map_err(|_| anyhow::anyhow!("Invalid key length in acceptance_gaps partition"))?;
            let value =
                AcceptanceGapValue::try_read_from_bytes(value.as_bytes()).map_err(|_| {
                    anyhow::anyhow!("Invalid value length in acceptance_gaps partition")
                })?;
            Ok(AcceptanceGap {
                from_daa: value.from_daa.get(),
                to_daa: key.to_daa.get(),
                from_block_hash: value.from_block_hash,
                to_block_hash: key.to_block_hash,
            })
        })
    }

    pub fn get_all_gaps(&self) -> impl DoubleEndedIterator<Item = Result<AcceptanceGap>> + '_ {
        self.0.inner().iter().map(|item| {
            let (key, value) = item?;
            let key = AcceptanceGapKey::try_read_from_bytes(key.as_bytes())
                .map_err(|_| anyhow::anyhow!("Invalid key length in acceptance_gaps partition"))?;
            let value =
                AcceptanceGapValue::try_read_from_bytes(value.as_bytes()).map_err(|_| {
                    anyhow::anyhow!("Invalid value length in acceptance_gaps partition")
                })?;
            Ok(AcceptanceGap {
                from_daa: value.from_daa.get(),
                to_daa: key.to_daa.get(),
                from_block_hash: value.from_block_hash,
                to_block_hash: key.to_block_hash,
            })
        })
    }
}
