use anyhow::Result;
use fjall::{CompressionType, PartitionCreateOptions, ReadTransaction, WriteTransaction};
use zerocopy::little_endian::U64;
use zerocopy::{FromBytes, Immutable, IntoBytes, LittleEndian, TryFromBytes, Unaligned};

/// Metadata partition for storing latest known cursors
/// Key: enum of metadata types
/// Value: cursor data (blue work + block hash + daa_score)
#[derive(Clone)]
pub struct MetadataPartition(pub fjall::TxPartition);

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MetadataKey {
    LatestBlockCursor = 0,
    LatestAcceptingBlockCursor = 1,
    DBVersion = 2,
}

#[repr(C)]
#[derive(
    Clone,
    Copy,
    Default,
    Debug,
    PartialEq,
    Eq,
    Ord,
    PartialOrd,
    IntoBytes,
    FromBytes,
    Immutable,
    Unaligned,
)]
pub struct Cursor {
    pub blue_work: [u8; 24], // Uint192 serialized as 24 BE bytes
    pub block_hash: [u8; 32],
    pub daa_score: U64,
}

impl MetadataPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(
            keyspace.open_partition(
                "metadata",
                PartitionCreateOptions::default()
                    .block_size(1024)
                    .compression(CompressionType::None),
            )?,
        ))
    }

    /// Store latest block cursor
    pub fn set_latest_block_cursor(&self, wtx: &mut WriteTransaction, hash: [u8; 32]) {
        let key = [MetadataKey::LatestBlockCursor as u8];
        wtx.insert(&self.0, key, hash);
    }

    /// Get latest block cursor
    pub fn get_latest_block_cursor_rtx(&self, rtx: &ReadTransaction) -> Result<Option<[u8; 32]>> {
        let key = [MetadataKey::LatestBlockCursor as u8];
        if let Some(bytes) = rtx.get(&self.0, key)? {
            Ok(Some(<[u8; 32]>::try_from(bytes.as_bytes())?))
        } else {
            Ok(None)
        }
    }

    /// Get latest block cursor
    pub fn get_latest_block_cursor(&self) -> Result<Option<[u8; 32]>> {
        let key = [MetadataKey::LatestBlockCursor as u8];
        if let Some(bytes) = self.0.get(key)? {
            Ok(Some(<[u8; 32]>::try_from(bytes.as_bytes())?))
        } else {
            Ok(None)
        }
    }

    /// Store latest accepting block cursor
    pub fn set_latest_accepting_block_cursor(
        &self,
        wtx: &mut WriteTransaction,
        cursor: Cursor,
    ) -> Result<()> {
        let key = [MetadataKey::LatestAcceptingBlockCursor as u8];
        wtx.fetch_update(&self.0, key, |old_value| match old_value {
            None => Some(cursor.as_bytes().into()),
            Some(old_value_bytes) => {
                let old_value = Cursor::try_read_from_bytes(old_value_bytes.as_bytes())
                    .expect("db corrupted, failed to read cursor");
                if cursor.blue_work > old_value.blue_work {
                    Some(cursor.as_bytes().into())
                } else {
                    Some(old_value_bytes.clone())
                }
            }
        })?;
        Ok(())
    }

    /// Get latest accepting block cursor
    pub fn get_latest_accepting_block_cursor_rtx(
        &self,
        rtx: &ReadTransaction,
    ) -> Result<Option<Cursor>> {
        let key = [MetadataKey::LatestAcceptingBlockCursor as u8];
        rtx.get(&self.0, key)?
            .map(|bytes| {
                Cursor::try_read_from_bytes(bytes.as_bytes())
                    .map_err(|_| anyhow::anyhow!("db corrupted, failed to read cursor"))
            })
            .transpose()
    }

    pub fn get_latest_accepting_block_cursor(&self) -> Result<Option<Cursor>> {
        let key = [MetadataKey::LatestAcceptingBlockCursor as u8];
        self.0
            .get(key)?
            .map(|bytes| {
                Cursor::try_read_from_bytes(bytes.as_bytes())
                    .map_err(|_| anyhow::anyhow!("db corrupted, failed to read cursor"))
            })
            .transpose()
    }

    /// Get latest accepting block cursor (write transaction version)
    pub fn get_latest_accepting_block_cursor_wtx(
        &self,
        wtx: &mut WriteTransaction,
    ) -> Result<Option<Cursor>> {
        let key = [MetadataKey::LatestAcceptingBlockCursor as u8];
        wtx.get(&self.0, key)?
            .map(|bytes| {
                Cursor::try_read_from_bytes(bytes.as_bytes())
                    .map_err(|_| anyhow::anyhow!("db corrupted, failed to read cursor"))
            })
            .transpose()
    }

    /// Get latest block cursor (write transaction version)
    pub fn get_latest_block_cursor_wtx(
        &self,
        wtx: &mut WriteTransaction,
    ) -> Result<Option<[u8; 32]>> {
        let key = [MetadataKey::LatestBlockCursor as u8];
        if let Some(bytes) = wtx.get(&self.0, key)? {
            Ok(Some(<[u8; 32]>::try_from(bytes.as_bytes())?))
        } else {
            Ok(None)
        }
    }

    /// Remove latest block cursor
    pub fn remove_latest_block_cursor(&self, wtx: &mut WriteTransaction) -> Result<()> {
        let key = [MetadataKey::LatestBlockCursor as u8];
        wtx.remove(&self.0, key);
        Ok(())
    }

    /// Remove latest accepting block cursor
    pub fn remove_latest_accepting_block_cursor(&self, wtx: &mut WriteTransaction) -> Result<()> {
        let key = [MetadataKey::LatestAcceptingBlockCursor as u8];
        wtx.remove(&self.0, key);
        Ok(())
    }

    pub fn db_version(&self) -> Result<u32> {
        let key = [MetadataKey::DBVersion as u8];
        let Some(bytes) = self.0.get(key)? else {
            return Ok(0);
        };
        let version = zerocopy::U32::<LittleEndian>::try_read_from_bytes(bytes.as_bytes())
            .map_err(|_| anyhow::anyhow!("db corrupted, failed to read db version"))?;
        Ok(version.get())
    }

    pub fn set_db_version_wtx(
        &self,
        wtx: &mut WriteTransaction,
        version: impl Into<zerocopy::U32<LittleEndian>>,
    ) {
        let key = [MetadataKey::DBVersion as u8];
        let version = version.into();
        wtx.insert(&self.0, key, version.as_bytes());
    }

    pub fn set_db_version(&self, version: impl Into<zerocopy::U32<LittleEndian>>) -> Result<()> {
        let key = [MetadataKey::DBVersion as u8];
        let version = version.into();
        self.0.insert(key, version.as_bytes())?;
        Ok(())
    }
}
