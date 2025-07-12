use crate::historical_syncer::Cursor;
use anyhow::Result;
use bytemuck::{AnyBitPattern, NoUninit};
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use std::cmp::Ordering;

/// Metadata partition for storing latest known cursors
/// Key: enum of metadata types
/// Value: cursor data (blue work + block hash)
#[derive(Clone)]
pub struct MetadataPartition(fjall::TxPartition);

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MetadataKey {
    LatestBlockCursor = 0,
    LatestAcceptingBlockCursor = 1,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq, Ord, PartialOrd)]
pub struct CursorValue {
    pub blue_work: [u8; 24], // Uint192 serialized as 24 bytes
    pub block_hash: [u8; 32],
}

impl MetadataPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(keyspace.open_partition(
            "metadata",
            PartitionCreateOptions::default(),
        )?))
    }

    /// Store latest block cursor
    pub fn set_latest_block_cursor(
        &self,
        wtx: &mut WriteTransaction,
        cursor: Cursor,
    ) -> Result<()> {
        let key = [MetadataKey::LatestBlockCursor as u8];
        let value = CursorValue {
            blue_work: cursor.blue_work.to_be_bytes(),
            block_hash: *cursor.hash.as_ref(),
        };
        wtx.fetch_update(&self.0, key, |old_value| match old_value {
            None => Some(bytemuck::bytes_of(&value).into()),
            Some(old_value) => {
                let old = bytemuck::from_bytes::<CursorValue>(old_value.as_ref());
                match value.cmp(old) {
                    Ordering::Greater => Some(bytemuck::bytes_of(&value).into()),
                    _ => Some(old_value.clone()),
                }
            }
        })?;
        Ok(())
    }

    /// Get latest block cursor
    pub fn get_latest_block_cursor(&self, rtx: &ReadTransaction) -> Result<Option<Cursor>> {
        let key = [MetadataKey::LatestBlockCursor as u8];
        if let Some(bytes) = rtx.get(&self.0, key)? {
            if bytes.len() == 56 {
                // 24 + 32
                let value: CursorValue = *bytemuck::from_bytes(&bytes);
                Ok(Some(Cursor {
                    blue_work: kaspa_math::Uint192::from_be_bytes(value.blue_work),
                    hash: kaspa_rpc_core::RpcHash::from_slice(&value.block_hash),
                }))
            } else {
                Ok(None)
            }
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
        let value = CursorValue {
            blue_work: cursor.blue_work.to_be_bytes(),
            block_hash: *cursor.hash.as_ref(),
        };
        wtx.insert(&self.0, key, bytemuck::bytes_of(&value));
        Ok(())
    }

    /// Get latest accepting block cursor
    pub fn get_latest_accepting_block_cursor(
        &self,
        rtx: &ReadTransaction,
    ) -> Result<Option<Cursor>> {
        let key = [MetadataKey::LatestAcceptingBlockCursor as u8];
        if let Some(bytes) = rtx.get(&self.0, key)? {
            if bytes.len() == 56 {
                // 24 + 32
                let value: CursorValue = *bytemuck::from_bytes(&bytes);
                Ok(Some(Cursor {
                    blue_work: kaspa_math::Uint192::from_be_bytes(value.blue_work),
                    hash: kaspa_rpc_core::RpcHash::from_slice(&value.block_hash),
                }))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Get latest accepting block cursor (write transaction version)
    pub fn get_latest_accepting_block_cursor_wtx(
        &self,
        wtx: &mut WriteTransaction,
    ) -> Result<Option<Cursor>> {
        let key = [MetadataKey::LatestAcceptingBlockCursor as u8];
        if let Some(bytes) = wtx.get(&self.0, key)? {
            if bytes.len() == 56 {
                // 24 + 32
                let value: CursorValue = *bytemuck::from_bytes(&bytes);
                Ok(Some(Cursor {
                    blue_work: kaspa_math::Uint192::from_be_bytes(value.blue_work),
                    hash: kaspa_rpc_core::RpcHash::from_slice(&value.block_hash),
                }))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Get latest block cursor (write transaction version)
    pub fn get_latest_block_cursor_wtx(
        &self,
        wtx: &mut WriteTransaction,
    ) -> Result<Option<Cursor>> {
        let key = [MetadataKey::LatestBlockCursor as u8];
        if let Some(bytes) = wtx.get(&self.0, key)? {
            if bytes.len() == 56 {
                // 24 + 32
                let value: CursorValue = *bytemuck::from_bytes(&bytes);
                Ok(Some(Cursor {
                    blue_work: kaspa_math::Uint192::from_be_bytes(value.blue_work),
                    hash: kaspa_rpc_core::RpcHash::from_slice(&value.block_hash),
                }))
            } else {
                Ok(None)
            }
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_key_serialization() {
        let key = MetadataKey::LatestBlockCursor;
        assert_eq!(key as u8, 0);

        let key = MetadataKey::LatestAcceptingBlockCursor;
        assert_eq!(key as u8, 1);
    }

    #[test]
    fn test_cursor_value_serialization() {
        let value = CursorValue {
            blue_work: [1u8; 24],
            block_hash: [2u8; 32],
        };

        let bytes = bytemuck::bytes_of(&value);
        assert_eq!(bytes.len(), 56); // 24 + 32

        let deserialized: CursorValue = *bytemuck::from_bytes(bytes);
        assert_eq!(deserialized, value);
    }

    #[test]
    fn test_cursor_conversion() {
        let cursor = Cursor {
            blue_work: kaspa_math::Uint192::from_be_bytes([1u8; 24]),
            hash: kaspa_rpc_core::RpcHash::from_slice(&[2u8; 32]),
        };

        let value = CursorValue {
            blue_work: cursor.blue_work.to_be_bytes(),
            block_hash: *cursor.hash.as_ref(),
        };

        let converted_back = Cursor {
            blue_work: kaspa_math::Uint192::from_be_bytes(value.blue_work),
            hash: kaspa_rpc_core::RpcHash::from_slice(&value.block_hash),
        };

        assert_eq!(cursor, converted_back);
    }

    #[test]
    fn test_metadata_keys_are_different() {
        assert_ne!(
            MetadataKey::LatestBlockCursor as u8,
            MetadataKey::LatestAcceptingBlockCursor as u8
        );
    }
}
