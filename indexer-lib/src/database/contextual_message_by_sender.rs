use std::fmt::{Debug, Formatter};
use anyhow::{Result, bail};
use bytemuck::{AnyBitPattern, NoUninit};
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction, UserKey};
use crate::database::handshake::AddressPayload;
use std::marker::PhantomData;
use std::ops::Deref;

/// Partition for storing contextual messages by sender and alias
/// Key: sender + alias + block_time + block_hash + version + tx_id
/// Value: sealed_hex data
/// Note: sender can be zeros (when not resolved yet)
/// Designed for REST API prefix search by sender, alias, time
#[derive(Clone)]
pub struct ContextualMessageBySenderPartition(fjall::TxPartition);

#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
#[repr(C)]
pub struct ContextualMessageBySenderKey {
    pub sender: AddressPayload,      // zeros when not resolved yet
    pub alias: [u8; 16],            // alias for prefix search, zero-padded
    pub block_time: [u8; 8],        // u64 BE for chronological ordering
    pub block_hash: [u8; 32],       // block hash for uniqueness
    pub version: u8,                // message version
    pub tx_id: [u8; 32],            // transaction id
}

#[repr(transparent)]
#[derive(Clone, PartialEq, Eq)]
pub struct LikeContextualMessageBySenderKey<T: AsRef<[u8]>> {
    bts: T,
    phantom_data: PhantomData<ContextualMessageBySenderKey>,
}

impl<T: AsRef<[u8]>> Debug for LikeContextualMessageBySenderKey<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

impl<T: AsRef<[u8]>> LikeContextualMessageBySenderKey<T> {
    pub fn new(bts: T) -> Self {
        Self {
            bts,
            phantom_data: PhantomData,
        }
    }
}

impl<T: AsRef<[u8]>> Deref for LikeContextualMessageBySenderKey<T> {
    type Target = ContextualMessageBySenderKey;

    fn deref(&self) -> &Self::Target {
        bytemuck::from_bytes(self.bts.as_ref())
    }
}

impl ContextualMessageBySenderPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(keyspace.open_partition(
            "contextual_message_by_sender",
            PartitionCreateOptions::default(),
        )?))
    }

    /// Insert a contextual message
    pub fn insert(
        &self,
        wtx: &mut WriteTransaction,
        sender: AddressPayload,
        alias: &[u8],
        block_time: u64,
        block_hash: [u8; 32],
        version: u8,
        tx_id: [u8; 32],
        sealed_hex: &[u8],
    ) -> Result<()> {
        if alias.len() > 16 {
            bail!("Alias length cannot exceed 16 bytes, got {}", alias.len());
        }

        let mut alias_bytes = [0u8; 16];
        alias_bytes[..alias.len()].copy_from_slice(alias);

        let key = ContextualMessageBySenderKey {
            sender,
            alias: alias_bytes,
            block_time: block_time.to_be_bytes(),
            block_hash,
            version,
            tx_id,
        };

        wtx.insert(&self.0, bytemuck::bytes_of(&key), sealed_hex);
        Ok(())
    }

    /// Get contextual message by exact key
    pub fn get(
        &self,
        rtx: &ReadTransaction,
        sender: AddressPayload,
        alias: &[u8],
        block_time: u64,
        block_hash: [u8; 32],
        version: u8,
        tx_id: [u8; 32],
    ) -> Result<Option<Vec<u8>>> {
        if alias.len() > 16 {
            bail!("Alias length cannot exceed 16 bytes, got {}", alias.len());
        }

        let mut alias_bytes = [0u8; 16];
        alias_bytes[..alias.len()].copy_from_slice(alias);

        let key = ContextualMessageBySenderKey {
            sender,
            alias: alias_bytes,
            block_time: block_time.to_be_bytes(),
            block_hash,
            version,
            tx_id,
        };

        Ok(rtx.get(&self.0, bytemuck::bytes_of(&key))?.map(|bytes| bytes.to_vec()))
    }

    /// Get all contextual messages for a sender (prefix search)
    pub fn get_by_sender_prefix(
        &self,
        rtx: &ReadTransaction,
        sender: &AddressPayload,
    ) -> impl DoubleEndedIterator<Item = Result<(LikeContextualMessageBySenderKey<UserKey>, Vec<u8>)>> + '_ {
        let prefix = bytemuck::bytes_of(sender);
        rtx.prefix(&self.0, prefix)
            .map(|item| {
                let (key_bytes, value_bytes) = item?;
                Ok((
                    LikeContextualMessageBySenderKey::new(key_bytes),
                    value_bytes.to_vec(),
                ))
            })
    }
    // todo fix me
    // /// Get all contextual messages for a sender and alias prefix (REST API support)
    // pub fn get_by_sender_and_alias_prefix(
    //     &self,
    //     rtx: &ReadTransaction,
    //     sender: AddressPayload,
    //     alias_prefix: &[u8],
    // ) -> Result<impl DoubleEndedIterator<Item = Result<(LikeContextualMessageBySenderKey<UserKey>, Vec<u8>)>> + '_> {
    //     if alias_prefix.len() > 16 {
    //         bail!("Alias prefix length cannot exceed 16 bytes, got {}", alias_prefix.len());
    //     }
    //
    //     let mut alias_bytes = [0u8; 16];
    //     alias_bytes[..alias_prefix.len()].copy_from_slice(alias_prefix);
    //
    //     let prefix_key = ContextualMessageBySenderKey {
    //         sender,
    //         alias: alias_bytes,
    //         block_time: [0u8; 8],
    //         block_hash: [0u8; 32],
    //         version: 0,
    //         tx_id: [0u8; 32],
    //     };
    //
    //     // Create prefix: sender + alias prefix
    //     let prefix_len = 34 + alias_prefix.len(); // AddressPayload + actual alias prefix length
    //     let prefix = &bytemuck::bytes_of(&prefix_key)[..prefix_len];
    //
    //     Ok(rtx.prefix(&self.0, prefix)
    //         .map(|item| {
    //             let (key_bytes, value_bytes) = item?;
    //             Ok((
    //                 LikeContextualMessageBySenderKey::new(key_bytes),
    //                 value_bytes.to_vec(),
    //             ))
    //         }))
    // }

    /// Get all contextual messages (for admin/debug purposes)
    pub fn get_all(
        &self,
        rtx: &ReadTransaction,
    ) -> impl DoubleEndedIterator<Item = Result<(LikeContextualMessageBySenderKey<UserKey>, Vec<u8>)>> + '_ {
        rtx.iter(&self.0)
            .map(|item| {
                let (key_bytes, value_bytes) = item?;
                Ok((
                    LikeContextualMessageBySenderKey::new(key_bytes),
                    value_bytes.to_vec(),
                ))
            })
    }

    /// Update sender address (when resolved from zeros)
    pub fn update_sender(
        &self,
        wtx: &mut WriteTransaction,
        old_sender: AddressPayload,
        new_sender: AddressPayload,
        alias: &[u8],
        block_time: u64,
        block_hash: [u8; 32],
        version: u8,
        tx_id: [u8; 32],
    ) -> Result<()> {
        if alias.len() > 16 {
            bail!("Alias length cannot exceed 16 bytes, got {}", alias.len());
        }

        let mut alias_bytes = [0u8; 16];
        alias_bytes[..alias.len()].copy_from_slice(alias);

        let old_key = ContextualMessageBySenderKey {
            sender: old_sender,
            alias: alias_bytes,
            block_time: block_time.to_be_bytes(),
            block_hash,
            version,
            tx_id,
        };

        if let Some(sealed_hex) = wtx.get(&self.0, bytemuck::bytes_of(&old_key))? {
            // Remove old entry
            wtx.remove(&self.0, bytemuck::bytes_of(&old_key));
            
            // Insert with new sender
            let new_key = ContextualMessageBySenderKey {
                sender: new_sender,
                alias: alias_bytes,
                block_time: block_time.to_be_bytes(),
                block_hash,
                version,
                tx_id,
            };
            
            wtx.insert(&self.0, bytemuck::bytes_of(&new_key), sealed_hex);
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_contextual_message_key_serialization() {
        let key = ContextualMessageBySenderKey {
            sender: AddressPayload::default(),
            alias: [1u8; 16],
            block_time: 12345u64.to_be_bytes(),
            block_hash: [2u8; 32],
            version: 1,
            tx_id: [3u8; 32],
        };
        
        let bytes = bytemuck::bytes_of(&key);
        assert_eq!(bytes.len(), 123); // 34 + 16 + 8 + 32 + 1 + 32
        
        let deserialized: ContextualMessageBySenderKey = *bytemuck::from_bytes(bytes);
        assert_eq!(deserialized, key);
    }

    #[test]
    fn test_like_contextual_message_key() {
        let key = ContextualMessageBySenderKey {
            sender: AddressPayload::default(),
            alias: [1u8; 16],
            block_time: 12345u64.to_be_bytes(),
            block_hash: [2u8; 32],
            version: 1,
            tx_id: [3u8; 32],
        };
        
        let bytes = bytemuck::bytes_of(&key).to_vec();
        let like_key = LikeContextualMessageBySenderKey::new(bytes);
        
        // Test zero-copy access via Deref
        assert_eq!(like_key.sender, key.sender);
        assert_eq!(like_key.alias, key.alias);
        assert_eq!(like_key.block_time, key.block_time);
        assert_eq!(like_key.block_hash, key.block_hash);
        assert_eq!(like_key.version, key.version);
        assert_eq!(like_key.tx_id, key.tx_id);
        
        // Test Clone
        let cloned = like_key.clone();
        assert_eq!(cloned.sender, key.sender);
    }

    #[test]
    fn test_alias_validation() {
        // Valid alias (16 bytes)
        let valid_alias = [1u8; 16];
        assert!(valid_alias.len() <= 16);
        
        // Invalid alias (17 bytes) - this would cause an error
        let invalid_alias = [1u8; 17];
        assert!(invalid_alias.len() > 16);
        
        // Test padding works correctly
        let test_alias = b"test_alias";
        assert!(test_alias.len() <= 16); // This should be fine
        let mut alias_bytes = [0u8; 16];
        alias_bytes[..test_alias.len()].copy_from_slice(test_alias);
        
        // Verify padding
        assert_eq!(alias_bytes[0..10], *test_alias);
        assert_eq!(alias_bytes[10], 0); // should be padded with zeros
    }

    #[test]
    fn test_key_ordering() {
        let key1 = ContextualMessageBySenderKey {
            sender: AddressPayload::default(),
            alias: [1u8; 16],
            block_time: 100u64.to_be_bytes(),
            block_hash: [2u8; 32],
            version: 1,
            tx_id: [3u8; 32],
        };

        let key2 = ContextualMessageBySenderKey {
            sender: AddressPayload::default(),
            alias: [1u8; 16],
            block_time: 200u64.to_be_bytes(),
            block_hash: [2u8; 32],
            version: 1,
            tx_id: [3u8; 32],
        };

        // Earlier time should come first
        assert!(bytemuck::bytes_of(&key1) < bytemuck::bytes_of(&key2));
    }
}