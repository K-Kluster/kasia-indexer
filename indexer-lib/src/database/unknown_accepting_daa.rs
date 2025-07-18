use crate::database::resolution_keys::{
    ContextualMessageKeyForResolution, DaaResolutionLikeKey, HandshakeKeyForResolution,
    LikeContextualMessageKeyForResolution, LikeHandshakeKeyForResolution,
    LikePaymentKeyForResolution, PaymentKeyForResolution,
};
use anyhow::{Result, bail};
use bytemuck::{AnyBitPattern, NoUninit};
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use kaspa_consensus_core::tx::TransactionId;
use kaspa_rpc_core::RpcHash;

/// FIFO partition for storing accepting block hashes with unknown DAA scores
/// Key: accepting_block_hash + partition_type (block hash FIRST for efficient querying)
/// Value: attempt_count (u8, 1 byte) + serialized key data for the partition that needs updating
/// Note: These are accepting blocks (selected blocks in GHOSTDAG), not regular blocks
///
/// Uses FIFO compaction strategy because:
/// - Unknown DAA scores are temporary - will be resolved when RPC call completes
/// - Older entries become irrelevant as blockchain progresses
/// - Self-balancing: automatically removes old entries when size limit reached
#[derive(Clone)]
pub struct UnknownAcceptingDaaPartition(fjall::TxPartition);

/// Partition types that can need DAA score resolution
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DaaResolutionPartitionType {
    HandshakeBySender = 0,
    ContextualMessageBySender = 1,
    PaymentBySender = 2,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
pub struct UnknownAcceptingDaaKey {
    pub accepting_block_hash: [u8; 32], // MUST be first for efficient querying by block hash
    pub tx_id: [u8; 32],
    pub partition_type: u8,
}

// DaaResolutionLikeKey is now imported from resolution_keys module

impl UnknownAcceptingDaaPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(
            keyspace.open_partition(
                "unknown_accepting_daa",
                PartitionCreateOptions::default()
                    .block_size(64 * 1024)
                    .compaction_strategy(fjall::compaction::Strategy::Fifo(
                        fjall::compaction::Fifo {
                            limit: 160 * 1024 * 1024,
                            ttl_seconds: None,
                        },
                    )),
            )?,
        ))
    }

    /// Mark a handshake as needing DAA score resolution
    pub fn mark_handshake_unknown_daa(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hash: RpcHash,
        tx_id: TransactionId,
        handshake_key: &HandshakeKeyForResolution,
    ) -> Result<()> {
        let key = UnknownAcceptingDaaKey {
            tx_id: *tx_id.as_ref(),
            accepting_block_hash: *accepting_block_hash.as_ref(),
            partition_type: DaaResolutionPartitionType::HandshakeBySender as u8,
        };
        wtx.insert(
            &self.0,
            bytemuck::bytes_of(&key),
            bytemuck::bytes_of(handshake_key),
        );
        Ok(())
    }

    /// Mark a contextual message as needing DAA score resolution
    pub fn mark_contextual_message_unknown_daa(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hash: RpcHash,
        tx_id: TransactionId,
        contextual_message_key: &ContextualMessageKeyForResolution,
    ) -> Result<()> {
        let key = UnknownAcceptingDaaKey {
            tx_id: *tx_id.as_ref(),
            accepting_block_hash: *accepting_block_hash.as_ref(),
            partition_type: DaaResolutionPartitionType::ContextualMessageBySender as u8,
        };
        wtx.insert(
            &self.0,
            bytemuck::bytes_of(&key),
            bytemuck::bytes_of(contextual_message_key),
        );
        Ok(())
    }

    /// Mark a payment as needing DAA score resolution
    pub fn mark_payment_unknown_daa(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hash: RpcHash,
        tx_id: TransactionId,
        payment_key: &PaymentKeyForResolution,
    ) -> Result<()> {
        let key = UnknownAcceptingDaaKey {
            tx_id: *tx_id.as_ref(),
            accepting_block_hash: *accepting_block_hash.as_ref(),
            partition_type: DaaResolutionPartitionType::PaymentBySender as u8,
        };
        wtx.insert(
            &self.0,
            bytemuck::bytes_of(&key),
            bytemuck::bytes_of(payment_key),
        );
        Ok(())
    }

    /// Get all entries for a specific accepting block hash (when DAA is resolved)
    /// Returns an iterator of like_key entries
    pub fn get_by_accepting_block_hash(
        &self,
        rtx: &ReadTransaction,
        accepting_block_hash: RpcHash,
    ) -> impl DoubleEndedIterator<Item = Result<DaaResolutionLikeKey>> + '_ {
        let prefix = accepting_block_hash.as_bytes();
        rtx.prefix(&self.0, prefix).map(|r| {
            let (key_bytes, value_bytes) = r?;
            if key_bytes.len() == 65 {
                // 32 + 1 + 32
                let key: UnknownAcceptingDaaKey = *bytemuck::from_bytes(&key_bytes);
                match key.partition_type {
                    0 => Ok(DaaResolutionLikeKey::HandshakeKey(
                        LikeHandshakeKeyForResolution::new(value_bytes),
                    )),
                    1 => Ok(DaaResolutionLikeKey::ContextualMessageKey(
                        LikeContextualMessageKeyForResolution::new(value_bytes),
                    )),
                    2 => Ok(DaaResolutionLikeKey::PaymentKey(
                        LikePaymentKeyForResolution::new(value_bytes),
                    )),
                    _ => Err(anyhow::anyhow!(
                        "Invalid partition type: {}",
                        key.partition_type
                    )),
                }
            } else {
                Err(anyhow::anyhow!(
                    "Invalid key length in unknown_accepting_daa partition"
                ))
            }
        })
    }

    /// Remove all entries for a specific accepting block hash (when DAA is resolved)
    /// Returns all the like_key entries that were removed
    pub fn remove_by_accepting_block_hash(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hash: RpcHash,
    ) -> Result<Vec<DaaResolutionLikeKey>> {
        let prefix = accepting_block_hash.as_bytes();
        let mut results = Vec::new();
        let mut keys_to_remove = Vec::new();

        // First collect all entries
        for item in wtx.prefix(&self.0, prefix) {
            let (key_bytes, value_bytes) = item?;
            if key_bytes.len() == 65 {
                // 32 + 1 + 32
                let key: UnknownAcceptingDaaKey = *bytemuck::from_bytes(&key_bytes);
                keys_to_remove.push(key);

                match key.partition_type {
                    0 => results.push(DaaResolutionLikeKey::HandshakeKey(
                        LikeHandshakeKeyForResolution::new(value_bytes),
                    )),
                    1 => results.push(DaaResolutionLikeKey::ContextualMessageKey(
                        LikeContextualMessageKeyForResolution::new(value_bytes),
                    )),
                    2 => results.push(DaaResolutionLikeKey::PaymentKey(
                        LikePaymentKeyForResolution::new(value_bytes),
                    )),
                    _ => {
                        return Err(anyhow::anyhow!(
                            "Invalid partition type: {}",
                            key.partition_type
                        ));
                    }
                }
            } else {
                return Err(anyhow::anyhow!(
                    "Invalid key length in unknown_accepting_daa partition"
                ));
            }
        }

        // Then remove all collected keys
        for key in keys_to_remove {
            wtx.remove(&self.0, bytemuck::bytes_of(&key));
        }

        Ok(results)
    }

    /// Check if an accepting block hash has any entries needing DAA score resolution
    pub fn has_unknown_daa(
        &self,
        rtx: &ReadTransaction,
        accepting_block_hash: RpcHash,
    ) -> Result<bool> {
        let prefix = accepting_block_hash.as_bytes();
        Ok(rtx.prefix(&self.0, prefix).next().is_some())
    }

    /// Get all accepting block hashes that need DAA score resolution
    /// Returns only the unique block hashes (no values)
    pub fn get_all_unknown_accepting_blocks(
        &self,
        rtx: &ReadTransaction,
    ) -> impl DoubleEndedIterator<Item = Result<RpcHash>> + '_ {
        rtx.keys(&self.0).map(|r| {
            let key_bytes = r?;
            if key_bytes.len() == 65 {
                // 32 + 1 + 32
                let key: UnknownAcceptingDaaKey = *bytemuck::from_bytes(&key_bytes);
                Ok(RpcHash::from_slice(&key.accepting_block_hash))
            } else {
                Err(anyhow::anyhow!(
                    "Invalid key length in unknown_accepting_daa partition"
                ))
            }
        })
    }

    /// Decrement attempt count for all entries with the given accepting block hash, removing entries that reach 0 attempts
    /// Returns a vector of like_keys that were removed due to reaching 0 attempts
    pub fn decrement_attempt_counts_by_block_hash(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hash: RpcHash,
    ) -> Result<()> {
        let prefix = accepting_block_hash.as_bytes();
        let mut keys_to_process = Vec::new();

        // First collect all keys for this block hash
        for item in wtx.prefix(&self.0, prefix) {
            let (key_bytes, _) = item?;
            if key_bytes.len() == 65 {
                // 32 + 1 + 32
                let key: UnknownAcceptingDaaKey = *bytemuck::from_bytes(&key_bytes);
                keys_to_process.push(key);
            }
        }

        // Process each key individually using fetch_update
        for key in keys_to_process {
            let partition_type = match key.partition_type {
                0 => DaaResolutionPartitionType::HandshakeBySender,
                1 => DaaResolutionPartitionType::ContextualMessageBySender,
                2 => DaaResolutionPartitionType::PaymentBySender,
                _ => bail!("Invalid partition type: {}", key.partition_type),
            };

            wtx.fetch_update(&self.0, bytemuck::bytes_of(&key), |current_value| {
                let value_bytes = current_value.unwrap();
                // Extract the current key data and create DaaResolutionLikeKey
                let mut current_like_key = match partition_type {
                    DaaResolutionPartitionType::HandshakeBySender => {
                        DaaResolutionLikeKey::HandshakeKey(
                            LikeHandshakeKeyForResolution::new(value_bytes.clone()).to_mutable(),
                        )
                    }
                    DaaResolutionPartitionType::ContextualMessageBySender => {
                        DaaResolutionLikeKey::ContextualMessageKey(
                            LikeContextualMessageKeyForResolution::new(value_bytes.clone())
                                .to_mutable(),
                        )
                    }
                    DaaResolutionPartitionType::PaymentBySender => {
                        DaaResolutionLikeKey::PaymentKey(
                            LikePaymentKeyForResolution::new(value_bytes.clone()).to_mutable(),
                        )
                    }
                };

                // Access the attempt_count and decrement it
                let new_attempt_count = match &mut current_like_key {
                    DaaResolutionLikeKey::HandshakeKey(key) => {
                        let c = key.attempt_count.saturating_sub(1);
                        key.attempt_count = c;
                        c
                    }
                    DaaResolutionLikeKey::ContextualMessageKey(key) => {
                        let c = key.attempt_count.saturating_sub(1);
                        key.attempt_count = c;
                        c
                    }
                    DaaResolutionLikeKey::PaymentKey(key) => {
                        let c = key.attempt_count.saturating_sub(1);
                        key.attempt_count = c;
                        c
                    }
                };

                if new_attempt_count == 0 {
                    None
                } else {
                    Some(current_like_key.inner().into())
                }
            })?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::contextual_message_by_sender::{
        ContextualMessageBySenderKey, LikeContextualMessageBySenderKey,
    };
    use crate::database::handshake::{
        AddressPayload, HandshakeKeyBySender, LikeHandshakeKeyBySender,
    };

    #[test]
    fn test_unknown_accepting_daa_key_serialization() {
        let key = UnknownAcceptingDaaKey {
            accepting_block_hash: [1u8; 32],
            tx_id: [2u8; 32],
            partition_type: 0,
        };

        let bytes = bytemuck::bytes_of(&key);
        assert_eq!(bytes.len(), 65); // 32 + 1 + 32

        let deserialized: UnknownAcceptingDaaKey = *bytemuck::from_bytes(bytes);
        assert_eq!(deserialized, key);
    }

    #[test]
    fn test_key_ordering_by_accepting_block_hash() {
        let key1 = UnknownAcceptingDaaKey {
            accepting_block_hash: [1u8; 32],
            tx_id: [2u8; 32],
            partition_type: 0,
        };

        let key2 = UnknownAcceptingDaaKey {
            accepting_block_hash: [2u8; 32],
            tx_id: [1u8; 32],
            partition_type: 0,
        };

        // Key with lower block hash should come first
        assert!(bytemuck::bytes_of(&key1) < bytemuck::bytes_of(&key2));
    }

    #[test]
    fn test_partition_type_enum() {
        assert_eq!(DaaResolutionPartitionType::HandshakeBySender as u8, 0);
        assert_eq!(
            DaaResolutionPartitionType::ContextualMessageBySender as u8,
            1
        );
    }

    #[test]
    fn test_handshake_key_storage() {
        let handshake_key = HandshakeKeyBySender {
            sender: AddressPayload::default(),
            block_time: [0u8; 8],
            block_hash: [1u8; 32],
            receiver: AddressPayload::default(),
            version: 1,
            tx_id: [2u8; 32],
        };

        let bytes = bytemuck::bytes_of(&handshake_key);
        let like_key = LikeHandshakeKeyBySender::new(bytes.to_vec());

        // Test that we can access the handshake key fields via Deref
        assert_eq!(like_key.version, 1);
        assert_eq!(like_key.tx_id, [2u8; 32]);
    }

    #[test]
    fn test_contextual_message_key_storage() {
        let contextual_key = ContextualMessageBySenderKey {
            sender: AddressPayload::default(),
            alias: [1u8; 16],
            block_time: [0u8; 8],
            block_hash: [2u8; 32],
            version: 1,
            tx_id: [3u8; 32],
        };

        let bytes = bytemuck::bytes_of(&contextual_key);
        let like_key = LikeContextualMessageBySenderKey::new(bytes.to_vec());

        // Test that we can access the contextual message key fields via Deref
        assert_eq!(like_key.version, 1);
        assert_eq!(like_key.alias, [1u8; 16]);
        assert_eq!(like_key.tx_id, [3u8; 32]);
    }
}
