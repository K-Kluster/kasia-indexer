use anyhow::Result;
use bytemuck::{AnyBitPattern, NoUninit};
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction, UserKey};
use kaspa_rpc_core::RpcTransactionId;
use crate::database::PartitionId;
use crate::database::resolution_keys::{
    HandshakeKeyForResolution, LikeHandshakeKeyForResolution,
    ContextualMessageKeyForResolution, LikeContextualMessageKeyForResolution,
    PaymentKeyForResolution, LikePaymentKeyForResolution,
    SenderResolutionLikeKey
};

/// FIFO partition for tracking transactions that need sender address/payload resolution
/// Key: accepting_daa_score + tx_id + partition_type
/// Value: key data for the partition that needs sender resolution
///
/// Uses FIFO compaction strategy because:
/// - Pending resolutions are temporary - will be processed and removed
/// - Older entries become irrelevant as blockchain progresses
/// - Self-balancing: automatically removes old entries when size limit reached
#[derive(Clone)]
pub struct PendingSenderResolutionPartition(fjall::TxPartition);

#[repr(C)]
#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq, PartialOrd, Ord)]
pub struct PendingResolutionKey {
    pub accepting_daa_score: [u8; 8], // BE - for processing order (lowest first)
    pub tx_id: [u8; 32],
    pub partition_type: u8,           // PartitionId as u8 - at the end
}

// SenderResolutionLikeKey is now imported from resolution_keys module

#[derive(Debug, Clone)]
pub struct PendingSenderResolution<T: AsRef<[u8]> + Clone = UserKey> {
    pub accepting_daa_score: u64,
    pub tx_id: RpcTransactionId,
    pub partition_type: PartitionId,
    pub like_key: SenderResolutionLikeKey<T>,
}

impl PendingSenderResolutionPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(
            keyspace.open_partition(
                "pending_sender_resolution",
                PartitionCreateOptions::default()
                    .max_memtable_size(64 * 1024 * 1024)
                    .block_size(64 * 1024)
                    .compaction_strategy(fjall::compaction::Strategy::Fifo(
                        fjall::compaction::Fifo {
                            limit: 64 * 1024 * 1024,
                            ttl_seconds: None,
                        },
                    )),
            )?,
        ))
    }

    /// Mark a handshake transaction as needing sender resolution
    pub fn mark_handshake_pending(
        &self,
        wtx: &mut WriteTransaction,
        accepting_daa_score: u64,
        tx_id: RpcTransactionId,
        handshake_key: &HandshakeKeyForResolution,
    ) -> Result<()> {
        let key = PendingResolutionKey {
            accepting_daa_score: accepting_daa_score.to_be_bytes(),
            tx_id: *tx_id.as_ref(),
            partition_type: PartitionId::HandshakeBySender as u8,
        };
        wtx.insert(&self.0, bytemuck::bytes_of(&key), bytemuck::bytes_of(handshake_key));
        Ok(())
    }

    /// Mark a contextual message transaction as needing sender resolution
    pub fn mark_contextual_message_pending(
        &self,
        wtx: &mut WriteTransaction,
        accepting_daa_score: u64,
        tx_id: RpcTransactionId,
        contextual_message_key: &ContextualMessageKeyForResolution,
    ) -> Result<()> {
        let key = PendingResolutionKey {
            accepting_daa_score: accepting_daa_score.to_be_bytes(),
            tx_id: *tx_id.as_ref(),
            partition_type: PartitionId::ContextualMessageBySender as u8,
        };
        wtx.insert(&self.0, bytemuck::bytes_of(&key), bytemuck::bytes_of(contextual_message_key));
        Ok(())
    }

    /// Mark a payment transaction as needing sender resolution
    pub fn mark_payment_pending(
        &self,
        wtx: &mut WriteTransaction,
        accepting_daa_score: u64,
        tx_id: RpcTransactionId,
        payment_key: &PaymentKeyForResolution,
    ) -> Result<()> {
        let key = PendingResolutionKey {
            accepting_daa_score: accepting_daa_score.to_be_bytes(),
            tx_id: *tx_id.as_ref(),
            partition_type: PartitionId::PaymentBySender as u8,
        };
        wtx.insert(&self.0, bytemuck::bytes_of(&key), bytemuck::bytes_of(payment_key));
        Ok(())
    }

    /// Remove all pending entries for a transaction (when resolution is complete)
    /// Returns all the key data for updating the target partitions
    pub fn remove_pending(
        &self,
        wtx: &mut WriteTransaction,
        accepting_daa_score: u64,
        tx_id: RpcTransactionId,
    ) -> Result<Vec<(PartitionId, SenderResolutionLikeKey)>> {
        let prefix_key = PendingResolutionKey {
            accepting_daa_score: accepting_daa_score.to_be_bytes(),
            tx_id: *tx_id.as_ref(),
            partition_type: 0,
        };
        
        // Use prefix up to tx_id (8 + 32 = 40 bytes)
        let prefix = &bytemuck::bytes_of(&prefix_key)[..40];
        
        let mut results = Vec::new();
        let mut keys_to_remove = Vec::new();
        
        // First collect all entries
        for item in wtx.prefix(&self.0, prefix) {
            let (key_bytes, value_bytes) = item?;
            if key_bytes.len() == 41 { // 8 + 32 + 1
                let key: PendingResolutionKey = *bytemuck::from_bytes(&key_bytes);
                keys_to_remove.push(key);
                
                let partition_type = match key.partition_type {
                    x if x == PartitionId::HandshakeBySender as u8 => PartitionId::HandshakeBySender,
                    x if x == PartitionId::ContextualMessageBySender as u8 => PartitionId::ContextualMessageBySender,
                    x if x == PartitionId::PaymentBySender as u8 => PartitionId::PaymentBySender,
                    _ => return Err(anyhow::anyhow!("Invalid partition type: {}", key.partition_type)),
                };
                
                let like_key = match partition_type {
                    PartitionId::HandshakeBySender => SenderResolutionLikeKey::HandshakeKey(
                        LikeHandshakeKeyForResolution::new(value_bytes)
                    ),
                    PartitionId::ContextualMessageBySender => SenderResolutionLikeKey::ContextualMessageKey(
                        LikeContextualMessageKeyForResolution::new(value_bytes)
                    ),
                    PartitionId::PaymentBySender => SenderResolutionLikeKey::PaymentKey(
                        LikePaymentKeyForResolution::new(value_bytes)
                    ),
                    _ => return Err(anyhow::anyhow!("Invalid partition type for sender resolution: {:?}", partition_type)),
                };
                
                results.push((partition_type, like_key));
            }
        }
        
        // Then remove all collected keys
        for key in keys_to_remove {
            wtx.remove(&self.0, bytemuck::bytes_of(&key));
        }
        
        Ok(results)
    }
    

    /// Get all pending resolutions (for processing in DAA score order)
    pub fn get_all_pending(
        &self,
        rtx: &ReadTransaction,
    ) -> impl DoubleEndedIterator<Item = Result<PendingSenderResolution>> + '_ {
        rtx.iter(&self.0).map(|item| {
            let (key_bytes, value_bytes) = item?;
            if key_bytes.len() == 41 { // 8 + 32 + 1
                let key: PendingResolutionKey = *bytemuck::from_bytes(&key_bytes);
                
                let accepting_daa_score = u64::from_be_bytes(key.accepting_daa_score);
                let tx_id = RpcTransactionId::from_slice(&key.tx_id);
                
                let partition_type = match key.partition_type {
                    x if x == PartitionId::HandshakeBySender as u8 => PartitionId::HandshakeBySender,
                    x if x == PartitionId::ContextualMessageBySender as u8 => PartitionId::ContextualMessageBySender,
                    x if x == PartitionId::PaymentBySender as u8 => PartitionId::PaymentBySender,
                    _ => return Err(anyhow::anyhow!("Invalid partition type: {}", key.partition_type)),
                };
                
                let like_key = match partition_type {
                    PartitionId::HandshakeBySender => SenderResolutionLikeKey::HandshakeKey(
                        LikeHandshakeKeyForResolution::new(value_bytes)
                    ),
                    PartitionId::ContextualMessageBySender => SenderResolutionLikeKey::ContextualMessageKey(
                        LikeContextualMessageKeyForResolution::new(value_bytes)
                    ),
                    PartitionId::PaymentBySender => SenderResolutionLikeKey::PaymentKey(
                        LikePaymentKeyForResolution::new(value_bytes)
                    ),
                    _ => return Err(anyhow::anyhow!("Invalid partition type for sender resolution: {:?}", partition_type)),
                };
                
                Ok(PendingSenderResolution {
                    accepting_daa_score,
                    tx_id,
                    partition_type,
                    like_key,
                })
            } else {
                Err(anyhow::anyhow!("Invalid key length in pending_sender_resolution partition"))
            }
        })
    }

    /// Check if a transaction has any pending sender resolution
    pub fn has_pending(
        &self,
        rtx: &ReadTransaction,
        accepting_daa_score: u64,
        tx_id: RpcTransactionId,
    ) -> Result<bool> {
        let prefix_key = PendingResolutionKey {
            accepting_daa_score: accepting_daa_score.to_be_bytes(),
            tx_id: *tx_id.as_ref(),
            partition_type: 0,
        };
        
        // Use prefix up to tx_id (8 + 32 = 40 bytes)
        let prefix = &bytemuck::bytes_of(&prefix_key)[..40];
        
        Ok(rtx.prefix(&self.0, prefix).next().is_some())
    }

    /// Mark multiple handshake transactions as needing sender resolution
    pub fn mark_handshakes_pending_batch<'a, I>(
        &self,
        wtx: &mut WriteTransaction,
        accepting_daa_score: u64,
        entries: I,
    ) -> Result<()>
    where
        I: Iterator<Item = (RpcTransactionId, &'a HandshakeKeyForResolution)>,
    {
        for (tx_id, handshake_key) in entries {
            self.mark_handshake_pending(wtx, accepting_daa_score, tx_id, handshake_key)?;
        }
        Ok(())
    }

    /// Mark multiple contextual message transactions as needing sender resolution
    pub fn mark_contextual_messages_pending_batch<'a, I>(
        &self,
        wtx: &mut WriteTransaction,
        accepting_daa_score: u64,
        entries: I,
    ) -> Result<()>
    where
        I: Iterator<Item = (RpcTransactionId, &'a ContextualMessageKeyForResolution)>,
    {
        for (tx_id, contextual_message_key) in entries {
            self.mark_contextual_message_pending(wtx, accepting_daa_score, tx_id, contextual_message_key)?;
        }
        Ok(())
    }

    /// Mark multiple payment transactions as needing sender resolution
    pub fn mark_payments_pending_batch<'a, I>(
        &self,
        wtx: &mut WriteTransaction,
        accepting_daa_score: u64,
        entries: I,
    ) -> Result<()>
    where
        I: Iterator<Item = (RpcTransactionId, &'a PaymentKeyForResolution)>,
    {
        for (tx_id, payment_key) in entries {
            self.mark_payment_pending(wtx, accepting_daa_score, tx_id, payment_key)?;
        }
        Ok(())
    }

    /// Decrement attempt count for all entries with the given DAA score and transaction ID, removing entries that reach 0 attempts
    /// Returns a vector of (PartitionId, like_key) pairs that were removed due to reaching 0 attempts
    pub fn decrement_attempt_counts_by_transaction(
        &self,
        wtx: &mut WriteTransaction,
        accepting_daa_score: u64,
        tx_id: RpcTransactionId,
    ) -> Result<Vec<(PartitionId, SenderResolutionLikeKey)>> {
        let prefix_key = PendingResolutionKey {
            accepting_daa_score: accepting_daa_score.to_be_bytes(),
            tx_id: *tx_id.as_ref(),
            partition_type: 0,
        };
        
        // Use prefix up to tx_id (8 + 32 = 40 bytes)
        let prefix = &bytemuck::bytes_of(&prefix_key)[..40];
        
        let mut removed_entries = Vec::new();
        let mut keys_to_process = Vec::new();
        
        // First collect all keys for this transaction
        for item in wtx.prefix(&self.0, prefix) {
            let (key_bytes, _) = item?;
            if key_bytes.len() == 41 { // 8 + 32 + 1
                let key: PendingResolutionKey = *bytemuck::from_bytes(&key_bytes);
                keys_to_process.push(key);
            }
        }
        
        // Process each key individually using fetch_update
        for key in keys_to_process {
            let partition_type = match key.partition_type {
                x if x == PartitionId::HandshakeBySender as u8 => PartitionId::HandshakeBySender,
                x if x == PartitionId::ContextualMessageBySender as u8 => PartitionId::ContextualMessageBySender,
                x if x == PartitionId::PaymentBySender as u8 => PartitionId::PaymentBySender,
                _ => continue, // Skip invalid partition types
            };
            
            let mut removed_entry = None;
            
            wtx.fetch_update(&self.0, bytemuck::bytes_of(&key), |current_value| {
                if let Some(value_bytes) = current_value {
                    // Extract the current key data and create SenderResolutionLikeKey
                    let current_like_key = match partition_type {
                        PartitionId::HandshakeBySender => SenderResolutionLikeKey::HandshakeKey(
                            LikeHandshakeKeyForResolution::new(value_bytes.clone())
                        ),
                        PartitionId::ContextualMessageBySender => SenderResolutionLikeKey::ContextualMessageKey(
                            LikeContextualMessageKeyForResolution::new(value_bytes.clone())
                        ),
                        PartitionId::PaymentBySender => SenderResolutionLikeKey::PaymentKey(
                            LikePaymentKeyForResolution::new(value_bytes.clone())
                        ),
                        _ => return None, // Invalid partition type
                    };
                    
                    // Access the attempt_count and decrement it
                    let new_attempt_count = match &current_like_key {
                        SenderResolutionLikeKey::HandshakeKey(key) => key.attempt_count.saturating_sub(1),
                        SenderResolutionLikeKey::ContextualMessageKey(key) => key.attempt_count.saturating_sub(1),
                        SenderResolutionLikeKey::PaymentKey(key) => key.attempt_count.saturating_sub(1),
                    };
                    
                    if new_attempt_count == 0 {
                        // Remove the entry by returning None
                        removed_entry = Some((partition_type, current_like_key));
                        None
                    } else {
                        // Update the attempt count and return the updated value
                        let mut updated_bytes = value_bytes.to_vec();
                        // Find the attempt_count field offset and update it
                        match partition_type {
                            PartitionId::HandshakeBySender => {
                                // attempt_count is at offset: 8 + 32 + 34 + 1 + 32 = 107
                                if updated_bytes.len() >= 108 {
                                    updated_bytes[107] = new_attempt_count;
                                }
                            },
                            PartitionId::ContextualMessageBySender => {
                                // attempt_count is at offset: 16 + 8 + 32 + 1 + 32 = 89
                                if updated_bytes.len() >= 90 {
                                    updated_bytes[89] = new_attempt_count;
                                }
                            },
                            PartitionId::PaymentBySender => {
                                // attempt_count is at offset: 8 + 32 + 34 + 1 + 32 = 107
                                if updated_bytes.len() >= 108 {
                                    updated_bytes[107] = new_attempt_count;
                                }
                            },
                            _ => return None, // Invalid partition type
                        }
                        Some(updated_bytes.into())
                    }
                } else {
                    // Entry doesn't exist, nothing to decrement
                    None
                }
            })?;
            
            if let Some(removed) = removed_entry {
                removed_entries.push(removed);
            }
        }
        
        Ok(removed_entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::handshake::AddressPayload;

    #[test]
    fn test_pending_resolution_key_serialization() {
        let key = PendingResolutionKey {
            accepting_daa_score: 12345u64.to_be_bytes(),
            tx_id: [1u8; 32],
            partition_type: PartitionId::HandshakeBySender as u8,
        };

        let bytes = bytemuck::bytes_of(&key);
        assert_eq!(bytes.len(), 41); // 8 + 32 + 1

        let deserialized: PendingResolutionKey = *bytemuck::from_bytes(bytes);
        assert_eq!(deserialized, key);

        let accepting_daa_score = u64::from_be_bytes(deserialized.accepting_daa_score);
        assert_eq!(accepting_daa_score, 12345);
    }

    #[test]
    fn test_key_ordering_by_daa_score() {
        let key1 = PendingResolutionKey {
            accepting_daa_score: 100u64.to_be_bytes(),
            tx_id: [1u8; 32],
            partition_type: PartitionId::HandshakeBySender as u8,
        };

        let key2 = PendingResolutionKey {
            accepting_daa_score: 200u64.to_be_bytes(),
            tx_id: [1u8; 32],
            partition_type: PartitionId::HandshakeBySender as u8,
        };

        // Lower DAA score should come first
        assert!(key1 < key2);
    }

    #[test]
    fn test_key_ordering_by_tx_id() {
        let key1 = PendingResolutionKey {
            accepting_daa_score: 100u64.to_be_bytes(),
            tx_id: [1u8; 32],
            partition_type: PartitionId::HandshakeBySender as u8,
        };

        let key2 = PendingResolutionKey {
            accepting_daa_score: 100u64.to_be_bytes(),
            tx_id: [2u8; 32],
            partition_type: PartitionId::HandshakeBySender as u8,
        };

        // Lower tx_id should come first when DAA score is same
        assert!(key1 < key2);
    }

    #[test]
    fn test_pending_sender_resolution_struct() {
        let handshake_key = HandshakeKeyForResolution {
            block_time: [0u8; 8],
            block_hash: [1u8; 32],
            receiver: AddressPayload::default(),
            version: 1,
            tx_id: [2u8; 32],
            attempt_count: 3,
        };

        let bytes = bytemuck::bytes_of(&handshake_key);
        let like_key = LikeHandshakeKeyForResolution::new(bytes.to_vec());

        let resolution = PendingSenderResolution {
            accepting_daa_score: 12345,
            tx_id: RpcTransactionId::from_slice(&[1u8; 32]),
            partition_type: PartitionId::HandshakeBySender,
            like_key: SenderResolutionLikeKey::HandshakeKey(like_key),
        };

        assert_eq!(resolution.accepting_daa_score, 12345);
        assert_eq!(resolution.tx_id.as_bytes(), [1u8; 32]);
        assert_eq!(resolution.partition_type, PartitionId::HandshakeBySender);
    }
}