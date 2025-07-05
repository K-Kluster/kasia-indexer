use std::collections::BTreeMap;
use anyhow::bail;
use bytemuck::{AnyBitPattern, NoUninit};
use fjall::{PartitionCreateOptions, ReadTransaction, UserKey, UserValue as UserSlice, WriteTransaction};
use kaspa_rpc_core::{RpcHash, RpcTransactionId, RpcScriptPublicKey};
use crate::database::handshake::{HandshakeKeyByReceiver, AddressPayload, SenderHandshakeBySenderPartition, HandshakeKeyBySender};
use std::marker::PhantomData;
use std::ops::Deref;

// todo first trigger is either handshake/payment transaction or acceptance
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerAction {
    /// Step 1: Resolve accepting block hash for transaction
    ResolveAcceptingBlock = 1,
    /// Step 2: Resolve DAA score from accepting block hash
    ResolveDaaScore = 2,
    /// Step 3: Find sender using DAA score and tx_id, then update handshake
    UpdateHandshakeSender = 3,
}

#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
#[repr(C)]
pub struct TriggerKey {
    pub tx_id: [u8; 32],
    pub action: u8,
    pub priority: u8, // 0 = highest priority
}

#[repr(transparent)]
#[derive(Clone, PartialEq, Eq)]
pub struct LikeTriggerKey<T: AsRef<[u8]>> {
    bts: T,
    phantom_data: PhantomData<TriggerKey>,
}

impl<T: AsRef<[u8]>> LikeTriggerKey<T> {
    pub fn new(bts: T) -> Self {
        Self {
            bts,
            phantom_data: PhantomData,
        }
    }
}

impl<T: AsRef<[u8]>> Deref for LikeTriggerKey<T> {
    type Target = TriggerKey;

    fn deref(&self) -> &Self::Target {
        bytemuck::from_bytes(self.bts.as_ref())
    }
}

/// Trigger arguments for resolving accepting block hash
pub type ResolveAcceptingBlockTriggerArg = HandshakeKeyByReceiver;

/// Trigger arguments for resolving DAA score
#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
#[repr(C)]
pub struct ResolveDaaScoreTriggerArg {
    pub accepting_block_hash: [u8; 32],
    pub handshake_key: HandshakeKeyByReceiver,
}

/// Trigger arguments for updating handshake sender
#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
#[repr(C)]
pub struct UpdateHandshakeSenderTriggerArg {
    pub accepting_daa_score: [u8; 8], // u64 big endian
    pub accepting_block_hash: [u8; 32],
    pub handshake_key: HandshakeKeyByReceiver,
}

#[derive(Clone)]
pub struct TriggerPartition(fjall::TxPartition);

impl TriggerPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> anyhow::Result<Self> {
        Ok(Self(keyspace.open_partition(
            "acceptance_trigger",
            PartitionCreateOptions::default(),
        )?))
    }

    /// Create trigger to resolve accepting block hash for transaction
    pub fn create_resolve_accepting_block_trigger(
        &self,
        tx_id: [u8; 32],
        handshake_key: HandshakeKeyByReceiver,
    ) -> anyhow::Result<()> {
        let key = TriggerKey {
            tx_id,
            action: TriggerAction::ResolveAcceptingBlock as u8,
            priority: 0, // Highest priority - needed for next steps
        };

        self.0.insert(bytemuck::bytes_of(&key), bytemuck::bytes_of(&handshake_key))?;
        Ok(())
    }

    /// Create trigger to resolve DAA score from accepting block hash
    pub fn create_resolve_daa_score_trigger(
        &self,
        wtx: &mut WriteTransaction,
        tx_id: [u8; 32],
        accepting_block_hash: [u8; 32],
        handshake_key: HandshakeKeyByReceiver,
    ) -> anyhow::Result<()> {
        let key = TriggerKey {
            tx_id,
            action: TriggerAction::ResolveDaaScore as u8,
            priority: 1, // Medium priority - depends on accepting block resolution
        };

        let arg = ResolveDaaScoreTriggerArg {
            accepting_block_hash,
            handshake_key,
        };

        wtx.insert(&self.0, bytemuck::bytes_of(&key), bytemuck::bytes_of(&arg));
        Ok(())
    }

    /// Create trigger to update handshake sender after DAA score is resolved
    pub fn create_update_handshake_sender_trigger(
        &self,
        wtx: &mut WriteTransaction,
        tx_id: [u8; 32],
        accepting_daa_score: u64,
        accepting_block_hash: [u8; 32],
        handshake_key: HandshakeKeyByReceiver,
    ) -> anyhow::Result<()> {
        let key = TriggerKey {
            tx_id,
            action: TriggerAction::UpdateHandshakeSender as u8,
            priority: 2, // Lowest priority - depends on DAA resolution
        };

        let arg = UpdateHandshakeSenderTriggerArg {
            accepting_daa_score: accepting_daa_score.to_be_bytes(),
            accepting_block_hash,
            handshake_key,
        };

        wtx.insert(&self.0, bytemuck::bytes_of(&key), bytemuck::bytes_of(&arg));
        Ok(())
    }
    

    /// Remove all triggers for specific transaction IDs (used during reorg)
    pub fn remove_triggers_for_tx_ids(
        &self,
        wtx: &mut WriteTransaction,
        tx_ids: &[RpcTransactionId],
    ) -> anyhow::Result<()> {
        for tx_id in tx_ids {
            let tx_id_bytes = tx_id.as_bytes();
            let start = tx_id_bytes;
            let mut end = [0u8; 33];
            end[..32].copy_from_slice(tx_id_bytes.as_slice());
            end[32] = u8::MAX;

            let triggers: Vec<_> = wtx.range(&self.0, start.as_slice()..=end.as_slice())
                .map(|r| r.map(|(k, _)| k))
                .collect::<Result<_, _>>()?;

            for trigger_key in triggers {
                wtx.remove(&self.0, trigger_key);
            }
        }
        Ok(())
    }

    /// Get all triggers for a specific action, ordered by priority
    pub fn get_triggers_for_action(
        &self,
        rtx: &ReadTransaction,
        action: TriggerAction,
    ) -> impl Iterator<Item = anyhow::Result<(LikeTriggerKey<UserKey>, UserSlice)>> {
        // Create range for this action
        let mut start = [0u8; 33];
        start[32] = action as u8;
        
        let mut end = [0u8; 33];
        end[32] = (action as u8) + 1;

        rtx.range(&self.0, start..end)
            .map(|r| r.map_err(Into::into)
                .map(|(k, v)| (LikeTriggerKey::new(k), v)))
    }

    /// Get all triggers for specific transaction IDs
    pub fn get_triggers_for_tx_ids_rtx(
        &self,
        rtx: &ReadTransaction,
        tx_ids: &[RpcTransactionId],
    ) -> impl Iterator<Item = anyhow::Result<(LikeTriggerKey<UserKey>, UserSlice)>> {
        let mut results = Vec::new();
        
        for tx_id in tx_ids {
            let tx_id_bytes = tx_id.as_bytes();
            // Get all triggers for this tx_id (all actions)
            let start = tx_id_bytes;
            let mut end = [0u8; 33];
            end[..32].copy_from_slice(tx_id_bytes.as_slice());
            end[32] = u8::MAX;
            
            let iter = rtx.range(&self.0, start.as_slice()..=end.as_slice())
                .map(|r| r.map_err(Into::into)
                    .map(|(k, v)| (LikeTriggerKey::new(k), v)));
            
            results.extend(iter);
        }
        
        results.into_iter()
    }

    pub fn get_triggers_for_tx_ids_wtx(
        &self,
        wtx: &mut WriteTransaction,
        tx_ids: &[RpcTransactionId],
    ) -> impl Iterator<Item = anyhow::Result<(LikeTriggerKey<UserKey>, UserSlice)>> {
        let mut results = Vec::new();

        for tx_id in tx_ids {
            let tx_id_bytes = tx_id.as_bytes();
            // Get all triggers for this tx_id (all actions)
            let start = tx_id_bytes;
            let mut end = [0u8; 33];
            end[..32].copy_from_slice(tx_id_bytes.as_slice());
            end[32] = u8::MAX;

            let iter = wtx.range(&self.0, start.as_slice()..=end.as_slice())
                .map(|r| r.map_err(Into::into)
                    .map(|(k, v)| (LikeTriggerKey::new(k), v)));

            results.extend(iter);
        }

        results.into_iter()
    }

    /// Remove trigger after execution
    pub fn remove_trigger(&self, wtx: &mut WriteTransaction, trigger_key: &TriggerKey) -> anyhow::Result<()> {
        wtx.remove(&self.0, bytemuck::bytes_of(trigger_key));
        Ok(())
    }

    /// Execute all triggers for given accepting transactions
    pub fn execute_triggers_for_acceptances(
        &self,
        wtx: &mut WriteTransaction,
        acceptances: &BTreeMap<RpcTransactionId, AcceptanceInfo>,
        sender_handshake_partition: &SenderHandshakeBySenderPartition,
    ) -> anyhow::Result<Vec<TriggerExecutionResult>> {
        if acceptances.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        let tx_ids: Vec<_> = acceptances.keys().cloned().collect();
        
        // Get all triggers for these transaction IDs
        let triggers = self.get_triggers_for_tx_ids_wtx(wtx, &tx_ids).collect::<Result<Vec<_>, _>>()?;
        
        for trigger_result in triggers {
            let (trigger_key, trigger_data) = trigger_result;
            let action = TriggerAction::try_from(trigger_key.action)
                .map_err(|_| anyhow::anyhow!("Invalid trigger action: {}", trigger_key.action))?;
            
            let tx_id = RpcTransactionId::from_slice(&trigger_key.tx_id);
            
            if let Some(acceptance) = acceptances.get(&tx_id) {
                let result = match action {
                    TriggerAction::ResolveAcceptingBlock => {
                        let arg = bytemuck::from_bytes(&trigger_data);
                        self.execute_resolve_accepting_block_trigger(wtx, &trigger_key, arg, acceptance)?
                    }
                    TriggerAction::ResolveDaaScore => {
                        let arg: ResolveDaaScoreTriggerArg = *bytemuck::from_bytes(&trigger_data);
                        self.execute_resolve_daa_score_trigger(wtx, &trigger_key, &arg, acceptance)?
                    }
                    TriggerAction::UpdateHandshakeSender => {
                        let arg: UpdateHandshakeSenderTriggerArg = *bytemuck::from_bytes(&trigger_data);
                        self.execute_update_handshake_sender_trigger(wtx, &trigger_key, &arg, acceptance, sender_handshake_partition)?
                    }
                };
                
                results.push(result);
                
                // Always remove trigger after execution
                self.remove_trigger(wtx, &trigger_key)?;
            }
        }
        
        Ok(results)
    }

    fn execute_resolve_accepting_block_trigger(
        &self,
        wtx: &mut WriteTransaction,
        trigger_key: &TriggerKey,
        arg: &ResolveAcceptingBlockTriggerArg,
        acceptance: &AcceptanceInfo,
    ) -> anyhow::Result<TriggerExecutionResult> {
        if let Some(accepting_block_hash) = acceptance.accepting_block_hash {
            // Create next trigger to resolve DAA score
            self.create_resolve_daa_score_trigger(
                wtx,
                trigger_key.tx_id,
                accepting_block_hash.as_bytes(),
                *arg,
            )?;
            
            Ok(TriggerExecutionResult::AcceptingBlockResolved {
                tx_id: trigger_key.tx_id,
                accepting_block_hash: accepting_block_hash.as_bytes(),
            })
        } else {
            // Still need to resolve accepting block hash
            Ok(TriggerExecutionResult::AcceptingBlockResolutionNeeded {
                tx_id: trigger_key.tx_id,
                handshake_key: *arg,
            })
        }
    }

    fn execute_resolve_daa_score_trigger(
        &self,
        wtx: &mut WriteTransaction,
        trigger_key: &TriggerKey,
        arg: &ResolveDaaScoreTriggerArg,
        acceptance: &AcceptanceInfo,
    ) -> anyhow::Result<TriggerExecutionResult> {
        // Check if we have DAA score for this accepting block
        if let Some(daa_score) = acceptance.accepting_block_daa_score {
            // Create next trigger to update handshake sender
            self.create_update_handshake_sender_trigger(
                wtx,
                trigger_key.tx_id,
                daa_score,
                arg.accepting_block_hash,
                arg.handshake_key,
            )?;
            
            Ok(TriggerExecutionResult::DaaScoreResolved {
                tx_id: trigger_key.tx_id,
                daa_score,
            })
        } else {
            // Still need to resolve DAA score
            Ok(TriggerExecutionResult::DaaScoreResolutionNeeded {
                tx_id: trigger_key.tx_id,
                accepting_block_hash: arg.accepting_block_hash,
            })
        }
    }

    fn execute_update_handshake_sender_trigger(
        &self,
        wtx: &mut WriteTransaction,
        trigger_key: &TriggerKey,
        arg: &UpdateHandshakeSenderTriggerArg,
        acceptance: &AcceptanceInfo,
        sender_handshake_partition: &SenderHandshakeBySenderPartition,
    ) -> anyhow::Result<TriggerExecutionResult> {
        if let Some(ref sender_script_public_key) = acceptance.sender_script_public_key {
            // Convert RpcScriptPublicKey to AddressPayload
            let sender_address = AddressPayload::try_from(sender_script_public_key)
                .map_err(|e| anyhow::anyhow!("Failed to convert script public key to address: {}", e))?;
            
            // Create HandshakeKeyBySender from the original handshake key and sender
            let handshake_key_by_sender = HandshakeKeyBySender {
                sender: sender_address,
                block_time: arg.handshake_key.block_time,
                block_hash: arg.handshake_key.block_hash,
                receiver: arg.handshake_key.receiver,
                version: arg.handshake_key.version,
                tx_id: arg.handshake_key.tx_id,
            };
            
            // Insert into sender handshake partition
            sender_handshake_partition.insert_wtx(wtx, &handshake_key_by_sender)?;
            
            Ok(TriggerExecutionResult::HandshakeUpdated {
                tx_id: trigger_key.tx_id,
                sender_address,
                handshake_key: arg.handshake_key,
            })
        } else {
            // Still need RPC call to resolve sender
            Ok(TriggerExecutionResult::SenderResolutionNeeded {
                tx_id: trigger_key.tx_id,
                daa_score: u64::from_be_bytes(arg.accepting_daa_score),
                accepting_block_hash: arg.accepting_block_hash,
                handshake_key: arg.handshake_key,
            })
        }
    }
}

#[derive(Debug, Clone)]
pub struct AcceptanceInfo {
    pub accepting_block_hash: Option<RpcHash>,
    pub accepting_block_daa_score: Option<u64>,
    pub sender_script_public_key: Option<RpcScriptPublicKey>,
}

#[derive(Debug, Clone)]
pub enum TriggerExecutionResult {
    AcceptingBlockResolved {
        tx_id: [u8; 32],
        accepting_block_hash: [u8; 32],
    },
    AcceptingBlockResolutionNeeded {
        tx_id: [u8; 32],
        handshake_key: HandshakeKeyByReceiver,
    },
    DaaScoreResolved {
        tx_id: [u8; 32],
        daa_score: u64,
    },
    DaaScoreResolutionNeeded {
        tx_id: [u8; 32],
        accepting_block_hash: [u8; 32],
    },
    SenderResolutionNeeded {
        tx_id: [u8; 32],
        daa_score: u64,
        accepting_block_hash: [u8; 32],
        handshake_key: HandshakeKeyByReceiver,
    },
    HandshakeUpdated {
        tx_id: [u8; 32],
        sender_address: AddressPayload,
        handshake_key: HandshakeKeyByReceiver,
    },
}

impl TryFrom<u8> for TriggerAction {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(TriggerAction::ResolveAcceptingBlock),
            2 => Ok(TriggerAction::ResolveDaaScore),
            3 => Ok(TriggerAction::UpdateHandshakeSender),
            _ => bail!("Invalid trigger action: {}", value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::handshake::AddressPayload;
    use kaspa_rpc_core::{RpcHash, RpcTransactionId};
    use std::collections::BTreeMap;

    // Helper function to create test handshake key
    fn create_test_handshake_key() -> HandshakeKeyByReceiver {
        HandshakeKeyByReceiver {
            receiver: AddressPayload::default(),
            block_time: 12345u64.to_be_bytes(),
            block_hash: [1u8; 32],
            version: 1,
            tx_id: [2u8; 32],
        }
    }

    #[test]
    fn test_trigger_action_enum() {
        // Test TriggerAction enum values
        assert_eq!(TriggerAction::ResolveAcceptingBlock as u8, 1);
        assert_eq!(TriggerAction::ResolveDaaScore as u8, 2);
        assert_eq!(TriggerAction::UpdateHandshakeSender as u8, 3);
        
        // Test TryFrom conversion
        assert_eq!(TriggerAction::try_from(1).unwrap(), TriggerAction::ResolveAcceptingBlock);
        assert_eq!(TriggerAction::try_from(2).unwrap(), TriggerAction::ResolveDaaScore);
        assert_eq!(TriggerAction::try_from(3).unwrap(), TriggerAction::UpdateHandshakeSender);
        assert!(TriggerAction::try_from(4).is_err());
    }

    #[test]
    fn test_trigger_key_serialization() {
        let tx_id = [1u8; 32];
        let action = TriggerAction::ResolveAcceptingBlock as u8;
        let priority = 0u8;
        
        let trigger_key = TriggerKey {
            tx_id,
            action,
            priority,
        };
        
        // Test that the key can be serialized/deserialized
        let bytes = bytemuck::bytes_of(&trigger_key);
        let deserialized: TriggerKey = *bytemuck::from_bytes(bytes);
        
        assert_eq!(deserialized.tx_id, tx_id);
        assert_eq!(deserialized.action, action);
        assert_eq!(deserialized.priority, priority);
    }

    #[test]
    fn test_trigger_args_serialization() {
        let handshake_key = create_test_handshake_key();
        
        // Test ResolveAcceptingBlockTriggerArg (which is just HandshakeKeyByReceiver)
        let bytes = bytemuck::bytes_of(&handshake_key);
        let deserialized: HandshakeKeyByReceiver = *bytemuck::from_bytes(bytes);
        assert_eq!(deserialized.tx_id, handshake_key.tx_id);
        
        // Test ResolveDaaScoreTriggerArg
        let resolve_daa_arg = ResolveDaaScoreTriggerArg {
            accepting_block_hash: [2u8; 32],
            handshake_key,
        };
        let bytes = bytemuck::bytes_of(&resolve_daa_arg);
        let deserialized: ResolveDaaScoreTriggerArg = *bytemuck::from_bytes(bytes);
        assert_eq!(deserialized.accepting_block_hash, [2u8; 32]);
        assert_eq!(deserialized.handshake_key.tx_id, handshake_key.tx_id);
        
        // Test UpdateHandshakeSenderTriggerArg
        let update_sender_arg = UpdateHandshakeSenderTriggerArg {
            accepting_daa_score: 12345u64.to_be_bytes(),
            accepting_block_hash: [3u8; 32],
            handshake_key,
        };
        let bytes = bytemuck::bytes_of(&update_sender_arg);
        let deserialized: UpdateHandshakeSenderTriggerArg = *bytemuck::from_bytes(bytes);
        assert_eq!(u64::from_be_bytes(deserialized.accepting_daa_score), 12345);
        assert_eq!(deserialized.accepting_block_hash, [3u8; 32]);
        assert_eq!(deserialized.handshake_key.tx_id, handshake_key.tx_id);
    }

    #[test]
    fn test_acceptance_info_creation() {
        let accepting_block_hash = RpcHash::from_bytes([1u8; 32]);
        let daa_score = 12345u64;
        
        let acceptance_info = AcceptanceInfo {
            accepting_block_hash: Some(accepting_block_hash),
            accepting_block_daa_score: Some(daa_score),
            sender_script_public_key: None,
        };
        
        assert_eq!(acceptance_info.accepting_block_hash.unwrap(), accepting_block_hash);
        assert_eq!(acceptance_info.accepting_block_daa_score.unwrap(), daa_score);
        assert!(acceptance_info.sender_script_public_key.is_none());
    }

    #[test]
    fn test_trigger_execution_result_variants() {
        let tx_id = [1u8; 32];
        let handshake_key = create_test_handshake_key();
        
        // Test AcceptingBlockResolved
        let result = TriggerExecutionResult::AcceptingBlockResolved {
            tx_id,
            accepting_block_hash: [2u8; 32],
        };
        match result {
            TriggerExecutionResult::AcceptingBlockResolved { tx_id: result_tx_id, .. } => {
                assert_eq!(result_tx_id, tx_id);
            }
            _ => panic!("Expected AcceptingBlockResolved"),
        }
        
        // Test AcceptingBlockResolutionNeeded
        let result = TriggerExecutionResult::AcceptingBlockResolutionNeeded {
            tx_id,
            handshake_key,
        };
        match result {
            TriggerExecutionResult::AcceptingBlockResolutionNeeded { tx_id: result_tx_id, .. } => {
                assert_eq!(result_tx_id, tx_id);
            }
            _ => panic!("Expected AcceptingBlockResolutionNeeded"),
        }
        
        // Test SenderResolutionNeeded
        let result = TriggerExecutionResult::SenderResolutionNeeded {
            tx_id,
            daa_score: 12345,
            accepting_block_hash: [3u8; 32],
            handshake_key,
        };
        match result {
            TriggerExecutionResult::SenderResolutionNeeded { daa_score, .. } => {
                assert_eq!(daa_score, 12345);
            }
            _ => panic!("Expected SenderResolutionNeeded"),
        }
        
        // Test HandshakeUpdated
        let result = TriggerExecutionResult::HandshakeUpdated {
            tx_id,
            sender_address: AddressPayload::default(),
            handshake_key,
        };
        match result {
            TriggerExecutionResult::HandshakeUpdated { tx_id: result_tx_id, .. } => {
                assert_eq!(result_tx_id, tx_id);
            }
            _ => panic!("Expected HandshakeUpdated"),
        }
    }

    #[test]
    fn test_like_trigger_key_wrapper() {
        let trigger_key = TriggerKey {
            tx_id: [1u8; 32],
            action: TriggerAction::ResolveAcceptingBlock as u8,
            priority: 0,
        };
        
        let bytes = bytemuck::bytes_of(&trigger_key);
        let like_key = LikeTriggerKey::new(bytes);
        
        // Test deref behavior
        assert_eq!(like_key.tx_id, trigger_key.tx_id);
        assert_eq!(like_key.action, trigger_key.action);
        assert_eq!(like_key.priority, trigger_key.priority);
    }

    #[test]
    fn test_btree_map_operations() {
        let tx_id1 = RpcTransactionId::from_slice(&[1u8; 32]);
        let tx_id2 = RpcTransactionId::from_slice(&[2u8; 32]);
        
        let mut acceptances = BTreeMap::new();
        acceptances.insert(tx_id1, AcceptanceInfo {
            accepting_block_hash: Some(RpcHash::from_bytes([1u8; 32])),
            accepting_block_daa_score: Some(100),
            sender_script_public_key: None,
        });
        acceptances.insert(tx_id2, AcceptanceInfo {
            accepting_block_hash: Some(RpcHash::from_bytes([2u8; 32])),
            accepting_block_daa_score: Some(200),
            sender_script_public_key: None,
        });
        
        assert_eq!(acceptances.len(), 2);
        assert!(acceptances.contains_key(&tx_id1));
        assert!(acceptances.contains_key(&tx_id2));
        
        // Test updating existing entry
        acceptances.get_mut(&tx_id1).unwrap().accepting_block_daa_score = Some(150);
        assert_eq!(acceptances.get(&tx_id1).unwrap().accepting_block_daa_score.unwrap(), 150);
    }
}