#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::handshake::{AddressPayload, HandshakeKeyByReceiver, SenderHandshakeBySenderPartition};
    use fjall::{Config, Keyspace};
    use kaspa_addresses::Version;
    use kaspa_rpc_core::{RpcHash, RpcScriptPublicKey, RpcTransactionId};
    use kaspa_txscript::script_class::ScriptClass;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    // Helper function to create test keyspace
    fn create_test_keyspace() -> (TempDir, Keyspace) {
        let temp_dir = TempDir::new().unwrap();
        let keyspace = Config::new(temp_dir.path()).open().unwrap();
        (temp_dir, keyspace)
    }

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

    // Helper function to create test RpcScriptPublicKey
    fn create_test_script_public_key() -> RpcScriptPublicKey {
        // Create a simple P2PK script: OP_DATA_32 <pubkey> OP_CHECKSIG
        let mut script = Vec::new();
        script.push(0x20); // OP_DATA_32
        script.extend_from_slice(&[3u8; 32]); // 32-byte pubkey
        script.push(0xac); // OP_CHECKSIG
        
        RpcScriptPublicKey::new(0, script)
    }

    #[test]
    fn test_create_resolve_accepting_block_trigger() {
        let (_temp_dir, keyspace) = create_test_keyspace();
        let tx_keyspace = keyspace.begin_write().unwrap();
        let trigger_partition = TriggerPartition::new(&tx_keyspace).unwrap();
        
        let tx_id = [1u8; 32];
        let handshake_key = create_test_handshake_key();
        
        // Create trigger
        trigger_partition
            .create_resolve_accepting_block_trigger(tx_id, handshake_key)
            .unwrap();
        
        // Verify trigger was created
        let triggers: Vec<_> = trigger_partition
            .get_triggers_for_action(&tx_keyspace.read(), TriggerAction::ResolveAcceptingBlock)
            .collect();
        
        assert_eq!(triggers.len(), 1);
        let (trigger_key, trigger_data) = triggers[0].as_ref().unwrap();
        assert_eq!(trigger_key.tx_id, tx_id);
        assert_eq!(trigger_key.action, TriggerAction::ResolveAcceptingBlock as u8);
        assert_eq!(trigger_key.priority, 0);
        
        // Verify trigger data
        let stored_handshake_key: HandshakeKeyByReceiver = *bytemuck::from_bytes(trigger_data);
        assert_eq!(stored_handshake_key.tx_id, handshake_key.tx_id);
    }

    #[test]
    fn test_create_resolve_daa_score_trigger() {
        let (_temp_dir, keyspace) = create_test_keyspace();
        let tx_keyspace = keyspace.begin_write().unwrap();
        let mut wtx = tx_keyspace;
        let trigger_partition = TriggerPartition::new(&wtx).unwrap();
        
        let tx_id = [1u8; 32];
        let accepting_block_hash = [2u8; 32];
        let handshake_key = create_test_handshake_key();
        
        // Create trigger
        trigger_partition
            .create_resolve_daa_score_trigger(&mut wtx, tx_id, accepting_block_hash, handshake_key)
            .unwrap();
        
        // Verify trigger was created
        let triggers: Vec<_> = trigger_partition
            .get_triggers_for_action(&wtx.read(), TriggerAction::ResolveDaaScore)
            .collect();
        
        assert_eq!(triggers.len(), 1);
        let (trigger_key, trigger_data) = triggers[0].as_ref().unwrap();
        assert_eq!(trigger_key.tx_id, tx_id);
        assert_eq!(trigger_key.action, TriggerAction::ResolveDaaScore as u8);
        assert_eq!(trigger_key.priority, 1);
        
        // Verify trigger data
        let stored_arg: ResolveDaaScoreTriggerArg = *bytemuck::from_bytes(trigger_data);
        assert_eq!(stored_arg.accepting_block_hash, accepting_block_hash);
        assert_eq!(stored_arg.handshake_key.tx_id, handshake_key.tx_id);
    }

    #[test]
    fn test_create_update_handshake_sender_trigger() {
        let (_temp_dir, keyspace) = create_test_keyspace();
        let tx_keyspace = keyspace.begin_write().unwrap();
        let mut wtx = tx_keyspace;
        let trigger_partition = TriggerPartition::new(&wtx).unwrap();
        
        let tx_id = [1u8; 32];
        let accepting_daa_score = 12345u64;
        let accepting_block_hash = [2u8; 32];
        let handshake_key = create_test_handshake_key();
        
        // Create trigger
        trigger_partition
            .create_update_handshake_sender_trigger(
                &mut wtx,
                tx_id,
                accepting_daa_score,
                accepting_block_hash,
                handshake_key,
            )
            .unwrap();
        
        // Verify trigger was created
        let triggers: Vec<_> = trigger_partition
            .get_triggers_for_action(&wtx.read(), TriggerAction::UpdateHandshakeSender)
            .collect();
        
        assert_eq!(triggers.len(), 1);
        let (trigger_key, trigger_data) = triggers[0].as_ref().unwrap();
        assert_eq!(trigger_key.tx_id, tx_id);
        assert_eq!(trigger_key.action, TriggerAction::UpdateHandshakeSender as u8);
        assert_eq!(trigger_key.priority, 2);
        
        // Verify trigger data
        let stored_arg: UpdateHandshakeSenderTriggerArg = *bytemuck::from_bytes(trigger_data);
        assert_eq!(u64::from_be_bytes(stored_arg.accepting_daa_score), accepting_daa_score);
        assert_eq!(stored_arg.accepting_block_hash, accepting_block_hash);
        assert_eq!(stored_arg.handshake_key.tx_id, handshake_key.tx_id);
    }

    #[test]
    fn test_remove_triggers_for_tx_ids() {
        let (_temp_dir, keyspace) = create_test_keyspace();
        let tx_keyspace = keyspace.begin_write().unwrap();
        let mut wtx = tx_keyspace;
        let trigger_partition = TriggerPartition::new(&wtx).unwrap();
        
        let tx_id1 = [1u8; 32];
        let tx_id2 = [2u8; 32];
        let handshake_key = create_test_handshake_key();
        
        // Create multiple triggers for different tx_ids
        trigger_partition
            .create_resolve_accepting_block_trigger(tx_id1, handshake_key)
            .unwrap();
        trigger_partition
            .create_resolve_accepting_block_trigger(tx_id2, handshake_key)
            .unwrap();
        trigger_partition
            .create_resolve_daa_score_trigger(&mut wtx, tx_id1, [3u8; 32], handshake_key)
            .unwrap();
        
        // Verify triggers exist
        let triggers_before: Vec<_> = trigger_partition
            .get_triggers_for_action(&wtx.read(), TriggerAction::ResolveAcceptingBlock)
            .collect();
        assert_eq!(triggers_before.len(), 2);
        
        // Remove triggers for tx_id1
        let tx_ids_to_remove = vec![RpcTransactionId::from_slice(&tx_id1)];
        trigger_partition
            .remove_triggers_for_tx_ids(&mut wtx, &tx_ids_to_remove)
            .unwrap();
        
        // Verify only tx_id2 triggers remain
        let triggers_after: Vec<_> = trigger_partition
            .get_triggers_for_action(&wtx.read(), TriggerAction::ResolveAcceptingBlock)
            .collect();
        assert_eq!(triggers_after.len(), 1);
        
        let (remaining_trigger, _) = triggers_after[0].as_ref().unwrap();
        assert_eq!(remaining_trigger.tx_id, tx_id2);
    }

    #[test]
    fn test_execute_resolve_accepting_block_trigger_success() {
        let (_temp_dir, keyspace) = create_test_keyspace();
        let tx_keyspace = keyspace.begin_write().unwrap();
        let mut wtx = tx_keyspace;
        let trigger_partition = TriggerPartition::new(&wtx).unwrap();
        
        let tx_id = [1u8; 32];
        let handshake_key = create_test_handshake_key();
        let accepting_block_hash = RpcHash::from_bytes([2u8; 32]);
        
        // Create trigger
        trigger_partition
            .create_resolve_accepting_block_trigger(tx_id, handshake_key)
            .unwrap();
        
        // Create acceptance info with resolved accepting block
        let mut acceptances = BTreeMap::new();
        acceptances.insert(
            RpcTransactionId::from_slice(&tx_id),
            AcceptanceInfo {
                accepting_block_hash: Some(accepting_block_hash),
                accepting_block_daa_score: None,
                sender_script_public_key: None,
            },
        );
        
        // Create mock sender handshake partition
        let sender_partition = SenderHandshakeBySenderPartition::new(&wtx).unwrap();
        
        // Execute triggers
        let results = trigger_partition
            .execute_triggers_for_acceptances(&mut wtx, &acceptances, &sender_partition)
            .unwrap();
        
        // Verify results
        assert_eq!(results.len(), 1);
        match &results[0] {
            TriggerExecutionResult::AcceptingBlockResolved {
                tx_id: result_tx_id,
                accepting_block_hash: result_hash,
            } => {
                assert_eq!(*result_tx_id, tx_id);
                assert_eq!(*result_hash, accepting_block_hash.as_bytes());
            }
            _ => panic!("Expected AcceptingBlockResolved result"),
        }
        
        // Verify next trigger was created
        let daa_triggers: Vec<_> = trigger_partition
            .get_triggers_for_action(&wtx.read(), TriggerAction::ResolveDaaScore)
            .collect();
        assert_eq!(daa_triggers.len(), 1);
    }

    #[test]
    fn test_execute_resolve_accepting_block_trigger_pending() {
        let (_temp_dir, keyspace) = create_test_keyspace();
        let tx_keyspace = keyspace.begin_write().unwrap();
        let mut wtx = tx_keyspace;
        let trigger_partition = TriggerPartition::new(&wtx).unwrap();
        
        let tx_id = [1u8; 32];
        let handshake_key = create_test_handshake_key();
        
        // Create trigger
        trigger_partition
            .create_resolve_accepting_block_trigger(tx_id, handshake_key)
            .unwrap();
        
        // Create acceptance info without accepting block
        let mut acceptances = BTreeMap::new();
        acceptances.insert(
            RpcTransactionId::from_slice(&tx_id),
            AcceptanceInfo {
                accepting_block_hash: None,
                accepting_block_daa_score: None,
                sender_script_public_key: None,
            },
        );
        
        // Create mock sender handshake partition
        let sender_partition = SenderHandshakeBySenderPartition::new(&wtx).unwrap();
        
        // Execute triggers
        let results = trigger_partition
            .execute_triggers_for_acceptances(&mut wtx, &acceptances, &sender_partition)
            .unwrap();
        
        // Verify results
        assert_eq!(results.len(), 1);
        match &results[0] {
            TriggerExecutionResult::AcceptingBlockResolutionNeeded {
                tx_id: result_tx_id,
                handshake_key: result_key,
            } => {
                assert_eq!(*result_tx_id, tx_id);
                assert_eq!(result_key.tx_id, handshake_key.tx_id);
            }
            _ => panic!("Expected AcceptingBlockResolutionNeeded result"),
        }
        
        // Verify no next trigger was created
        let daa_triggers: Vec<_> = trigger_partition
            .get_triggers_for_action(&wtx.read(), TriggerAction::ResolveDaaScore)
            .collect();
        assert_eq!(daa_triggers.len(), 0);
    }

    #[test]
    fn test_execute_resolve_daa_score_trigger_success() {
        let (_temp_dir, keyspace) = create_test_keyspace();
        let tx_keyspace = keyspace.begin_write().unwrap();
        let mut wtx = tx_keyspace;
        let trigger_partition = TriggerPartition::new(&wtx).unwrap();
        
        let tx_id = [1u8; 32];
        let accepting_block_hash = [2u8; 32];
        let handshake_key = create_test_handshake_key();
        let daa_score = 12345u64;
        
        // Create trigger
        trigger_partition
            .create_resolve_daa_score_trigger(&mut wtx, tx_id, accepting_block_hash, handshake_key)
            .unwrap();
        
        // Create acceptance info with DAA score
        let mut acceptances = BTreeMap::new();
        acceptances.insert(
            RpcTransactionId::from_slice(&tx_id),
            AcceptanceInfo {
                accepting_block_hash: Some(RpcHash::from_bytes(accepting_block_hash)),
                accepting_block_daa_score: Some(daa_score),
                sender_script_public_key: None,
            },
        );
        
        // Create mock sender handshake partition
        let sender_partition = SenderHandshakeBySenderPartition::new(&wtx).unwrap();
        
        // Execute triggers
        let results = trigger_partition
            .execute_triggers_for_acceptances(&mut wtx, &acceptances, &sender_partition)
            .unwrap();
        
        // Verify results
        assert_eq!(results.len(), 1);
        match &results[0] {
            TriggerExecutionResult::DaaScoreResolved {
                tx_id: result_tx_id,
                daa_score: result_score,
            } => {
                assert_eq!(*result_tx_id, tx_id);
                assert_eq!(*result_score, daa_score);
            }
            _ => panic!("Expected DaaScoreResolved result"),
        }
        
        // Verify next trigger was created
        let sender_triggers: Vec<_> = trigger_partition
            .get_triggers_for_action(&wtx.read(), TriggerAction::UpdateHandshakeSender)
            .collect();
        assert_eq!(sender_triggers.len(), 1);
    }

    #[test]
    fn test_execute_resolve_daa_score_trigger_pending() {
        let (_temp_dir, keyspace) = create_test_keyspace();
        let tx_keyspace = keyspace.begin_write().unwrap();
        let mut wtx = tx_keyspace;
        let trigger_partition = TriggerPartition::new(&wtx).unwrap();
        
        let tx_id = [1u8; 32];
        let accepting_block_hash = [2u8; 32];
        let handshake_key = create_test_handshake_key();
        
        // Create trigger
        trigger_partition
            .create_resolve_daa_score_trigger(&mut wtx, tx_id, accepting_block_hash, handshake_key)
            .unwrap();
        
        // Create acceptance info without DAA score
        let mut acceptances = BTreeMap::new();
        acceptances.insert(
            RpcTransactionId::from_slice(&tx_id),
            AcceptanceInfo {
                accepting_block_hash: Some(RpcHash::from_bytes(accepting_block_hash)),
                accepting_block_daa_score: None,
                sender_script_public_key: None,
            },
        );
        
        // Create mock sender handshake partition
        let sender_partition = SenderHandshakeBySenderPartition::new(&wtx).unwrap();
        
        // Execute triggers
        let results = trigger_partition
            .execute_triggers_for_acceptances(&mut wtx, &acceptances, &sender_partition)
            .unwrap();
        
        // Verify results
        assert_eq!(results.len(), 1);
        match &results[0] {
            TriggerExecutionResult::DaaScoreResolutionNeeded {
                tx_id: result_tx_id,
                accepting_block_hash: result_hash,
            } => {
                assert_eq!(*result_tx_id, tx_id);
                assert_eq!(*result_hash, accepting_block_hash);
            }
            _ => panic!("Expected DaaScoreResolutionNeeded result"),
        }
        
        // Verify no next trigger was created
        let sender_triggers: Vec<_> = trigger_partition
            .get_triggers_for_action(&wtx.read(), TriggerAction::UpdateHandshakeSender)
            .collect();
        assert_eq!(sender_triggers.len(), 0);
    }

    #[test]
    fn test_execute_update_handshake_sender_trigger_success() {
        let (_temp_dir, keyspace) = create_test_keyspace();
        let tx_keyspace = keyspace.begin_write().unwrap();
        let mut wtx = tx_keyspace;
        let trigger_partition = TriggerPartition::new(&wtx).unwrap();
        
        let tx_id = [1u8; 32];
        let accepting_daa_score = 12345u64;
        let accepting_block_hash = [2u8; 32];
        let handshake_key = create_test_handshake_key();
        let script_public_key = create_test_script_public_key();
        
        // Create trigger
        trigger_partition
            .create_update_handshake_sender_trigger(
                &mut wtx,
                tx_id,
                accepting_daa_score,
                accepting_block_hash,
                handshake_key,
            )
            .unwrap();
        
        // Create acceptance info with sender script
        let mut acceptances = BTreeMap::new();
        acceptances.insert(
            RpcTransactionId::from_slice(&tx_id),
            AcceptanceInfo {
                accepting_block_hash: Some(RpcHash::from_bytes(accepting_block_hash)),
                accepting_block_daa_score: Some(accepting_daa_score),
                sender_script_public_key: Some(script_public_key),
            },
        );
        
        // Create sender handshake partition
        let sender_partition = SenderHandshakeBySenderPartition::new(&wtx).unwrap();
        
        // Execute triggers
        let results = trigger_partition
            .execute_triggers_for_acceptances(&mut wtx, &acceptances, &sender_partition)
            .unwrap();
        
        // Verify results
        assert_eq!(results.len(), 1);
        match &results[0] {
            TriggerExecutionResult::HandshakeUpdated {
                tx_id: result_tx_id,
                sender_address,
                handshake_key: result_key,
            } => {
                assert_eq!(*result_tx_id, tx_id);
                assert_eq!(result_key.tx_id, handshake_key.tx_id);
                // Verify sender address was computed correctly
                assert_ne!(sender_address.inverse_version, 0); // Should be set
            }
            _ => panic!("Expected HandshakeUpdated result"),
        }
    }

    #[test]
    fn test_execute_update_handshake_sender_trigger_pending() {
        let (_temp_dir, keyspace) = create_test_keyspace();
        let tx_keyspace = keyspace.begin_write().unwrap();
        let mut wtx = tx_keyspace;
        let trigger_partition = TriggerPartition::new(&wtx).unwrap();
        
        let tx_id = [1u8; 32];
        let accepting_daa_score = 12345u64;
        let accepting_block_hash = [2u8; 32];
        let handshake_key = create_test_handshake_key();
        
        // Create trigger
        trigger_partition
            .create_update_handshake_sender_trigger(
                &mut wtx,
                tx_id,
                accepting_daa_score,
                accepting_block_hash,
                handshake_key,
            )
            .unwrap();
        
        // Create acceptance info without sender script
        let mut acceptances = BTreeMap::new();
        acceptances.insert(
            RpcTransactionId::from_slice(&tx_id),
            AcceptanceInfo {
                accepting_block_hash: Some(RpcHash::from_bytes(accepting_block_hash)),
                accepting_block_daa_score: Some(accepting_daa_score),
                sender_script_public_key: None,
            },
        );
        
        // Create sender handshake partition
        let sender_partition = SenderHandshakeBySenderPartition::new(&wtx).unwrap();
        
        // Execute triggers
        let results = trigger_partition
            .execute_triggers_for_acceptances(&mut wtx, &acceptances, &sender_partition)
            .unwrap();
        
        // Verify results
        assert_eq!(results.len(), 1);
        match &results[0] {
            TriggerExecutionResult::SenderResolutionNeeded {
                tx_id: result_tx_id,
                daa_score: result_score,
                accepting_block_hash: result_hash,
                handshake_key: result_key,
            } => {
                assert_eq!(*result_tx_id, tx_id);
                assert_eq!(*result_score, accepting_daa_score);
                assert_eq!(*result_hash, accepting_block_hash);
                assert_eq!(result_key.tx_id, handshake_key.tx_id);
            }
            _ => panic!("Expected SenderResolutionNeeded result"),
        }
    }

    #[test]
    fn test_complete_workflow_success() {
        let (_temp_dir, keyspace) = create_test_keyspace();
        let tx_keyspace = keyspace.begin_write().unwrap();
        let mut wtx = tx_keyspace;
        let trigger_partition = TriggerPartition::new(&wtx).unwrap();
        let sender_partition = SenderHandshakeBySenderPartition::new(&wtx).unwrap();
        
        let tx_id = [1u8; 32];
        let handshake_key = create_test_handshake_key();
        let accepting_block_hash = RpcHash::from_bytes([2u8; 32]);
        let daa_score = 12345u64;
        let script_public_key = create_test_script_public_key();
        
        // Step 1: Create initial trigger
        trigger_partition
            .create_resolve_accepting_block_trigger(tx_id, handshake_key)
            .unwrap();
        
        // Step 2: Execute with accepting block info
        let mut acceptances = BTreeMap::new();
        acceptances.insert(
            RpcTransactionId::from_slice(&tx_id),
            AcceptanceInfo {
                accepting_block_hash: Some(accepting_block_hash),
                accepting_block_daa_score: None,
                sender_script_public_key: None,
            },
        );
        
        let results = trigger_partition
            .execute_triggers_for_acceptances(&mut wtx, &acceptances, &sender_partition)
            .unwrap();
        assert_eq!(results.len(), 1);
        
        // Step 3: Execute with DAA score info
        acceptances.get_mut(&RpcTransactionId::from_slice(&tx_id))
            .unwrap()
            .accepting_block_daa_score = Some(daa_score);
        
        let results = trigger_partition
            .execute_triggers_for_acceptances(&mut wtx, &acceptances, &sender_partition)
            .unwrap();
        assert_eq!(results.len(), 1);
        
        // Step 4: Execute with sender script info
        acceptances.get_mut(&RpcTransactionId::from_slice(&tx_id))
            .unwrap()
            .sender_script_public_key = Some(script_public_key);
        
        let results = trigger_partition
            .execute_triggers_for_acceptances(&mut wtx, &acceptances, &sender_partition)
            .unwrap();
        assert_eq!(results.len(), 1);
        
        // Verify final result is handshake update
        match &results[0] {
            TriggerExecutionResult::HandshakeUpdated { .. } => {}
            _ => panic!("Expected HandshakeUpdated result"),
        }
        
        // Verify no triggers remain
        let remaining_triggers: Vec<_> = trigger_partition
            .get_triggers_for_tx_ids_wtx(&mut wtx, &[RpcTransactionId::from_slice(&tx_id)])
            .collect();
        assert_eq!(remaining_triggers.len(), 0);
    }

    #[test]
    fn test_reorg_scenario() {
        let (_temp_dir, keyspace) = create_test_keyspace();
        let tx_keyspace = keyspace.begin_write().unwrap();
        let mut wtx = tx_keyspace;
        let trigger_partition = TriggerPartition::new(&wtx).unwrap();
        
        let tx_id1 = [1u8; 32];
        let tx_id2 = [2u8; 32];
        let handshake_key = create_test_handshake_key();
        
        // Create triggers for multiple transactions
        trigger_partition
            .create_resolve_accepting_block_trigger(tx_id1, handshake_key)
            .unwrap();
        trigger_partition
            .create_resolve_accepting_block_trigger(tx_id2, handshake_key)
            .unwrap();
        trigger_partition
            .create_resolve_daa_score_trigger(&mut wtx, tx_id1, [3u8; 32], handshake_key)
            .unwrap();
        
        // Verify triggers exist
        let all_triggers: Vec<_> = trigger_partition
            .get_triggers_for_tx_ids_wtx(&mut wtx, &[
                RpcTransactionId::from_slice(&tx_id1),
                RpcTransactionId::from_slice(&tx_id2),
            ])
            .collect();
        assert_eq!(all_triggers.len(), 3);
        
        // Simulate reorg - remove triggers for tx_id1
        trigger_partition
            .remove_triggers_for_tx_ids(&mut wtx, &[RpcTransactionId::from_slice(&tx_id1)])
            .unwrap();
        
        // Verify only tx_id2 triggers remain
        let remaining_triggers: Vec<_> = trigger_partition
            .get_triggers_for_tx_ids_wtx(&mut wtx, &[
                RpcTransactionId::from_slice(&tx_id1),
                RpcTransactionId::from_slice(&tx_id2),
            ])
            .collect();
        assert_eq!(remaining_triggers.len(), 1);
        
        let (remaining_trigger, _) = remaining_triggers[0].as_ref().unwrap();
        assert_eq!(remaining_trigger.tx_id, tx_id2);
        
        // Create new trigger for tx_id1 after reorg
        trigger_partition
            .create_resolve_accepting_block_trigger(tx_id1, handshake_key)
            .unwrap();
        
        // Verify new trigger was created
        let new_triggers: Vec<_> = trigger_partition
            .get_triggers_for_tx_ids_wtx(&mut wtx, &[RpcTransactionId::from_slice(&tx_id1)])
            .collect();
        assert_eq!(new_triggers.len(), 1);
    }

    #[test]
    fn test_trigger_priority_ordering() {
        let (_temp_dir, keyspace) = create_test_keyspace();
        let tx_keyspace = keyspace.begin_write().unwrap();
        let mut wtx = tx_keyspace;
        let trigger_partition = TriggerPartition::new(&wtx).unwrap();
        
        let tx_id = [1u8; 32];
        let handshake_key = create_test_handshake_key();
        
        // Create triggers in reverse priority order
        trigger_partition
            .create_update_handshake_sender_trigger(&mut wtx, tx_id, 12345, [3u8; 32], handshake_key)
            .unwrap();
        trigger_partition
            .create_resolve_daa_score_trigger(&mut wtx, tx_id, [2u8; 32], handshake_key)
            .unwrap();
        trigger_partition
            .create_resolve_accepting_block_trigger(tx_id, handshake_key)
            .unwrap();
        
        // Get all triggers for this tx_id
        let triggers: Vec<_> = trigger_partition
            .get_triggers_for_tx_ids_wtx(&mut wtx, &[RpcTransactionId::from_slice(&tx_id)])
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        
        assert_eq!(triggers.len(), 3);
        
        // Verify they are ordered by priority (0, 1, 2)
        let priorities: Vec<u8> = triggers.iter().map(|(k, _)| k.priority).collect();
        assert_eq!(priorities, vec![0, 1, 2]);
        
        // Verify actions match priorities
        let actions: Vec<u8> = triggers.iter().map(|(k, _)| k.action).collect();
        assert_eq!(actions, vec![
            TriggerAction::ResolveAcceptingBlock as u8,
            TriggerAction::ResolveDaaScore as u8,
            TriggerAction::UpdateHandshakeSender as u8,
        ]);
    }
}