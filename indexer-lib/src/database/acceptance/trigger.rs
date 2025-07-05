// use std::collections::{BTreeMap};
// use anyhow::bail;
// use bytemuck::{AnyBitPattern, NoUninit};
// use fjall::PartitionCreateOptions;
// use kaspa_rpc_core::{RpcHash, RpcTransactionId};
// use crate::database::handshake::HandshakeKeyByReceiver;
// use crate::database::PartitionId;
//
// #[repr(u8)]
// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
// pub enum Action {
//     UpdateAndInsertHandshakeSender,
// }
//
// pub type UpdateAndInsertHandshakeSenderTriggerArg = HandshakeKeyByReceiver;
//
// #[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
// #[repr(C)]
// pub struct TriggerKey {
//     tx_id: [u8; 32],
//     partition_id: u8,
//     action: u8,
//     delete_after_execution: u8,
// }
//
// #[derive(Clone)]
// pub struct TriggerPartition(fjall::TxPartition);
//
// impl TriggerPartition {
//     pub fn new(keyspace: &fjall::TxKeyspace) -> anyhow::Result<Self> {
//         Ok(Self(keyspace.open_partition(
//             "acceptance_trigger",
//             PartitionCreateOptions::default(),
//         )?))
//     }
//
//     pub fn create_trigger_update_and_insert_handshake_sender(&self, tx_id: [u8; 32], partition_id: PartitionId, action: Action, arg: &UpdateAndInsertHandshakeSenderTriggerArg) -> anyhow::Result<()> {
//         let key = TriggerKey{
//             tx_id,
//             partition_id: partition_id as u8,
//             action: action as u8,
//             delete_after_execution: true as u8,
//         };
//
//         self.0.insert(bytemuck::bytes_of(&key), bytemuck::bytes_of(arg))?;
//         Ok(())
//     }
//
//     pub fn execute_triggers(&self, tx_ids: &BTreeMap<RpcTransactionId, AcceptanceSomething>) -> anyhow::Result<()> {
//         if tx_ids.is_empty() {
//             bail!("No tx_ids to execute triggers for");
//         }
//         let first = *tx_ids.first().as_ref().unwrap();
//         let last = *tx_ids.last().as_ref().unwrap();
//         let
//     }
// }
//
// pub struct AcceptanceSomething {
//     pub accepting_block: RpcHash,
//     pub accepting_block_daa_score: u64,
// }
