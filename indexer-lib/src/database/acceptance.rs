use bytemuck::{AnyBitPattern, NoUninit};
use fjall::{PartitionCreateOptions, ReadTransaction, UserKey, WriteTransaction};
use std::marker::PhantomData;
use std::ops::Deref;
use crate::database::PartitionId;
use crate::database::resolution_keys::{
    HandshakeKeyForResolution, LikeHandshakeKeyForResolution,
    ContextualMessageKeyForResolution, LikeContextualMessageKeyForResolution,
    PaymentKeyForResolution, LikePaymentKeyForResolution,
};
use anyhow::Result;

/// Enum for accepting block resolution data types
#[derive(Debug, Clone)]
pub enum AcceptingBlockResolutionData {
    HandshakeKey(LikeHandshakeKeyForResolution<UserKey>),
    ContextualMessageKey(LikeContextualMessageKeyForResolution<UserKey>),
    PaymentKey(LikePaymentKeyForResolution<UserKey>),
    None,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
pub struct TxAcceptanceKey {
    pub accepted_by_block_hash: [u8; 32],

    pub accepted_at_daa: [u8; 8], // be
    pub tx_id: [u8; 32],
}

#[repr(transparent)]
#[derive(Clone, PartialEq, Eq)]
pub struct LikeTxAcceptanceKey<T: AsRef<[u8]>> {
    bts: T,
    phantom_data: PhantomData<TxAcceptanceKey>,
}

impl<T: AsRef<[u8]>> LikeTxAcceptanceKey<T> {
    fn new(bts: T) -> Self {
        Self {
            bts,
            phantom_data: PhantomData,
        }
    }
}

impl<T: AsRef<[u8]>> Deref for LikeTxAcceptanceKey<T> {
    type Target = TxAcceptanceKey;

    fn deref(&self) -> &Self::Target {
        bytemuck::from_bytes(self.bts.as_ref())
    }
}

pub struct AcceptanceToTxIDPartition(fjall::TxPartition);

impl AcceptanceToTxIDPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> anyhow::Result<Self> {
        Ok(Self(keyspace.open_partition(
            "acceptance_to_tx_id",
            PartitionCreateOptions::default(),
        )?))
    }

    pub fn insert(
        &self,
        tx_id: [u8; 32],
        acceptance: Option<([u8; 32], [u8; 8])>,
    ) -> anyhow::Result<()> {
        let acceptance = acceptance.unwrap_or_default();
        let key = TxAcceptanceKey {
            tx_id,
            accepted_at_daa: acceptance.1,
            accepted_by_block_hash: acceptance.0,
        };
        Ok(self.0.insert(bytemuck::bytes_of(&key), [])?)
    }

    pub fn insert_wtx(
        &self,
        wtx: &mut WriteTransaction,
        tx_id: [u8; 32],
    ) {
        let key = TxAcceptanceKey {
            tx_id,
            accepted_at_daa: Default::default(),
            accepted_by_block_hash: Default::default(),
        };
        wtx.insert(&self.0, bytemuck::bytes_of(&key), []);
    }

    pub fn unknown_acceptance(
        &self,
        rtx: &ReadTransaction,
    ) -> impl DoubleEndedIterator<Item = anyhow::Result<LikeTxAcceptanceKey<UserKey>>> {
        let mut to = [0u8; 40];
        to[39] = 1;
        rtx.range(&self.0, ..to).map(|r| {
            r.map_err(Into::into)
                .map(|(bts, _)| LikeTxAcceptanceKey::new(bts))
        })
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
pub struct AcceptanceTxKey {
    pub tx_id: [u8; 32],
    pub accepted_at_daa: [u8; 8], // be
    pub accepted_by_block_hash: [u8; 32],
    pub partition_id: u8,         // PartitionId as u8 (added to end as requested)
}

#[repr(transparent)]
#[derive(Clone, PartialEq, Eq)]
pub struct LikeAcceptanceTxKey<T: AsRef<[u8]>> {
    bts: T,
    phantom_data: PhantomData<AcceptanceTxKey>,
}

// impl<T: AsRef<[u8]>> LikeAcceptanceTxKey<T> {
//     fn new(bts: T) -> Self {
//         Self {
//             bts,
//             phantom_data: PhantomData,
//         }
//     }
// }

impl<T: AsRef<[u8]>> Deref for LikeAcceptanceTxKey<T> {
    type Target = AcceptanceTxKey;

    fn deref(&self) -> &Self::Target {
        bytemuck::from_bytes(self.bts.as_ref())
    }
}

pub struct TxIDToAcceptancePartition(fjall::TxPartition);

impl TxIDToAcceptancePartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> anyhow::Result<Self> {
        Ok(Self(keyspace.open_partition(
            "tx_id_to_acceptance",
            PartitionCreateOptions::default(),
        )?))
    }

    /// Insert a handshake ForResolution key
    pub fn insert_handshake_wtx(
        &self,
        wtx: &mut WriteTransaction,
        tx_id: [u8; 32],
        handshake_key: &HandshakeKeyForResolution,
    ) {
        let key = AcceptanceTxKey {
            tx_id,
            accepted_at_daa: Default::default(),
            accepted_by_block_hash: Default::default(),
            partition_id: PartitionId::HandshakeBySender as u8,
        };
        wtx.insert(&self.0, bytemuck::bytes_of(&key), bytemuck::bytes_of(handshake_key));
    }

    /// Insert a contextual message ForResolution key
    pub fn insert_contextual_message_wtx(
        &self,
        wtx: &mut WriteTransaction,
        tx_id: [u8; 32],
        contextual_message_key: &ContextualMessageKeyForResolution,
    ) -> Result<()> {
        let key = AcceptanceTxKey {
            tx_id,
            accepted_at_daa:  Default::default(),
            accepted_by_block_hash:  Default::default(),
            partition_id: PartitionId::ContextualMessageBySender as u8,
        };
        wtx.insert(&self.0, bytemuck::bytes_of(&key), bytemuck::bytes_of(contextual_message_key));
        Ok(())
    }

    /// Insert a payment ForResolution key
    pub fn insert_payment_wtx(
        &self,
        wtx: &mut WriteTransaction,
        tx_id: [u8; 32],
        payment_key: &PaymentKeyForResolution,
    ) -> Result<()> {
        let key = AcceptanceTxKey {
            tx_id,
            accepted_at_daa: Default::default(),
            accepted_by_block_hash:  Default::default(),
            partition_id: PartitionId::PaymentBySender as u8,
        };
        wtx.insert(&self.0, bytemuck::bytes_of(&key), bytemuck::bytes_of(payment_key));
        Ok(())
    }

    /// Get all accepting block resolution data for a specific transaction ID
    /// Returns iterator of (PartitionId, AcceptingBlockResolutionData) pairs
    pub fn get_by_tx_id(
        &self,
        rtx: &ReadTransaction,
        tx_id: [u8; 32],
    ) -> impl DoubleEndedIterator<Item = Result<(PartitionId, AcceptingBlockResolutionData)>> + '_ {
        rtx.prefix(&self.0, tx_id).map(|r| {
            let (key_bytes, value_bytes) = r?;
            if key_bytes.len() == 73 { // 32 + 8 + 32 + 1
                let key: AcceptanceTxKey = *bytemuck::from_bytes(&key_bytes);
                
                let partition_id = match key.partition_id {
                    x if x == PartitionId::HandshakeBySender as u8 => PartitionId::HandshakeBySender,
                    x if x == PartitionId::ContextualMessageBySender as u8 => PartitionId::ContextualMessageBySender,
                    x if x == PartitionId::PaymentBySender as u8 => PartitionId::PaymentBySender,
                    _ => return Err(anyhow::anyhow!("Invalid partition ID: {}", key.partition_id)),
                };
                
                let resolution_data = match partition_id {
                    PartitionId::HandshakeBySender => AcceptingBlockResolutionData::HandshakeKey(
                        LikeHandshakeKeyForResolution::new(value_bytes)
                    ),
                    PartitionId::ContextualMessageBySender => AcceptingBlockResolutionData::ContextualMessageKey(
                        LikeContextualMessageKeyForResolution::new(value_bytes)
                    ),
                    PartitionId::PaymentBySender => AcceptingBlockResolutionData::PaymentKey(
                        LikePaymentKeyForResolution::new(value_bytes)
                    ),
                    _ => return Err(anyhow::anyhow!("Invalid partition ID for accepting block resolution: {:?}", partition_id)),
                };
                
                Ok((partition_id, resolution_data))
            } else {
                Err(anyhow::anyhow!("Invalid key length in tx_id_to_acceptance partition"))
            }
        })
    }

    // todo resolution
    
    // /// Remove all accepting block resolution data for a specific transaction ID
    // /// Returns all the resolution data that was removed
    // pub fn remove_by_tx_id(
    //     &self,
    //     wtx: &mut WriteTransaction,
    //     tx_id: [u8; 32],
    // ) -> Result<Vec<(PartitionId, AcceptingBlockResolutionData)>> {
    //     let prefix = tx_id.as_slice();
    //     let mut results = Vec::new();
    //     let mut keys_to_remove = Vec::new();
    //     
    //     // First collect all entries
    //     for item in wtx.prefix(&self.0, prefix) {
    //         let (key_bytes, value_bytes) = item?;
    //         if key_bytes.len() == 73 { // 32 + 8 + 32 + 1
    //             let key: AcceptanceTxKey = *bytemuck::from_bytes(&key_bytes);
    //             keys_to_remove.push(key);
    //             
    //             let partition_id = match key.partition_id {
    //                 x if x == PartitionId::HandshakeBySender as u8 => PartitionId::HandshakeBySender,
    //                 x if x == PartitionId::ContextualMessageBySender as u8 => PartitionId::ContextualMessageBySender,
    //                 x if x == PartitionId::PaymentBySender as u8 => PartitionId::PaymentBySender,
    //                 x if x == PartitionId::None as u8 => PartitionId::None,
    //                 _ => return Err(anyhow::anyhow!("Invalid partition ID: {}", key.partition_id)),
    //             };
    //             
    //             let resolution_data = match partition_id {
    //                 PartitionId::HandshakeBySender => AcceptingBlockResolutionData::HandshakeKey(
    //                     LikeHandshakeKeyForResolution::new(value_bytes)
    //                 ),
    //                 PartitionId::ContextualMessageBySender => AcceptingBlockResolutionData::ContextualMessageKey(
    //                     LikeContextualMessageKeyForResolution::new(value_bytes)
    //                 ),
    //                 PartitionId::PaymentBySender => AcceptingBlockResolutionData::PaymentKey(
    //                     LikePaymentKeyForResolution::new(value_bytes)
    //                 ),
    //                 PartitionId::None => AcceptingBlockResolutionData::None,
    //                 _ => return Err(anyhow::anyhow!("Invalid partition ID for accepting block resolution: {:?}", partition_id)),
    //             };
    //             
    //             results.push((partition_id, resolution_data));
    //         }
    //     }
    //     
    //     // Then remove all collected keys
    //     for key in keys_to_remove {
    //         wtx.remove(&self.0, bytemuck::bytes_of(&key));
    //     }
    //     
    //     Ok(results)
    // }
}
