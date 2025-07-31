use crate::database::resolution_keys::{
    ContextualMessageKeyForResolution, HandshakeKeyForResolution,
    LikeContextualMessageKeyForResolution, LikeHandshakeKeyForResolution,
    LikePaymentKeyForResolution, PaymentKeyForResolution,
};
use crate::database::{LikeTxIds, PartitionId};
use anyhow::Result;
use bytemuck::{AnyBitPattern, NoUninit};
use fjall::{
    KvSeparationOptions, PartitionCreateOptions, ReadTransaction, UserKey, UserValue,
    WriteTransaction,
};
use kaspa_rpc_core::{RpcHash, RpcTransactionId};
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::Deref;
use tracing::{debug, log, warn};

/// Enum for accepting block resolution data types
#[derive(Debug, Clone)]
pub enum AcceptingBlockResolutionData {
    HandshakeKey(LikeHandshakeKeyForResolution<UserKey>),
    ContextualMessageKey(LikeContextualMessageKeyForResolution<UserKey>),
    PaymentKey(LikePaymentKeyForResolution<UserKey>),
    None,
}

#[derive(Clone)]
pub struct AcceptingBlockToTxIDPartition(fjall::TxPartition);

impl AcceptingBlockToTxIDPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> anyhow::Result<Self> {
        Ok(Self(keyspace.open_partition(
            "accepting_block_to_tx_id",
            PartitionCreateOptions::default().with_kv_separation(KvSeparationOptions::default()),
        )?))
    }

    pub fn insert_wtx(
        &self,
        wtx: &mut WriteTransaction,
        accepted_by_block_hash: &RpcHash,
        tx_ids: &[[u8; 32]],
    ) {
        wtx.insert(
            &self.0,
            accepted_by_block_hash.as_bytes(),
            tx_ids.as_flattened(),
        );
    }

    pub fn remove_wtx(
        &self,
        wtx: &mut WriteTransaction,
        accepted_by_block_hash: &RpcHash,
    ) -> Result<Option<LikeTxIds<UserValue>>> {
        let old = wtx.fetch_update(&self.0, accepted_by_block_hash.as_bytes(), |_old| None)?;
        Ok(old.map(LikeTxIds::new))
    }

    pub fn remove(&self, accepted_by_block_hash: &RpcHash) -> Result<()> {
        Ok(self.0.inner().remove(accepted_by_block_hash.as_bytes())?)
    }

    pub fn get_rtx(
        &self,
        rtx: &ReadTransaction,
        accepted_by_block_hash: &RpcHash,
    ) -> Result<Option<LikeTxIds<UserValue>>> {
        Ok(rtx
            .get(&self.0, accepted_by_block_hash.as_bytes())?
            .map(LikeTxIds::new))
    }

    pub fn get_wtx(
        &self,
        wtx: &mut WriteTransaction,
        accepted_by_block_hash: &RpcHash,
    ) -> Result<Option<LikeTxIds<UserValue>>> {
        Ok(wtx
            .get(&self.0, accepted_by_block_hash.as_bytes())?
            .map(LikeTxIds::new))
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
pub struct AcceptanceTxKey {
    pub tx_id: [u8; 32],
    pub accepted_at_daa: [u8; 8], // be
    pub accepted_by_block_hash: [u8; 32],
    pub partition_id: u8,
}

#[repr(transparent)]
#[derive(Clone, PartialEq, Eq)]
pub struct LikeAcceptanceTxKey<T: AsRef<[u8]> = UserKey> {
    bts: T,
    phantom_data: PhantomData<AcceptanceTxKey>,
}

impl<T: AsRef<[u8]>> Debug for LikeAcceptanceTxKey<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

impl<T: AsRef<[u8]>> LikeAcceptanceTxKey<T> {
    fn new(bts: T) -> Self {
        let _ = bytemuck::from_bytes::<AcceptanceTxKey>(bts.as_ref());
        Self {
            bts,
            phantom_data: PhantomData,
        }
    }

    fn inner(self) -> T {
        self.bts
    }
}

impl<T: AsRef<[u8]>> Deref for LikeAcceptanceTxKey<T> {
    type Target = AcceptanceTxKey;

    fn deref(&self) -> &Self::Target {
        bytemuck::from_bytes(self.bts.as_ref())
    }
}

#[derive(Clone)]
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
        accepted_at_daa: Option<u64>,
        accepted_by_block_hash: Option<[u8; 32]>,
    ) {
        let key = AcceptanceTxKey {
            tx_id,
            accepted_at_daa: accepted_at_daa.unwrap_or_default().to_be_bytes(),
            accepted_by_block_hash: accepted_by_block_hash.unwrap_or_default(),
            partition_id: PartitionId::HandshakeBySender as u8,
        };
        wtx.insert(
            &self.0,
            bytemuck::bytes_of(&key),
            bytemuck::bytes_of(handshake_key),
        );
    }

    /// Insert a contextual message ForResolution key
    pub fn insert_contextual_message_wtx(
        &self,
        wtx: &mut WriteTransaction,
        tx_id: [u8; 32],
        contextual_message_key: &ContextualMessageKeyForResolution,
        accepted_at_daa: Option<u64>,
        accepted_by_block_hash: Option<[u8; 32]>,
    ) {
        let key = AcceptanceTxKey {
            tx_id,
            accepted_at_daa: accepted_at_daa.unwrap_or_default().to_be_bytes(),
            accepted_by_block_hash: accepted_by_block_hash.unwrap_or_default(),
            partition_id: PartitionId::ContextualMessageBySender as u8,
        };
        wtx.insert(
            &self.0,
            bytemuck::bytes_of(&key),
            bytemuck::bytes_of(contextual_message_key),
        );
    }

    /// Insert a payment ForResolution key
    pub fn insert_payment_wtx(
        &self,
        wtx: &mut WriteTransaction,
        tx_id: [u8; 32],
        payment_key: &PaymentKeyForResolution,
        accepted_at_daa: Option<u64>,
        accepted_by_block_hash: Option<[u8; 32]>,
    ) {
        let key = AcceptanceTxKey {
            tx_id,
            accepted_at_daa: accepted_at_daa.unwrap_or_default().to_be_bytes(),
            accepted_by_block_hash: accepted_by_block_hash.unwrap_or_default(),
            partition_id: PartitionId::PaymentBySender as u8,
        };
        wtx.insert(
            &self.0,
            bytemuck::bytes_of(&key),
            bytemuck::bytes_of(payment_key),
        );
    }

    pub fn insert_wtx(
        &self,
        wtx: &mut WriteTransaction,
        key: &AcceptanceTxKey,
        value: AcceptingBlockResolutionData,
    ) {
        wtx.insert(
            &self.0,
            bytemuck::bytes_of(key),
            match value {
                AcceptingBlockResolutionData::None => {
                    let tx = RpcTransactionId::from_bytes(key.tx_id);
                    warn!(tx = %tx, "insert_wtx called with AcceptingBlockResolutionData::None");
                    UserValue::from([])
                }
                AcceptingBlockResolutionData::HandshakeKey(hk) => hk.inner(),
                AcceptingBlockResolutionData::ContextualMessageKey(cmk) => cmk.inner(),
                AcceptingBlockResolutionData::PaymentKey(pmk) => pmk.inner(),
            },
        )
    }

    /// Insert a payment ForResolution key
    pub fn raw_keys_by_txid(
        &self,
        rtx: &ReadTransaction,
        tx_id: [u8; 32],
    ) -> impl Iterator<Item = Result<UserKey>> {
        rtx.prefix(&self.0, tx_id)
            .map(|r| r.map(|kv| kv.0).map_err(Into::into))
    }

    pub fn resolve_by_raw_keys<I: Iterator<Item = Result<UserKey>>>(
        &self,
        wtx: &mut WriteTransaction,
        iter: I,
    ) -> Result<()> {
        if log::log_enabled!(log::Level::Debug) {
            let mut resolved_txs = Vec::new();
            for k in iter {
                let k = k?;
                let lk = LikeAcceptanceTxKey::new(k.clone());
                resolved_txs.push(RpcTransactionId::from_bytes(lk.tx_id));
                wtx.update_fetch(&self.0, k, |_old| Some([].into()))?;
            }
            debug!("resolved_txs: {:?}", resolved_txs);
        } else {
            for k in iter {
                wtx.update_fetch(&self.0, k?, |_old| Some([].into()))?;
            }
        }

        Ok(())
    }

    /// Get all accepting block resolution data for a specific transaction ID
    /// Returns iterator of (PartitionId, AcceptingBlockResolutionData) pairs
    pub fn get_by_tx_id(
        &self,
        rtx: &ReadTransaction,
        tx_id: &[u8; 32],
    ) -> impl DoubleEndedIterator<
        Item = Result<(LikeAcceptanceTxKey<UserKey>, AcceptingBlockResolutionData)>,
    > + '_ {
        rtx.prefix(&self.0, tx_id).map(|r| {
            let (key_bytes, value_bytes) = r?;
            if key_bytes.len() == 73 {
                // 32 + 8 + 32 + 1
                let key = LikeAcceptanceTxKey::new(key_bytes);

                let partition_id = match key.partition_id {
                    x if x == PartitionId::HandshakeBySender as u8 => {
                        PartitionId::HandshakeBySender
                    }
                    x if x == PartitionId::ContextualMessageBySender as u8 => {
                        PartitionId::ContextualMessageBySender
                    }
                    x if x == PartitionId::PaymentBySender as u8 => PartitionId::PaymentBySender,
                    _ => {
                        return Err(anyhow::anyhow!(
                            "Invalid partition ID: {}",
                            key.partition_id
                        ));
                    }
                };

                let resolution_data = if value_bytes.is_empty() {
                    AcceptingBlockResolutionData::None
                } else {
                    match partition_id {
                        PartitionId::HandshakeBySender => {
                            AcceptingBlockResolutionData::HandshakeKey(
                                LikeHandshakeKeyForResolution::new(value_bytes),
                            )
                        }
                        PartitionId::ContextualMessageBySender => {
                            AcceptingBlockResolutionData::ContextualMessageKey(
                                LikeContextualMessageKeyForResolution::new(value_bytes),
                            )
                        }
                        PartitionId::PaymentBySender => AcceptingBlockResolutionData::PaymentKey(
                            LikePaymentKeyForResolution::new(value_bytes),
                        ),
                        _ => {
                            return Err(anyhow::anyhow!(
                                "Invalid partition ID for accepting block resolution: {:?}",
                                partition_id
                            ));
                        }
                    }
                };

                Ok((key, resolution_data))
            } else {
                Err(anyhow::anyhow!(
                    "Invalid key length in tx_id_to_acceptance partition"
                ))
            }
        })
    }

    pub fn remove(&self, wtx: &mut WriteTransaction, key: LikeAcceptanceTxKey<UserKey>) {
        wtx.remove(&self.0, key.inner())
    }

    /// Remove all accepting block resolution data for a specific transaction ID
    /// Returns all the resolution data that was removed
    pub fn remove_by_tx_id(&self, wtx: &mut WriteTransaction, tx_id: [u8; 32]) -> Result<()> {
        let prefix = tx_id.as_slice();
        let mut keys_to_remove = Vec::new();

        // First collect all entries
        for item in wtx.prefix(&self.0, prefix) {
            let (key_bytes, _value_bytes) = item?;
            keys_to_remove.push(key_bytes);
        }

        // Then remove all collected keys
        for key in keys_to_remove {
            wtx.remove(&self.0, key);
        }

        Ok(())
    }
}
