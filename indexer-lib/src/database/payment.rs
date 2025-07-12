use crate::database::handshake::AddressPayload;
use anyhow::bail;
use bytemuck::{AnyBitPattern, NoUninit};
use fjall::{PartitionCreateOptions, WriteTransaction};
use kaspa_rpc_core::RpcTransactionId;
use std::marker::PhantomData;
use std::ops::Deref;

#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
#[repr(C)]
pub struct PaymentKeyBySender {
    pub sender: AddressPayload, // allows retrieving every tx with unknown sender
    pub block_time: [u8; 8],    // u64_be
    pub block_hash: [u8; 32],
    pub receiver: AddressPayload,
    pub version: u8,
    pub tx_id: [u8; 32],
}

#[repr(transparent)]
#[derive(Clone, PartialEq, Eq)]
pub struct LikePaymentKeyBySender<T: AsRef<[u8]>> {
    bts: T,
    phantom_data: PhantomData<PaymentKeyBySender>,
}

impl<T: AsRef<[u8]>> LikePaymentKeyBySender<T> {
    pub fn new(bts: T) -> Self {
        Self {
            bts,
            phantom_data: PhantomData,
        }
    }
}

impl<T: AsRef<[u8]>> Deref for LikePaymentKeyBySender<T> {
    type Target = PaymentKeyBySender;

    fn deref(&self) -> &Self::Target {
        bytemuck::from_bytes(self.bts.as_ref())
    }
}

#[derive(Clone)]
pub struct PaymentBySenderPartition(fjall::TxPartition);

impl PaymentBySenderPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> anyhow::Result<Self> {
        Ok(Self(keyspace.open_partition(
            "payment_by_sender",
            PartitionCreateOptions::default(),
        )?))
    }

    pub fn insert(&self, key: &PaymentKeyBySender) -> anyhow::Result<()> {
        self.0.insert(bytemuck::bytes_of(key), [])?;
        Ok(())
    }

    pub fn insert_wtx(
        &self,
        wtx: &mut WriteTransaction,
        key: &PaymentKeyBySender,
    ) -> anyhow::Result<()> {
        wtx.insert(&self.0, bytemuck::bytes_of(key), []);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
#[repr(C)]
pub struct PaymentKeyByReceiver {
    pub receiver: AddressPayload,
    pub block_time: [u8; 8], // be
    pub block_hash: [u8; 32],
    pub version: u8,
    pub tx_id: [u8; 32],
}

#[repr(transparent)]
#[derive(Clone, PartialEq, Eq)]
pub struct LikePaymentKeyByReceiver<T: AsRef<[u8]>> {
    bts: T,
    phantom_data: PhantomData<PaymentKeyByReceiver>,
}

impl<T: AsRef<[u8]>> LikePaymentKeyByReceiver<T> {
    pub fn new(bts: T) -> Self {
        Self {
            bts,
            phantom_data: PhantomData,
        }
    }
}

impl<T: AsRef<[u8]>> Deref for LikePaymentKeyByReceiver<T> {
    type Target = PaymentKeyByReceiver;

    fn deref(&self) -> &Self::Target {
        bytemuck::from_bytes(self.bts.as_ref())
    }
}

#[derive(Clone)]
pub struct PaymentByReceiverPartition(fjall::TxPartition);

impl PaymentByReceiverPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> anyhow::Result<Self> {
        Ok(Self(keyspace.open_partition(
            "payment_by_receiver",
            PartitionCreateOptions::default(),
        )?))
    }

    pub fn insert(
        &self,
        key: &PaymentKeyByReceiver,
        sender: Option<AddressPayload>,
    ) -> anyhow::Result<()> {
        let sender = sender.unwrap_or_default();
        self.0
            .insert(bytemuck::bytes_of(key), bytemuck::bytes_of(&sender))?;
        Ok(())
    }

    pub fn insert_wtx(
        &self,
        wtx: &mut WriteTransaction,
        key: &PaymentKeyByReceiver,
        sender: Option<AddressPayload>,
    ) {
        let sender = sender.unwrap_or_default();
        wtx.insert(
            &self.0,
            bytemuck::bytes_of(key),
            bytemuck::bytes_of(&sender),
        );
    }
}

#[derive(Clone)]
pub struct TxIdToPaymentPartition(fjall::TxPartition);

impl TxIdToPaymentPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> anyhow::Result<Self> {
        Ok(Self(keyspace.open_partition(
            "tx_id_to_payment",
            PartitionCreateOptions::default(),
        )?))
    }

    pub fn insert(&self, tx_id: &[u8], amount: u64, sealed_hex: &[u8]) -> anyhow::Result<()> {
        if tx_id.len() != 32 {
            bail!("Transaction ID must be 32 bytes, got {}", tx_id.len());
        }

        // Value: amount (8 bytes BE) + sealed_hex (remaining bytes)
        let mut value_bytes = Vec::with_capacity(8 + sealed_hex.len());
        value_bytes.extend_from_slice(&amount.to_be_bytes());
        value_bytes.extend_from_slice(sealed_hex);

        self.0.insert(tx_id, value_bytes)?;
        Ok(())
    }

    pub fn insert_wtx(
        &self,
        wtx: &mut WriteTransaction,
        tx_id: &[u8],
        amount: u64,
        sealed_hex: &[u8],
    ) -> anyhow::Result<()> {
        if tx_id.len() != 32 {
            bail!("Transaction ID must be 32 bytes, got {}", tx_id.len());
        }

        // Value: amount (8 bytes BE) + sealed_hex (remaining bytes)
        let mut value_bytes = Vec::with_capacity(8 + sealed_hex.len());
        value_bytes.extend_from_slice(&amount.to_be_bytes());
        value_bytes.extend_from_slice(sealed_hex);

        wtx.insert(&self.0, tx_id, value_bytes);
        Ok(())
    }

    pub fn get(&self, tx_id: &[u8]) -> anyhow::Result<Option<(u64, Vec<u8>)>> {
        if tx_id.len() != 32 {
            bail!("Transaction ID must be 32 bytes, got {}", tx_id.len());
        }

        if let Some(value_bytes) = self.0.get(tx_id)? {
            if value_bytes.len() >= 8 {
                // Split at position 8: first 8 bytes = amount, rest = sealed_hex
                let amount_bytes: [u8; 8] = value_bytes[..8]
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("Failed to parse amount bytes"))?;
                let amount = u64::from_be_bytes(amount_bytes);
                let sealed_hex = value_bytes[8..].to_vec();
                Ok(Some((amount, sealed_hex)))
            } else {
                bail!(
                    "Invalid value length in tx_id_to_payment partition: expected at least 8 bytes, got {}",
                    value_bytes.len()
                )
            }
        } else {
            Ok(None)
        }
    }

    pub fn get_tx_id(&self, tx_id: RpcTransactionId) -> anyhow::Result<Option<(u64, Vec<u8>)>> {
        self.get(&tx_id.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_payment_key_by_sender_serialization() {
        let key = PaymentKeyBySender {
            sender: AddressPayload::default(),
            block_time: 12345u64.to_be_bytes(),
            block_hash: [1u8; 32],
            receiver: AddressPayload::default(),
            version: 1,
            tx_id: [2u8; 32],
        };

        let bytes = bytemuck::bytes_of(&key);
        let deserialized: PaymentKeyBySender = *bytemuck::from_bytes(bytes);
        assert_eq!(deserialized, key);
    }

    #[test]
    fn test_payment_key_by_receiver_serialization() {
        let key = PaymentKeyByReceiver {
            receiver: AddressPayload::default(),
            block_time: 12345u64.to_be_bytes(),
            block_hash: [1u8; 32],
            version: 1,
            tx_id: [2u8; 32],
        };

        let bytes = bytemuck::bytes_of(&key);
        let deserialized: PaymentKeyByReceiver = *bytemuck::from_bytes(bytes);
        assert_eq!(deserialized, key);
    }

    #[test]
    fn test_tx_id_to_payment_value_format() {
        let amount = 12345u64;
        let sealed_hex = b"test_sealed_data";

        // Simulate the value format: amount (8 bytes) + sealed_hex
        let mut value_bytes = Vec::new();
        value_bytes.extend_from_slice(&amount.to_be_bytes());
        value_bytes.extend_from_slice(sealed_hex);

        // Test splitting at position 8
        assert!(value_bytes.len() >= 8);
        let amount_bytes: [u8; 8] = value_bytes[..8].try_into().unwrap();
        let recovered_amount = u64::from_be_bytes(amount_bytes);
        let recovered_sealed_hex = &value_bytes[8..];

        assert_eq!(recovered_amount, amount);
        assert_eq!(recovered_sealed_hex, sealed_hex);
    }

    #[test]
    fn test_like_payment_key_by_sender() {
        let key = PaymentKeyBySender {
            sender: AddressPayload::default(),
            block_time: 12345u64.to_be_bytes(),
            block_hash: [1u8; 32],
            receiver: AddressPayload::default(),
            version: 1,
            tx_id: [2u8; 32],
        };

        let bytes = bytemuck::bytes_of(&key).to_vec();
        let like_key = LikePaymentKeyBySender::new(bytes);

        // Test zero-copy access via Deref
        assert_eq!(like_key.version, 1);
        assert_eq!(like_key.tx_id, [2u8; 32]);
        assert_eq!(like_key.block_time, 12345u64.to_be_bytes());

        // Test Clone
        let cloned = like_key.clone();
        assert_eq!(cloned.version, 1);
    }

    #[test]
    fn test_like_payment_key_by_receiver() {
        let key = PaymentKeyByReceiver {
            receiver: AddressPayload::default(),
            block_time: 12345u64.to_be_bytes(),
            block_hash: [1u8; 32],
            version: 1,
            tx_id: [2u8; 32],
        };

        let bytes = bytemuck::bytes_of(&key).to_vec();
        let like_key = LikePaymentKeyByReceiver::new(bytes);

        // Test zero-copy access via Deref
        assert_eq!(like_key.version, 1);
        assert_eq!(like_key.tx_id, [2u8; 32]);
        assert_eq!(like_key.block_time, 12345u64.to_be_bytes());

        // Test Clone
        let cloned = like_key.clone();
        assert_eq!(cloned.version, 1);
    }
}
