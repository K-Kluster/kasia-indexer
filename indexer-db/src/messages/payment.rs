use crate::{AddressPayload, SharedImmutable};
use anyhow::bail;
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use zerocopy::big_endian::U64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

#[repr(C)]
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Immutable, FromBytes, IntoBytes, Unaligned, KnownLayout,
)]
pub struct PaymentKeyBySender {
    pub sender: AddressPayload, // allows retrieving every tx with unknown sender
    pub block_time: U64,        // u64_be
    pub block_hash: [u8; 32],
    pub receiver: AddressPayload,
    pub version: u8,
    pub tx_id: [u8; 32],
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

    pub fn insert_wtx(&self, wtx: &mut WriteTransaction, key: &PaymentKeyBySender) {
        wtx.insert(&self.0, key.as_bytes(), []);
    }

    pub fn approximate_len(&self) -> usize {
        self.0.approximate_len()
    }

    pub fn get_by_sender_from_block_time(
        &self,
        rtx: &ReadTransaction,
        sender: &AddressPayload,
        from_block_time: u64,
    ) -> impl DoubleEndedIterator<Item = Result<SharedImmutable<PaymentKeyBySender>, anyhow::Error>> + '_
    {
        // Create range start: sender (34 bytes) + block_time (8 bytes)
        let mut range_start = [0u8; 42]; // 34 + 8
        range_start[..34].copy_from_slice(sender.as_bytes());
        range_start[34..42].copy_from_slice(&from_block_time.to_be_bytes());

        // Create range end: sender (34 bytes) + max block_time (8 bytes)
        let mut range_end = [0xFFu8; 42]; // 34 + 8
        range_end[..34].copy_from_slice(sender.as_bytes());

        rtx.range(&self.0, range_start..=range_end).map(|item| {
            item.map(|(key, _value)| SharedImmutable::new(key))
                .map_err(anyhow::Error::from)
        })
    }
}

#[repr(C)]
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Immutable, FromBytes, IntoBytes, Unaligned, KnownLayout,
)]
pub struct PaymentKeyByReceiver {
    pub receiver: AddressPayload,
    pub block_time: U64, // be
    pub block_hash: [u8; 32],
    pub version: u8,
    pub tx_id: [u8; 32],
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

    pub fn insert_wtx(
        &self,
        wtx: &mut WriteTransaction,
        key: &PaymentKeyByReceiver,
        sender: Option<AddressPayload>,
    ) -> anyhow::Result<()> {
        let sender = sender.unwrap_or_default();
        wtx.update_fetch(&self.0, key.as_bytes(), |old| match old {
            None => Some(sender.as_bytes().into()),
            Some(old) => {
                let old_sender = AddressPayload::try_ref_from_bytes(old.as_bytes()).unwrap();
                if old_sender != &AddressPayload::default() {
                    Some(old.clone())
                } else {
                    Some(sender.as_bytes().into())
                }
            }
        })?;
        Ok(())
    }

    pub fn get_by_receiver_from_block_time(
        &self,
        rtx: &ReadTransaction,
        receiver: &AddressPayload,
        from_block_time: u64,
    ) -> impl DoubleEndedIterator<
        Item = Result<
            (
                SharedImmutable<PaymentKeyByReceiver>,
                SharedImmutable<AddressPayload>,
            ),
            anyhow::Error,
        >,
    > + '_ {
        // Create range start: receiver (34 bytes) + block_time (8 bytes)
        let mut range_start = [0u8; 42]; // 34 + 8
        range_start[..34].copy_from_slice(receiver.as_bytes());
        range_start[34..42].copy_from_slice(&from_block_time.to_be_bytes());

        // Create range end: receiver (34 bytes) + max block_time (8 bytes)
        let mut range_end = [0xFFu8; 42]; // 34 + 8
        range_end[..34].copy_from_slice(receiver.as_bytes());

        rtx.range(&self.0, range_start..=range_end).map(|item| {
            item.map(|(key, value)| (SharedImmutable::new(key), SharedImmutable::new(value)))
                .map_err(anyhow::Error::from)
        })
    }
}

#[derive(Debug, FromBytes, Immutable)]
pub struct PaymentData {
    pub amount: zerocopy::U64<zerocopy::LE>,
    pub sealed_hex: [u8],
}

impl SharedImmutable<PaymentData> {
    pub fn amount(&self) -> u64 {
        u64::from_le_bytes(self.inner.split_at(8).0.try_into().unwrap())
    }

    pub fn sealed_hex(&self) -> &[u8] {
        self.inner.split_at(8).1
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

    pub fn len(&self) -> anyhow::Result<usize> {
        Ok(self.0.inner().len()?)
    }

    pub fn is_empty(&self) -> anyhow::Result<bool> {
        Ok(self.0.inner().is_empty()?)
    }

    pub fn approximate_len(&self) -> usize {
        self.0.approximate_len()
    }

    pub fn insert_wtx(
        &self,
        wtx: &mut WriteTransaction,
        tx_id: &[u8; 32],
        amount: u64,
        sealed_hex: &[u8],
    ) -> anyhow::Result<()> {
        // Value: amount (8 bytes BE) + sealed_hex (remaining bytes)
        let mut value_bytes = Vec::with_capacity(8 + sealed_hex.len());
        value_bytes.extend_from_slice(&amount.to_le_bytes());
        value_bytes.extend_from_slice(sealed_hex);

        wtx.insert(&self.0, tx_id, value_bytes);
        Ok(())
    }

    pub fn get(&self, tx_id: &[u8; 32]) -> anyhow::Result<Option<(u64, Vec<u8>)>> {
        if let Some(value_bytes) = self.0.get(tx_id)? {
            if value_bytes.len() >= 8 {
                // Split at position 8: first 8 bytes = amount, rest = sealed_hex
                let amount_bytes: [u8; 8] = value_bytes[..8]
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("Failed to parse amount bytes"))?;
                let amount = u64::from_le_bytes(amount_bytes);
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

    pub fn get_rtx(
        &self,
        rtx: &ReadTransaction,
        tx_id: &[u8],
    ) -> anyhow::Result<Option<SharedImmutable<PaymentData>>> {
        if tx_id.len() != 32 {
            bail!("Transaction ID must be 32 bytes, got {}", tx_id.len());
        }

        if let Some(value_bytes) = rtx.get(&self.0, tx_id)? {
            Ok(Some(SharedImmutable::new(value_bytes)))
        } else {
            Ok(None)
        }
    }
}
