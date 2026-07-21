use crate::{AddressPayload, SharedImmutable};
use fjall::{PartitionCreateOptions, ReadTransaction, WriteTransaction};
use zerocopy::IntoBytes;

#[derive(Clone)]
pub struct DeviceRegistrationPartition(fjall::TxPartition);

impl DeviceRegistrationPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> anyhow::Result<Self> {
        Ok(Self(keyspace.open_partition(
            "device_registration",
            PartitionCreateOptions::default(),
        )?))
    }

    pub fn insert_wtx(&self, wtx: &mut WriteTransaction, token: &[u8], value: &[u8]) {
        wtx.insert(&self.0, token, value);
    }

    pub fn remove_wtx(&self, wtx: &mut WriteTransaction, token: &[u8]) {
        wtx.remove(&self.0, token);
    }

    pub fn get_rtx(
        &self,
        rtx: &ReadTransaction,
        token: &[u8],
    ) -> anyhow::Result<Option<SharedImmutable<[u8]>>> {
        rtx.get(&self.0, token)
            .map(|bts| bts.map(SharedImmutable::new))
            .map_err(anyhow::Error::from)
    }

    pub fn approximate_len(&self) -> usize {
        self.0.approximate_len()
    }
}

#[derive(Clone)]
pub struct WatchedAddressPartition(fjall::TxPartition);

impl WatchedAddressPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> anyhow::Result<Self> {
        Ok(Self(keyspace.open_partition(
            "watched_address_to_device",
            PartitionCreateOptions::default(),
        )?))
    }

    pub fn insert_wtx(
        &self,
        wtx: &mut WriteTransaction,
        address: &AddressPayload,
        token: &[u8],
    ) {
        let mut key = Vec::with_capacity(address.as_bytes().len() + token.len());
        key.extend_from_slice(address.as_bytes());
        key.extend_from_slice(token);
        wtx.insert(&self.0, key, []);
    }

    pub fn remove_wtx(
        &self,
        wtx: &mut WriteTransaction,
        address: &AddressPayload,
        token: &[u8],
    ) {
        let mut key = Vec::with_capacity(address.as_bytes().len() + token.len());
        key.extend_from_slice(address.as_bytes());
        key.extend_from_slice(token);
        wtx.remove(&self.0, key);
    }

    pub fn remove_raw_key_wtx(&self, wtx: &mut WriteTransaction, key: &[u8]) {
        wtx.remove(&self.0, key);
    }

    pub fn get_by_address_prefix(
        &self,
        rtx: &ReadTransaction,
        address: &AddressPayload,
    ) -> impl DoubleEndedIterator<Item = anyhow::Result<SharedImmutable<[u8]>>> + '_
    {
        let prefix = address.as_bytes();
        rtx.prefix(&self.0, prefix).map(|item| {
            let (key_bytes, _value_bytes) = item?;
            Ok(SharedImmutable::new(key_bytes))
        })
    }
}
