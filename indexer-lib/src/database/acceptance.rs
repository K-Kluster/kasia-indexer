use bytemuck::{AnyBitPattern, NoUninit};
use fjall::{PartitionCreateOptions, ReadTransaction, UserKey, WriteTransaction};
use std::marker::PhantomData;
use std::ops::Deref;


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
        acceptance: Option<([u8; 32], [u8; 8])>,
    ) -> anyhow::Result<()> {
        let acceptance = acceptance.unwrap_or_default();
        let key = TxAcceptanceKey {
            tx_id,
            accepted_at_daa: acceptance.1,
            accepted_by_block_hash: acceptance.0,
        };
        wtx.insert(&self.0, bytemuck::bytes_of(&key), []);
        Ok(())
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
}

#[repr(transparent)]
#[derive(Clone, PartialEq, Eq)]
pub struct LikeAcceptanceTxKey<T: AsRef<[u8]>> {
    bts: T,
    phantom_data: PhantomData<AcceptanceTxKey>,
}

impl<T: AsRef<[u8]>> LikeAcceptanceTxKey<T> {
    fn new(bts: T) -> Self {
        Self {
            bts,
            phantom_data: PhantomData,
        }
    }
}

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

    pub fn insert(
        &self,
        tx_id: [u8; 32],
        acceptance: Option<([u8; 32], [u8; 8])>,
    ) -> anyhow::Result<()> {
        let acceptance = acceptance.unwrap_or_default();
        let key = AcceptanceTxKey {
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
        acceptance: Option<([u8; 32], [u8; 8])>,
    ) -> anyhow::Result<()> {
        let acceptance = acceptance.unwrap_or_default();
        let key = AcceptanceTxKey {
            tx_id,
            accepted_at_daa: acceptance.1,
            accepted_by_block_hash: acceptance.0,
        };
        wtx.insert(&self.0, bytemuck::bytes_of(&key), []);
        Ok(())
    }

    pub fn get_wtx(
        &self,
        wtx: &mut WriteTransaction,
        tx_id: [u8; 32],
    ) -> anyhow::Result<Option<AcceptanceTxKey>> {
        // We need to scan for the key since tx_id is the first field
        let start = tx_id;
        let mut end = [0u8; 72]; // tx_id(32) + daa(8) + block_hash(32)
        end[..32].copy_from_slice(&tx_id);
        end[32..].fill(0xff);
        
        let mut result = None;
        for item in wtx.range(&self.0, start.as_slice()..=end.as_slice()) {
            let (key_bytes, _) = item?;
            let key: AcceptanceTxKey = *bytemuck::from_bytes(&key_bytes);
            if key.tx_id == tx_id {
                result = Some(key);
                break;
            }
        }
        Ok(result)
    }
}
