use std::fmt::{Debug, Formatter};
use anyhow::bail;
use bytemuck::{AnyBitPattern, NoUninit};
use fjall::{PartitionCreateOptions, WriteTransaction};
use kaspa_addresses::Version;
use kaspa_rpc_core::RpcScriptPublicKey;
use kaspa_txscript::script_class::ScriptClass;
use std::marker::PhantomData;
use std::ops::Deref;

const EMPTY_VERSION: u8 = 0; // used when we don't know address at all

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, AnyBitPattern, NoUninit)]
pub struct AddressPayload {
    pub inverse_version: u8,
    pub payload: [u8; 33], // last byte is unused in case of scripthash and XonlyPubkey
}

impl Default for AddressPayload {
    fn default() -> Self {
        Self {
            inverse_version: EMPTY_VERSION,
            payload: [0u8; 33],
        }
    }
}

impl TryFrom<&RpcScriptPublicKey> for AddressPayload {
    type Error = anyhow::Error;

    fn try_from(script_public_key: &RpcScriptPublicKey) -> anyhow::Result<Self> {
        let class = ScriptClass::from_script(script_public_key);
        if script_public_key.version() > class.version() {
            bail!(
                "Invalid version for script class: {}",
                script_public_key.version()
            );
        }
        let script = script_public_key.script();
        let mut payload = [0u8; 33];
        match class {
            ScriptClass::NonStandard => bail!("Invalid script class: {}", class),
            ScriptClass::PubKey => {
                payload[..32].copy_from_slice(&script[1..33]);
                Ok(AddressPayload {
                    inverse_version: u8::MAX - Version::PubKey as u8,
                    payload,
                })
            }
            ScriptClass::PubKeyECDSA => {
                payload.copy_from_slice(&script[1..34]);
                Ok(AddressPayload {
                    inverse_version: u8::MAX - Version::PubKeyECDSA as u8,
                    payload,
                })
            }
            ScriptClass::ScriptHash => {
                payload[..32].copy_from_slice(&script[2..34]);
                Ok(AddressPayload {
                    inverse_version: u8::MAX - Version::ScriptHash as u8,
                    payload,
                })
            }
        }
    }
}

#[derive(Clone)]
pub struct HandshakeBySenderPartition(fjall::TxPartition);

impl HandshakeBySenderPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> anyhow::Result<Self> {
        Ok(Self(keyspace.open_partition(
            "handshake_by_receiver",
            PartitionCreateOptions::default(),
        )?))
    }
}

#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
#[repr(C)]
pub struct HandshakeKeyBySender {
    pub sender: AddressPayload, // allows retrieving every tx with unknown sender
    pub block_time: [u8; 8],    /*u64_be*/
    pub block_hash: [u8; 32],
    pub receiver: AddressPayload,
    pub version: u8,
    pub tx_id: [u8; 32],
}

#[repr(transparent)]
#[derive(Clone, PartialEq, Eq)]
pub struct LikeHandshakeKeyBySender<T: AsRef<[u8]>> {
    bts: T,
    phantom_data: PhantomData<HandshakeKeyBySender>,
}

impl<T: AsRef<[u8]>> Debug for LikeHandshakeKeyBySender<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}
impl<T: AsRef<[u8]>> LikeHandshakeKeyBySender<T> {
    #[allow(dead_code)]
    pub(crate) fn new(bts: T) -> Self {
        Self {
            bts,
            phantom_data: PhantomData,
        }
    }
}

impl<T: AsRef<[u8]>> Deref for LikeHandshakeKeyBySender<T> {
    type Target = HandshakeKeyBySender;

    fn deref(&self) -> &Self::Target {
        bytemuck::from_bytes(self.bts.as_ref())
    }
}

impl HandshakeBySenderPartition {
    pub fn insert(&self, key: &HandshakeKeyBySender) -> anyhow::Result<()> {
        self.0.insert(bytemuck::bytes_of(key), [])?;
        Ok(())
    }

    pub fn insert_wtx(
        &self,
        wtx: &mut WriteTransaction,
        key: &HandshakeKeyBySender,
    ) -> anyhow::Result<()> {
        wtx.insert(&self.0, bytemuck::bytes_of(key), []);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
#[repr(C)]
pub struct HandshakeKeyByReceiver {
    pub receiver: AddressPayload,
    pub block_time: [u8; 8], // be
    pub block_hash: [u8; 32],
    pub version: u8,
    pub tx_id: [u8; 32],
}

#[derive(Clone)]
pub struct HandshakeByReceiverPartition(fjall::TxPartition);

impl HandshakeByReceiverPartition {
    pub fn insert(
        &self,
        key: &HandshakeKeyByReceiver,
        sender: Option<AddressPayload>,
    ) -> anyhow::Result<()> {
        let sender = sender.unwrap_or_default();
        self.0
            .insert(bytemuck::bytes_of(key), bytemuck::bytes_of(&sender))?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct TxIdToHandshakePartition(fjall::TxPartition);

impl TxIdToHandshakePartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> anyhow::Result<Self> {
        Ok(Self(keyspace.open_partition(
            "tx-id-to-handshake",
            PartitionCreateOptions::default(),
        )?))
    }

    pub fn insert(&self, tx_id: &[u8], sealed_hex: &[u8]) -> anyhow::Result<()> {
        assert_eq!(tx_id.len(), 32);
        self.0.insert(tx_id, sealed_hex)?;
        Ok(())
    }
}
