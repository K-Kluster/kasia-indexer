use anyhow::bail;
use fjall::UserKey;
use kaspa_addresses::{Address, Version};
use kaspa_consensus_core::tx::ScriptPublicKey;
use kaspa_txscript::pay_to_address_script;
use kaspa_txscript::script_class::ScriptClass;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::ops::Deref;
pub use zerocopy::{self, FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

pub mod headers;
pub mod messages;
pub mod metadata;
pub mod migration;
pub mod processing;

pub const EMPTY_VERSION: u8 = 0; // used when we don't know address at all

#[repr(C)]
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Immutable, IntoBytes, FromBytes, Unaligned, KnownLayout,
)]
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

impl TryFrom<&ScriptPublicKey> for AddressPayload {
    type Error = anyhow::Error;

    fn try_from(script_public_key: &ScriptPublicKey) -> anyhow::Result<Self> {
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
impl TryFrom<&Address> for AddressPayload {
    type Error = anyhow::Error;

    fn try_from(value: &Address) -> Result<Self, Self::Error> {
        (&pay_to_address_script(value)).try_into()
    }
}

#[repr(transparent)]
#[derive(Clone, PartialEq, Eq)]
pub struct SharedImmutable<T: ?Sized> {
    inner: UserKey,
    phantom: PhantomData<T>,
}

impl<T> Debug for SharedImmutable<T>
where
    T: Debug + AsRef<T> + FromBytes + KnownLayout + Immutable,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.as_ref().fmt(f)
    }
}
impl<T: ?Sized> SharedImmutable<T> {
    pub(crate) fn new(inner: UserKey) -> Self {
        Self {
            inner,
            phantom: PhantomData,
        }
    }
}

impl<T> Deref for SharedImmutable<T>
where
    T: FromBytes + KnownLayout + Immutable + ?Sized,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        T::ref_from_bytes(self.inner.as_ref()).unwrap()
    }
}

impl<T> AsRef<T> for SharedImmutable<T>
where
    T: FromBytes + KnownLayout + Immutable + ?Sized,
{
    fn as_ref(&self) -> &T {
        T::ref_from_bytes(self.inner.as_ref()).unwrap()
    }
}

#[repr(u8)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, TryFromBytes, IntoBytes, Unaligned, Immutable, KnownLayout,
)]
pub enum PartitionId {
    Metadata = 1,
    BlockCompactHeaders = 2,
    BlockDaaIndex = 3,
    BlockGaps = 4,
    HandshakeByReceiver = 5,
    HandshakeBySender = 6,
    TxIdToHandshake = 7,
    ContextualMessageBySender = 8,
    PaymentByReceiver = 9,
    PaymentBySender = 10,
    TxIdToPayment = 11,
    AcceptingBlockToTxIds = 12,
    TxIdToAcceptance = 13,

    PendingSenders = 14,
    SelfStashByOwner = 15,
    TxIDToSelfStash = 16,
}
