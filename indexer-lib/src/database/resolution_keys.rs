use crate::database::messages::AddressPayload;
use bytemuck::{AnyBitPattern, NoUninit};
use fjall::UserKey;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

/// Handshake key for resolution - missing sender field (that's what we're resolving)
#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
#[repr(C)]
pub struct HandshakeKeyForResolution {
    pub block_time: [u8; 8], // u64_be
    pub block_hash: [u8; 32],
    pub receiver: AddressPayload,
    pub version: u8,
    pub tx_id: [u8; 32],
    pub attempt_count: u8, // attempts remaining for resolution
}

#[repr(transparent)]
#[derive(Clone, PartialEq, Eq)]
pub struct LikeHandshakeKeyForResolution<T: AsRef<[u8]>> {
    bts: T,
    phantom_data: PhantomData<HandshakeKeyForResolution>,
}

impl<T: AsRef<[u8]>> LikeHandshakeKeyForResolution<T> {
    pub fn new(bts: T) -> Self {
        let _ = bytemuck::from_bytes::<HandshakeKeyForResolution>(bts.as_ref());
        Self {
            bts,
            phantom_data: PhantomData,
        }
    }

    pub fn to_mutable(&self) -> LikeHandshakeKeyForResolution<Vec<u8>> {
        LikeHandshakeKeyForResolution::new(self.bts.as_ref().to_vec())
    }

    pub fn inner(self) -> T {
        self.bts
    }
}

impl<T: AsRef<[u8]>> Deref for LikeHandshakeKeyForResolution<T> {
    type Target = HandshakeKeyForResolution;

    fn deref(&self) -> &Self::Target {
        bytemuck::from_bytes(self.bts.as_ref())
    }
}

impl DerefMut for LikeHandshakeKeyForResolution<Vec<u8>> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        bytemuck::from_bytes_mut(self.bts.as_mut())
    }
}

impl<T: AsRef<[u8]>> Debug for LikeHandshakeKeyForResolution<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

/// Contextual message key for resolution - missing sender field (that's what we're resolving)
#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
#[repr(C)]
pub struct ContextualMessageKeyForResolution {
    pub alias: [u8; 16],      // alias for prefix search, zero-padded
    pub block_time: [u8; 8],  // u64 BE for chronological ordering
    pub block_hash: [u8; 32], // block hash for uniqueness
    pub version: u8,          // message version
    pub tx_id: [u8; 32],      // transaction id
    pub attempt_count: u8,    // attempts remaining for resolution
}

#[repr(transparent)]
#[derive(Clone, PartialEq, Eq)]
pub struct LikeContextualMessageKeyForResolution<T: AsRef<[u8]>> {
    bts: T,
    phantom_data: PhantomData<ContextualMessageKeyForResolution>,
}

impl<T: AsRef<[u8]>> LikeContextualMessageKeyForResolution<T> {
    pub fn new(bts: T) -> Self {
        Self {
            bts,
            phantom_data: PhantomData,
        }
    }
    pub fn to_mutable(&self) -> LikeContextualMessageKeyForResolution<Vec<u8>> {
        LikeContextualMessageKeyForResolution::new(self.bts.as_ref().to_vec())
    }

    pub fn inner(self) -> T {
        self.bts
    }
}

impl<T: AsRef<[u8]>> Deref for LikeContextualMessageKeyForResolution<T> {
    type Target = ContextualMessageKeyForResolution;

    fn deref(&self) -> &Self::Target {
        bytemuck::from_bytes(self.bts.as_ref())
    }
}

impl DerefMut for LikeContextualMessageKeyForResolution<Vec<u8>> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        bytemuck::from_bytes_mut(self.bts.as_mut())
    }
}

impl<T: AsRef<[u8]>> Debug for LikeContextualMessageKeyForResolution<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

/// Payment key for resolution - missing sender field (that's what we're resolving)
#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
#[repr(C)]
pub struct PaymentKeyForResolution {
    pub block_time: [u8; 8], // u64_be
    pub block_hash: [u8; 32],
    pub receiver: AddressPayload,
    pub version: u8,
    pub tx_id: [u8; 32],
    pub attempt_count: u8, // attempts remaining for resolution
}

#[repr(transparent)]
#[derive(Clone, PartialEq, Eq)]
pub struct LikePaymentKeyForResolution<T: AsRef<[u8]>> {
    bts: T,
    phantom_data: PhantomData<PaymentKeyForResolution>,
}

impl<T: AsRef<[u8]>> LikePaymentKeyForResolution<T> {
    pub fn new(bts: T) -> Self {
        Self {
            bts,
            phantom_data: PhantomData,
        }
    }
    pub fn to_mutable(&self) -> LikePaymentKeyForResolution<Vec<u8>> {
        LikePaymentKeyForResolution::new(self.bts.as_ref().to_vec())
    }

    pub fn inner(self) -> T {
        self.bts
    }
}

impl<T: AsRef<[u8]>> Deref for LikePaymentKeyForResolution<T> {
    type Target = PaymentKeyForResolution;

    fn deref(&self) -> &Self::Target {
        bytemuck::from_bytes(self.bts.as_ref())
    }
}

impl DerefMut for LikePaymentKeyForResolution<Vec<u8>> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        bytemuck::from_bytes_mut(self.bts.as_mut())
    }
}

impl<T: AsRef<[u8]>> Debug for LikePaymentKeyForResolution<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

/// SelfStash key for resolution - missing owner field (that's what we're resolving)
#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
#[repr(C)]
pub struct SelfStashKeyForResolution {
    pub scope: [u8; 255],
    pub block_time: [u8; 8], // be
    pub block_hash: [u8; 32],
    pub version: u8,
    pub tx_id: [u8; 32],
    pub attempt_count: u8, // attempts remaining for resolution
}

#[repr(transparent)]
#[derive(Clone, PartialEq, Eq)]
pub struct LikeSelfStashKeyForResolution<T: AsRef<[u8]>> {
    bts: T,
    phantom_data: PhantomData<SelfStashKeyForResolution>,
}

impl<T: AsRef<[u8]>> LikeSelfStashKeyForResolution<T> {
    pub fn new(bts: T) -> Self {
        let _ = bytemuck::from_bytes::<SelfStashKeyForResolution>(bts.as_ref());
        Self {
            bts,
            phantom_data: PhantomData,
        }
    }

    pub fn to_mutable(&self) -> LikeSelfStashKeyForResolution<Vec<u8>> {
        LikeSelfStashKeyForResolution::new(self.bts.as_ref().to_vec())
    }

    pub fn inner(self) -> T {
        self.bts
    }
}

impl<T: AsRef<[u8]>> Deref for LikeSelfStashKeyForResolution<T> {
    type Target = SelfStashKeyForResolution;

    fn deref(&self) -> &Self::Target {
        bytemuck::from_bytes(self.bts.as_ref())
    }
}

impl DerefMut for LikeSelfStashKeyForResolution<Vec<u8>> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        bytemuck::from_bytes_mut(self.bts.as_mut())
    }
}

impl<T: AsRef<[u8]>> Debug for LikeSelfStashKeyForResolution<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

/// Return type for DAA resolution - contains the appropriate Like key type for resolution
#[derive(Debug, Clone)]
pub enum DaaResolutionLikeKey<T: AsRef<[u8]> = UserKey> {
    HandshakeKey(LikeHandshakeKeyForResolution<T>),
    ContextualMessageKey(LikeContextualMessageKeyForResolution<T>),
    PaymentKey(LikePaymentKeyForResolution<T>),
    SelfStashKey(LikeSelfStashKeyForResolution<T>),
}

impl<T: AsRef<[u8]>> DaaResolutionLikeKey<T> {
    pub fn inner(self) -> T {
        match self {
            DaaResolutionLikeKey::HandshakeKey(hk) => hk.inner(),
            DaaResolutionLikeKey::ContextualMessageKey(cmk) => cmk.inner(),
            DaaResolutionLikeKey::PaymentKey(pk) => pk.inner(),
            DaaResolutionLikeKey::SelfStashKey(ssk) => ssk.inner(),
        }
    }
}

/// Return type for sender resolution - contains the appropriate Like key type for resolution
#[derive(Debug, Clone)]
pub enum SenderResolutionLikeKey<T: AsRef<[u8]> + Clone = UserKey> {
    HandshakeKey(LikeHandshakeKeyForResolution<T>),
    ContextualMessageKey(LikeContextualMessageKeyForResolution<T>),
    PaymentKey(LikePaymentKeyForResolution<T>),
    SelfStashKey(LikeSelfStashKeyForResolution<T>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handshake_key_for_resolution_serialization() {
        let key = HandshakeKeyForResolution {
            block_time: 12345u64.to_be_bytes(),
            block_hash: [1u8; 32],
            receiver: AddressPayload::default(),
            version: 1,
            tx_id: [2u8; 32],
            attempt_count: 3,
        };

        let bytes = bytemuck::bytes_of(&key);
        // 8 + 32 + 34 + 1 + 32 + 1 = 108 bytes (no sender field, +1 for attempt_count)
        assert_eq!(bytes.len(), 108);

        let deserialized: HandshakeKeyForResolution = *bytemuck::from_bytes(bytes);
        assert_eq!(deserialized, key);
    }

    #[test]
    fn test_contextual_message_key_for_resolution_serialization() {
        let key = ContextualMessageKeyForResolution {
            alias: [1u8; 16],
            block_time: 12345u64.to_be_bytes(),
            block_hash: [2u8; 32],
            version: 1,
            tx_id: [3u8; 32],
            attempt_count: 3,
        };

        let bytes = bytemuck::bytes_of(&key);
        // 16 + 8 + 32 + 1 + 32 + 1 = 90 bytes (no sender field, +1 for attempt_count)
        assert_eq!(bytes.len(), 90);

        let deserialized: ContextualMessageKeyForResolution = *bytemuck::from_bytes(bytes);
        assert_eq!(deserialized, key);
    }

    #[test]
    fn test_payment_key_for_resolution_serialization() {
        let key = PaymentKeyForResolution {
            block_time: 12345u64.to_be_bytes(),
            block_hash: [1u8; 32],
            receiver: AddressPayload::default(),
            version: 1,
            tx_id: [2u8; 32],
            attempt_count: 3,
        };

        let bytes = bytemuck::bytes_of(&key);
        // 8 + 32 + 34 + 1 + 32 + 1 = 108 bytes (no sender field, +1 for attempt_count)
        assert_eq!(bytes.len(), 108);

        let deserialized: PaymentKeyForResolution = *bytemuck::from_bytes(bytes);
        assert_eq!(deserialized, key);
    }

    #[test]
    fn test_self_stash_key_for_resolution_serialization() {
        let key = SelfStashKeyForResolution {
            scope: [0u8; 255],
            block_time: 12345u64.to_be_bytes(),
            block_hash: [1u8; 32],
            version: 1,
            tx_id: [2u8; 32],
            attempt_count: 3,
        };

        let bytes = bytemuck::bytes_of(&key);

        assert_eq!(bytes.len(), 329);

        let deserialized: SelfStashKeyForResolution = *bytemuck::from_bytes(bytes);
        assert_eq!(deserialized, key);
    }

    #[test]
    fn test_like_handshake_key_for_resolution() {
        let key = HandshakeKeyForResolution {
            block_time: 12345u64.to_be_bytes(),
            block_hash: [1u8; 32],
            receiver: AddressPayload::default(),
            version: 1,
            tx_id: [2u8; 32],
            attempt_count: 3,
        };

        let bytes = bytemuck::bytes_of(&key).to_vec();
        let like_key = LikeHandshakeKeyForResolution::new(bytes);

        // Test zero-copy access via Deref
        assert_eq!(like_key.version, 1);
        assert_eq!(like_key.tx_id, [2u8; 32]);
        assert_eq!(like_key.block_time, 12345u64.to_be_bytes());
        assert_eq!(like_key.attempt_count, 3);

        // Test Clone
        let cloned = like_key.clone();
        assert_eq!(cloned.version, 1);
    }
}
