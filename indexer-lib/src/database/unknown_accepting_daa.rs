use crate::database::resolution_keys::{
    ContextualMessageKeyForResolution, DaaResolutionLikeKey, HandshakeKeyForResolution,
    LikeContextualMessageKeyForResolution, LikeHandshakeKeyForResolution,
    LikePaymentKeyForResolution, PaymentKeyForResolution,
};
use anyhow::Result;
use bytemuck::{AnyBitPattern, NoUninit};
use fjall::{PartitionCreateOptions, ReadTransaction, UserValue, WriteTransaction};
use kaspa_rpc_core::RpcHash;

/// Helper function to calculate maximum of two values at compile time
const fn max(a: usize, b: usize) -> usize {
    [a, b][(a < b) as usize]
}

/// Maximum size of any resolution key, calculated from actual struct sizes
const MAX_RESOLUTION_KEY_SIZE: usize = max(
    max(
        std::mem::size_of::<HandshakeKeyForResolution>(),
        std::mem::size_of::<ContextualMessageKeyForResolution>(),
    ),
    std::mem::size_of::<PaymentKeyForResolution>(),
);

/// Array entry that stores txid + partition_type + resolution key (padded to max length)
#[repr(C)]
#[derive(Clone, Copy, Debug, AnyBitPattern, NoUninit, PartialEq, Eq)]
pub struct PaddedResolutionEntry {
    pub tx_id: [u8; 32],
    pub partition_type: u8,
    pub resolution_key_data: [u8; MAX_RESOLUTION_KEY_SIZE], // Padded to max size
}

impl PaddedResolutionEntry {
    /// Extract the resolution key from the padded data based on partition type
    pub fn get_resolution_key(&self) -> Result<DaaResolutionLikeKey<&[u8]>> {
        match self.partition_type {
            x if x == DaaResolutionPartitionType::HandshakeBySender as u8 => {
                let size = std::mem::size_of::<HandshakeKeyForResolution>();
                Ok(DaaResolutionLikeKey::HandshakeKey(
                    LikeHandshakeKeyForResolution::new(&self.resolution_key_data[..size]),
                ))
            }
            x if x == DaaResolutionPartitionType::ContextualMessageBySender as u8 => {
                let size = std::mem::size_of::<ContextualMessageKeyForResolution>();
                Ok(DaaResolutionLikeKey::ContextualMessageKey(
                    LikeContextualMessageKeyForResolution::new(&self.resolution_key_data[..size]),
                ))
            }
            x if x == DaaResolutionPartitionType::PaymentBySender as u8 => {
                let size = std::mem::size_of::<PaymentKeyForResolution>();
                Ok(DaaResolutionLikeKey::PaymentKey(
                    LikePaymentKeyForResolution::new(&self.resolution_key_data[..size]),
                ))
            }
            _ => Err(anyhow::anyhow!(
                "Invalid partition type: {}",
                self.partition_type
            )),
        }
    }
}

/// wrapper that stores resolution entries and implements AsRef<[u8]>
/// Format: [attempt_count (1 byte)] + [flattened PaddedResolutionEntry bytes]
#[repr(transparent)]
#[derive(Clone, PartialEq, Eq)]
pub struct ResolutionEntries<T: AsRef<[u8]>> {
    bts: T,
    phantom_data: std::marker::PhantomData<(u8, PaddedResolutionEntry)>,
}

impl<T: AsRef<[u8]>> ResolutionEntries<T> {
    pub fn to_mutable(&self) -> ResolutionEntries<Vec<u8>> {
        ResolutionEntries::with_data(self.attempt_count(), &self.bts.as_ref()[1..])
    }

    pub fn inner(self) -> T {
        self.bts
    }

    /// Get attempt count (first byte)
    pub fn attempt_count(&self) -> u8 {
        self.bts.as_ref().first().copied().unwrap_or(0)
    }

    /// Get entries as slice of arrays
    pub fn as_entry_slice(&self) -> Result<&[PaddedResolutionEntry]> {
        let bytes = self.bts.as_ref();
        if bytes.is_empty() {
            return Err(anyhow::anyhow!(
                "ResolutionEntryVector bytes cannot be empty"
            ));
        }

        let entry_bytes = &bytes[1..]; // Skip attempt_count byte
        let entry_size = std::mem::size_of::<PaddedResolutionEntry>();
        if entry_bytes.len() % entry_size != 0 {
            return Err(anyhow::anyhow!(
                "Invalid entry bytes length for ResolutionEntryVector"
            ));
        }
        Ok(bytemuck::cast_slice(entry_bytes))
    }

    pub fn is_empty(&self) -> bool {
        self.bts.as_ref().len() <= 1 // Only attempt_count byte, no entries
    }

    pub fn len(&self) -> usize {
        let bytes = self.bts.as_ref();
        if bytes.len() <= 1 {
            return 0;
        }
        let entry_size = std::mem::size_of::<PaddedResolutionEntry>();
        (bytes.len() - 1) / entry_size // Subtract 1 for attempt_count byte
    }
}

impl ResolutionEntries<Vec<u8>> {
    pub fn new(attempt_count: u8) -> Self {
        Self {
            bts: vec![attempt_count],
            phantom_data: Default::default(),
        }
    }

    pub fn with_data(attempt_count: u8, data: &[u8]) -> Self {
        let mut v = Vec::with_capacity(data.len() + 1);
        v.push(attempt_count);
        v.extend_from_slice(data);
        Self {
            bts: v,
            phantom_data: Default::default(),
        }
    }

    /// Push a handshake resolution entry
    pub fn push_handshake(&mut self, tx_id: [u8; 32], key: &HandshakeKeyForResolution) {
        let mut resolution_key_data = [0u8; MAX_RESOLUTION_KEY_SIZE];
        let key_bytes = bytemuck::bytes_of(key);
        resolution_key_data[..key_bytes.len()].copy_from_slice(key_bytes);

        let entry = PaddedResolutionEntry {
            tx_id,
            partition_type: DaaResolutionPartitionType::HandshakeBySender as u8,
            resolution_key_data,
        };

        let entry_bytes = bytemuck::bytes_of(&entry);
        self.bts.extend_from_slice(entry_bytes);
    }

    /// Push a contextual message resolution entry
    pub fn push_contextual_message(
        &mut self,
        tx_id: [u8; 32],
        key: &ContextualMessageKeyForResolution,
    ) {
        let mut resolution_key_data = [0u8; MAX_RESOLUTION_KEY_SIZE];
        let key_bytes = bytemuck::bytes_of(key);
        resolution_key_data[..key_bytes.len()].copy_from_slice(key_bytes);

        let entry = PaddedResolutionEntry {
            tx_id,
            partition_type: DaaResolutionPartitionType::ContextualMessageBySender as u8,
            resolution_key_data,
        };

        let entry_bytes = bytemuck::bytes_of(&entry);
        self.bts.extend_from_slice(entry_bytes);
    }

    /// Push a payment resolution entry
    pub fn push_payment(&mut self, tx_id: [u8; 32], key: &PaymentKeyForResolution) {
        let mut resolution_key_data = [0u8; MAX_RESOLUTION_KEY_SIZE];
        let key_bytes = bytemuck::bytes_of(key);
        resolution_key_data[..key_bytes.len()].copy_from_slice(key_bytes);

        let entry = PaddedResolutionEntry {
            tx_id,
            partition_type: DaaResolutionPartitionType::PaymentBySender as u8,
            resolution_key_data,
        };

        let entry_bytes = bytemuck::bytes_of(&entry);
        self.bts.extend_from_slice(entry_bytes);
    }

    pub fn decrement_attempt_count(&mut self) -> u8 {
        if !self.bts.is_empty() {
            self.bts[0] = self.bts[0].saturating_sub(1);
        }
        self.attempt_count()
    }

    pub fn extend_from_slice(&mut self, data: &[PaddedResolutionEntry]) {
        let entry_bytes = bytemuck::cast_slice(data);
        self.bts.extend_from_slice(entry_bytes);
    }
}

impl<T: AsRef<[u8]>> AsRef<[u8]> for ResolutionEntries<T> {
    fn as_ref(&self) -> &[u8] {
        self.bts.as_ref()
    }
}

impl<T: AsRef<[u8]>> std::fmt::Debug for ResolutionEntries<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.as_entry_slice() {
            Ok(entries) => write!(f, "LikeResolutionEntryVector({} entries)", entries.len()),
            Err(_) => write!(f, "LikeResolutionEntryVector(invalid)"),
        }
    }
}

impl From<UserValue> for ResolutionEntries<fjall::UserKey> {
    fn from(value: UserValue) -> Self {
        assert_eq!(
            value.len().saturating_sub(1) % size_of::<PaddedResolutionEntry>(),
            0
        );
        Self {
            bts: value,
            phantom_data: Default::default(),
        }
    }
}

impl<'a> From<&'a UserValue> for ResolutionEntries<&'a fjall::UserKey> {
    fn from(value: &'a UserValue) -> Self {
        assert_eq!(
            value.len().saturating_sub(1) % size_of::<PaddedResolutionEntry>(),
            0
        );
        Self {
            bts: value,
            phantom_data: Default::default(),
        }
    }
}

/// FIFO partition for storing accepting block hashes with unknown DAA scores
/// Key: accepting_block_hash (32 bytes only)
/// Value: ResolutionEntryVector containing entries with txid + partition_type + resolution key data
/// Note: These are accepting blocks (selected blocks in GHOSTDAG), not regular blocks
///
/// Uses FIFO compaction strategy because:
/// - Unknown DAA scores are temporary - will be resolved when RPC call completes
/// - Older entries become irrelevant as blockchain progresses
/// - Self-balancing: automatically removes old entries when size limit reached
#[derive(Clone)]
pub struct UnknownAcceptingDaaPartition(fjall::TxPartition);

/// Partition types that can need DAA score resolution
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DaaResolutionPartitionType {
    HandshakeBySender = 0,
    ContextualMessageBySender = 1,
    PaymentBySender = 2,
}

// DaaResolutionLikeKey is now imported from resolution_keys module

impl UnknownAcceptingDaaPartition {
    pub fn new(keyspace: &fjall::TxKeyspace) -> Result<Self> {
        Ok(Self(
            keyspace.open_partition(
                "unknown_accepting_daa",
                PartitionCreateOptions::default()
                    .block_size(64 * 1024)
                    .compaction_strategy(fjall::compaction::Strategy::Fifo(
                        fjall::compaction::Fifo {
                            limit: 160 * 1024 * 1024,
                            ttl_seconds: None,
                        },
                    )),
            )?,
        ))
    }

    /// Insert a complete resolution entry vector for an accepting block hash
    pub fn insert_wtx<T: AsRef<[u8]>>(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hash: RpcHash,
        entry_vector: &ResolutionEntries<T>,
    ) -> Result<()> {
        wtx.insert(
            &self.0,
            accepting_block_hash.as_bytes(),
            entry_vector.as_ref(),
        );
        Ok(())
    }

    /// Get the resolution entry vector for a specific accepting block hash
    pub fn get_by_accepting_block_hash(
        &self,
        rtx: &ReadTransaction,
        accepting_block_hash: RpcHash,
    ) -> Result<Option<ResolutionEntries<fjall::UserKey>>> {
        if let Some(value_bytes) = rtx.get(&self.0, accepting_block_hash.as_bytes())? {
            Ok(Some(ResolutionEntries::from(value_bytes)))
        } else {
            Ok(None)
        }
    }

    /// Remove the resolution entry vector for a specific accepting block hash (when DAA is resolved)
    /// Returns the resolution entry vector that was removed
    pub fn remove_by_accepting_block_hash(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hash: RpcHash,
    ) -> Result<Option<ResolutionEntries<fjall::UserKey>>> {
        if let Some(value_bytes) =
            wtx.fetch_update(&self.0, accepting_block_hash.as_bytes(), |_old| None)?
        {
            Ok(Some(ResolutionEntries::from(value_bytes)))
        } else {
            Ok(None)
        }
    }

    /// Check if an accepting block hash has any entries needing DAA score resolution
    pub fn has_unknown_daa(
        &self,
        rtx: &ReadTransaction,
        accepting_block_hash: RpcHash,
    ) -> Result<bool> {
        Ok(rtx.get(&self.0, accepting_block_hash.as_bytes())?.is_some())
    }

    /// Get all accepting block hashes that need DAA score resolution
    /// Returns only the unique block hashes (no values)
    pub fn get_all_unknown_accepting_blocks(
        &self,
        rtx: &ReadTransaction,
    ) -> impl DoubleEndedIterator<Item = Result<RpcHash>> + '_ {
        rtx.keys(&self.0).map(|r| {
            let key_bytes = r?;
            if key_bytes.len() == 32 {
                Ok(RpcHash::from_slice(&key_bytes))
            } else {
                Err(anyhow::anyhow!(
                    "Invalid key length in unknown_accepting_daa partition: expected 32, got {}",
                    key_bytes.len()
                ))
            }
        })
    }

    /// Decrement attempt count for the given accepting block hash, removing the entry if it reaches 0 attempts
    /// Returns true if the entry was removed, false if it was updated
    pub fn decrement_attempt_counts_by_block_hash(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hash: RpcHash,
    ) -> Result<bool> {
        let mut was_removed = false;

        wtx.fetch_update(&self.0, accepting_block_hash.as_bytes(), |current_value| {
            let value_bytes = current_value?;
            let like_vector = ResolutionEntries::from(value_bytes.clone());

            // Decrement the attempt count
            let new_attempt_count = like_vector.attempt_count().saturating_sub(1);
            if new_attempt_count == 0 {
                was_removed = true;
                None // Remove the entire entry
            } else {
                // Update the attempt count in the first byte
                let mut new_v = like_vector.to_mutable();
                new_v.decrement_attempt_count();
                Some(new_v.inner().into())
            }
        })?;

        Ok(was_removed)
    }

    pub fn extend_by_accepting_block_hash(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hash: &RpcHash,
        extended: ResolutionEntries<Vec<u8>>,
    ) -> Result<()> {
        let mut extended = Some(extended);
        wtx.fetch_update(&self.0, accepting_block_hash.as_bytes(), |current_value| {
            let mut extended = extended.take().unwrap();
            match current_value {
                None => Some(extended.inner().into()),
                Some(value) => {
                    extended.extend_from_slice(
                        ResolutionEntries::from(value).as_entry_slice().unwrap(),
                    );
                    Some(extended.inner().into())
                }
            }
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_type_enum() {
        assert_eq!(DaaResolutionPartitionType::HandshakeBySender as u8, 0);
        assert_eq!(
            DaaResolutionPartitionType::ContextualMessageBySender as u8,
            1
        );
        assert_eq!(DaaResolutionPartitionType::PaymentBySender as u8, 2);
    }

    #[test]
    fn test_resolution_entry_vector_format() {
        let mut vector = ResolutionEntries::new(3);
        assert_eq!(vector.attempt_count(), 3);
        assert_eq!(vector.len(), 0);
        assert!(vector.is_empty());

        // Test serialization format: first byte should be attempt_count
        let bytes = vector.as_ref();
        assert_eq!(bytes.len(), 1);
        assert_eq!(bytes[0], 3);

        // Test decrement
        let new_count = vector.decrement_attempt_count();
        assert_eq!(new_count, 2);
        assert_eq!(vector.attempt_count(), 2);
        assert_eq!(vector.as_ref()[0], 2);
    }

    #[test]
    fn test_padded_resolution_entry_size() {
        let entry_size = std::mem::size_of::<PaddedResolutionEntry>();
        // 32 (tx_id) + 1 (partition_type) + MAX_RESOLUTION_KEY_SIZE
        let expected_size = 32 + 1 + MAX_RESOLUTION_KEY_SIZE;
        assert_eq!(entry_size, expected_size);
    }
}
