use crate::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

#[derive(Copy, Clone, Unaligned, Immutable, KnownLayout, IntoBytes, FromBytes)]
#[repr(C)]
pub struct DaaIndexKeyV0 {
    pub daa_score: zerocopy::U64<zerocopy::BigEndian>,
    pub block_hash: [u8; 32],
}
