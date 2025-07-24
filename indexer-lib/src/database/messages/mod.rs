//! Message protocol data storage.
//!
//! Contains partitions for different types of encrypted messages sent through
//! Kaspa transactions: handshakes, payments, and contextual application messages.

pub mod contextual_messages;
pub mod handshakes;
pub mod payments;

pub use contextual_messages::*;
pub use handshakes::*;
pub use payments::*;
