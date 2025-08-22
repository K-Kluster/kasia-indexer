use crate::operation::{
    SealedContextualMessageV1, SealedMessageOrSealedHandshakeVNone, SealedOperation,
    SealedPaymentV1, SealedSelfStashV1,
};
use tracing::warn;

pub const PROTOCOL_PREFIX: &str = "ciph_msg:";
pub const VERSION_1_PART: &str = "1:";

pub fn parse_sealed_operation(payload_bytes: &[u8]) -> Option<SealedOperation<'_>> {
    let payload_without_protocol = payload_bytes.strip_prefix(PROTOCOL_PREFIX.as_bytes())?;
    if payload_without_protocol.is_empty() {
        return None;
    }

    match payload_without_protocol.strip_prefix(VERSION_1_PART.as_bytes()) {
        // Handle SealedHandshake or SealedMessage
        None => Some(SealedOperation::SealedMessageOrSealedHandshakeVNone(
            SealedMessageOrSealedHandshakeVNone {
                sealed_hex: payload_without_protocol,
            },
        )),
        Some(
            [
                b'p',
                b'a',
                b'y',
                b'm',
                b'e',
                b'n',
                b't',
                b':',
                sealed_hex @ ..,
            ],
        ) => Some(SealedOperation::PaymentV1(SealedPaymentV1 { sealed_hex })),
        Some([b'c', b'o', b'm', b'm', b':', remaining @ ..]) => {
            let delimiter_idx = remaining.iter().position(|b| b == &b':')?;
            let alias = &remaining[..delimiter_idx];
            let contextual_message_hex = &remaining[delimiter_idx + 1..];
            Some(SealedOperation::ContextualMessageV1(
                SealedContextualMessageV1 {
                    alias,
                    sealed_hex: contextual_message_hex,
                },
            ))
        }
        Some(
            [
                b's',
                b'e',
                b'l',
                b'f',
                b'_',
                b's',
                b't',
                b'a',
                b's',
                b'h',
                b':',
                remaining @ ..,
            ],
        ) => {
            let delimiter_idx_option = remaining.iter().position(|b| b == &b':');

            match delimiter_idx_option {
                Some(delimiter_idx) => {
                    let key = &remaining[..delimiter_idx];
                    let sealed_data = &remaining[delimiter_idx + 1..];
                    Some(SealedOperation::SelfStashV1(SealedSelfStashV1 {
                        key: Some(key),
                        sealed_hex: sealed_data,
                    }))
                }
                None => Some(SealedOperation::SelfStashV1(SealedSelfStashV1 {
                    key: None,
                    sealed_hex: remaining,
                })),
            }
        }
        Some(msg_type_and_content) => {
            let msg_type_and_content = faster_hex::hex_string(msg_type_and_content);
            warn!("Unknown operation type: {msg_type_and_content}");
            None
        }
    }
}
