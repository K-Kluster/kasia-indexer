use deserializer::parse_sealed_operation;

pub mod deserializer;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Payment {
    pub r#type: String,
    pub amount: u64,
    pub message: String,
    pub timestamp: String,
    pub version: u32,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Handshake {
    pub alias: String,
    pub timestamp: String,
    pub conversation_id: String,
    pub version: u32,
    pub recipient_address: String,
    pub send_to_recipient: bool,
    pub is_response: Option<bool>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Message {
    pub content: String,
}

#[derive(Debug, PartialEq, Eq, Clone)]
/**
 * ContextualMessage is a message that is sent only once handshake is done.
 */
pub struct ContextualMessage {
    pub alias: String,
    pub content: String,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SealedHandshake {
    pub alias: String,
    pub timestamp: String,
    pub conversation_id: String,
    pub version: u32,
    pub recipient_address: String,
    pub send_to_recipient: bool,
    pub is_response: Option<bool>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SealedMessage {
    pub alias: String,
    pub sealed_hex: String,
}

#[derive(Debug, PartialEq, Eq, Clone)]
/**
 * SealedContextualMessage is a message that is sent only once handshake is done.
 */
pub struct SealedContextualMessageV1<'a> {
    pub alias: &'a [u8],
    pub sealed_hex: &'a [u8],
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SealedPaymentV1<'a> {
    pub sealed_hex: &'a [u8],
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SealedHandshakeV2<'a> {
    pub sealed_hex: &'a [u8],
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct SealedMessageOrSealedHandshakeVNone<'a> {
    pub sealed_hex: &'a [u8],
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SealedOperation<'a> {
    /**
     * "ciph_msg:{{SealedHandshake_as_json_string_as_hex}}"
     */

    /**
     * "ciph_msg:{{SealedMessage_as_json_string_as_hex}}"
     */
    SealedMessageOrSealedHandshakeVNone(SealedMessageOrSealedHandshakeVNone<'a>),
    /**
     * "ciph_msg:1:comm:{alias_as_string}:{{SealedContextualMessage_as_hex}}"
     */
    ContextualMessageV1(SealedContextualMessageV1<'a>),
    /**
     * "ciph_msg:1:payment:{{SealedPayment_as_json_string_as_hex}}"
     */
    PaymentV1(SealedPaymentV1<'a>),

    // V2
    /**
     * "ciph_msg:1:handshake:{{SealedHandshake_as_hex}}"
     */
    SealedHandshakeV2(SealedHandshakeV2<'a>),
}

impl<'a> SealedOperation<'a> {
    pub fn from_payload(payload: &'a [u8]) -> Option<SealedOperation<'a>> {
        parse_sealed_operation(payload)
    }

    pub fn op_type_name(&self) -> &'static str {
        match self {
            SealedOperation::SealedMessageOrSealedHandshakeVNone(_) => "HandshakeVNone",
            SealedOperation::ContextualMessageV1(_) => "ContextualMessageV1",
            SealedOperation::PaymentV1(_) => "PaymentV1",
            SealedOperation::SealedHandshakeV2(_) => "HandshakeV2",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_sealed_payment() {
        let payload = b"ciph_msg:1:payment:abc123";
        let result = parse_sealed_operation(payload);
        assert_eq!(
            result,
            Some(SealedOperation::PaymentV1(SealedPaymentV1 {
                sealed_hex: b"abc123",
            }))
        );
    }

    #[test]
    fn test_deserialize_sealed_contextual_message() {
        let payload = b"ciph_msg:1:comm:alias123:abc123";
        let result = parse_sealed_operation(payload);
        assert_eq!(
            result,
            Some(SealedOperation::ContextualMessageV1(
                SealedContextualMessageV1 {
                    alias: b"alias123",
                    sealed_hex: b"abc123",
                }
            ))
        );
    }

    #[test]
    fn test_deserialize_sealed_message_or_sealed_handshake() {
        let payload = b"ciph_msg:abc123";
        let result = parse_sealed_operation(payload);
        assert_eq!(
            result,
            Some(SealedOperation::SealedMessageOrSealedHandshakeVNone(
                SealedMessageOrSealedHandshakeVNone {
                    sealed_hex: b"abc123",
                }
            ))
        );
    }

    #[test]
    fn test_deserialize_sealed_handshake_v2() {
        let payload = b"ciph_msg:1:handshake:abc123";
        let result = parse_sealed_operation(payload);
        assert_eq!(
            result,
            Some(SealedOperation::SealedHandshakeV2(SealedHandshakeV2 {
                sealed_hex: b"abc123",
            }))
        );
    }

    #[test]
    fn test_deserialize_invalid_payload() {
        let payload = b"invalid_payload";
        let result = parse_sealed_operation(payload);
        assert_eq!(result, None);
    }
}
