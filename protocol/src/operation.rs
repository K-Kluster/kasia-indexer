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
pub struct SealedContextualMessage<'a> {
    pub alias: &'a [u8],
    pub sealed_hex: &'a [u8],
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SealedPayment<'a> {
    pub sealed_hex: &'a [u8],
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SealedMessageOrSealedHandshake<'a> {
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
    SealedMessageOrSealedHandshake(SealedMessageOrSealedHandshake<'a>),
    /**
     * "ciph_msg:1:comm:{alias_as_string}:{{SealedContextualMessage_as_hex}}"
     */
    ContextualMessage(SealedContextualMessage<'a>),
    /**
     * "ciph_msg:1:payment:{{SealedPayment_as_json_string_as_hex}}"
     */
    Payment(SealedPayment<'a>),
}

impl<'a> SealedOperation<'a> {
    pub fn from_payload(payload: &'a [u8]) -> Option<SealedOperation<'a>> {
        parse_sealed_operation(payload)
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
            Some(SealedOperation::Payment(SealedPayment {
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
            Some(SealedOperation::ContextualMessage(
                SealedContextualMessage {
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
            Some(SealedOperation::SealedMessageOrSealedHandshake(
                SealedMessageOrSealedHandshake {
                    sealed_hex: b"abc123",
                }
            ))
        );
    }

    #[test]
    fn test_deserialize_invalid_payload() {
        let payload = b"invalid_payload";
        let result = parse_sealed_operation(payload);
        assert_eq!(result, None);
    }
}
