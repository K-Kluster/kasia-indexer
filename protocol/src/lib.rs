pub mod protocol;

#[cfg(test)]
mod tests {

    use super::protocol::deserializer::deserialize_sealed_operation;
    use super::protocol::operation::*;

    #[test]
    fn test_deserialize_sealed_payment() {
        let payload = b"ciph_msg:1:payment:abc123";
        let result = deserialize_sealed_operation(payload);
        assert_eq!(
            result,
            Some(SealedOperation::Payment(SealedPayment {
                sealed_hex: "abc123".to_string(),
            }))
        );
    }

    #[test]
    fn test_deserialize_sealed_contextual_message() {
        let payload = b"ciph_msg:1:comm:alias123:abc123";
        let result = deserialize_sealed_operation(payload);
        assert_eq!(
            result,
            Some(SealedOperation::ContextualMessage(
                SealedContextualMessage {
                    alias: "alias123".to_string(),
                    sealed_hex: "abc123".to_string(),
                }
            ))
        );
    }

    #[test]
    fn test_deserialize_sealed_message_or_sealed_handshake() {
        let payload = b"ciph_msg:abc123";
        let result = deserialize_sealed_operation(payload);
        assert_eq!(
            result,
            Some(SealedOperation::SealedMessageOrSealedHandshake(
                SealedMessageOrSealedHandshake {
                    sealed_hex: "abc123".to_string(),
                }
            ))
        );
    }

    #[test]
    fn test_deserialize_invalid_payload() {
        let payload = b"invalid_payload";
        let result = deserialize_sealed_operation(payload);
        assert_eq!(result, None);
    }
}
