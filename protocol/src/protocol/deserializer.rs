use crate::protocol::operation::{
    SealedContextualMessage, SealedMessageOrSealedHandshake, SealedOperation, SealedPayment,
};

pub fn deserialize_sealed_operation(payload_bytes: &[u8]) -> Option<SealedOperation> {
    // Convert bytes to JSON string
    let payload_string = match String::from_utf8(payload_bytes.to_vec()) {
        Ok(payload_string) => payload_string,
        Err(_) => return None,
    };

    // Check for specific operation types
    if let Some(payment_hex) = payload_string.strip_prefix("ciph_msg:1:payment:") {
        return Some(SealedOperation::Payment(SealedPayment {
            sealed_hex: payment_hex.to_string(),
        }));
    } else if payload_string.starts_with("ciph_msg:1:comm:") {
        let parts: Vec<&str> = payload_string.split(':').collect();
        if parts.len() >= 5 {
            let alias = parts[3];
            let contextual_message_hex = parts[4];
            return Some(SealedOperation::ContextualMessage(
                SealedContextualMessage {
                    alias: alias.to_string(),
                    sealed_hex: contextual_message_hex.to_string(),
                },
            ));
        }
    } else if payload_string.starts_with("ciph_msg:") {
        // Handle Handshake or Message
        let parts: Vec<&str> = payload_string.split(':').collect();

        if parts.len() >= 2 {
            let hex_string = parts[1];

            return Some(SealedOperation::SealedMessageOrSealedHandshake(
                SealedMessageOrSealedHandshake {
                    sealed_hex: hex_string.to_string(),
                },
            ));
        }
    }

    None
}
