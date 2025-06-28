#[derive(Debug, PartialEq)]
pub struct Payment {
    pub r#type: String,
    pub amount: u64,
    pub message: String,
    pub timestamp: String,
    pub version: u32,
}

#[derive(Debug, PartialEq)]
pub struct Handshake {
    pub alias: String,
    pub timestamp: String,
    pub conversation_id: String,
    pub version: u32,
    pub recipient_address: String,
    pub send_to_recipient: bool,
    pub is_response: Option<bool>,
}

#[derive(Debug, PartialEq)]
pub struct Message {
    pub content: String,
}

#[derive(Debug, PartialEq)]
/**
 * ContextualMessage is a message that is sent only once handshake is done.
 */
pub struct ContextualMessage {
    pub alias: String,
    pub content: String,
}

#[derive(Debug, PartialEq)]
pub struct SealedHandshake {
    pub alias: String,
    pub timestamp: String,
    pub conversation_id: String,
    pub version: u32,
    pub recipient_address: String,
    pub send_to_recipient: bool,
    pub is_response: Option<bool>,
}

#[derive(Debug, PartialEq)]
pub struct SealedMessage {
    pub alias: String,
    pub sealed_hex: String,
}

#[derive(Debug, PartialEq)]
/**
 * SealedContextualMessage is a message that is sent only once handshake is done.
 */
pub struct SealedContextualMessage {
    pub alias: String,
    pub sealed_hex: String,
}

#[derive(Debug, PartialEq)]
pub struct SealedPayment {
    pub sealed_hex: String,
}

#[derive(Debug, PartialEq)]
pub struct SealedMessageOrSealedHandshake {
    pub sealed_hex: String,
}

#[derive(Debug)]
pub enum SealedOperation {
    /**
     * "ciph_msg:{{SealedHandshake_as_json_string_as_hex}}"
     */

    /**
     * "ciph_msg:{{SealedMessage_as_json_string_as_hex}}"
     */
    SealedMessageOrSealedHandshake(SealedMessageOrSealedHandshake),
    /**
     * "ciph_msg:1:comm:{alias_as_string}:{{SealedContextualMessage_as_hex}}"
     */
    ContextualMessage(SealedContextualMessage),
    /**
     * "ciph_msg:1:payment:{{SealedPayment_as_json_string_as_hex}}"
     */
    Payment(SealedPayment),
}

impl PartialEq for SealedOperation {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                SealedOperation::SealedMessageOrSealedHandshake(handshake1),
                SealedOperation::SealedMessageOrSealedHandshake(handshake2),
            ) => handshake1 == handshake2,
            (SealedOperation::Payment(payment1), SealedOperation::Payment(payment2)) => {
                payment1 == payment2
            }
            (
                SealedOperation::ContextualMessage(contextual_message1),
                SealedOperation::ContextualMessage(contextual_message2),
            ) => contextual_message1 == contextual_message2,
            _ => false,
        }
    }
}
