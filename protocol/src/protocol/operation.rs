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
pub struct SealedContextualMessage {
    pub alias: String,
    pub sealed_hex: String,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SealedPayment {
    pub sealed_hex: String,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SealedMessageOrSealedHandshake {
    pub sealed_hex: String,
}

#[derive(Debug, PartialEq, Eq, Clone)]
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
