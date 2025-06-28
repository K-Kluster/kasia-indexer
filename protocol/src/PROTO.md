### Current Protocol

#### Handshakes Request (tx sent to someone's address)

`ciph_msg:{{SealedHandshake_as_json_string_as_hex}}`

with:

- `SealedHandshake_as_json_string_as_hex` being a hex-encoded JSON string of the following object:

```json
{
  "alias": "myAliasAsASender",
  "timestamp": "DateString",
  "conversation_id": "conversationId",
  "version": 1,
  "recipient_address": "kaspa:...",
  "send_to_recipient": true,
  "is_response": null
}
```

#### Handshakes Response (tx sent to someone's address)

`ciph_msg:{{SealedHandshake_as_json_string_as_hex}}`

with:

- `SealedHandshake_as_json_string_as_hex` being a hex-encoded JSON string of the following object:

```json
{
  "alias": "myAliasAsAReceiver",
  "timestamp": "DateString",
  "conversation_id": "conversationId",
  "version": 1,
  "recipient_address": "kaspa:...",
  "send_to_recipient": true,
  "is_response": true
}
```

#### Send Message (without context) (tx sent to our address)

`ciph_msg:{{SealedMessage_as_json_string_as_hex}}`

with:

- `SealedMessage_as_json_string_as_hex` being a hex-encoded JSON string of the following object:

```json
{
  "sealed_hex": "hex_encoded_encrypted_message"
}
```

#### Send Message (with context) (tx sent to our address)

`ciph_msg:1:comm:{their_alias}:{{SealedContextualMessage_as_hex}}`

with:

- `SealedContextualMessage_as_hex` being the hex-encoded `SealedContextualMessage` (from cipher-wasm)

#### Payment (tx sent to someone's address)

`ciph_msg:1:payment:{{SealedPayment_as_json_string_as_hex}}`

with:

- `SealedPayment_as_json_string_as_hex` being the hex-encoded `SealedPayment` (from cipher-wasm)

The encrypted `part` is a JSON string of the following object:

```json
{
  "type": "payment",
  "message": "String",
  "amount": 1,
  "timestamp": "DateString",
  "version": 1
}
```

### Limitations

- **Handshake and Message Differentiation**: The current protocol does not differentiate between handshakes and messages. Both are represented as `SealedMessageOrSealedHandshake` in the `SealedOperation` enum. This limitation means that the deserializer cannot distinguish between handshakes and messages based on the payload alone.
