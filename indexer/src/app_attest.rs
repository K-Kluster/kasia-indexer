use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD};
use ring::signature::{ECDSA_P256_SHA256_ASN1, ECDSA_P256_SHA256_FIXED, UnparsedPublicKey};
use sha2::{Digest, Sha256};

const MAX_CBOR_DEPTH: usize = 24;

#[derive(Debug, Clone)]
pub struct AppAttestedKeyData {
    pub key_id: String,
    pub public_key_spki_b64: String,
    pub sign_count: u32,
}

#[derive(Debug, Clone)]
pub struct AppAttestVerifier {
    app_id: String,
    rp_id_hash: [u8; 32],
}

impl AppAttestVerifier {
    pub fn new(team_id: &str, bundle_id: &str) -> Option<Self> {
        let team_id = team_id.trim();
        let bundle_id = bundle_id.trim();
        if team_id.is_empty() || bundle_id.is_empty() {
            return None;
        }

        let app_id = app_attest_app_id(team_id, bundle_id);
        let rp_id_hash: [u8; 32] = Sha256::digest(app_id.as_bytes()).into();
        Some(Self { app_id, rp_id_hash })
    }

    pub fn verify_attestation(
        &self,
        attestation_b64: &str,
        key_id: &str,
        challenge: &str,
    ) -> Result<AppAttestedKeyData, String> {
        let attestation_bytes = decode_base64_any(attestation_b64, "app_attest_attestation")?;
        let canonical_key_id = canonical_key_id(key_id)?;
        let key_id_bytes = decode_base64_any(key_id, "app_attest_key_id")?;
        if key_id_bytes.len() != 32 {
            return Err("app_attest_key_id must decode to 32 bytes".to_string());
        }

        let cbor = parse_cbor(&attestation_bytes)?;
        let root = cbor
            .as_map()
            .ok_or_else(|| "App Attest payload must be a CBOR map".to_string())?;

        let fmt = map_get_text(root, "fmt")
            .and_then(CborValue::as_text)
            .ok_or_else(|| "Missing fmt in app_attest_attestation".to_string())?;
        if fmt != "apple-appattest" {
            return Err("Unexpected app attest format".to_string());
        }

        let auth_data = map_get_text(root, "authData")
            .and_then(CborValue::as_bytes)
            .ok_or_else(|| "Missing authData in app_attest_attestation".to_string())?;
        let parsed_auth = parse_auth_data(auth_data)?;

        if parsed_auth.rp_id_hash != self.rp_id_hash {
            return Err(format!(
                "Attestation rpIdHash does not match configured App ID ({})",
                self.app_id
            ));
        }

        // App Attest attestation must include attested credential data.
        if parsed_auth.flags & 0x40 == 0 {
            return Err("Attestation missing attested credential data flag".to_string());
        }

        // Attestation signCount should start at 0 for a newly attested key.
        if parsed_auth.sign_count != 0 {
            return Err("Attestation signCount must be 0".to_string());
        }

        let is_valid_aaguid = parsed_auth.aaguid == *b"appattestdevelop"
            || parsed_auth.aaguid == *b"appattest\0\0\0\0\0\0\0";
        if !is_valid_aaguid {
            return Err("Invalid App Attest AAGUID".to_string());
        }

        if parsed_auth.credential_id != key_id_bytes {
            return Err("Attestation credentialId does not match app_attest_key_id".to_string());
        }

        let att_stmt = map_get_text(root, "attStmt")
            .and_then(CborValue::as_map)
            .ok_or_else(|| "Missing attStmt in app_attest_attestation".to_string())?;
        let x5c = map_get_text(att_stmt, "x5c")
            .and_then(CborValue::as_array)
            .ok_or_else(|| "Missing attStmt.x5c in app_attest_attestation".to_string())?;
        if x5c.is_empty() {
            return Err("Attestation x5c chain is empty".to_string());
        }

        let mut certs = Vec::with_capacity(x5c.len());
        for item in x5c {
            let cert = item
                .as_bytes()
                .ok_or_else(|| "attStmt.x5c must contain byte strings".to_string())?;
            certs.push(cert.to_vec());
        }

        let client_data_hash: [u8; 32] = Sha256::digest(challenge.as_bytes()).into();
        let mut nonce_input = auth_data.to_vec();
        nonce_input.extend_from_slice(&client_data_hash);
        let expected_nonce: [u8; 32] = Sha256::digest(&nonce_input).into();
        if !leaf_cert_has_nonce(&certs[0], &expected_nonce)? {
            return Err("Attestation nonce not found in certificate extension".to_string());
        }

        let public_key_spki_b64 = STANDARD.encode(&parsed_auth.credential_public_key);
        Ok(AppAttestedKeyData {
            key_id: canonical_key_id,
            public_key_spki_b64,
            sign_count: parsed_auth.sign_count,
        })
    }

    pub fn verify_assertion(
        &self,
        assertion_b64: &str,
        public_key_spki_b64: &str,
        challenge: &str,
        min_sign_count: u32,
    ) -> Result<u32, String> {
        let assertion_bytes = decode_base64_any(assertion_b64, "app_attest_assertion")?;
        let cbor = parse_cbor(&assertion_bytes)?;
        let root = cbor
            .as_map()
            .ok_or_else(|| "App Attest assertion must be a CBOR map".to_string())?;

        let authenticator_data = map_get_text(root, "authenticatorData")
            .and_then(CborValue::as_bytes)
            .ok_or_else(|| "Missing authenticatorData in app_attest_assertion".to_string())?;
        let signature = map_get_text(root, "signature")
            .and_then(CborValue::as_bytes)
            .ok_or_else(|| "Missing signature in app_attest_assertion".to_string())?;

        let parsed_assertion = parse_assertion_auth_data(authenticator_data)?;
        if parsed_assertion.rp_id_hash != self.rp_id_hash {
            return Err(format!(
                "Assertion rpIdHash does not match configured App ID ({})",
                self.app_id
            ));
        }

        if parsed_assertion.sign_count <= min_sign_count {
            return Err("App Attest assertion signCount did not increase".to_string());
        }

        let client_data_hash: [u8; 32] = Sha256::digest(challenge.as_bytes()).into();
        let mut signed_data = Vec::with_capacity(authenticator_data.len() + client_data_hash.len());
        signed_data.extend_from_slice(authenticator_data);
        signed_data.extend_from_slice(&client_data_hash);

        let public_key_sec1 = decode_base64_any(public_key_spki_b64, "app_attest_public_key")?;
        if public_key_sec1.len() != 65 || public_key_sec1[0] != 0x04 {
            return Err("Invalid App Attest public key format".to_string());
        }
        verify_p256_signature(&public_key_sec1, &signed_data, signature)
            .map_err(|_| "Invalid App Attest assertion signature".to_string())?;

        Ok(parsed_assertion.sign_count)
    }
}

pub fn canonical_key_id(key_id: &str) -> Result<String, String> {
    let key_id = key_id.trim();
    if key_id.is_empty() {
        return Err("app_attest_key_id must not be empty".to_string());
    }
    let key_id_bytes = decode_base64_any(key_id, "app_attest_key_id")?;
    if key_id_bytes.len() != 32 {
        return Err("app_attest_key_id must decode to 32 bytes".to_string());
    }
    Ok(URL_SAFE_NO_PAD.encode(key_id_bytes))
}

fn app_attest_app_id(team_id: &str, bundle_id: &str) -> String {
    let prefixed = format!("{team_id}.");
    if bundle_id.starts_with(&prefixed) {
        bundle_id.to_string()
    } else {
        format!("{team_id}.{bundle_id}")
    }
}

fn verify_p256_signature(
    public_key_sec1: &[u8],
    signed_data: &[u8],
    signature: &[u8],
) -> Result<(), ()> {
    let verifier_asn1 = UnparsedPublicKey::new(&ECDSA_P256_SHA256_ASN1, public_key_sec1);
    if verifier_asn1.verify(signed_data, signature).is_ok() {
        return Ok(());
    }
    let verifier_fixed = UnparsedPublicKey::new(&ECDSA_P256_SHA256_FIXED, public_key_sec1);
    verifier_fixed
        .verify(signed_data, signature)
        .map_err(|_| ())
}

fn decode_base64_any(value: &str, field_name: &str) -> Result<Vec<u8>, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(format!("{field_name} must not be empty"));
    }
    if let Ok(decoded) = STANDARD.decode(value) {
        return Ok(decoded);
    }
    if let Ok(decoded) = URL_SAFE_NO_PAD.decode(value) {
        return Ok(decoded);
    }
    if let Ok(decoded) = URL_SAFE.decode(value) {
        return Ok(decoded);
    }
    Err(format!("{field_name} is invalid base64"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedAuthData {
    rp_id_hash: [u8; 32],
    flags: u8,
    sign_count: u32,
    aaguid: [u8; 16],
    credential_id: Vec<u8>,
    credential_public_key: Vec<u8>,
}

fn parse_auth_data(auth_data: &[u8]) -> Result<ParsedAuthData, String> {
    if auth_data.len() < 55 {
        return Err("authData too short for App Attest".to_string());
    }

    let rp_id_hash: [u8; 32] = auth_data[0..32]
        .try_into()
        .map_err(|_| "Invalid rpIdHash length".to_string())?;
    let flags = auth_data[32];
    let sign_count = u32::from_be_bytes(
        auth_data[33..37]
            .try_into()
            .map_err(|_| "Invalid signCount bytes".to_string())?,
    );
    let aaguid: [u8; 16] = auth_data[37..53]
        .try_into()
        .map_err(|_| "Invalid AAGUID length".to_string())?;
    let credential_id_len = u16::from_be_bytes(
        auth_data[53..55]
            .try_into()
            .map_err(|_| "Invalid credentialId length bytes".to_string())?,
    ) as usize;
    let credential_start: usize = 55;
    let credential_end = credential_start.saturating_add(credential_id_len);
    if credential_end > auth_data.len() {
        return Err("authData credentialId is out of bounds".to_string());
    }
    let credential_public_key_cbor = auth_data
        .get(credential_end..)
        .ok_or_else(|| "Missing credential public key in authData".to_string())?;
    let (credential_public_key_value, _consumed) = parse_cbor_prefix(credential_public_key_cbor)?;
    let credential_public_key = extract_cose_p256_public_key(&credential_public_key_value)
        .map_err(|err| format!("Invalid credential public key in authData: {err}"))?;

    Ok(ParsedAuthData {
        rp_id_hash,
        flags,
        sign_count,
        aaguid,
        credential_id: auth_data[credential_start..credential_end].to_vec(),
        credential_public_key,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedAssertionAuthData {
    rp_id_hash: [u8; 32],
    sign_count: u32,
}

fn parse_assertion_auth_data(auth_data: &[u8]) -> Result<ParsedAssertionAuthData, String> {
    if auth_data.len() < 37 {
        return Err("authenticatorData is too short".to_string());
    }
    let rp_id_hash: [u8; 32] = auth_data[0..32]
        .try_into()
        .map_err(|_| "Invalid assertion rpIdHash length".to_string())?;
    let sign_count = u32::from_be_bytes(
        auth_data[33..37]
            .try_into()
            .map_err(|_| "Invalid assertion signCount bytes".to_string())?,
    );
    Ok(ParsedAssertionAuthData {
        rp_id_hash,
        sign_count,
    })
}

fn leaf_cert_has_nonce(leaf_cert_der: &[u8], expected_nonce: &[u8]) -> Result<bool, String> {
    // DER OID for 1.2.840.113635.100.8.2
    const NONCE_OID_DER: &[u8] = &[
        0x06, 0x09, 0x2A, 0x86, 0x48, 0x86, 0xF7, 0x63, 0x64, 0x08, 0x02,
    ];
    for (index, window) in leaf_cert_der.windows(NONCE_OID_DER.len()).enumerate() {
        if window == NONCE_OID_DER {
            let start = index;
            let end = std::cmp::min(leaf_cert_der.len(), index + 1024);
            let region = &leaf_cert_der[start..end];
            if region
                .windows(expected_nonce.len())
                .any(|chunk| chunk == expected_nonce)
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CborValue {
    Unsigned(u64),
    Negative(i64),
    Bytes(Vec<u8>),
    Text(String),
    Array(Vec<CborValue>),
    Map(Vec<(CborValue, CborValue)>),
    Bool(bool),
    Null,
}

impl CborValue {
    fn as_map(&self) -> Option<&[(CborValue, CborValue)]> {
        match self {
            CborValue::Map(values) => Some(values.as_slice()),
            _ => None,
        }
    }

    fn as_array(&self) -> Option<&[CborValue]> {
        match self {
            CborValue::Array(values) => Some(values.as_slice()),
            _ => None,
        }
    }

    fn as_text(&self) -> Option<&str> {
        match self {
            CborValue::Text(value) => Some(value.as_str()),
            _ => None,
        }
    }

    fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            CborValue::Bytes(value) => Some(value.as_slice()),
            _ => None,
        }
    }

    fn as_i64(&self) -> Option<i64> {
        match self {
            CborValue::Unsigned(value) => i64::try_from(*value).ok(),
            CborValue::Negative(value) => Some(*value),
            _ => None,
        }
    }
}

fn map_get_text<'a>(map: &'a [(CborValue, CborValue)], key: &str) -> Option<&'a CborValue> {
    for (map_key, value) in map {
        if let Some(text_key) = map_key.as_text() {
            if text_key == key {
                return Some(value);
            }
        }
    }
    None
}

fn map_get_i64<'a>(map: &'a [(CborValue, CborValue)], key: i64) -> Option<&'a CborValue> {
    for (map_key, value) in map {
        if map_key.as_i64() == Some(key) {
            return Some(value);
        }
    }
    None
}

fn extract_cose_p256_public_key(value: &CborValue) -> Result<Vec<u8>, String> {
    let map = value
        .as_map()
        .ok_or_else(|| "COSE key must be a map".to_string())?;

    let kty = map_get_i64(map, 1)
        .and_then(CborValue::as_i64)
        .ok_or_else(|| "COSE key is missing kty".to_string())?;
    if kty != 2 {
        return Err("COSE key kty must be EC2 (2)".to_string());
    }

    let alg = map_get_i64(map, 3)
        .and_then(CborValue::as_i64)
        .ok_or_else(|| "COSE key is missing alg".to_string())?;
    if alg != -7 {
        return Err("COSE key alg must be ES256 (-7)".to_string());
    }

    let curve = map_get_i64(map, -1)
        .and_then(CborValue::as_i64)
        .ok_or_else(|| "COSE key is missing curve".to_string())?;
    if curve != 1 {
        return Err("COSE key curve must be P-256 (1)".to_string());
    }

    let x = map_get_i64(map, -2)
        .and_then(CborValue::as_bytes)
        .ok_or_else(|| "COSE key is missing x coordinate".to_string())?;
    let y = map_get_i64(map, -3)
        .and_then(CborValue::as_bytes)
        .ok_or_else(|| "COSE key is missing y coordinate".to_string())?;
    if x.len() != 32 || y.len() != 32 {
        return Err("COSE key coordinates must be 32 bytes".to_string());
    }

    let mut public_key = Vec::with_capacity(65);
    public_key.push(0x04);
    public_key.extend_from_slice(x);
    public_key.extend_from_slice(y);
    Ok(public_key)
}

fn parse_cbor(input: &[u8]) -> Result<CborValue, String> {
    let (value, offset) = parse_cbor_prefix(input)?;
    if offset != input.len() {
        return Err("Trailing bytes after CBOR payload".to_string());
    }
    Ok(value)
}

fn parse_cbor_prefix(input: &[u8]) -> Result<(CborValue, usize), String> {
    let mut offset = 0usize;
    let value = parse_cbor_value(input, &mut offset, 0)?;
    Ok((value, offset))
}

fn parse_cbor_value(input: &[u8], offset: &mut usize, depth: usize) -> Result<CborValue, String> {
    if depth > MAX_CBOR_DEPTH {
        return Err("CBOR nesting is too deep".to_string());
    }
    let initial = *input
        .get(*offset)
        .ok_or_else(|| "Unexpected end of CBOR payload".to_string())?;
    *offset += 1;

    let major = initial >> 5;
    let add = initial & 0x1f;
    match major {
        0 => {
            let value = read_cbor_len(input, offset, add)?
                .ok_or_else(|| "Invalid CBOR uint".to_string())?;
            Ok(CborValue::Unsigned(value))
        }
        1 => {
            let value = read_cbor_len(input, offset, add)?
                .ok_or_else(|| "Invalid CBOR negative int".to_string())?;
            if value > i64::MAX as u64 {
                return Err("CBOR negative int is out of range".to_string());
            }
            Ok(CborValue::Negative(-1 - (value as i64)))
        }
        2 => {
            let data = read_cbor_bytes(input, offset, add, depth)?;
            Ok(CborValue::Bytes(data))
        }
        3 => {
            let bytes = read_cbor_bytes(input, offset, add, depth)?;
            let text =
                String::from_utf8(bytes).map_err(|_| "Invalid CBOR UTF-8 text".to_string())?;
            Ok(CborValue::Text(text))
        }
        4 => {
            let len = read_cbor_len(input, offset, add)?;
            let mut values = Vec::new();
            match len {
                Some(count) => {
                    let count = usize::try_from(count)
                        .map_err(|_| "CBOR array length is too large".to_string())?;
                    values.reserve(count);
                    for _ in 0..count {
                        values.push(parse_cbor_value(input, offset, depth + 1)?);
                    }
                }
                None => loop {
                    if is_cbor_break(input, *offset) {
                        *offset += 1;
                        break;
                    }
                    values.push(parse_cbor_value(input, offset, depth + 1)?);
                },
            }
            Ok(CborValue::Array(values))
        }
        5 => {
            let len = read_cbor_len(input, offset, add)?;
            let mut values = Vec::new();
            match len {
                Some(count) => {
                    let count = usize::try_from(count)
                        .map_err(|_| "CBOR map length is too large".to_string())?;
                    values.reserve(count);
                    for _ in 0..count {
                        let key = parse_cbor_value(input, offset, depth + 1)?;
                        let value = parse_cbor_value(input, offset, depth + 1)?;
                        values.push((key, value));
                    }
                }
                None => loop {
                    if is_cbor_break(input, *offset) {
                        *offset += 1;
                        break;
                    }
                    let key = parse_cbor_value(input, offset, depth + 1)?;
                    let value = parse_cbor_value(input, offset, depth + 1)?;
                    values.push((key, value));
                },
            }
            Ok(CborValue::Map(values))
        }
        7 => match add {
            20 => Ok(CborValue::Bool(false)),
            21 => Ok(CborValue::Bool(true)),
            22 => Ok(CborValue::Null),
            _ => Err("Unsupported CBOR simple value".to_string()),
        },
        _ => Err("Unsupported CBOR major type".to_string()),
    }
}

fn read_cbor_bytes(
    input: &[u8],
    offset: &mut usize,
    add: u8,
    _depth: usize,
) -> Result<Vec<u8>, String> {
    let len = read_cbor_len(input, offset, add)?;
    match len {
        Some(len) => {
            let len = usize::try_from(len)
                .map_err(|_| "CBOR byte string length is too large".to_string())?;
            read_exact(input, offset, len).map(|slice| slice.to_vec())
        }
        None => {
            let mut out = Vec::new();
            loop {
                if is_cbor_break(input, *offset) {
                    *offset += 1;
                    break;
                }
                let initial = *input
                    .get(*offset)
                    .ok_or_else(|| "Unexpected end of CBOR byte chunks".to_string())?;
                let major = initial >> 5;
                let add = initial & 0x1f;
                if major != 2 {
                    return Err("Indefinite CBOR byte string has non-byte chunk".to_string());
                }
                *offset += 1;
                let chunk_len = read_cbor_len(input, offset, add)?
                    .ok_or_else(|| "Nested indefinite CBOR chunk is not supported".to_string())?;
                let chunk_len = usize::try_from(chunk_len)
                    .map_err(|_| "CBOR byte chunk length is too large".to_string())?;
                let chunk = read_exact(input, offset, chunk_len)?;
                out.extend_from_slice(chunk);
            }
            Ok(out)
        }
    }
}

fn read_cbor_len(input: &[u8], offset: &mut usize, add: u8) -> Result<Option<u64>, String> {
    match add {
        0..=23 => Ok(Some(add as u64)),
        24 => Ok(Some(read_uint(input, offset, 1)?)),
        25 => Ok(Some(read_uint(input, offset, 2)?)),
        26 => Ok(Some(read_uint(input, offset, 4)?)),
        27 => Ok(Some(read_uint(input, offset, 8)?)),
        31 => Ok(None),
        _ => Err("Unsupported CBOR additional info".to_string()),
    }
}

fn read_uint(input: &[u8], offset: &mut usize, width: usize) -> Result<u64, String> {
    let bytes = read_exact(input, offset, width)?;
    let value = match width {
        1 => bytes[0] as u64,
        2 => u16::from_be_bytes(
            bytes
                .try_into()
                .map_err(|_| "Invalid CBOR u16 length".to_string())?,
        ) as u64,
        4 => u32::from_be_bytes(
            bytes
                .try_into()
                .map_err(|_| "Invalid CBOR u32 length".to_string())?,
        ) as u64,
        8 => u64::from_be_bytes(
            bytes
                .try_into()
                .map_err(|_| "Invalid CBOR u64 length".to_string())?,
        ),
        _ => return Err("Invalid CBOR integer width".to_string()),
    };
    Ok(value)
}

fn read_exact<'a>(input: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8], String> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| "CBOR offset overflow".to_string())?;
    if end > input.len() {
        return Err("Unexpected end of CBOR payload".to_string());
    }
    let slice = &input[*offset..end];
    *offset = end;
    Ok(slice)
}

fn is_cbor_break(input: &[u8], offset: usize) -> bool {
    matches!(input.get(offset), Some(0xff))
}

#[cfg(test)]
mod tests {
    use super::{
        CborValue, canonical_key_id, map_get_text, parse_assertion_auth_data, parse_cbor,
        verify_p256_signature,
    };
    use base64::Engine;
    use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
    use ring::rand::SystemRandom;
    use ring::signature::{
        ECDSA_P256_SHA256_ASN1_SIGNING, ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair, KeyPair,
    };

    #[test]
    fn parses_basic_attestation_shape() {
        // {
        //   "fmt": "apple-appattest",
        //   "authData": h'010203'
        // }
        let payload: Vec<u8> = vec![
            0xA2, 0x63, b'f', b'm', b't', 0x6F, b'a', b'p', b'p', b'l', b'e', b'-', b'a', b'p',
            b'p', b'a', b't', b't', b'e', b's', b't', 0x68, b'a', b'u', b't', b'h', b'D', b'a',
            b't', b'a', 0x43, 0x01, 0x02, 0x03,
        ];
        let value = parse_cbor(&payload).expect("valid cbor");
        let map = value.as_map().expect("map");
        assert_eq!(
            map_get_text(map, "fmt").and_then(CborValue::as_text),
            Some("apple-appattest")
        );
        assert_eq!(
            map_get_text(map, "authData").and_then(CborValue::as_bytes),
            Some([1, 2, 3].as_slice())
        );
    }

    #[test]
    fn canonicalizes_key_id_to_urlsafe_no_pad() {
        let bytes: Vec<u8> = (0u8..32u8).collect();
        let standard = STANDARD.encode(&bytes);
        let canonical = canonical_key_id(&standard).expect("canonical key id");
        assert_eq!(canonical, URL_SAFE_NO_PAD.encode(&bytes));
    }

    #[test]
    fn parses_assertion_auth_data_counter() {
        let mut auth_data = vec![0u8; 37];
        auth_data[33..37].copy_from_slice(&5u32.to_be_bytes());
        let parsed = parse_assertion_auth_data(&auth_data).expect("parsed assertion auth data");
        assert_eq!(parsed.sign_count, 5);
    }

    #[test]
    fn accepts_asn1_and_fixed_ecdsa_signatures() {
        let rng = SystemRandom::new();
        let message = b"kasia-app-attest-signature-test";

        let asn1_pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &rng)
            .expect("generate asn1 pkcs8");
        let asn1_key_pair =
            EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, asn1_pkcs8.as_ref(), &rng)
                .expect("parse asn1 pkcs8");
        let asn1_signature = asn1_key_pair
            .sign(&rng, message)
            .expect("sign asn1 message");
        assert!(
            verify_p256_signature(
                asn1_key_pair.public_key().as_ref(),
                message,
                asn1_signature.as_ref()
            )
            .is_ok()
        );

        let fixed_pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng)
            .expect("generate fixed pkcs8");
        let fixed_key_pair =
            EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, fixed_pkcs8.as_ref(), &rng)
                .expect("parse fixed pkcs8");
        let fixed_signature = fixed_key_pair
            .sign(&rng, message)
            .expect("sign fixed message");
        assert!(
            verify_p256_signature(
                fixed_key_pair.public_key().as_ref(),
                message,
                fixed_signature.as_ref()
            )
            .is_ok()
        );
    }
}
