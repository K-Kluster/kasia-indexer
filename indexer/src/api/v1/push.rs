use crate::config::PushAuthMode;
use crate::push::{DeviceKeyBinding, PushRegistryHandle, WalletBinding};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, post, put};
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE, URL_SAFE_NO_PAD};
use kaspa_addresses::{Address, Version};
use kaspa_rpc_core::{RpcAddress, RpcNetworkType};
use rand::RngCore;
use ring::signature::{ECDSA_P256_SHA256_ASN1, ECDSA_P256_SHA256_FIXED, UnparsedPublicKey};
use secp256k1::schnorr::Signature as SchnorrSignature;
use secp256k1::{Message, Secp256k1, XOnlyPublicKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::warn;
use utoipa::ToSchema;

const AUTH_DOMAIN: &str = "kasia-push-auth:v1";
const DEVICE_AUTH_DOMAIN: &str = "kasia-push-device-auth:v1";
const DEVICE_AUTH_SCHEME: &str = "device_key_v1";
const NONCE_TTL_MS: u64 = 60_000;
const MAX_SIGNATURE_WINDOW_MS: u64 = 60_000;
const MAX_CLOCK_SKEW_MS: u64 = 60_000;
const MAX_NONCE_STORE_ENTRIES: usize = 50_000;

#[derive(Clone)]
pub struct PushApi {
    registry: PushRegistryHandle,
    auth_mode: PushAuthMode,
    network_type: RpcNetworkType,
    nonces: Arc<StdMutex<NonceStore>>,
}

impl PushApi {
    pub fn new(
        registry: PushRegistryHandle,
        network_type: RpcNetworkType,
        auth_mode: PushAuthMode,
        _app_attest_team_id: Option<String>,
        _app_attest_bundle_id: Option<String>,
    ) -> Self {
        Self {
            registry,
            auth_mode,
            network_type,
            nonces: Arc::new(StdMutex::new(NonceStore::default())),
        }
    }

    pub fn router() -> Router<Self> {
        Router::new()
            .route("/challenge", post(create_challenge))
            .route("/register", post(register_device))
            .route("/update", put(update_registration))
            .route("/unregister", delete(unregister_device))
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct PushRegistrationRequest {
    #[serde(rename = "device_token")]
    pub device_token: String,
    pub platform: String,
    #[serde(rename = "watched_addresses")]
    pub watched_addresses: Vec<String>,
    #[serde(default)]
    #[serde(rename = "primary_address")]
    pub primary_address: Option<String>,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub auth: Option<PushAuthRequest>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct PushUpdateRequest {
    #[serde(rename = "device_token")]
    pub device_token: String,
    #[serde(rename = "watched_addresses")]
    pub watched_addresses: Vec<String>,
    #[serde(default)]
    #[serde(rename = "primary_address")]
    pub primary_address: Option<String>,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub auth: Option<PushAuthRequest>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct PushUnregisterRequest {
    #[serde(rename = "device_token")]
    pub device_token: String,
    #[serde(default)]
    pub auth: Option<PushAuthRequest>,
}

#[derive(Debug, Deserialize, ToSchema, Clone)]
pub struct PushAuthRequest {
    #[serde(rename = "wallet_pubkey")]
    pub wallet_pubkey: String,
    #[serde(rename = "wallet_address")]
    pub wallet_address: String,
    pub nonce: String,
    #[serde(rename = "timestamp_ms")]
    pub timestamp_ms: u64,
    #[serde(rename = "expires_at_ms")]
    pub expires_at_ms: u64,
    pub signature: String,
    #[allow(dead_code)]
    #[serde(default)]
    #[serde(rename = "devicecheck_token")]
    pub devicecheck_token: Option<String>,
    /// Kept as optional for backwards compatibility with old clients that still send these fields.
    #[allow(dead_code)]
    #[serde(default)]
    #[serde(rename = "app_attest_key_id")]
    pub app_attest_key_id: Option<String>,
    #[allow(dead_code)]
    #[serde(default)]
    #[serde(rename = "app_attest_attestation")]
    pub app_attest_attestation: Option<String>,
    #[allow(dead_code)]
    #[serde(default)]
    #[serde(rename = "app_attest_assertion")]
    pub app_attest_assertion: Option<String>,
    #[serde(default)]
    #[serde(rename = "device_auth")]
    pub device_auth: Option<PushDeviceAuthRequest>,
}

#[derive(Debug, Deserialize, ToSchema, Clone)]
pub struct PushDeviceAuthRequest {
    pub scheme: String,
    #[serde(rename = "key_id")]
    pub key_id: String,
    pub pubkey: String,
    pub counter: u64,
    pub signature: String,
}

#[derive(Debug)]
struct VerifiedPushAuth {
    wallet_binding: Option<WalletBinding>,
    device_binding: Option<DeviceKeyBinding>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PushResponse {
    status: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PushChallengeResponse {
    nonce: String,
    #[serde(rename = "issued_at_ms")]
    issued_at_ms: u64,
    #[serde(rename = "expires_at_ms")]
    expires_at_ms: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    error: String,
}

#[utoipa::path(
    post,
    path = "/v1/push/challenge",
    responses(
        (status = 200, description = "Issue a short-lived push auth nonce", body = PushChallengeResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
async fn create_challenge(
    State(state): State<PushApi>,
) -> Result<Json<PushChallengeResponse>, (StatusCode, Json<ErrorResponse>)> {
    let now = unix_time_ms();
    let mut nonces = match state.nonces.lock() {
        Ok(guard) => guard,
        Err(_) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Failed to lock nonce store".to_string(),
                }),
            ));
        }
    };

    let (nonce, expires_at_ms) = nonces.issue(now);
    Ok(Json(PushChallengeResponse {
        nonce,
        issued_at_ms: now,
        expires_at_ms,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/push/register",
    request_body = PushRegistrationRequest,
    responses(
        (status = 200, description = "Device registered", body = PushResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
async fn register_device(
    State(state): State<PushApi>,
    Json(payload): Json<PushRegistrationRequest>,
) -> impl IntoResponse {
    let verified_auth = match authenticate_push_request(
        &state,
        "POST",
        "/v1/push/register",
        &payload.device_token,
        &payload.watched_addresses,
        payload.primary_address.as_deref(),
        &payload.aliases,
        payload.auth.as_ref(),
    ) {
        Ok(binding) => binding,
        Err(err) => {
            warn!("Push register auth rejected: {}", err.message);
            return Err(err.into_response());
        }
    };

    let result = state
        .registry
        .register(
            payload.device_token,
            payload.platform,
            payload.watched_addresses,
            payload.primary_address,
            payload.aliases,
            verified_auth.wallet_binding,
            verified_auth.device_binding,
        )
        .await;

    match result {
        Ok(()) => Ok(Json(PushResponse {
            status: "ok".to_string(),
        })),
        Err(err) => Err((
            status_code_for_push_error(&err),
            Json(ErrorResponse {
                error: err.to_string(),
            }),
        )),
    }
}

#[utoipa::path(
    put,
    path = "/v1/push/update",
    request_body = PushUpdateRequest,
    responses(
        (status = 200, description = "Registration updated", body = PushResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
async fn update_registration(
    State(state): State<PushApi>,
    Json(payload): Json<PushUpdateRequest>,
) -> impl IntoResponse {
    let verified_auth = match authenticate_push_request(
        &state,
        "PUT",
        "/v1/push/update",
        &payload.device_token,
        &payload.watched_addresses,
        payload.primary_address.as_deref(),
        &payload.aliases,
        payload.auth.as_ref(),
    ) {
        Ok(binding) => binding,
        Err(err) => {
            warn!("Push update auth rejected: {}", err.message);
            return Err(err.into_response());
        }
    };

    let result = state
        .registry
        .update(
            payload.device_token,
            payload.watched_addresses,
            payload.primary_address,
            payload.aliases,
            verified_auth.wallet_binding,
            verified_auth.device_binding,
        )
        .await;

    match result {
        Ok(()) => Ok(Json(PushResponse {
            status: "ok".to_string(),
        })),
        Err(err) => Err((
            status_code_for_push_error(&err),
            Json(ErrorResponse {
                error: err.to_string(),
            }),
        )),
    }
}

#[utoipa::path(
    delete,
    path = "/v1/push/unregister",
    request_body = PushUnregisterRequest,
    responses(
        (status = 200, description = "Device unregistered", body = PushResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 401, description = "Unauthorized", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
async fn unregister_device(
    State(state): State<PushApi>,
    Json(payload): Json<PushUnregisterRequest>,
) -> impl IntoResponse {
    let verified_auth =
        match authenticate_unregister_request(&state, &payload.device_token, payload.auth.as_ref())
        {
            Ok(binding) => binding,
            Err(err) => {
                warn!("Push unregister auth rejected: {}", err.message);
                return Err(err.into_response());
            }
        };

    let normalized_token = match normalize_device_token(&payload.device_token) {
        Ok(token) => token,
        Err(err) => return Err(err.into_response()),
    };

    let wallet_pubkey = verified_auth
        .wallet_binding
        .as_ref()
        .map(|binding| binding.wallet_pubkey.clone());
    let result = state
        .registry
        .unregister_authenticated(
            normalized_token,
            wallet_pubkey,
            verified_auth.device_binding,
        )
        .await;

    match result {
        Ok(()) => Ok(Json(PushResponse {
            status: "ok".to_string(),
        })),
        Err(err) => Err((
            status_code_for_push_error(&err),
            Json(ErrorResponse {
                error: err.to_string(),
            }),
        )),
    }
}

fn authenticate_push_request(
    state: &PushApi,
    method: &str,
    path: &str,
    device_token: &str,
    watched_addresses: &[String],
    primary_address: Option<&str>,
    aliases: &[String],
    auth: Option<&PushAuthRequest>,
) -> Result<VerifiedPushAuth, PushApiError> {
    let Some(auth) = auth else {
        return match state.auth_mode {
            PushAuthMode::Strict => Err(PushApiError::unauthorized(
                "Signed auth is required for push mutations",
            )),
            PushAuthMode::Legacy | PushAuthMode::Mixed => Ok(VerifiedPushAuth {
                wallet_binding: None,
                device_binding: None,
            }),
        };
    };

    let now_ms = unix_time_ms();
    validate_auth_timing(auth, now_ms)?;
    let normalized_device_token = normalize_device_token(device_token)?;
    let normalized_primary = normalize_primary_for_auth(primary_address)?;
    let wallet_binding = verify_wallet_binding_from_auth(
        state,
        method,
        path,
        &normalized_device_token,
        watched_addresses,
        &normalized_primary,
        aliases,
        auth,
    )?;
    let device_binding =
        verify_device_key_binding_from_auth(method, path, &normalized_device_token, auth)?;

    consume_nonce(state, auth, now_ms)?;

    Ok(VerifiedPushAuth {
        wallet_binding: Some(wallet_binding),
        device_binding,
    })
}

fn authenticate_unregister_request(
    state: &PushApi,
    device_token: &str,
    auth: Option<&PushAuthRequest>,
) -> Result<VerifiedPushAuth, PushApiError> {
    let Some(auth) = auth else {
        return match state.auth_mode {
            PushAuthMode::Strict => Err(PushApiError::unauthorized(
                "Signed auth is required for push mutations",
            )),
            PushAuthMode::Legacy | PushAuthMode::Mixed => Ok(VerifiedPushAuth {
                wallet_binding: None,
                device_binding: None,
            }),
        };
    };

    let now_ms = unix_time_ms();
    validate_auth_timing(auth, now_ms)?;
    let normalized_device_token = normalize_device_token(device_token)?;
    let wallet_binding = verify_wallet_binding_from_auth(
        state,
        "DELETE",
        "/v1/push/unregister",
        &normalized_device_token,
        &[],
        "",
        &[],
        auth,
    )
    .ok();
    let device_binding = verify_device_key_binding_from_auth(
        "DELETE",
        "/v1/push/unregister",
        &normalized_device_token,
        auth,
    )?;

    if wallet_binding.is_none() && device_binding.is_none() {
        return Err(PushApiError::unauthorized(
            "Valid wallet or device auth is required for unregister",
        ));
    }

    consume_nonce(state, auth, now_ms)?;

    Ok(VerifiedPushAuth {
        wallet_binding,
        device_binding,
    })
}

fn consume_nonce(state: &PushApi, auth: &PushAuthRequest, now_ms: u64) -> Result<(), PushApiError> {
    let nonce_expiry = {
        let mut nonces = state
            .nonces
            .lock()
            .map_err(|_| PushApiError::internal("Failed to lock nonce store"))?;
        nonces.consume(auth.nonce.trim(), now_ms)?
    };
    if nonce_expiry != auth.expires_at_ms {
        return Err(PushApiError::unauthorized(
            "nonce expiry does not match signed payload",
        ));
    }
    Ok(())
}

fn verify_wallet_binding_from_auth(
    state: &PushApi,
    method: &str,
    path: &str,
    normalized_device_token: &str,
    watched_addresses: &[String],
    normalized_primary: &str,
    aliases: &[String],
    auth: &PushAuthRequest,
) -> Result<WalletBinding, PushApiError> {
    let wallet_pubkey = normalize_hex_field(&auth.wallet_pubkey, 32, "wallet_pubkey")?;
    let wallet_address = normalize_wallet_address(&auth.wallet_address)?;
    let derived_wallet_address = derive_wallet_address(&wallet_pubkey, state.network_type)?;
    if wallet_address != derived_wallet_address {
        return Err(PushApiError::unauthorized(
            "wallet_address does not match wallet_pubkey",
        ));
    }

    let preimage = build_auth_preimage(AuthPreimage {
        nonce: auth.nonce.trim(),
        method,
        path,
        device_token: normalized_device_token,
        watched_addresses,
        primary_address: normalized_primary,
        aliases,
        wallet_pubkey: &wallet_pubkey,
        wallet_address: &wallet_address,
        timestamp_ms: auth.timestamp_ms,
        expires_at_ms: auth.expires_at_ms,
    });

    verify_schnorr_signature(&wallet_pubkey, &preimage, auth.signature.trim())?;
    Ok(WalletBinding {
        wallet_pubkey,
        wallet_address,
    })
}

fn verify_device_key_binding_from_auth(
    method: &str,
    path: &str,
    normalized_device_token: &str,
    auth: &PushAuthRequest,
) -> Result<Option<DeviceKeyBinding>, PushApiError> {
    let Some(device_auth) = auth.device_auth.as_ref() else {
        return Ok(None);
    };

    if device_auth.scheme.trim() != DEVICE_AUTH_SCHEME {
        return Err(PushApiError::unauthorized("Unsupported device auth scheme"));
    }
    if device_auth.counter == 0 {
        return Err(PushApiError::bad_request("device_auth.counter must be > 0"));
    }

    let public_key = decode_base64_any(&device_auth.pubkey, "device_auth.pubkey")?;
    if public_key.len() != 65 || public_key[0] != 0x04 {
        return Err(PushApiError::bad_request(
            "device_auth.pubkey must be uncompressed P-256 key",
        ));
    }
    let key_id = normalize_device_key_id(&device_auth.key_id, &public_key)?;
    let signature = decode_base64_any(&device_auth.signature, "device_auth.signature")?;
    let preimage = build_device_auth_preimage(DeviceAuthPreimage {
        nonce: auth.nonce.trim(),
        method,
        path,
        device_token: normalized_device_token,
        key_id: &key_id,
        counter: device_auth.counter,
        timestamp_ms: auth.timestamp_ms,
        expires_at_ms: auth.expires_at_ms,
    });
    verify_p256_signature(&public_key, preimage.as_bytes(), &signature)
        .map_err(|_| PushApiError::unauthorized("Invalid device key signature"))?;

    Ok(Some(DeviceKeyBinding {
        key_id,
        public_key_b64: STANDARD.encode(public_key),
        counter: device_auth.counter,
    }))
}

fn validate_auth_timing(auth: &PushAuthRequest, now_ms: u64) -> Result<(), PushApiError> {
    if auth.expires_at_ms < auth.timestamp_ms {
        return Err(PushApiError::bad_request(
            "expires_at_ms must be >= timestamp_ms",
        ));
    }

    let validity_window = auth.expires_at_ms.saturating_sub(auth.timestamp_ms);
    if validity_window > MAX_SIGNATURE_WINDOW_MS {
        return Err(PushApiError::bad_request(
            "Signed request validity window is too large",
        ));
    }

    if auth.timestamp_ms > now_ms.saturating_add(MAX_CLOCK_SKEW_MS) {
        return Err(PushApiError::unauthorized(
            "timestamp_ms is too far in the future",
        ));
    }

    if now_ms > auth.expires_at_ms.saturating_add(MAX_CLOCK_SKEW_MS) {
        return Err(PushApiError::unauthorized("Signed request is expired"));
    }

    Ok(())
}

fn normalize_wallet_address(address: &str) -> Result<String, PushApiError> {
    let normalized = address.trim();
    if normalized.is_empty() {
        return Err(PushApiError::bad_request(
            "wallet_address must not be empty",
        ));
    }
    RpcAddress::try_from(normalized)
        .map(|address| address.to_string())
        .map_err(|_| PushApiError::bad_request("wallet_address is invalid"))
}

fn normalize_primary_for_auth(primary_address: Option<&str>) -> Result<String, PushApiError> {
    let Some(primary_address) = primary_address else {
        return Ok(String::new());
    };
    let trimmed = primary_address.trim();
    if trimmed.is_empty() {
        return Ok(String::new());
    }
    RpcAddress::try_from(trimmed)
        .map(|address| address.to_string())
        .map_err(|_| PushApiError::bad_request("primary_address is invalid"))
}

fn derive_wallet_address(
    wallet_pubkey_hex: &str,
    network_type: RpcNetworkType,
) -> Result<String, PushApiError> {
    let wallet_pubkey_bytes = decode_hex(wallet_pubkey_hex, "wallet_pubkey")?;
    if wallet_pubkey_bytes.len() != 32 {
        return Err(PushApiError::bad_request(
            "wallet_pubkey must be 32-byte hex",
        ));
    }

    Ok(Address::new(network_type.into(), Version::PubKey, &wallet_pubkey_bytes).to_string())
}

fn verify_schnorr_signature(
    wallet_pubkey_hex: &str,
    preimage: &str,
    signature_hex: &str,
) -> Result<(), PushApiError> {
    let pubkey_bytes = decode_hex(wallet_pubkey_hex, "wallet_pubkey")?;
    if pubkey_bytes.len() != 32 {
        return Err(PushApiError::bad_request(
            "wallet_pubkey must be 32-byte hex",
        ));
    }

    let signature_bytes = decode_hex(signature_hex, "signature")?;
    if signature_bytes.len() != 64 {
        return Err(PushApiError::bad_request("signature must be 64-byte hex"));
    }

    let digest: [u8; 32] = Sha256::digest(preimage.as_bytes()).into();
    let message = Message::from_digest(digest);
    let pubkey = XOnlyPublicKey::from_slice(&pubkey_bytes)
        .map_err(|_| PushApiError::bad_request("wallet_pubkey is malformed"))?;
    let signature = SchnorrSignature::from_slice(&signature_bytes)
        .map_err(|_| PushApiError::bad_request("signature is malformed"))?;

    let secp = Secp256k1::verification_only();
    secp.verify_schnorr(&signature, &message, &pubkey)
        .map_err(|_| PushApiError::unauthorized("Invalid Schnorr signature"))
}

fn normalize_device_token(token: &str) -> Result<String, PushApiError> {
    let cleaned: String = token
        .chars()
        .filter(|character| character.is_ascii_hexdigit())
        .collect();
    if cleaned.len() < 64 || cleaned.len() > 512 || cleaned.len() % 2 != 0 {
        return Err(PushApiError::bad_request("Invalid device token length"));
    }
    Ok(cleaned.to_ascii_lowercase())
}

fn normalize_hex_field(
    value: &str,
    expected_len_bytes: usize,
    field: &str,
) -> Result<String, PushApiError> {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.len() != expected_len_bytes * 2 {
        return Err(PushApiError::bad_request(format!(
            "{field} must be {}-byte hex",
            expected_len_bytes
        )));
    }
    if !normalized.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(PushApiError::bad_request(format!("{field} must be hex")));
    }
    Ok(normalized)
}

fn decode_hex(value: &str, field: &str) -> Result<Vec<u8>, PushApiError> {
    let normalized = value.trim();
    if normalized.len() % 2 != 0 {
        return Err(PushApiError::bad_request(format!(
            "{field} must be even-length hex",
        )));
    }

    let mut out = Vec::with_capacity(normalized.len() / 2);
    let bytes = normalized.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let hi = decode_hex_nibble(bytes[index]).ok_or_else(|| {
            PushApiError::bad_request(format!("{field} contains non-hex characters"))
        })?;
        let lo = decode_hex_nibble(bytes[index + 1]).ok_or_else(|| {
            PushApiError::bad_request(format!("{field} contains non-hex characters"))
        })?;
        out.push((hi << 4) | lo);
        index += 2;
    }
    Ok(out)
}

fn decode_hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn decode_base64_any(value: &str, field_name: &str) -> Result<Vec<u8>, PushApiError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(PushApiError::bad_request(format!(
            "{field_name} must not be empty"
        )));
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
    Err(PushApiError::bad_request(format!(
        "{field_name} is invalid base64"
    )))
}

fn normalize_device_key_id(key_id: &str, public_key: &[u8]) -> Result<String, PushApiError> {
    let normalized = key_id.trim().to_ascii_lowercase();
    if normalized.len() != 64 || !normalized.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(PushApiError::bad_request(
            "device_auth.key_id must be 32-byte hex",
        ));
    }
    let expected: [u8; 32] = Sha256::digest(public_key).into();
    if normalized != hex_encode(&expected) {
        return Err(PushApiError::unauthorized(
            "device_auth.key_id does not match pubkey",
        ));
    }
    Ok(normalized)
}

fn verify_p256_signature(public_key: &[u8], message: &[u8], signature: &[u8]) -> Result<(), ()> {
    let verifier_asn1 = UnparsedPublicKey::new(&ECDSA_P256_SHA256_ASN1, public_key);
    if verifier_asn1.verify(message, signature).is_ok() {
        return Ok(());
    }
    let verifier_fixed = UnparsedPublicKey::new(&ECDSA_P256_SHA256_FIXED, public_key);
    verifier_fixed.verify(message, signature).map_err(|_| ())
}

struct AuthPreimage<'a> {
    nonce: &'a str,
    method: &'a str,
    path: &'a str,
    device_token: &'a str,
    watched_addresses: &'a [String],
    primary_address: &'a str,
    aliases: &'a [String],
    wallet_pubkey: &'a str,
    wallet_address: &'a str,
    timestamp_ms: u64,
    expires_at_ms: u64,
}

struct DeviceAuthPreimage<'a> {
    nonce: &'a str,
    method: &'a str,
    path: &'a str,
    device_token: &'a str,
    key_id: &'a str,
    counter: u64,
    timestamp_ms: u64,
    expires_at_ms: u64,
}

fn build_auth_preimage(preimage: AuthPreimage<'_>) -> String {
    let watched_hash =
        hash_string(&canonicalize_watched_addresses(preimage.watched_addresses).join("\n"));
    let aliases_hash = hash_string(&canonicalize_aliases(preimage.aliases).join("\n"));
    let device_token_hash = hash_string(preimage.device_token);

    [
        format!("domain={AUTH_DOMAIN}"),
        format!("nonce={}", preimage.nonce),
        format!("method={}", preimage.method),
        format!("path={}", preimage.path),
        format!("device_token_hash={device_token_hash}"),
        format!("watched_addresses_hash={watched_hash}"),
        format!("primary_address={}", preimage.primary_address),
        format!("aliases_hash={aliases_hash}"),
        format!("wallet_pubkey={}", preimage.wallet_pubkey),
        format!("wallet_address={}", preimage.wallet_address),
        format!("timestamp_ms={}", preimage.timestamp_ms),
        format!("expires_at_ms={}", preimage.expires_at_ms),
    ]
    .join("\n")
}

fn build_device_auth_preimage(preimage: DeviceAuthPreimage<'_>) -> String {
    let device_token_hash = hash_string(preimage.device_token);
    [
        format!("domain={DEVICE_AUTH_DOMAIN}"),
        format!("nonce={}", preimage.nonce),
        format!("method={}", preimage.method),
        format!("path={}", preimage.path),
        format!("device_token_hash={device_token_hash}"),
        format!("key_id={}", preimage.key_id),
        format!("counter={}", preimage.counter),
        format!("timestamp_ms={}", preimage.timestamp_ms),
        format!("expires_at_ms={}", preimage.expires_at_ms),
    ]
    .join("\n")
}

fn canonicalize_watched_addresses(values: &[String]) -> Vec<String> {
    canonicalize_set(values, |value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_ascii_lowercase())
        }
    })
}

fn canonicalize_aliases(values: &[String]) -> Vec<String> {
    canonicalize_set(values, |value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn canonicalize_set<F>(values: &[String], normalize: F) -> Vec<String>
where
    F: Fn(&str) -> Option<String>,
{
    let mut out = HashSet::new();
    for value in values {
        if let Some(normalized) = normalize(value) {
            out.insert(normalized);
        }
    }
    let mut out: Vec<String> = out.into_iter().collect();
    out.sort_unstable();
    out
}

fn hash_string(value: &str) -> String {
    let digest: [u8; 32] = Sha256::digest(value.as_bytes()).into();
    hex_encode(&digest)
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(hex_char(byte >> 4));
        out.push(hex_char(byte & 0x0f));
    }
    out
}

fn hex_char(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'a' + (nibble - 10)) as char,
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

#[derive(Default)]
struct NonceStore {
    expiries_by_nonce: HashMap<String, u64>,
    expiry_order: VecDeque<(u64, String)>,
}

impl NonceStore {
    fn issue(&mut self, now_ms: u64) -> (String, u64) {
        self.prune_expired(now_ms);

        let expires_at_ms = now_ms.saturating_add(NONCE_TTL_MS);
        let mut nonce = random_nonce_hex();
        while self.expiries_by_nonce.contains_key(&nonce) {
            nonce = random_nonce_hex();
        }

        self.expiries_by_nonce.insert(nonce.clone(), expires_at_ms);
        self.expiry_order.push_back((expires_at_ms, nonce.clone()));

        while self.expiries_by_nonce.len() > MAX_NONCE_STORE_ENTRIES {
            let Some((_expiry, oldest_nonce)) = self.expiry_order.pop_front() else {
                break;
            };
            self.expiries_by_nonce.remove(&oldest_nonce);
        }

        (nonce, expires_at_ms)
    }

    fn consume(&mut self, nonce: &str, now_ms: u64) -> Result<u64, PushApiError> {
        self.prune_expired(now_ms);
        let Some(expires_at_ms) = self.expiries_by_nonce.remove(nonce) else {
            return Err(PushApiError::unauthorized(
                "nonce is invalid, expired, or already used",
            ));
        };
        if now_ms > expires_at_ms {
            return Err(PushApiError::unauthorized("nonce has expired"));
        }
        Ok(expires_at_ms)
    }

    fn prune_expired(&mut self, now_ms: u64) {
        while let Some((expiry, _nonce)) = self.expiry_order.front() {
            if *expiry > now_ms {
                break;
            }
            let Some((_expired_at, expired_nonce)) = self.expiry_order.pop_front() else {
                break;
            };
            self.expiries_by_nonce.remove(&expired_nonce);
        }
    }
}

fn random_nonce_hex() -> String {
    let mut bytes = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    hex_encode(&bytes)
}

fn status_code_for_push_error(err: &anyhow::Error) -> StatusCode {
    let message = err.to_string().to_ascii_lowercase();
    if message.contains("auth is required")
        || message.contains("bound to another wallet")
        || message.contains("unauthorized")
    {
        StatusCode::UNAUTHORIZED
    } else if message.contains("push registry actor") {
        StatusCode::INTERNAL_SERVER_ERROR
    } else {
        StatusCode::BAD_REQUEST
    }
}

#[derive(Debug)]
struct PushApiError {
    status: StatusCode,
    message: String,
}

impl PushApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }

    fn into_response(self) -> (StatusCode, Json<ErrorResponse>) {
        (
            self.status,
            Json(ErrorResponse {
                error: self.message,
            }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secp256k1::{Keypair, Message, Secp256k1, SecretKey, XOnlyPublicKey};

    #[test]
    fn canonicalize_sets_are_stable() {
        let watched = vec![
            "B".to_string(),
            " a ".to_string(),
            "b".to_string(),
            "".to_string(),
        ];
        let aliases = vec![
            " Alice ".to_string(),
            "Bob".to_string(),
            "Alice".to_string(),
            " ".to_string(),
        ];

        assert_eq!(
            canonicalize_watched_addresses(&watched),
            vec!["a".to_string(), "b".to_string()]
        );
        assert_eq!(
            canonicalize_aliases(&aliases),
            vec!["Alice".to_string(), "Bob".to_string()]
        );
    }

    #[test]
    fn nonce_store_enforces_single_use_and_ttl() {
        let mut store = NonceStore::default();
        let now = 1_000;
        let (nonce, expires_at_ms) = store.issue(now);

        assert_eq!(
            store
                .consume(&nonce, now + 1)
                .expect("first consume should pass"),
            expires_at_ms
        );
        assert!(store.consume(&nonce, now + 2).is_err());

        let (expired_nonce, _) = store.issue(now);
        assert!(
            store
                .consume(&expired_nonce, now + NONCE_TTL_MS + 1)
                .is_err()
        );
    }

    #[test]
    fn schnorr_verification_accepts_valid_signature() {
        let secp = Secp256k1::new();
        let secret = SecretKey::from_slice(&[0x11; 32]).expect("valid secret");
        let keypair = Keypair::from_secret_key(&secp, &secret);
        let (xonly_pubkey, _) = XOnlyPublicKey::from_keypair(&keypair);
        let wallet_pubkey = hex_encode(&xonly_pubkey.serialize());
        let wallet_address =
            derive_wallet_address(&wallet_pubkey, RpcNetworkType::Mainnet).expect("address");

        let watched_addresses = vec!["kaspa:qqexamplewatch".to_string()];
        let aliases = vec!["alias-a".to_string(), "alias-b".to_string()];
        let preimage = build_auth_preimage(AuthPreimage {
            nonce: "abcd",
            method: "POST",
            path: "/v1/push/register",
            device_token: "00112233445566778899aabbccddeeff",
            watched_addresses: &watched_addresses,
            primary_address: &wallet_address,
            aliases: &aliases,
            wallet_pubkey: &wallet_pubkey,
            wallet_address: &wallet_address,
            timestamp_ms: 10,
            expires_at_ms: 20,
        });

        let digest: [u8; 32] = Sha256::digest(preimage.as_bytes()).into();
        let message = Message::from_digest(digest);
        let signature = secp.sign_schnorr_no_aux_rand(&message, &keypair);
        let signature_hex = hex_encode(signature.as_ref());

        verify_schnorr_signature(&wallet_pubkey, &preimage, &signature_hex)
            .expect("signature should verify");
    }

    #[test]
    fn schnorr_verification_rejects_tampered_signature() {
        let secp = Secp256k1::new();
        let secret = SecretKey::from_slice(&[0x22; 32]).expect("valid secret");
        let keypair = Keypair::from_secret_key(&secp, &secret);
        let (xonly_pubkey, _) = XOnlyPublicKey::from_keypair(&keypair);
        let wallet_pubkey = hex_encode(&xonly_pubkey.serialize());
        let preimage = "domain=kasia-push-auth:v1\nnonce=n".to_string();
        let digest: [u8; 32] = Sha256::digest(preimage.as_bytes()).into();
        let message = Message::from_digest(digest);
        let signature = secp.sign_schnorr_no_aux_rand(&message, &keypair);
        let mut signature_hex = hex_encode(signature.as_ref());
        signature_hex.replace_range(0..2, "ff");

        assert!(verify_schnorr_signature(&wallet_pubkey, &preimage, &signature_hex).is_err());
    }
}
