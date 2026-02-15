use crate::app_attest::{AppAttestVerifier, canonical_key_id};
use crate::config::PushAuthMode;
use crate::push::{AppAttestBinding, DeviceRegistration, PushRegistry, WalletBinding};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{delete, post, put};
use axum::{Json, Router};
use kaspa_addresses::{Address, Version};
use kaspa_rpc_core::{RpcAddress, RpcNetworkType};
use rand::RngCore;
use secp256k1::schnorr::Signature as SchnorrSignature;
use secp256k1::{Message, Secp256k1, XOnlyPublicKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, warn};
use utoipa::ToSchema;

const AUTH_DOMAIN: &str = "kasia-push-auth:v1";
const NONCE_TTL_MS: u64 = 60_000;
const MAX_SIGNATURE_WINDOW_MS: u64 = 60_000;
const MAX_CLOCK_SKEW_MS: u64 = 60_000;
const MAX_NONCE_STORE_ENTRIES: usize = 50_000;

#[derive(Clone)]
pub struct PushApi {
    registry: PushRegistry,
    auth_mode: PushAuthMode,
    network_type: RpcNetworkType,
    nonces: Arc<StdMutex<NonceStore>>,
    app_attest_verifier: Option<AppAttestVerifier>,
}

impl PushApi {
    pub fn new(
        registry: PushRegistry,
        network_type: RpcNetworkType,
        auth_mode: PushAuthMode,
        app_attest_team_id: Option<String>,
        app_attest_bundle_id: Option<String>,
    ) -> Self {
        let app_attest_verifier = match (app_attest_team_id, app_attest_bundle_id) {
            (Some(team_id), Some(bundle_id)) => AppAttestVerifier::new(&team_id, &bundle_id),
            _ => None,
        };
        Self {
            registry,
            auth_mode,
            network_type,
            nonces: Arc::new(StdMutex::new(NonceStore::default())),
            app_attest_verifier,
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
    #[serde(default)]
    #[serde(rename = "app_attest_key_id")]
    pub app_attest_key_id: Option<String>,
    #[serde(default)]
    #[serde(rename = "app_attest_attestation")]
    pub app_attest_attestation: Option<String>,
    #[serde(default)]
    #[serde(rename = "app_attest_assertion")]
    pub app_attest_assertion: Option<String>,
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
    let auth_binding = match authenticate_push_request(
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

    let registry = state.registry.clone();
    let result = tokio::task::spawn_blocking(move || {
        registry.register(
            payload.device_token,
            payload.platform,
            payload.watched_addresses,
            payload.primary_address,
            payload.aliases,
            auth_binding,
        )
    })
    .await;

    match result {
        Ok(Ok(())) => Ok(Json(PushResponse {
            status: "ok".to_string(),
        })),
        Ok(Err(err)) => Err((
            status_code_for_push_error(&err),
            Json(ErrorResponse {
                error: err.to_string(),
            }),
        )),
        Err(err) => {
            error!("Push register failed: {err}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Internal server error".to_string(),
                }),
            ))
        }
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
    let auth_binding = match authenticate_push_request(
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

    let registry = state.registry.clone();
    let result = tokio::task::spawn_blocking(move || {
        registry.update(
            payload.device_token,
            payload.watched_addresses,
            payload.primary_address,
            payload.aliases,
            auth_binding,
        )
    })
    .await;

    match result {
        Ok(Ok(())) => Ok(Json(PushResponse {
            status: "ok".to_string(),
        })),
        Ok(Err(err)) => Err((
            status_code_for_push_error(&err),
            Json(ErrorResponse {
                error: err.to_string(),
            }),
        )),
        Err(err) => {
            error!("Push update failed: {err}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Internal server error".to_string(),
                }),
            ))
        }
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
    let auth_binding = match authenticate_push_request(
        &state,
        "DELETE",
        "/v1/push/unregister",
        &payload.device_token,
        &[],
        None,
        &[],
        payload.auth.as_ref(),
    ) {
        Ok(binding) => binding,
        Err(err) => {
            warn!("Push unregister auth rejected: {}", err.message);
            return Err(err.into_response());
        }
    };

    let wallet_pubkey = auth_binding.map(|binding| binding.wallet_pubkey);
    let registry = state.registry.clone();
    let token = payload.device_token;
    let result =
        tokio::task::spawn_blocking(move || registry.unregister_authorized(token, wallet_pubkey))
            .await;

    match result {
        Ok(Ok(())) => Ok(Json(PushResponse {
            status: "ok".to_string(),
        })),
        Ok(Err(err)) => Err((
            status_code_for_push_error(&err),
            Json(ErrorResponse {
                error: err.to_string(),
            }),
        )),
        Err(err) => {
            error!("Push unregister failed: {err}");
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: "Internal server error".to_string(),
                }),
            ))
        }
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
) -> Result<Option<WalletBinding>, PushApiError> {
    let Some(auth) = auth else {
        return match state.auth_mode {
            PushAuthMode::Strict => Err(PushApiError::unauthorized(
                "Signed auth is required for push mutations",
            )),
            PushAuthMode::Legacy | PushAuthMode::Mixed => Ok(None),
        };
    };

    let now_ms = unix_time_ms();
    validate_auth_timing(auth, now_ms)?;

    let wallet_pubkey = normalize_hex_field(&auth.wallet_pubkey, 32, "wallet_pubkey")?;
    let wallet_address = normalize_wallet_address(&auth.wallet_address)?;
    let derived_wallet_address = derive_wallet_address(&wallet_pubkey, state.network_type)?;
    if wallet_address != derived_wallet_address {
        return Err(PushApiError::unauthorized(
            "wallet_address does not match wallet_pubkey",
        ));
    }

    let normalized_device_token = normalize_device_token(device_token)?;
    let normalized_primary = normalize_primary_for_auth(primary_address)?;

    let preimage = build_auth_preimage(AuthPreimage {
        nonce: auth.nonce.trim(),
        method,
        path,
        device_token: &normalized_device_token,
        watched_addresses,
        primary_address: &normalized_primary,
        aliases,
        wallet_pubkey: &wallet_pubkey,
        wallet_address: &wallet_address,
        timestamp_ms: auth.timestamp_ms,
        expires_at_ms: auth.expires_at_ms,
    });

    verify_schnorr_signature(&wallet_pubkey, &preimage, auth.signature.trim())?;

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

    let existing_registration = state
        .registry
        .get_registration(&normalized_device_token)
        .map_err(|_| PushApiError::internal("Failed to load device registration"))?;
    let app_attest_binding = verify_app_attest(
        state,
        auth,
        &preimage,
        existing_registration.as_ref(),
        &wallet_pubkey,
    )?;

    Ok(Some(WalletBinding {
        wallet_pubkey,
        wallet_address,
        app_attest: app_attest_binding,
    }))
}

fn verify_app_attest(
    state: &PushApi,
    auth: &PushAuthRequest,
    challenge: &str,
    existing_registration: Option<&DeviceRegistration>,
    wallet_pubkey: &str,
) -> Result<Option<AppAttestBinding>, PushApiError> {
    let existing_app_attest = existing_registration.and_then(existing_app_attest_binding);
    let should_enforce = matches!(state.auth_mode, PushAuthMode::Strict)
        || existing_app_attest.is_some()
        || auth.app_attest_key_id.is_some()
        || auth.app_attest_attestation.is_some()
        || auth.app_attest_assertion.is_some();
    if !should_enforce {
        return Ok(existing_app_attest);
    }

    let verifier = state.app_attest_verifier.as_ref().ok_or_else(|| {
        PushApiError::internal(
            "App Attest verification is not configured (missing APNS_TEAM_ID/APNS_TOPIC)",
        )
    })?;

    let key_id = auth
        .app_attest_key_id
        .as_deref()
        .ok_or_else(|| PushApiError::unauthorized("app_attest_key_id is required"))?;
    let key_id = canonical_key_id(key_id).map_err(PushApiError::bad_request)?;

    match existing_app_attest {
        Some(existing) => {
            if existing.key_id != key_id {
                if let Some(attestation) = auth.app_attest_attestation.as_deref() {
                    let attested_key = verifier
                        .verify_attestation(attestation, &key_id, challenge)
                        .map_err(PushApiError::unauthorized)?;
                    if existing_registration
                        .and_then(|registration| registration.wallet_pubkey.as_deref())
                        .is_some_and(|existing_wallet_pubkey| {
                            existing_wallet_pubkey != wallet_pubkey
                        })
                    {
                        return Err(PushApiError::unauthorized(
                            "device token is bound to another wallet",
                        ));
                    }
                    return Ok(Some(AppAttestBinding {
                        key_id: attested_key.key_id,
                        public_key_spki_b64: attested_key.public_key_spki_b64,
                        sign_count: attested_key.sign_count,
                    }));
                }
                return Err(PushApiError::unauthorized(
                    "device token is bound to another App Attest key",
                ));
            }

            if let Some(assertion) = auth.app_attest_assertion.as_deref() {
                match verifier.verify_assertion(
                    assertion,
                    &existing.public_key_spki_b64,
                    challenge,
                    existing.sign_count,
                ) {
                    Ok(new_sign_count) => {
                        return Ok(Some(AppAttestBinding {
                            key_id: existing.key_id,
                            public_key_spki_b64: existing.public_key_spki_b64,
                            sign_count: new_sign_count,
                        }));
                    }
                    Err(assertion_err) => {
                        // Recovery path for stale/corrupt stored binding:
                        // if client provides fresh attestation for the same key, re-bind.
                        if let Some(attestation) = auth.app_attest_attestation.as_deref() {
                            let attested_key = verifier
                                .verify_attestation(attestation, &key_id, challenge)
                                .map_err(PushApiError::unauthorized)?;
                            if attested_key.key_id != existing.key_id {
                                return Err(PushApiError::unauthorized(
                                    "device token is bound to another App Attest key",
                                ));
                            }
                            return Ok(Some(AppAttestBinding {
                                key_id: attested_key.key_id,
                                public_key_spki_b64: attested_key.public_key_spki_b64,
                                sign_count: attested_key.sign_count,
                            }));
                        }
                        return Err(PushApiError::unauthorized(assertion_err));
                    }
                }
            }

            if let Some(attestation) = auth.app_attest_attestation.as_deref() {
                let attested_key = verifier
                    .verify_attestation(attestation, &key_id, challenge)
                    .map_err(PushApiError::unauthorized)?;
                if attested_key.key_id != existing.key_id {
                    return Err(PushApiError::unauthorized(
                        "device token is bound to another App Attest key",
                    ));
                }
                return Ok(Some(AppAttestBinding {
                    key_id: attested_key.key_id,
                    public_key_spki_b64: attested_key.public_key_spki_b64,
                    sign_count: attested_key.sign_count,
                }));
            }

            Err(PushApiError::unauthorized(
                "app_attest_assertion is required for bound token",
            ))
        }
        None => {
            let attestation = auth.app_attest_attestation.as_deref().ok_or_else(|| {
                PushApiError::unauthorized("app_attest_attestation is required for enrollment")
            })?;
            let attested_key = verifier
                .verify_attestation(attestation, &key_id, challenge)
                .map_err(PushApiError::unauthorized)?;
            if existing_registration
                .and_then(|registration| registration.wallet_pubkey.as_deref())
                .is_some_and(|existing_wallet_pubkey| existing_wallet_pubkey != wallet_pubkey)
            {
                return Err(PushApiError::unauthorized(
                    "device token is bound to another wallet",
                ));
            }
            Ok(Some(AppAttestBinding {
                key_id: attested_key.key_id,
                public_key_spki_b64: attested_key.public_key_spki_b64,
                sign_count: attested_key.sign_count,
            }))
        }
    }
}

fn existing_app_attest_binding(registration: &DeviceRegistration) -> Option<AppAttestBinding> {
    let key_id = registration.app_attest_key_id.as_ref()?;
    let public_key = registration.app_attest_public_key_spki_b64.as_ref()?;
    let canonical_key_id = canonical_key_id(key_id).ok()?;
    Some(AppAttestBinding {
        key_id: canonical_key_id,
        public_key_spki_b64: public_key.clone(),
        sign_count: registration.app_attest_sign_count.unwrap_or(0),
    })
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
