use crate::api::to_rpc_address;
use crate::config::ApnsEnvironment;
use crate::context::IndexerContext;
use indexer_actors::metrics::SharedMetrics;
use indexer_actors::push::{PushEvent, PushEventKind};
use indexer_actors::util::ToHex;
use indexer_db::AddressPayload;
use indexer_db::push::{DeviceRegistrationPartition, WatchedAddressPartition};
use jsonwebtoken::{EncodingKey, Header};
use kaspa_rpc_core::{RpcAddress, RpcNetworkType};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tracing::{info, warn};

const MAX_WATCHED_ADDRESSES: usize = 256;
const MAX_ALIASES: usize = 256;
const MAX_ALIAS_LEN_BYTES: usize = 64;
const MAX_ADDRESS_LEN_BYTES: usize = 128;
const MAX_PLATFORM_LEN_BYTES: usize = 16;
const WALLET_PUBKEY_HEX_LEN: usize = 64;
const SUPPORTED_PLATFORM: &str = "ios";

#[derive(Debug, Clone)]
pub struct AppAttestBinding {
    pub key_id: String,
    pub public_key_spki_b64: String,
    pub sign_count: u32,
}

#[derive(Debug, Clone)]
pub struct WalletBinding {
    pub wallet_pubkey: String,
    pub wallet_address: String,
    pub app_attest: Option<AppAttestBinding>,
}

#[derive(Clone)]
pub struct PushRegistry {
    tx_keyspace: fjall::TxKeyspace,
    device_partition: DeviceRegistrationPartition,
    watched_partition: WatchedAddressPartition,
    metrics: SharedMetrics,
    alias_cache: Arc<StdMutex<HashMap<String, HashSet<String>>>>,
    primary_cache: Arc<StdMutex<HashMap<String, Option<AddressPayload>>>>,
}

impl PushRegistry {
    pub fn new(
        tx_keyspace: fjall::TxKeyspace,
        device_partition: DeviceRegistrationPartition,
        watched_partition: WatchedAddressPartition,
        metrics: SharedMetrics,
    ) -> Self {
        Self {
            tx_keyspace,
            device_partition,
            watched_partition,
            metrics,
            alias_cache: Arc::new(StdMutex::new(HashMap::new())),
            primary_cache: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    pub fn register(
        &self,
        token: String,
        platform: String,
        watched_addresses: Vec<String>,
        primary_address: Option<String>,
        aliases: Vec<String>,
        wallet_binding: Option<WalletBinding>,
    ) -> anyhow::Result<()> {
        self.metrics.increment_push_register_calls_total();
        validate_registration_limits(&watched_addresses, &aliases)?;
        let platform = normalize_platform(platform)?;
        let token = normalize_device_token(&token)?;
        let now = unix_time_secs();
        let (addresses, payloads) = normalize_addresses(watched_addresses)?;
        let normalized_aliases = normalize_aliases_vec(aliases);
        let normalized_alias_set = normalize_aliases(normalized_aliases.clone());
        let normalized_primary_address = normalize_primary_address(primary_address);
        if addresses.is_empty() {
            anyhow::bail!("watched_addresses must not be empty");
        }

        let existing = self.get_registration(&token)?;
        let effective_wallet_binding = resolve_wallet_binding(existing.as_ref(), wallet_binding)?;
        let effective_wallet_pubkey = effective_wallet_binding
            .as_ref()
            .map(|binding| binding.wallet_pubkey.clone());
        let effective_wallet_address = effective_wallet_binding
            .as_ref()
            .map(|binding| binding.wallet_address.clone());
        let effective_app_attest_key_id = effective_wallet_binding
            .as_ref()
            .and_then(|binding| binding.app_attest.as_ref())
            .map(|attest| attest.key_id.clone());
        let effective_app_attest_public_key = effective_wallet_binding
            .as_ref()
            .and_then(|binding| binding.app_attest.as_ref())
            .map(|attest| attest.public_key_spki_b64.clone());
        let effective_app_attest_sign_count = effective_wallet_binding
            .as_ref()
            .and_then(|binding| binding.app_attest.as_ref())
            .map(|attest| attest.sign_count);
        let created_at = existing.as_ref().map(|reg| reg.created_at).unwrap_or(now);
        let last_seen_refresh = existing
            .as_ref()
            .map(|reg| should_refresh_last_seen(&token, reg.last_seen, now))
            .unwrap_or(false);
        let addresses_unchanged = existing
            .as_ref()
            .map(|reg| reg.watched_addresses == addresses)
            .unwrap_or(false);
        let platform_unchanged = existing
            .as_ref()
            .map(|reg| reg.platform == platform)
            .unwrap_or(false);
        let aliases_unchanged = existing
            .as_ref()
            .map(|reg| normalize_aliases(reg.aliases.clone()) == normalized_alias_set)
            .unwrap_or(false);
        let primary_unchanged = existing
            .as_ref()
            .map(|reg| {
                normalize_primary_address(reg.primary_address.clone()) == normalized_primary_address
            })
            .unwrap_or(false);
        let wallet_binding_unchanged = existing
            .as_ref()
            .map(|reg| {
                reg.wallet_pubkey == effective_wallet_pubkey
                    && reg.wallet_address == effective_wallet_address
                    && reg.app_attest_key_id == effective_app_attest_key_id
                    && reg.app_attest_public_key_spki_b64 == effective_app_attest_public_key
                    && reg.app_attest_sign_count == effective_app_attest_sign_count
            })
            .unwrap_or(false);
        if addresses_unchanged
            && platform_unchanged
            && aliases_unchanged
            && primary_unchanged
            && wallet_binding_unchanged
            && !last_seen_refresh
        {
            // Fast path: payload unchanged and heartbeat refresh is not due yet.
            self.metrics.increment_push_fast_path_skips_total();
            self.update_aliases(&token, normalized_aliases);
            self.update_primary_address(&token, normalized_primary_address);
            return Ok(());
        }

        let registration = DeviceRegistration {
            device_token: token.clone(),
            platform,
            watched_addresses: addresses,
            aliases: normalized_aliases.clone(),
            primary_address: normalized_primary_address.clone(),
            wallet_pubkey: effective_wallet_pubkey,
            wallet_address: effective_wallet_address,
            app_attest_key_id: effective_app_attest_key_id,
            app_attest_public_key_spki_b64: effective_app_attest_public_key,
            app_attest_sign_count: effective_app_attest_sign_count,
            created_at,
            last_seen: now,
        };

        let token_key = token.as_bytes();
        let registration_bytes = serde_json::to_vec(&registration)?;

        let had_existing = existing.is_some();
        self.metrics.increment_db_write_ops_total(1);
        let db_write_started = Instant::now();
        let mut wtx = self.tx_keyspace.write_tx()?;
        if let Some(existing) = existing {
            if !addresses_unchanged {
                let new_set: HashSet<&str> = registration
                    .watched_addresses
                    .iter()
                    .map(|addr| addr.as_str())
                    .collect();
                let old_set: HashSet<&str> = existing
                    .watched_addresses
                    .iter()
                    .map(|addr| addr.as_str())
                    .collect();
                for address in &existing.watched_addresses {
                    if !new_set.contains(address.as_str()) {
                        if let Ok(payload) = address_to_payload(address) {
                            self.watched_partition
                                .remove_wtx(&mut wtx, &payload, token_key);
                        }
                    }
                }
                for (address, payload) in registration.watched_addresses.iter().zip(payloads.iter())
                {
                    if !old_set.contains(address.as_str()) {
                        self.watched_partition
                            .insert_wtx(&mut wtx, payload, token_key);
                    }
                }
            }
        } else {
            for payload in payloads {
                self.watched_partition
                    .insert_wtx(&mut wtx, &payload, token_key);
            }
        }
        self.device_partition
            .insert_wtx(&mut wtx, token.as_bytes(), &registration_bytes);
        let commit = wtx.commit();
        self.metrics
            .increment_db_write_time_ms_total(elapsed_ms_u64(db_write_started));
        match commit {
            Ok(result) if result.is_ok() => {
                self.update_aliases(&token, normalized_aliases);
                self.update_primary_address(&token, normalized_primary_address);
                if !had_existing {
                    self.metrics.increment_push_registered_devices(1);
                }
                Ok(())
            }
            Ok(_) => {
                self.metrics.increment_db_commit_conflicts_total();
                self.metrics.increment_db_errors_total();
                anyhow::bail!("Commit conflict")
            }
            Err(err) => {
                self.metrics.increment_db_errors_total();
                Err(err.into())
            }
        }
    }

    pub fn update(
        &self,
        token: String,
        watched_addresses: Vec<String>,
        primary_address: Option<String>,
        aliases: Vec<String>,
        wallet_binding: Option<WalletBinding>,
    ) -> anyhow::Result<()> {
        self.metrics.increment_push_update_calls_total();
        validate_registration_limits(&watched_addresses, &aliases)?;
        let token = normalize_device_token(&token)?;
        let now = unix_time_secs();
        let (addresses, payloads) = normalize_addresses(watched_addresses)?;
        let normalized_aliases = normalize_aliases_vec(aliases);
        let normalized_alias_set = normalize_aliases(normalized_aliases.clone());
        let normalized_primary_address = normalize_primary_address(primary_address);
        if addresses.is_empty() {
            anyhow::bail!("watched_addresses must not be empty");
        }

        let existing = self.get_registration(&token)?;
        let effective_wallet_binding = resolve_wallet_binding(existing.as_ref(), wallet_binding)?;
        let effective_wallet_pubkey = effective_wallet_binding
            .as_ref()
            .map(|binding| binding.wallet_pubkey.clone());
        let effective_wallet_address = effective_wallet_binding
            .as_ref()
            .map(|binding| binding.wallet_address.clone());
        let effective_app_attest_key_id = effective_wallet_binding
            .as_ref()
            .and_then(|binding| binding.app_attest.as_ref())
            .map(|attest| attest.key_id.clone());
        let effective_app_attest_public_key = effective_wallet_binding
            .as_ref()
            .and_then(|binding| binding.app_attest.as_ref())
            .map(|attest| attest.public_key_spki_b64.clone());
        let effective_app_attest_sign_count = effective_wallet_binding
            .as_ref()
            .and_then(|binding| binding.app_attest.as_ref())
            .map(|attest| attest.sign_count);
        let created_at = existing.as_ref().map(|reg| reg.created_at).unwrap_or(now);
        let platform = existing
            .as_ref()
            .map(|reg| reg.platform.clone())
            .unwrap_or_else(|| "ios".to_string());
        let last_seen_refresh = existing
            .as_ref()
            .map(|reg| should_refresh_last_seen(&token, reg.last_seen, now))
            .unwrap_or(false);
        let addresses_unchanged = existing
            .as_ref()
            .map(|reg| reg.watched_addresses == addresses)
            .unwrap_or(false);
        let aliases_unchanged = existing
            .as_ref()
            .map(|reg| normalize_aliases(reg.aliases.clone()) == normalized_alias_set)
            .unwrap_or(false);
        let primary_unchanged = existing
            .as_ref()
            .map(|reg| {
                normalize_primary_address(reg.primary_address.clone()) == normalized_primary_address
            })
            .unwrap_or(false);
        let wallet_binding_unchanged = existing
            .as_ref()
            .map(|reg| {
                reg.wallet_pubkey == effective_wallet_pubkey
                    && reg.wallet_address == effective_wallet_address
                    && reg.app_attest_key_id == effective_app_attest_key_id
                    && reg.app_attest_public_key_spki_b64 == effective_app_attest_public_key
                    && reg.app_attest_sign_count == effective_app_attest_sign_count
            })
            .unwrap_or(false);
        if addresses_unchanged
            && aliases_unchanged
            && primary_unchanged
            && wallet_binding_unchanged
            && !last_seen_refresh
        {
            self.metrics.increment_push_fast_path_skips_total();
            self.update_aliases(&token, normalized_aliases);
            self.update_primary_address(&token, normalized_primary_address);
            return Ok(());
        }

        let registration = DeviceRegistration {
            device_token: token.clone(),
            platform,
            watched_addresses: addresses,
            aliases: normalized_aliases.clone(),
            primary_address: normalized_primary_address.clone(),
            wallet_pubkey: effective_wallet_pubkey,
            wallet_address: effective_wallet_address,
            app_attest_key_id: effective_app_attest_key_id,
            app_attest_public_key_spki_b64: effective_app_attest_public_key,
            app_attest_sign_count: effective_app_attest_sign_count,
            created_at,
            last_seen: now,
        };

        let token_key = token.as_bytes();
        let registration_bytes = serde_json::to_vec(&registration)?;

        let had_existing = existing.is_some();
        self.metrics.increment_db_write_ops_total(1);
        let db_write_started = Instant::now();
        let mut wtx = self.tx_keyspace.write_tx()?;
        if let Some(existing) = existing {
            if !addresses_unchanged {
                let new_set: HashSet<&str> = registration
                    .watched_addresses
                    .iter()
                    .map(|addr| addr.as_str())
                    .collect();
                let old_set: HashSet<&str> = existing
                    .watched_addresses
                    .iter()
                    .map(|addr| addr.as_str())
                    .collect();
                for address in &existing.watched_addresses {
                    if !new_set.contains(address.as_str()) {
                        if let Ok(payload) = address_to_payload(address) {
                            self.watched_partition
                                .remove_wtx(&mut wtx, &payload, token_key);
                        }
                    }
                }
                for (address, payload) in registration.watched_addresses.iter().zip(payloads.iter())
                {
                    if !old_set.contains(address.as_str()) {
                        self.watched_partition
                            .insert_wtx(&mut wtx, payload, token_key);
                    }
                }
            }
        } else {
            for payload in payloads {
                self.watched_partition
                    .insert_wtx(&mut wtx, &payload, token_key);
            }
        }
        self.device_partition
            .insert_wtx(&mut wtx, token.as_bytes(), &registration_bytes);
        let commit = wtx.commit();
        self.metrics
            .increment_db_write_time_ms_total(elapsed_ms_u64(db_write_started));
        match commit {
            Ok(result) if result.is_ok() => {
                self.update_aliases(&token, normalized_aliases);
                self.update_primary_address(&token, normalized_primary_address);
                if !had_existing {
                    self.metrics.increment_push_registered_devices(1);
                }
                Ok(())
            }
            Ok(_) => {
                self.metrics.increment_db_commit_conflicts_total();
                self.metrics.increment_db_errors_total();
                anyhow::bail!("Commit conflict")
            }
            Err(err) => {
                self.metrics.increment_db_errors_total();
                Err(err.into())
            }
        }
    }

    pub fn unregister(&self, token: String) -> anyhow::Result<()> {
        self.unregister_inner(token, None, false)
    }

    pub fn unregister_authorized(
        &self,
        token: String,
        wallet_pubkey: Option<String>,
    ) -> anyhow::Result<()> {
        let wallet_pubkey = wallet_pubkey
            .as_deref()
            .map(normalize_wallet_pubkey)
            .transpose()?;
        self.unregister_inner(token, wallet_pubkey, true)
    }

    fn unregister_inner(
        &self,
        token: String,
        wallet_pubkey: Option<String>,
        enforce_binding: bool,
    ) -> anyhow::Result<()> {
        self.metrics.increment_push_unregister_calls_total();
        let token = normalize_device_token(&token)?;
        let existing = self.get_registration(&token)?;
        if enforce_binding {
            validate_unregister_binding(existing.as_ref(), wallet_pubkey.as_deref())?;
        }
        let had_existing = existing.is_some();
        let token_key = token.as_bytes();

        self.metrics.increment_db_write_ops_total(1);
        let db_write_started = Instant::now();
        let mut wtx = self.tx_keyspace.write_tx()?;
        if let Some(existing) = existing {
            for address in existing.watched_addresses {
                if let Ok(payload) = address_to_payload(&address) {
                    self.watched_partition
                        .remove_wtx(&mut wtx, &payload, token_key);
                }
            }
        }
        self.device_partition.remove_wtx(&mut wtx, token.as_bytes());
        let commit = wtx.commit();
        self.metrics
            .increment_db_write_time_ms_total(elapsed_ms_u64(db_write_started));
        match commit {
            Ok(result) if result.is_ok() => {
                self.clear_aliases(&token);
                self.clear_primary_address(&token);
                if had_existing {
                    self.metrics.decrement_push_registered_devices(1);
                }
                Ok(())
            }
            Ok(_) => {
                self.metrics.increment_db_commit_conflicts_total();
                self.metrics.increment_db_errors_total();
                anyhow::bail!("Commit conflict")
            }
            Err(err) => {
                self.metrics.increment_db_errors_total();
                Err(err.into())
            }
        }
    }

    pub fn tokens_for_address(&self, address: &AddressPayload) -> anyhow::Result<Vec<String>> {
        self.metrics.increment_db_read_ops_total(1);
        let db_read_started = Instant::now();
        let result = (|| {
            let rtx = self.tx_keyspace.read_tx();
            let mut tokens = Vec::new();
            for entry in self.watched_partition.get_by_address_prefix(&rtx, address) {
                let key = entry?;
                if let Some(token) = token_from_watched_key_bytes(key.as_ref()) {
                    tokens.push(token);
                }
            }
            Ok(tokens)
        })();
        self.metrics
            .increment_db_read_time_ms_total(elapsed_ms_u64(db_read_started));
        if result.is_err() {
            self.metrics.increment_db_errors_total();
        }
        result
    }

    pub fn prune_address_watchers(&self, address: &AddressPayload) -> anyhow::Result<()> {
        self.metrics.increment_db_read_ops_total(1);
        let db_read_started = Instant::now();
        let rtx = self.tx_keyspace.read_tx();
        let keys = self
            .watched_partition
            .get_by_address_prefix(&rtx, address)
            .collect::<anyhow::Result<Vec<_>>>();
        self.metrics
            .increment_db_read_time_ms_total(elapsed_ms_u64(db_read_started));

        let keys = match keys {
            Ok(keys) => keys,
            Err(err) => {
                self.metrics.increment_db_errors_total();
                return Err(err);
            }
        };

        self.metrics.increment_db_write_ops_total(1);
        let db_write_started = Instant::now();
        let mut wtx = self.tx_keyspace.write_tx()?;
        for key in keys {
            self.watched_partition
                .remove_raw_key_wtx(&mut wtx, key.as_ref());
        }

        let result = match wtx.commit() {
            Ok(commit) if commit.is_ok() => Ok(()),
            Ok(_) => {
                self.metrics.increment_db_commit_conflicts_total();
                self.metrics.increment_db_errors_total();
                anyhow::bail!("Commit conflict")
            }
            Err(err) => {
                self.metrics.increment_db_errors_total();
                Err(err.into())
            }
        };

        self.metrics
            .increment_db_write_time_ms_total(elapsed_ms_u64(db_write_started));
        result
    }

    pub fn get_registration(&self, token: &str) -> anyhow::Result<Option<DeviceRegistration>> {
        self.metrics.increment_db_read_ops_total(1);
        let db_read_started = Instant::now();
        let result = (|| {
            let rtx = self.tx_keyspace.read_tx();
            let value = self.device_partition.get_rtx(&rtx, token.as_bytes())?;
            let Some(bytes) = value else {
                return Ok(None);
            };
            Ok(Some(serde_json::from_slice(bytes.as_ref())?))
        })();
        self.metrics
            .increment_db_read_time_ms_total(elapsed_ms_u64(db_read_started));
        if result.is_err() {
            self.metrics.increment_db_errors_total();
        }
        result
    }

    pub fn token_allows_alias(&self, token: &str, alias: &str) -> bool {
        {
            let cache = self.alias_cache.lock().ok();
            let Some(cache) = cache else { return false };
            if let Some(aliases) = cache.get(token) {
                if aliases.is_empty() {
                    return true;
                }
                return aliases.contains(alias);
            }
        }

        self.hydrate_filter_caches(token);

        let cache = self.alias_cache.lock().ok();
        let Some(cache) = cache else { return false };
        match cache.get(token) {
            Some(aliases) if aliases.is_empty() => true,
            Some(aliases) => aliases.contains(alias),
            None => false,
        }
    }

    pub fn token_primary_matches(&self, token: &str, receiver: &AddressPayload) -> bool {
        {
            let cache = self.primary_cache.lock().ok();
            let Some(cache) = cache else { return false };
            if let Some(primary) = cache.get(token) {
                return primary
                    .as_ref()
                    .map(|primary| primary == receiver)
                    .unwrap_or(false);
            }
        }

        self.hydrate_filter_caches(token);

        let cache = self.primary_cache.lock().ok();
        let Some(cache) = cache else { return false };
        match cache.get(token) {
            Some(Some(primary)) => primary == receiver,
            None => false,
            Some(None) => false,
        }
    }

    fn update_aliases(&self, token: &str, aliases: Vec<String>) {
        let normalized = normalize_aliases(aliases);
        let Ok(mut cache) = self.alias_cache.lock() else {
            return;
        };
        // Keep empty set as an explicit "allow all aliases" marker to avoid DB re-hydration loops.
        cache.insert(token.to_string(), normalized);
    }

    fn clear_aliases(&self, token: &str) {
        let Ok(mut cache) = self.alias_cache.lock() else {
            return;
        };
        cache.remove(token);
    }

    fn update_primary_address(&self, token: &str, address: Option<String>) {
        let Ok(mut cache) = self.primary_cache.lock() else {
            return;
        };
        let payload = address.and_then(|address| address_to_payload(&address).ok());
        // Keep None as an explicit "no primary" marker to avoid DB re-hydration loops.
        cache.insert(token.to_string(), payload);
    }

    fn clear_primary_address(&self, token: &str) {
        let Ok(mut cache) = self.primary_cache.lock() else {
            return;
        };
        cache.remove(token);
    }

    pub fn metrics(&self) -> SharedMetrics {
        self.metrics.clone()
    }

    fn hydrate_filter_caches(&self, token: &str) {
        let Ok(Some(registration)) = self.get_registration(token) else {
            return;
        };
        self.update_aliases(token, registration.aliases);
        self.update_primary_address(token, registration.primary_address);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceRegistration {
    pub device_token: String,
    pub platform: String,
    pub watched_addresses: Vec<String>,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub primary_address: Option<String>,
    #[serde(default)]
    pub wallet_pubkey: Option<String>,
    #[serde(default)]
    pub wallet_address: Option<String>,
    #[serde(default)]
    pub app_attest_key_id: Option<String>,
    #[serde(default)]
    pub app_attest_public_key_spki_b64: Option<String>,
    #[serde(default)]
    pub app_attest_sign_count: Option<u32>,
    pub created_at: u64,
    pub last_seen: u64,
}

pub struct PushDispatcher {
    rx: flume::Receiver<PushEvent>,
    registry: PushRegistry,
    metrics: SharedMetrics,
    apns: Option<ApnsClient>,
    network_type: RpcNetworkType,
    sent_cache: SentTxCache,
    invalid_token_counts: HashMap<String, u8>,
}

impl PushDispatcher {
    pub fn new(
        rx: flume::Receiver<PushEvent>,
        registry: PushRegistry,
        context: &IndexerContext,
    ) -> Self {
        let apns = match ApnsClient::from_context(context) {
            Ok(client) => Some(client),
            Err(err) => {
                warn!("[Push] APNs disabled: {err}");
                None
            }
        };
        Self {
            rx,
            metrics: registry.metrics(),
            registry,
            apns,
            network_type: context.network_type.into(),
            sent_cache: SentTxCache::new(Duration::from_secs(60)),
            invalid_token_counts: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        while let Ok(event) = self.rx.recv_async().await {
            self.metrics.increment_push_events_total();
            if self.apns.is_none() {
                continue;
            }
            if let Err(err) = self.handle_event(event).await {
                warn!("[Push] Failed to handle event: {err}");
            }
        }
    }

    async fn handle_event(&mut self, event: PushEvent) -> anyhow::Result<()> {
        let apns = match &self.apns {
            Some(apns) => apns,
            None => return Ok(()),
        };

        let sender_addr = to_rpc_address(&event.sender, self.network_type)?;
        let Some(sender_addr) = sender_addr else {
            return Ok(());
        };
        let sender = sender_addr.to_string();

        let tokens = tokio::task::spawn_blocking({
            let registry = self.registry.clone();
            let watched = event.watched_address;
            move || registry.tokens_for_address(&watched)
        })
        .await??;
        self.metrics
            .increment_push_tokens_looked_up_total(tokens.len() as u64);

        if tokens.is_empty() {
            let registry = self.registry.clone();
            let watched = event.watched_address;
            tokio::task::spawn_blocking(move || {
                let _ = registry.prune_address_watchers(&watched);
            })
            .await
            .ok();
            return Ok(());
        }

        let alias_filter = event.alias.as_deref();
        let receiver_filter = if matches!(
            event.kind,
            PushEventKind::Payment | PushEventKind::Handshake
        ) {
            Some(event.receiver)
        } else {
            None
        };
        let tx_id = event.tx_id.to_hex();
        if !self.sent_cache.mark_seen(&tx_id) {
            self.metrics.increment_push_dedup_dropped_total();
            tracing::debug!("[Push] Duplicate tx {} ignored", tx_id);
            return Ok(());
        }
        let payload_type = match event.kind {
            PushEventKind::Contextual => "contextual",
            PushEventKind::Payment => "payment",
            PushEventKind::Handshake => "handshake",
            PushEventKind::SelfStash => "contextual",
        };
        let watched_addr = to_rpc_address(&event.watched_address, self.network_type)?
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let payload_len = event.payload.as_ref().map(|p| p.len()).unwrap_or(0);
        let payload_included = event
            .payload
            .as_ref()
            .map(|p| p.as_bytes().len() <= MAX_PUSH_PAYLOAD_BYTES)
            .unwrap_or(false);
        info!(
            "[Push] event type={} sender={} watched={} tx={} tokens={} payload_len={} payload_included={}",
            payload_type,
            sender,
            watched_addr,
            tx_id,
            tokens.len(),
            payload_len,
            payload_included
        );
        let alert = PushAlert::from_type(payload_type, &sender);
        let payload = PushPayload {
            aps: PushAps {
                alert,
                mutable_content: 1,
                content_available: 1,
            },
            tx_id,
            sender,
            message_type: payload_type.to_string(),
            amount: event.amount,
            payload: event.payload.and_then(payload_within_limit),
            timestamp: event.timestamp,
            daa_score: event.daa_score,
        };

        for token in tokens {
            if let Some(alias) = alias_filter {
                if !self.registry.token_allows_alias(&token, alias) {
                    self.metrics.increment_push_filtered_alias_total();
                    continue;
                }
            }
            if let Some(receiver) = receiver_filter {
                if !self.registry.token_primary_matches(&token, &receiver) {
                    self.metrics.increment_push_filtered_primary_total();
                    continue;
                }
            }
            let token_short = token
                .get(token.len().saturating_sub(8)..)
                .unwrap_or(token.as_str());
            match apns.send(&token, &payload).await {
                Ok(()) => {
                    info!("[Push] Delivered to ...{}", token_short);
                    self.metrics.increment_push_sent_ok_total();
                    self.invalid_token_counts.remove(&token);
                }
                Err(ApnsError::Unregistered) => {
                    warn!("[Push] Unregistered token ...{}, removing", token_short);
                    self.metrics.increment_push_send_failed_total();
                    self.metrics.increment_push_unregistered_removed_total();
                    let registry = self.registry.clone();
                    let token_clone = token.clone();
                    tokio::task::spawn_blocking(move || registry.unregister(token_clone))
                        .await
                        .ok();
                    self.invalid_token_counts.remove(&token);
                }
                Err(ApnsError::Auth(err)) => {
                    self.metrics.increment_push_send_failed_total();
                    warn!(
                        "[Push] APNs auth failure for ...{}; keeping token registered: {}",
                        token_short, err
                    );
                }
                Err(ApnsError::InvalidToken) => {
                    self.metrics.increment_push_send_failed_total();
                    self.metrics.increment_push_invalid_token_total();
                    let count = self.invalid_token_counts.entry(token.clone()).or_insert(0);
                    *count = count.saturating_add(1);
                    warn!(
                        "[Push] Invalid token ...{} ({} consecutive)",
                        token_short, count
                    );
                    if *count >= 10 {
                        warn!(
                            "[Push] Invalid token threshold reached for ...{}, removing",
                            token_short
                        );
                        self.metrics.increment_push_unregistered_removed_total();
                        let registry = self.registry.clone();
                        let token_clone = token.clone();
                        tokio::task::spawn_blocking(move || registry.unregister(token_clone))
                            .await
                            .ok();
                        self.invalid_token_counts.remove(&token);
                    }
                }
                Err(err) => {
                    self.metrics.increment_push_send_failed_total();
                    warn!("[Push] Failed to deliver to ...{}: {err}", token_short);
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct PushPayload {
    aps: PushAps,
    tx_id: String,
    sender: String,
    #[serde(rename = "type")]
    message_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    amount: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<String>,
    timestamp: u64,
    daa_score: u64,
}

#[derive(Debug, Serialize)]
struct PushAps {
    alert: PushAlert,
    #[serde(rename = "mutable-content")]
    mutable_content: u8,
    #[serde(rename = "content-available")]
    content_available: u8,
}

#[derive(Debug, Serialize)]
struct PushAlert {
    title: String,
    body: String,
}

impl PushAlert {
    fn from_type(payload_type: &str, sender: &str) -> Self {
        let title = sender.to_string();
        let body = match payload_type {
            "payment" => "Payment received".to_string(),
            "handshake" => "Started a conversation".to_string(),
            _ => "New message".to_string(),
        };
        Self { title, body }
    }
}

const MAX_PUSH_PAYLOAD_BYTES: usize = 3_500;

fn payload_within_limit(payload: String) -> Option<String> {
    if payload.as_bytes().len() <= MAX_PUSH_PAYLOAD_BYTES {
        Some(payload)
    } else {
        None
    }
}

struct SentTxCache {
    ttl: Duration,
    seen: HashMap<String, Instant>,
    order: VecDeque<(Instant, String)>,
}

impl SentTxCache {
    fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            seen: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn mark_seen(&mut self, tx_id: &str) -> bool {
        let now = Instant::now();
        self.prune(now);
        if self.seen.contains_key(tx_id) {
            return false;
        }
        let id = tx_id.to_string();
        self.seen.insert(id.clone(), now);
        self.order.push_back((now, id));
        true
    }

    fn prune(&mut self, now: Instant) {
        while let Some((ts, _id)) = self.order.front() {
            if now.duration_since(*ts) <= self.ttl {
                break;
            }
            let (_, id) = self.order.pop_front().expect("front exists");
            self.seen.remove(&id);
        }
    }
}

#[derive(Debug)]
enum ApnsError {
    Request(reqwest::Error),
    Auth(String),
    Rejected { status: u16, reason: Option<String> },
    Unregistered,
    InvalidToken,
}

impl std::fmt::Display for ApnsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApnsError::Request(err) => write!(f, "request error: {err}"),
            ApnsError::Auth(err) => write!(f, "auth error: {err}"),
            ApnsError::Rejected { status, reason } => {
                write!(
                    f,
                    "rejected ({status}): {}",
                    reason.as_deref().unwrap_or("unknown")
                )
            }
            ApnsError::Unregistered => write!(f, "unregistered"),
            ApnsError::InvalidToken => write!(f, "invalid token"),
        }
    }
}

struct ApnsClient {
    client: reqwest::Client,
    endpoint: String,
    key_id: String,
    team_id: String,
    topic: String,
    key: EncodingKey,
    auth_cache: Mutex<Option<AuthCache>>,
}

struct AuthCache {
    token: String,
    issued_at: u64,
}

#[derive(Serialize)]
struct ApnsClaims<'a> {
    iss: &'a str,
    iat: u64,
}

impl ApnsClient {
    fn from_context(context: &IndexerContext) -> anyhow::Result<Self> {
        let config = &context.config;
        let team_id = config
            .apns_team_id
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("APNS_TEAM_ID missing"))?;
        let key_id = config
            .apns_key_id
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("APNS_KEY_ID missing"))?;
        let topic = config
            .apns_topic
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("APNS_TOPIC missing"))?;

        let key_pem = load_apns_key(config.apns_key_path.as_ref(), config.apns_key.as_ref())?;
        let key = EncodingKey::from_ec_pem(key_pem.as_bytes())?;

        let endpoint = match config.apns_environment {
            ApnsEnvironment::Sandbox => "https://api.sandbox.push.apple.com",
            ApnsEnvironment::Production => "https://api.push.apple.com",
        };

        let client = reqwest::Client::builder().build()?;

        Ok(Self {
            client,
            endpoint: endpoint.to_string(),
            key_id: key_id.clone(),
            team_id: team_id.clone(),
            topic: topic.clone(),
            key,
            auth_cache: Mutex::new(None),
        })
    }

    async fn auth_token(&self) -> anyhow::Result<String> {
        let mut cache = self.auth_cache.lock().await;
        let now = unix_time_secs();
        if let Some(cache) = cache.as_ref() {
            if now.saturating_sub(cache.issued_at) < 50 * 60 {
                return Ok(cache.token.clone());
            }
        }

        let header = Header {
            alg: jsonwebtoken::Algorithm::ES256,
            kid: Some(self.key_id.clone()),
            ..Default::default()
        };
        let claims = ApnsClaims {
            iss: &self.team_id,
            iat: now,
        };
        let token = jsonwebtoken::encode(&header, &claims, &self.key)?;
        *cache = Some(AuthCache {
            token: token.clone(),
            issued_at: now,
        });
        Ok(token)
    }

    async fn send<T: Serialize>(&self, token: &str, payload: &T) -> Result<(), ApnsError> {
        let auth_token = self
            .auth_token()
            .await
            .map_err(|err| ApnsError::Auth(err.to_string()))?;
        let url = format!("{}/3/device/{}", self.endpoint, token);
        let resp = self
            .client
            .post(url)
            .header("authorization", format!("bearer {}", auth_token))
            .header("apns-topic", &self.topic)
            .header("apns-push-type", "alert")
            .header("apns-priority", "10")
            .json(payload)
            .send()
            .await
            .map_err(ApnsError::Request)?;

        if resp.status().is_success() {
            return Ok(());
        }

        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        let reason = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|value| {
                value
                    .get("reason")
                    .and_then(|r| r.as_str())
                    .map(|s| s.to_string())
            });

        match reason.as_deref() {
            Some("Unregistered") => Err(ApnsError::Unregistered),
            Some("BadDeviceToken") | Some("DeviceTokenNotForTopic") => Err(ApnsError::InvalidToken),
            _ => Err(ApnsError::Rejected { status, reason }),
        }
    }
}

fn normalize_device_token(token: &str) -> anyhow::Result<String> {
    let cleaned: String = token.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    // APNs treats the token as opaque; length may vary across environments/devices.
    if cleaned.len() < 64 || cleaned.len() > 512 || cleaned.len() % 2 != 0 {
        anyhow::bail!("Invalid device token length");
    }
    Ok(cleaned.to_lowercase())
}

fn normalize_platform(platform: String) -> anyhow::Result<String> {
    let normalized = platform.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        anyhow::bail!("platform must not be empty");
    }
    if normalized.len() > MAX_PLATFORM_LEN_BYTES {
        anyhow::bail!("platform is too long");
    }
    if normalized != SUPPORTED_PLATFORM {
        anyhow::bail!("Unsupported platform");
    }
    Ok(normalized)
}

fn validate_registration_limits(
    watched_addresses: &[String],
    aliases: &[String],
) -> anyhow::Result<()> {
    if watched_addresses.len() > MAX_WATCHED_ADDRESSES {
        anyhow::bail!(
            "Too many watched addresses: {} (max {})",
            watched_addresses.len(),
            MAX_WATCHED_ADDRESSES
        );
    }
    if aliases.len() > MAX_ALIASES {
        anyhow::bail!("Too many aliases: {} (max {})", aliases.len(), MAX_ALIASES);
    }
    for address in watched_addresses {
        let trimmed = address.trim();
        if trimmed.is_empty() {
            anyhow::bail!("watched_addresses must not contain empty entries");
        }
        if trimmed.len() > MAX_ADDRESS_LEN_BYTES {
            anyhow::bail!(
                "Address is too long: {} bytes (max {})",
                trimmed.len(),
                MAX_ADDRESS_LEN_BYTES
            );
        }
    }
    for alias in aliases {
        let trimmed = alias.trim();
        if trimmed.len() > MAX_ALIAS_LEN_BYTES {
            anyhow::bail!(
                "Alias is too long: {} bytes (max {})",
                trimmed.len(),
                MAX_ALIAS_LEN_BYTES
            );
        }
    }
    Ok(())
}

fn normalize_wallet_pubkey(pubkey: &str) -> anyhow::Result<String> {
    let normalized = pubkey.trim().to_ascii_lowercase();
    if normalized.len() != WALLET_PUBKEY_HEX_LEN {
        anyhow::bail!("wallet_pubkey must be 32-byte hex");
    }
    if !normalized.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        anyhow::bail!("wallet_pubkey must be hex");
    }
    Ok(normalized)
}

fn registration_wallet_binding(registration: &DeviceRegistration) -> Option<WalletBinding> {
    let wallet_pubkey = registration.wallet_pubkey.clone()?;
    let wallet_address = registration.wallet_address.clone()?;
    let app_attest = match (
        registration.app_attest_key_id.clone(),
        registration.app_attest_public_key_spki_b64.clone(),
    ) {
        (Some(key_id), Some(public_key_spki_b64)) => Some(AppAttestBinding {
            key_id,
            public_key_spki_b64,
            sign_count: registration.app_attest_sign_count.unwrap_or(0),
        }),
        _ => None,
    };
    Some(WalletBinding {
        wallet_pubkey,
        wallet_address,
        app_attest,
    })
}

fn resolve_wallet_binding(
    existing: Option<&DeviceRegistration>,
    provided: Option<WalletBinding>,
) -> anyhow::Result<Option<WalletBinding>> {
    let existing_binding = existing.and_then(registration_wallet_binding);
    match (existing_binding, provided) {
        (Some(existing_binding), Some(provided_binding)) => {
            let provided_pubkey = normalize_wallet_pubkey(&provided_binding.wallet_pubkey)?;
            if provided_pubkey != existing_binding.wallet_pubkey
                || provided_binding.wallet_address != existing_binding.wallet_address
            {
                anyhow::bail!("device token is bound to another wallet");
            }

            let app_attest = match (&existing_binding.app_attest, &provided_binding.app_attest) {
                (Some(existing_attest), Some(provided_attest)) => {
                    if existing_attest.key_id == provided_attest.key_id
                        && existing_attest.public_key_spki_b64
                            == provided_attest.public_key_spki_b64
                        && provided_attest.sign_count < existing_attest.sign_count
                    {
                        anyhow::bail!("App Attest signCount replay detected");
                    }
                    // Verified API layer may rotate/re-bind App Attest key for the same wallet-bound token.
                    Some(provided_attest.clone())
                }
                (Some(_), None) => {
                    anyhow::bail!("app attest assertion is required for a bound device token")
                }
                (None, Some(provided_attest)) => Some(provided_attest.clone()),
                (None, None) => None,
            };

            Ok(Some(WalletBinding {
                wallet_pubkey: existing_binding.wallet_pubkey,
                wallet_address: existing_binding.wallet_address,
                app_attest,
            }))
        }
        (Some(_existing_binding), None) => {
            anyhow::bail!("auth is required for a wallet-bound device token")
        }
        (None, Some(provided_binding)) => {
            let provided_pubkey = normalize_wallet_pubkey(&provided_binding.wallet_pubkey)?;
            Ok(Some(WalletBinding {
                wallet_pubkey: provided_pubkey,
                wallet_address: provided_binding.wallet_address,
                app_attest: provided_binding.app_attest,
            }))
        }
        (None, None) => Ok(None),
    }
}

fn validate_unregister_binding(
    existing: Option<&DeviceRegistration>,
    wallet_pubkey: Option<&str>,
) -> anyhow::Result<()> {
    let Some(existing) = existing else {
        return Ok(());
    };
    let Some(existing_pubkey) = existing.wallet_pubkey.as_deref() else {
        return Ok(());
    };
    let Some(provided_pubkey) = wallet_pubkey else {
        anyhow::bail!("auth is required for a wallet-bound device token");
    };
    if existing_pubkey != provided_pubkey {
        anyhow::bail!("device token is bound to another wallet");
    }
    Ok(())
}

fn token_from_watched_key_bytes(key: &[u8]) -> Option<String> {
    let address_prefix_len = std::mem::size_of::<AddressPayload>();
    if key.len() <= address_prefix_len {
        return None;
    }
    let token_bytes = &key[address_prefix_len..];
    if !token_bytes.iter().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let token = std::str::from_utf8(token_bytes).ok()?;
    Some(token.to_ascii_lowercase())
}

fn normalize_addresses(
    addresses: Vec<String>,
) -> anyhow::Result<(Vec<String>, Vec<AddressPayload>)> {
    let mut seen = HashSet::new();
    let mut normalized = Vec::new();
    let mut payloads = Vec::new();
    for address in addresses {
        let rpc = RpcAddress::try_from(address.as_str())
            .map_err(|err| anyhow::anyhow!("Invalid address: {err}"))?;
        let payload = AddressPayload::try_from(&rpc)
            .map_err(|err| anyhow::anyhow!("Invalid address payload: {err}"))?;
        let string = rpc.to_string();
        if seen.insert(string.clone()) {
            normalized.push(string);
            payloads.push(payload);
        }
    }
    Ok((normalized, payloads))
}

fn normalize_aliases(aliases: Vec<String>) -> HashSet<String> {
    let mut normalized = HashSet::new();
    for alias in aliases {
        let trimmed = alias.trim();
        if trimmed.is_empty() {
            continue;
        }
        normalized.insert(trimmed.to_string());
    }
    normalized
}

fn normalize_aliases_vec(aliases: Vec<String>) -> Vec<String> {
    let mut normalized: Vec<String> = normalize_aliases(aliases).into_iter().collect();
    normalized.sort_unstable();
    normalized
}

fn normalize_primary_address(address: Option<String>) -> Option<String> {
    let Some(address) = address else {
        return None;
    };
    RpcAddress::try_from(address.trim())
        .ok()
        .map(|rpc| rpc.to_string())
}

const LAST_SEEN_BASE_REFRESH_SECS: u64 = 3 * 24 * 60 * 60;
const LAST_SEEN_JITTER_MIN_SECS: u64 = 24 * 60 * 60;
const LAST_SEEN_JITTER_MAX_SECS: u64 = 72 * 60 * 60;

fn should_refresh_last_seen(token: &str, last_seen: u64, now: u64) -> bool {
    let elapsed = now.saturating_sub(last_seen);
    elapsed >= LAST_SEEN_BASE_REFRESH_SECS + last_seen_refresh_jitter_secs(token, last_seen)
}

fn elapsed_ms_u64(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u64::MAX as u128) as u64
}

fn last_seen_refresh_jitter_secs(token: &str, last_seen: u64) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    token.hash(&mut hasher);
    last_seen.hash(&mut hasher);
    let span = LAST_SEEN_JITTER_MAX_SECS
        .saturating_sub(LAST_SEEN_JITTER_MIN_SECS)
        .saturating_add(1);
    LAST_SEEN_JITTER_MIN_SECS + (hasher.finish() % span)
}

fn address_to_payload(address: &str) -> anyhow::Result<AddressPayload> {
    let rpc = RpcAddress::try_from(address)?;
    AddressPayload::try_from(&rpc).map_err(anyhow::Error::from)
}

fn load_apns_key(
    key_path: Option<&PathBuf>,
    key_inline: Option<&String>,
) -> anyhow::Result<String> {
    if let Some(key) = key_inline {
        return Ok(key.replace("\\n", "\n"));
    }
    let Some(path) = key_path else {
        anyhow::bail!("APNS_KEY or APNS_KEY_PATH missing");
    };
    Ok(std::fs::read_to_string(path)?)
}

fn unix_time_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::{
        AppAttestBinding, DeviceRegistration, MAX_ADDRESS_LEN_BYTES, MAX_ALIAS_LEN_BYTES,
        MAX_ALIASES, MAX_WATCHED_ADDRESSES, WalletBinding, normalize_platform,
        normalize_wallet_pubkey, resolve_wallet_binding, validate_registration_limits,
    };

    #[test]
    fn normalize_platform_accepts_ios() {
        assert_eq!(
            normalize_platform(" iOS ".to_string()).expect("ios should be accepted"),
            "ios"
        );
    }

    #[test]
    fn normalize_platform_rejects_non_ios() {
        assert!(normalize_platform("android".to_string()).is_err());
        assert!(normalize_platform("".to_string()).is_err());
    }

    #[test]
    fn validate_registration_limits_rejects_large_vectors() {
        let addresses = vec!["a".to_string(); MAX_WATCHED_ADDRESSES + 1];
        let aliases = vec!["b".to_string(); MAX_ALIASES + 1];
        assert!(validate_registration_limits(&addresses, &Vec::<String>::new()).is_err());
        assert!(validate_registration_limits(&Vec::<String>::new(), &aliases).is_err());
    }

    #[test]
    fn validate_registration_limits_rejects_oversized_entries() {
        let long_address = "a".repeat(MAX_ADDRESS_LEN_BYTES + 1);
        let long_alias = "b".repeat(MAX_ALIAS_LEN_BYTES + 1);
        assert!(validate_registration_limits(&[long_address], &Vec::<String>::new()).is_err());
        assert!(validate_registration_limits(&Vec::<String>::new(), &[long_alias]).is_err());
    }

    #[test]
    fn normalize_wallet_pubkey_rejects_invalid_values() {
        assert!(normalize_wallet_pubkey("").is_err());
        assert!(normalize_wallet_pubkey("abc").is_err());
        assert!(normalize_wallet_pubkey(&"g".repeat(64)).is_err());
        assert!(normalize_wallet_pubkey(&"a".repeat(66)).is_err());
        assert!(normalize_wallet_pubkey(&"f".repeat(64)).is_ok());
    }

    #[test]
    fn resolve_wallet_binding_enforces_existing_binding() {
        let existing = DeviceRegistration {
            device_token: "token".to_string(),
            platform: "ios".to_string(),
            watched_addresses: vec![],
            aliases: vec![],
            primary_address: None,
            wallet_pubkey: Some("a".repeat(64)),
            wallet_address: Some("kaspa:qwalletbound".to_string()),
            app_attest_key_id: Some("key_id".to_string()),
            app_attest_public_key_spki_b64: Some("public_key".to_string()),
            app_attest_sign_count: Some(2),
            created_at: 0,
            last_seen: 0,
        };
        let wrong = WalletBinding {
            wallet_pubkey: "b".repeat(64),
            wallet_address: existing.wallet_address.clone().unwrap_or_default(),
            app_attest: Some(AppAttestBinding {
                key_id: "key_id".to_string(),
                public_key_spki_b64: "public_key".to_string(),
                sign_count: 3,
            }),
        };

        assert!(resolve_wallet_binding(Some(&existing), None).is_err());
        assert!(resolve_wallet_binding(Some(&existing), Some(wrong)).is_err());
        assert!(
            resolve_wallet_binding(
                Some(&existing),
                Some(WalletBinding {
                    wallet_pubkey: "a".repeat(64),
                    wallet_address: existing.wallet_address.clone().unwrap_or_default(),
                    app_attest: Some(AppAttestBinding {
                        key_id: "key_id".to_string(),
                        public_key_spki_b64: "public_key".to_string(),
                        sign_count: 3,
                    }),
                })
            )
            .is_ok()
        );
        assert!(
            resolve_wallet_binding(
                Some(&existing),
                Some(WalletBinding {
                    wallet_pubkey: "a".repeat(64),
                    wallet_address: existing.wallet_address.clone().unwrap_or_default(),
                    app_attest: Some(AppAttestBinding {
                        key_id: "rotated_key_id".to_string(),
                        public_key_spki_b64: "rotated_public_key".to_string(),
                        sign_count: 0,
                    }),
                })
            )
            .is_ok()
        );
    }
}
