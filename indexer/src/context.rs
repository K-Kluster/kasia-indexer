use std::{path::PathBuf, str::FromStr, sync::Arc};

use kaspa_wrpc_client::{
    KaspaRpcClient, WrpcEncoding,
    prelude::{NetworkId, NetworkType},
};
use tracing::info;

struct InnerIndexerContext {
    db_path: PathBuf,
    log_path: PathBuf,
    network_type: NetworkType,
    rpc_client: KaspaRpcClient,
}

#[derive(Clone)]
pub struct IndexerContext {
    inner: Arc<InnerIndexerContext>,
}

fn create_rpc_client(network_type: NetworkType) -> anyhow::Result<KaspaRpcClient> {
    let encoding = WrpcEncoding::Borsh;

    let url = std::env::var("KASPA_NODE_WBORSH_URL").ok();
    let resolver = if url.is_some() {
        None
    } else {
        Some(kaspa_wrpc_client::Resolver::default())
    };

    let selected_network = if network_type == NetworkType::Mainnet {
        Some(NetworkId::new(NetworkType::Mainnet))
    } else {
        Some(NetworkId::with_suffix(network_type, 10))
    };

    let subscription_context = None;

    info!(
        "Creating RPC client for network: {:?}, with url {:?}",
        network_type, url
    );

    let client = KaspaRpcClient::new(
        encoding,
        url.as_deref(),
        resolver,
        selected_network,
        subscription_context,
    )
    .map_err(|e| anyhow::anyhow!("Failed to create RPC client: {}", e))?;

    info!(
        "Creating RPC client for network: {:?}, with url {:?}",
        network_type, url
    );

    Ok(client)
}

impl IndexerContext {
    pub fn try_new() -> anyhow::Result<Self> {
        let network_type = std::env::var("NETWORK_TYPE")
            .map(|s| NetworkType::from_str(&s).unwrap_or(NetworkType::Mainnet))
            .unwrap_or(NetworkType::Mainnet);

        let db_path = std::env::var("KASIA_INDEXER_DB_ROOT")
            .map(|s| PathBuf::from(s).join(network_type.to_string()))
            .unwrap_or_else(|_| {
                std::env::home_dir()
                    .unwrap()
                    .join(".kasia-indexer")
                    .join(network_type.to_string())
            });
        let log_path = db_path.join("app_logs");

        std::fs::create_dir_all(&log_path)?;

        let rpc_client = create_rpc_client(network_type)?;

        Ok(Self {
            inner: Arc::new(InnerIndexerContext {
                db_path,
                log_path,
                network_type,
                rpc_client,
            }),
        })
    }

    pub fn db_path(&self) -> &PathBuf {
        &self.inner.db_path
    }

    pub fn log_path(&self) -> &PathBuf {
        &self.inner.log_path
    }

    pub fn network_type(&self) -> NetworkType {
        self.inner.network_type
    }

    pub fn rpc_client(&self) -> KaspaRpcClient {
        self.inner.rpc_client.clone()
    }
}
