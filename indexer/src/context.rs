use std::{path::PathBuf, str::FromStr};

use kaspa_wrpc_client::{
    KaspaRpcClient, WrpcEncoding,
    prelude::{NetworkId, NetworkType},
};
use tracing::info;

#[derive(Clone)]
pub struct IndexerContext {
    pub db_path: PathBuf,
    pub log_path: PathBuf,
    pub network_type: NetworkType,
    pub rpc_client: KaspaRpcClient,
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

pub fn get_indexer_context() -> anyhow::Result<IndexerContext> {
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

    Ok(IndexerContext {
        db_path,
        log_path,
        network_type,
        rpc_client,
    })
}
