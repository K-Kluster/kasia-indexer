use crate::config::IndexerConfig;
use kaspa_wrpc_client::{
    KaspaRpcClient, WrpcEncoding,
    prelude::{NetworkId, NetworkType},
};
use std::path::PathBuf;
use tracing::info;

#[derive(Clone)]
pub struct IndexerContext {
    pub config: IndexerConfig,
    pub db_path: PathBuf,
    pub log_path: PathBuf,
    pub network_type: NetworkType,
    pub rpc_client: KaspaRpcClient,
}

fn create_rpc_client(indexer_config: &IndexerConfig) -> anyhow::Result<KaspaRpcClient> {
    let network_type = indexer_config.network_type;
    let encoding = WrpcEncoding::Borsh;

    let url = indexer_config.kaspa_node_wborsh_url.clone();
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

    Ok(client)
}

pub fn get_indexer_context(indexer_config: &IndexerConfig) -> anyhow::Result<IndexerContext> {
    let network_type = indexer_config.network_type;

    let db_path = indexer_config
        .kasia_indexer_db_root
        .join(network_type.to_string());

    let log_path = db_path.join("app_logs");

    std::fs::create_dir_all(&log_path)?;

    let rpc_client = create_rpc_client(indexer_config)?;

    Ok(IndexerContext {
        db_path,
        log_path,
        network_type,
        rpc_client,
        config: indexer_config.clone(),
    })
}
