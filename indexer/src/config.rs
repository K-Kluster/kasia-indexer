use std::path::PathBuf;

use kaspa_wrpc_client::prelude::NetworkType;
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use tracing::level_filters::LevelFilter;

#[serde_as]
#[derive(Deserialize, Debug, Clone)]
pub struct IndexerConfig {
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_rust_log")]
    pub rust_log: LevelFilter,
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_rust_log")]
    pub rust_log_file: LevelFilter,
    #[serde(default = "default_kasia_indexer_db_root")]
    pub kasia_indexer_db_root: PathBuf,
    #[serde(default = "default_network_type")]
    pub network_type: NetworkType,
    pub kaspa_node_wborsh_url: Option<String>,
}

fn default_rust_log() -> LevelFilter {
    LevelFilter::INFO
}

fn default_network_type() -> NetworkType {
    NetworkType::Mainnet
}

fn default_kasia_indexer_db_root() -> PathBuf {
    std::env::home_dir().unwrap().join(".kasia-indexer")
}

pub fn get_indexer_config() -> anyhow::Result<IndexerConfig> {
    Ok(envy::from_env::<IndexerConfig>()?)
}
