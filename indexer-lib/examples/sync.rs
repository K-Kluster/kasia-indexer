use indexer_lib::BlockOrMany;
use indexer_lib::historical_syncer::{Cursor, HistoricalDataSyncer};
use kaspa_rpc_core::{GetBlockDagInfoResponse, GetServerInfoResponse, api::rpc::RpcApi};
use kaspa_wrpc_client::{
    KaspaRpcClient, Resolver, WrpcEncoding,
    client::{ConnectOptions, ConnectStrategy},
    prelude::NetworkId,
    prelude::NetworkType,
};
use std::process::ExitCode;
use std::time::Duration;
use tokio::signal;
use tracing::{Level, debug, error, info, warn};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> ExitCode {
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    info!("Starting Kaspa Historical Data Syncer");

    match run_syncer().await {
        Ok(_) => {
            info!("Syncer completed successfully!");
            ExitCode::SUCCESS
        }
        Err(error) => {
            error!("Syncer failed: {}", error);
            ExitCode::FAILURE
        }
    }
}

async fn run_syncer() -> anyhow::Result<()> {
    // Setup RPC client connection
    let client = setup_rpc_client().await?;

    // Get node and network information
    let (server_info, dag_info) = get_node_info(&client).await?;

    // Validate node state
    validate_node_state(&server_info, &dag_info)?;

    // Setup sync cursors
    let (start_cursor, target_cursor) = setup_sync_cursors(&client, &dag_info).await?;

    info!(
        "Sync range: {} blocks (blue work {} -> {})",
        target_cursor
            .blue_work
            .saturating_sub(start_cursor.blue_work),
        start_cursor.blue_work,
        target_cursor.blue_work
    );

    // Create communication channels
    let (block_tx, block_rx) = flume::unbounded::<BlockOrMany>();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    // Clone client for syncer task
    let syncer_client = client.clone();

    info!("Starting syncer and block processor tasks");

    // Task 1: Historical data syncer
    let syncer_handle = tokio::spawn(async move {
        let mut syncer = HistoricalDataSyncer::new(
            syncer_client,
            start_cursor,
            target_cursor,
            block_tx,
            shutdown_rx,
        );

        if let Err(e) = syncer.sync().await {
            error!("Syncer task failed: {}", e);
        } else {
            info!("Syncer task completed successfully");
        }
    });

    // Task 2: Block processor (reads from flume channel)
    let processor_handle = tokio::spawn(async move {
        let mut total_blocks_processed = 0u64;
        let mut batch_count = 0u64;

        info!("Block processor started, waiting for blocks...");

        while let Ok(blocks) = block_rx.recv_async().await {
            batch_count += 1;
            let batch_size = blocks.len();
            total_blocks_processed += batch_size as u64;

            debug!(
                "Processing batch {} with {} blocks",
                batch_count, batch_size
            );

            // Process each block in the batch
            for (idx, block) in blocks.iter().enumerate() {
                let block_hash = &block.header.hash;
                let blue_work = block.header.blue_work;

                // Log block information
                if idx == 0 || idx == batch_size - 1 {
                    info!("Block: hash={:?}, blue_work={}", block_hash, blue_work);
                }

                // Here you could add additional block processing logic:
                // - Store blocks in database
                // - Extract transactions
                // - Build indexes
                // - etc.
            }

            // Log batch completion
            if batch_count % 10 == 0 {
                info!(
                    "Processed {} batches, {} total blocks. Latest blue work: {}",
                    batch_count,
                    total_blocks_processed,
                    blocks
                        .last()
                        .map(|b| b.header.blue_work)
                        .unwrap_or(0.into())
                );
            }
        }

        info!(
            "Block processor finished. Total: {} blocks in {} batches",
            total_blocks_processed, batch_count
        );
    });

    // Wait for Ctrl+C or task completion
    tokio::select! {
        _ = signal::ctrl_c() => {
            warn!("Shutdown signal received");
            let _ = shutdown_tx.send(());
        }
        result = syncer_handle => {
            match result {
                Ok(_) => info!("Syncer task completed"),
                Err(e) => error!("Syncer task panicked: {}", e),
            }
        }
        result = processor_handle => {
            match result {
                Ok(_) => info!("Processor task completed"),
                Err(e) => error!("Processor task panicked: {}", e),
            }
        }
    }

    // Cleanup
    client.disconnect().await?;
    info!("Disconnected from Kaspa node");

    Ok(())
}

async fn setup_rpc_client() -> anyhow::Result<KaspaRpcClient> {
    let encoding = WrpcEncoding::Borsh;

    let resolver = Some(Resolver::default());
    let url = None;
    let network_type = NetworkType::Mainnet;
    let selected_network = Some(NetworkId::new(network_type));

    let subscription_context = None;

    info!("Creating RPC client for network: {:?}", network_type);

    let client = KaspaRpcClient::new(
        encoding,
        url,
        resolver,
        selected_network,
        subscription_context,
    )
    .map_err(|e| anyhow::anyhow!("Failed to create RPC client: {}", e))?;

    let options = ConnectOptions {
        block_async_connect: true,
        connect_timeout: Some(Duration::from_millis(10_000)),
        strategy: ConnectStrategy::Fallback,
        ..Default::default()
    };

    info!("Connecting to Kaspa node...");
    client
        .connect(Some(options))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to node: {}", e))?;

    info!("Successfully connected to Kaspa node");
    Ok(client)
}

async fn get_node_info(
    client: &KaspaRpcClient,
) -> anyhow::Result<(GetServerInfoResponse, GetBlockDagInfoResponse)> {
    info!("Fetching node information...");

    let server_info = client
        .get_server_info()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get server info: {}", e))?;

    let dag_info = client
        .get_block_dag_info()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get DAG info: {}", e))?;

    info!("Node version: {}", server_info.server_version);
    info!("Network: {}", server_info.network_id);
    info!("Node synced: {}", server_info.is_synced);
    info!("UTXO index: {}", server_info.has_utxo_index);
    info!("Virtual DAA score: {}", dag_info.virtual_daa_score);
    info!("Block count: {}", dag_info.block_count);

    Ok((server_info, dag_info))
}

fn validate_node_state(
    server_info: &GetServerInfoResponse,
    dag_info: &GetBlockDagInfoResponse,
) -> anyhow::Result<()> {
    if !server_info.is_synced {
        return Err(anyhow::anyhow!("Node is not fully synced"));
    }

    if dag_info.block_count == 0 {
        return Err(anyhow::anyhow!("No blocks available"));
    }

    info!("Node validation passed");
    Ok(())
}

async fn setup_sync_cursors(
    client: &KaspaRpcClient,
    dag_info: &GetBlockDagInfoResponse,
) -> anyhow::Result<(Cursor, Cursor)> {
    info!("Setting up sync cursors...");

    // Start from pruning point
    let pruning_point_hash = dag_info.pruning_point_hash;
    info!("Pruning point hash: {:?}", pruning_point_hash);

    // Get pruning point block to extract blue work
    let pruning_point_block = client
        .get_block(pruning_point_hash, true)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get pruning point block: {}", e))?;

    let start_cursor = Cursor::new(pruning_point_block.header.blue_work, pruning_point_hash);

    // Target is the sink
    let sink_hash = dag_info.sink;
    info!("Sink hash: {:?}", sink_hash);

    // Get sink block to extract blue work
    let sink_block = client
        .get_block(sink_hash, true)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get sink block: {}", e))?;

    let target_cursor = Cursor::new(sink_block.header.blue_work, sink_hash);

    // Validate sync range
    if start_cursor.blue_work >= target_cursor.blue_work {
        return Err(anyhow::anyhow!(
            "Invalid sync range: start blue work ({}) >= target blue work ({})",
            start_cursor.blue_work,
            target_cursor.blue_work
        ));
    }

    info!(
        "Sync cursors: start={:?} (blue_work={}), target={:?} (blue_work={})",
        start_cursor.hash, start_cursor.blue_work, target_cursor.hash, target_cursor.blue_work
    );

    Ok((start_cursor, target_cursor))
}
