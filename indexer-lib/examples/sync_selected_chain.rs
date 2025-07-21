use fjall::{Config, TxKeyspace};
use indexer_lib::acceptance_worker::VirtualChainChangedNotificationAndBlueWork;
use indexer_lib::{
    BlockOrMany,
    database::{
        block_compact_header::BlockCompactHeaderPartition, block_gaps::BlockGapsPartition,
        metadata::MetadataPartition,
    },
    selected_chain_syncer::{Intake, SelectedChainSyncer},
    subscriber::Subscriber,
};
use kaspa_wrpc_client::{
    KaspaRpcClient, Resolver, WrpcEncoding,
    client::{ConnectOptions, ConnectStrategy},
    prelude::NetworkId,
    prelude::NetworkType,
};
use std::process::ExitCode;
use std::time::Duration;
use tokio::signal;
use tracing::{error, info, warn};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> ExitCode {
    // Initialize tracing
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    info!("Starting Kaspa Selected Chain Syncer Example");

    match run_selected_chain_syncer().await {
        Ok(_) => {
            info!("Selected chain syncer completed successfully!");
            ExitCode::SUCCESS
        }
        Err(error) => {
            error!("Selected chain syncer failed: {}", error);
            ExitCode::FAILURE
        }
    }
}

async fn run_selected_chain_syncer() -> anyhow::Result<()> {
    // Setup RPC client connection
    let client = setup_rpc_client().await?;

    // Setup database
    let tx_keyspace = TxKeyspace::open(Config::default().temporary(true))?;
    let metadata_partition = MetadataPartition::new(&tx_keyspace)?;
    let block_compact_header_partition = BlockCompactHeaderPartition::new(&tx_keyspace)?;
    let block_gaps_partition = BlockGapsPartition::new(&tx_keyspace)?;

    // Create communication channels
    let (block_tx, block_rx) = flume::bounded::<BlockOrMany>(256);
    let (intake_tx, intake_rx) = tokio::sync::mpsc::channel::<Intake>(256);
    let (historical_sync_done_tx, historical_sync_done_rx) = tokio::sync::mpsc::channel(256);
    let (worker_tx, worker_rx) = flume::bounded(256);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    // Create selected chain syncer
    let mut selected_chain_syncer = SelectedChainSyncer::new(
        client.clone(),
        metadata_partition,
        block_compact_header_partition,
        intake_rx,
        historical_sync_done_rx,
        historical_sync_done_tx,
        worker_tx,
        shutdown_rx,
    );

    let (subscriber_shutdown_tx, subscriber_shutdown_rx) = tokio::sync::oneshot::channel();
    // Create subscriber for real-time notifications
    let subscriber_client = client.clone();
    let subscriber_handle = tokio::spawn(async move {
        let mut subscriber = Subscriber::new(
            subscriber_client,
            block_tx,
            subscriber_shutdown_rx,
            block_gaps_partition,
            intake_tx,
            None,
        );
        if let Err(e) = subscriber.task().await {
            error!("Subscriber task failed: {}", e);
        } else {
            info!("Subscriber task completed successfully");
        }
    });

    // Create worker task that prints VCC notifications from selected chain syncer
    let worker_handle = std::thread::spawn(move || {
        info!("Starting selected chain VCC worker");

        while let Ok(VirtualChainChangedNotificationAndBlueWork {
            vcc,
            last_block_blue_work,
        }) = worker_rx.recv()
        {
            if !vcc.added_chain_block_hashes.is_empty() {
                let first_hash = vcc.added_chain_block_hashes.first().unwrap();
                let last_hash = vcc.added_chain_block_hashes.last().unwrap();

                info!(
                    "⛓️  SELECTED CHAIN: {} blocks added | First: {:?} | Last: {:?} | LastBlueWork: {}",
                    vcc.added_chain_block_hashes.len(),
                    first_hash,
                    last_hash,
                    last_block_blue_work,
                );
            }

            if !vcc.removed_chain_block_hashes.is_empty() {
                let first_hash = vcc.removed_chain_block_hashes.first().unwrap();
                let last_hash = vcc.removed_chain_block_hashes.last().unwrap();

                info!(
                    "🔄 SELECTED CHAIN: {} blocks removed | First: {:?} | Last: {:?}",
                    vcc.removed_chain_block_hashes.len(),
                    first_hash,
                    last_hash
                );
            }
        }

        info!("Selected chain VCC worker completed");
    });

    // Create block processor task (just to consume blocks)
    let block_processor_handle = std::thread::spawn(move || {
        info!("Starting block processor worker");

        while let Ok(block) = block_rx.recv() {
            match block {
                BlockOrMany::Block(block) => {
                    info!("📋 BLOCK RECEIVED: {:?}", block.header.hash);
                }
                BlockOrMany::Many(blocks) => {
                    info!("📋 BLOCKS RECEIVED: {} blocks", blocks.len());
                }
            }
        }

        info!("Block processor worker completed");
    });

    // Create selected chain syncer task
    let syncer_handle = tokio::spawn(async move {
        if let Err(e) = selected_chain_syncer.process().await {
            error!("Selected chain syncer task failed: {}", e);
        } else {
            info!("Selected chain syncer task completed successfully");
        }
    });

    info!("All tasks started, waiting for completion or shutdown signal");

    // Wait for Ctrl+C or task completion
    tokio::select! {
        _ = signal::ctrl_c() => {
            warn!("Shutdown signal received");
            let _ = shutdown_tx.send(());
            let _ = subscriber_shutdown_tx.send(());
        }
        result = syncer_handle => {
            match result {
                Ok(_) => info!("Selected chain syncer task completed"),
                Err(e) => error!("Selected chain syncer task panicked: {}", e),
            }
        }
        result = subscriber_handle => {
            match result {
                Ok(_) => info!("Subscriber task completed"),
                Err(e) => error!("Subscriber task panicked: {}", e),
            }
        }
    }

    // Wait for worker threads to finish
    if worker_handle.join().is_err() {
        error!("Selected chain VCC worker thread panicked");
    }
    if block_processor_handle.join().is_err() {
        error!("Block processor worker thread panicked");
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
        block_async_connect: false,
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
