use fjall::{Config, TxKeyspace};
use indexer_lib::{
    BlockOrMany, database::block_gaps::BlockGapsPartition, selected_chain_syncer::Intake,
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

    info!("Starting Kaspa Subscriber Example");

    match run_subscriber().await {
        Ok(_) => {
            info!("Subscriber completed successfully!");
            ExitCode::SUCCESS
        }
        Err(error) => {
            error!("Subscriber failed: {}", error);
            ExitCode::FAILURE
        }
    }
}

async fn run_subscriber() -> anyhow::Result<()> {
    // Create communication channels
    let (block_tx, block_rx) = flume::bounded::<BlockOrMany>(256);
    let (intake_tx, mut intake_rx) = tokio::sync::mpsc::channel::<Intake>(256);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    // Create worker task that prints intake events and block information
    let worker_handle = tokio::spawn(async move {
        info!("Starting intake processor worker");

        while let Some(intake) = intake_rx.recv().await {
            match intake {
                Intake::Connected => {
                    info!("🔗 CONNECTION EVENT: Connected to Kaspa node");
                }
                Intake::Disconnect => {
                    info!("💔 CONNECTION EVENT: Disconnected from Kaspa node");
                }
                Intake::VirtualChainChangedNotification(vcc) => {
                    if !vcc.added_chain_block_hashes.is_empty() {
                        let first_hash = vcc.added_chain_block_hashes.first().unwrap();
                        let last_hash = vcc.added_chain_block_hashes.last().unwrap();

                        info!(
                            "📦 VCC NOTIFICATION: {} blocks added | First: {:?} | Last: {:?}",
                            vcc.added_chain_block_hashes.len(),
                            first_hash,
                            last_hash
                        );
                    }

                    if !vcc.removed_chain_block_hashes.is_empty() {
                        info!(
                            "🗑️  VCC NOTIFICATION: {} blocks removed | First: {:?} | Last: {:?}",
                            vcc.removed_chain_block_hashes.len(),
                            vcc.removed_chain_block_hashes.first().unwrap(),
                            vcc.removed_chain_block_hashes.last().unwrap()
                        );
                    }
                }
            }
        }

        info!("Intake processor worker completed");
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

    // Setup RPC client connection
    let client = setup_rpc_client().await?;

    // Setup database for block gaps
    let tx_keyspace = TxKeyspace::open(Config::default().temporary(true))?;
    let block_gaps_partition = BlockGapsPartition::new(&tx_keyspace)?;

    // Create subscriber for real-time notifications
    let subscriber_client = client.clone();
    let subscriber_handle = tokio::spawn(async move {
        let mut subscriber = Subscriber::new(
            subscriber_client,
            block_tx,
            shutdown_rx,
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

    info!("All tasks started, waiting for completion or shutdown signal");

    // Wait for Ctrl+C or task completion
    tokio::select! {
        _ = signal::ctrl_c() => {
            warn!("Shutdown signal received");
            let _ = shutdown_tx.send(());
        }
        result = subscriber_handle => {
            match result {
                Ok(_) => info!("Subscriber task completed"),
                Err(e) => error!("Subscriber task panicked: {}", e),
            }
        }
        result = worker_handle => {
            match result {
                Ok(_) => info!("Intake processor worker completed"),
                Err(e) => error!("Intake processor worker panicked: {}", e),
            }
        }
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
