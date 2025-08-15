use std::{str::FromStr, sync::Arc, time::Duration};

use clap::{Command, arg, command};
use dotenv::dotenv;
use fjall::Config;
use indexer_lib::{
    BlockOrMany,
    block_processor::BlockProcessor,
    database::{
        self,
        headers::{BlockCompactHeaderPartition, BlockGapsPartition, DaaIndexPartition},
        messages::{
            ContextualMessageBySenderPartition, HandshakeByReceiverPartition,
            HandshakeBySenderPartition, PaymentByReceiverPartition, PaymentBySenderPartition,
            TxIdToHandshakePartition, TxIdToPaymentPartition,
            self_stashes::{SelfStashByOwnerPartition, TxIdToSelfStashPartition},
        },
        processing::{
            AcceptingBlockToTxIDPartition, PendingSenderResolutionPartition,
            SkipTxByBlockPartition, SkipTxPartition, TxIDToAcceptancePartition,
            UnknownAcceptingDaaPartition, UnknownTxPartition,
        },
    },
    fifo_set::FifoSet,
    metrics::{IndexerMetricsSnapshot, create_shared_metrics_from_snapshot},
};
use kaspa_consensus_core::{
    Hash,
    network::{NetworkId, NetworkType},
};
use kaspa_wrpc_client::{KaspaRpcClient, WrpcEncoding, client::ConnectOptions, prelude::RpcApi};
use tokio::time::sleep;
use tracing::{error, info};
use tracing_subscriber::{Layer, layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::{CliConfig, get_cli_config};

mod config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ignore faillures as .env might not be present at runtime, and this use-case is tolerated
    dotenv()
        .inspect_err(|err| println!("[WARN] reading .env files is failed with err {err}"))
        .ok();

    let config = get_cli_config()?;

    // logs
    let (non_blocking_appender, _guard_stdout) = tracing_appender::non_blocking(std::io::stdout());
    let stdout_subscriber = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking_appender)
        .with_filter(config.rust_log);
    tracing_subscriber::registry()
        .with(stdout_subscriber)
        .init();

    let rpc_client = create_rpc_client(&config)?;

    rpc_client
        .connect(Some(ConnectOptions {
            block_async_connect: true,
            ..Default::default()
        }))
        .await?;

    // Database

    let db_path = config
        .kasia_indexer_db_root
        .join(config.network_type.to_string());

    let config = Config::new(db_path).max_write_buffer_size(512 * 1024 * 1024);
    let tx_keyspace = config.open_transactional()?;

    let metadata_partition = database::metadata::MetadataPartition::new(&tx_keyspace)?;
    let handshake_by_receiver_partition = HandshakeByReceiverPartition::new(&tx_keyspace)?;
    let tx_id_to_handshake_partition = TxIdToHandshakePartition::new(&tx_keyspace)?;
    let contextual_message_partition = ContextualMessageBySenderPartition::new(&tx_keyspace)?;
    let payment_by_receiver_partition = PaymentByReceiverPartition::new(&tx_keyspace)?;
    let self_stash_by_owner_partition = SelfStashByOwnerPartition::new(&tx_keyspace)?;
    let tx_id_to_self_stash_partition = TxIdToSelfStashPartition::new(&tx_keyspace)?;
    let tx_id_to_payment_partition = TxIdToPaymentPartition::new(&tx_keyspace)?;
    let tx_id_to_acceptance_partition = TxIDToAcceptancePartition::new(&tx_keyspace)?;
    let skip_tx_partition = SkipTxPartition::new(&tx_keyspace)?;
    let skip_tx_by_block_partition = SkipTxByBlockPartition::new(&tx_keyspace)?;
    let block_compact_header_partition = BlockCompactHeaderPartition::new(&tx_keyspace)?;
    let _acceptance_to_tx_id_partition = AcceptingBlockToTxIDPartition::new(&tx_keyspace)?;
    let _unknown_tx_partition = UnknownTxPartition::new(&tx_keyspace)?;
    let unknown_accepting_daa_partition = UnknownAcceptingDaaPartition::new(&tx_keyspace)?;
    let pending_sender_resolution_partition = PendingSenderResolutionPartition::new(&tx_keyspace)?;
    let handshake_by_sender_partition = HandshakeBySenderPartition::new(&tx_keyspace)?;
    let payment_by_sender_partition = PaymentBySenderPartition::new(&tx_keyspace)?;
    let _block_gaps_partition = BlockGapsPartition::new(&tx_keyspace)?;
    let block_daa_index_partition = DaaIndexPartition::new(&tx_keyspace)?;

    let (block_intake_tx, block_intake_rx) = flume::bounded(1);
    let (shutdown_block_worker_tx, shutdown_block_worker_rx) = flume::bounded(1);

    let metrics = create_shared_metrics_from_snapshot(IndexerMetricsSnapshot {
        handshakes_by_sender: handshake_by_sender_partition.approximate_len() as u64,
        handshakes_by_receiver: tx_id_to_handshake_partition.approximate_len() as u64,
        payments_by_sender: payment_by_sender_partition.approximate_len() as u64,
        payments_by_receiver: tx_id_to_payment_partition.approximate_len() as u64,
        contextual_messages: contextual_message_partition.len()? as u64,
        blocks_processed: block_compact_header_partition.len()? as u64,
        self_stashes: self_stash_by_owner_partition.approximate_len() as u64,
        latest_block: metadata_partition
            .get_latest_block_cursor_rtx(&tx_keyspace.read_tx())?
            .unwrap_or_default()
            .hash,
        latest_accepting_block: metadata_partition
            .get_latest_accepting_block_cursor()?
            .unwrap_or_default()
            .hash,
        unknown_daa_entries: unknown_accepting_daa_partition.len()? as u64,
        unknown_sender_entries: pending_sender_resolution_partition.len()? as u64,
        unknown_tx_entries: 0,
        resolved_daa: 0,
        resolved_senders: 0,
    });

    let mut block_worker = BlockProcessor::builder()
        .processed_blocks(FifoSet::new(256))
        .intake(block_intake_rx)
        .shutdown(shutdown_block_worker_rx)
        .tx_keyspace(tx_keyspace.clone())
        .metadata_partition(metadata_partition.clone())
        .handshake_by_receiver_partition(handshake_by_receiver_partition.clone())
        .tx_id_to_handshake_partition(tx_id_to_handshake_partition.clone())
        .contextual_message_partition(contextual_message_partition.clone())
        .payment_by_receiver_partition(payment_by_receiver_partition.clone())
        .self_stash_by_owner_partition(self_stash_by_owner_partition.clone())
        .tx_id_to_self_stash_partition(tx_id_to_self_stash_partition.clone())
        .tx_id_to_payment_partition(tx_id_to_payment_partition.clone())
        .tx_id_to_acceptance_partition(tx_id_to_acceptance_partition.clone())
        .skip_tx_partition(skip_tx_partition.clone())
        .skip_tx_by_block_partition(skip_tx_by_block_partition.clone())
        .block_compact_header_partition(block_compact_header_partition.clone())
        .metrics(metrics.clone())
        .processed_txs(FifoSet::new(
            300/*txs per block*/ * 255, /*max mergeset size*/
        ))
        .block_daa_index(block_daa_index_partition.clone())
        .build();

    let block_worker_handle = std::thread::spawn(move || {
        block_worker
            .process()
            .inspect(|_| info!("block worker has stopped"))
            .inspect_err(|err| error!("block worker stopped with error: {err}"))
    });

    let matches = command!() // requires `cargo` feature
        .propagate_version(true)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("block")
                .alias("b")
                .about("process block by id")
                .arg(arg!([BLOCK_ID]).required(true)),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("block", sub_matches)) => {
            let block_id = sub_matches
                .get_one::<String>("BLOCK_ID")
                .map(|s| s.as_str())
                .unwrap();

            let block_hash = Hash::from_str(&block_id).expect("Invalid block id");

            let block = rpc_client.get_block(block_hash, true).await?;

            block_intake_tx.send(BlockOrMany::Block(Arc::new(block)))?;

            sleep(Duration::from_secs(1)).await;
        }
        _ => (),
    };

    shutdown_block_worker_tx.send(())?;

    _ = shutdown_block_worker_tx
        .send(())
        .inspect_err(|err| error!("failed to shutdown block worker: {}", err));

    _ = block_worker_handle
        .join()
        .expect("failed to join block_worker thread");

    Ok(())
}

fn create_rpc_client(indexer_config: &CliConfig) -> anyhow::Result<KaspaRpcClient> {
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
