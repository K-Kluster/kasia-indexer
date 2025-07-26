use dotenv::dotenv;
use fjall::Config;
use indexer_lib::database::headers::{
    BlockCompactHeaderPartition, BlockGap, BlockGapsPartition, DaaIndexPartition,
};
use indexer_lib::database::messages::{
    ContextualMessageBySenderPartition, HandshakeByReceiverPartition, HandshakeBySenderPartition,
    PaymentByReceiverPartition, PaymentBySenderPartition, TxIdToHandshakePartition,
    TxIdToPaymentPartition,
};
use indexer_lib::database::processing::{
    AcceptingBlockToTxIDPartition, PendingSenderResolutionPartition, SkipTxByBlockPartition,
    SkipTxPartition, TxIDToAcceptancePartition, UnknownAcceptingDaaPartition, UnknownTxPartition,
};
use indexer_lib::fifo_set::FifoSet;
use indexer_lib::historical_syncer::{Cursor, HistoricalDataSyncer};
use indexer_lib::metrics::IndexerMetricsSnapshot;
use indexer_lib::periodic_processor::{run_ticker, Notification, PeriodicProcessor};
use indexer_lib::virtual_chain_processor::VirtualChainProcessor;
use indexer_lib::{
    block_processor::BlockProcessor,
    database::{self},
    metrics::create_shared_metrics_from_snapshot,
    resolver::Resolver,
    selected_chain_syncer::SelectedChainSyncer,
    subscriber::Subscriber,
    APP_IS_RUNNING,
};
use kaspa_wrpc_client::client::{ConnectOptions, ConnectStrategy};
use kaspa_wrpc_client::prelude::{NetworkId, NetworkType, RpcApi};
use kaspa_wrpc_client::{KaspaRpcClient, WrpcEncoding};
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use time::macros::format_description;
use tracing::{error, info};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::filter::Directive;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();

    let db_path = std::env::var("KASIA_INDEXER_DB_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::home_dir().unwrap().join(".kasia-indexer"));
    let log_path = db_path.join("app_logs");
    std::fs::create_dir_all(&log_path)?;
    let _g = init_logs(log_path)?;
    let config = Config::new(&db_path);
    let tx_keyspace = config.open_transactional()?;
    let reorg_lock = Arc::new(Mutex::new(()));
    // Partitions
    let metadata_partition = database::metadata::MetadataPartition::new(&tx_keyspace)?;
    {
        metadata_partition.0.inner().major_compact()?;
    }

    let handshake_by_receiver_partition = HandshakeByReceiverPartition::new(&tx_keyspace)?;
    let tx_id_to_handshake_partition = TxIdToHandshakePartition::new(&tx_keyspace)?;
    let contextual_message_partition = ContextualMessageBySenderPartition::new(&tx_keyspace)?;
    let payment_by_receiver_partition = PaymentByReceiverPartition::new(&tx_keyspace)?;
    let tx_id_to_payment_partition = TxIdToPaymentPartition::new(&tx_keyspace)?;
    let tx_id_to_acceptance_partition = TxIDToAcceptancePartition::new(&tx_keyspace)?;
    let skip_tx_partition = SkipTxPartition::new(&tx_keyspace)?;
    let skip_tx_by_block_partition = SkipTxByBlockPartition::new(&tx_keyspace)?;
    let block_compact_header_partition = BlockCompactHeaderPartition::new(&tx_keyspace)?;
    let acceptance_to_tx_id_partition = AcceptingBlockToTxIDPartition::new(&tx_keyspace)?;
    let unknown_tx_partition = UnknownTxPartition::new(&tx_keyspace)?;
    let unknown_accepting_daa_partition = UnknownAcceptingDaaPartition::new(&tx_keyspace)?;
    let pending_sender_resolution_partition = PendingSenderResolutionPartition::new(&tx_keyspace)?;
    let handshake_by_sender_partition = HandshakeBySenderPartition::new(&tx_keyspace)?;
    let payment_by_sender_partition = PaymentBySenderPartition::new(&tx_keyspace)?;
    let block_gaps_partition = BlockGapsPartition::new(&tx_keyspace)?;
    let block_daa_index_partition = DaaIndexPartition::new(&tx_keyspace)?;

    let metrics = create_shared_metrics_from_snapshot(IndexerMetricsSnapshot {
        handshakes_by_sender: handshake_by_sender_partition.approximate_len() as u64,
        handshakes_by_receiver: tx_id_to_handshake_partition.approximate_len() as u64,
        payments_by_sender: payment_by_sender_partition.approximate_len() as u64,
        payments_by_receiver: tx_id_to_payment_partition.approximate_len() as u64,
        contextual_messages: contextual_message_partition.len()? as u64,
        blocks_processed: block_compact_header_partition.len()? as u64,
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

    let (block_intake_tx, block_intake_rx) = flume::bounded(4096);
    // let (block_intake_tx, block_intake_rx) = flume::unbounded();
    // let (vcc_intake_tx, vcc_intake_rx) = flume::unbounded();

    let (vcc_intake_tx, vcc_intake_rx) = flume::bounded(4096);
    let (shutdown_block_worker_tx, shutdown_block_worker_rx) = flume::bounded(1);
    let (shutdown_acceptance_worker_tx, shutdown_acceptance_worker_rx) = flume::bounded(1);
    let virtual_daa = Arc::new(AtomicU64::new(0));

    let rpc_client = create_rpc_client()?;

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

    let mut acceptance_worker = VirtualChainProcessor::builder()
        .daa_resolution_attempt_count(5)
        .reorg_log(reorg_lock.clone())
        .vcc_rx(vcc_intake_rx)
        .shutdown(shutdown_acceptance_worker_rx)
        .tx_keyspace(tx_keyspace.clone())
        .metadata_partition(metadata_partition.clone())
        .skip_tx_partition(skip_tx_partition.clone())
        .tx_id_to_acceptance_partition(tx_id_to_acceptance_partition.clone())
        .acceptance_to_tx_id_partition(acceptance_to_tx_id_partition.clone())
        .unknown_tx_partition(unknown_tx_partition.clone())
        .unknown_accepting_daa_partition(unknown_accepting_daa_partition.clone())
        .block_compact_header_partition(block_compact_header_partition.clone())
        .pending_sender_resolution_partition(pending_sender_resolution_partition.clone())
        .build();

    let (resolver_block_request_tx, resolver_block_request_rx) =
        workflow_core::channel::bounded(16384);
    // workflow_core::channel::unbounded();
    let (resolver_sender_request_tx, resolver_sender_request_rx) =
        workflow_core::channel::bounded(16384);
    // workflow_core::channel::unbounded();
    let (resolver_response_tx, resolver_response_rx) = workflow_core::channel::bounded(32768);
    // let (resolver_response_tx, resolver_response_rx) = workflow_core::channel::unbounded();
    let (shutdown_resolver_tx, shutdown_resolver_rx) = tokio::sync::oneshot::channel();

    let requests_in_progress = Arc::new(AtomicU64::new(0));
    let mut resolver = Resolver::new(
        shutdown_resolver_rx,
        resolver_block_request_rx,
        resolver_sender_request_rx,
        resolver_response_tx.clone(),
        rpc_client.clone(),
        requests_in_progress.clone(),
    );

    let (scan_worker_job_done_tx, scan_worker_job_done_rx) = workflow_core::channel::bounded(1);

    let mut scan_worker = PeriodicProcessor::builder()
        .tick_and_resolution_rx(resolver_response_rx)
        .resolver_request_block_tx(resolver_block_request_tx)
        .resolver_request_sender_tx(resolver_sender_request_tx)
        .job_done_tx(scan_worker_job_done_tx)
        .reorg_lock(reorg_lock)
        .tx_keyspace(tx_keyspace.clone())
        .tx_id_to_acceptance_partition(tx_id_to_acceptance_partition.clone())
        .unknown_tx_partition(unknown_tx_partition.clone())
        .skip_tx_partition(skip_tx_partition.clone())
        .skip_tx_by_block_partition(skip_tx_by_block_partition.clone())
        .unknown_accepting_daa_partition(unknown_accepting_daa_partition.clone())
        .block_compact_header_partition(block_compact_header_partition.clone())
        .daa_resolution_attempt_count(5)
        .pending_sender_resolution_partition(pending_sender_resolution_partition.clone())
        .handshake_by_receiver_partition(handshake_by_receiver_partition.clone())
        .handshake_by_sender_partition(handshake_by_sender_partition.clone())
        .contextual_message_by_sender_partition(contextual_message_partition.clone())
        .payment_by_receiver_partition(payment_by_receiver_partition.clone())
        .payment_by_sender_partition(payment_by_sender_partition.clone())
        .tx_id_to_payment_partition(tx_id_to_payment_partition.clone())
        .tx_id_to_handshake_partition(tx_id_to_handshake_partition.clone())
        .metrics(metrics)
        .metrics_snapshot_interval(Duration::from_secs(10))
        .metadata_partition(metadata_partition.clone())
        .resolver_requests_in_progress(requests_in_progress)
        .block_daa_index(block_daa_index_partition)
        .virtual_daa(virtual_daa.clone())
        .build();

    let (selected_chain_intake_tx, selected_chain_intake_rx) = tokio::sync::mpsc::channel(4096);
    let (historical_sync_done_tx, historical_sync_done_rx) = tokio::sync::mpsc::channel(1);

    let (shutdown_selected_chain_syncer_tx, shutdown_selected_chain_syncer_rx) =
        tokio::sync::oneshot::channel();
    let mut selected_chain_syncer = SelectedChainSyncer::new(
        rpc_client.clone(),
        metadata_partition.clone(),
        block_compact_header_partition.clone(),
        selected_chain_intake_rx,
        historical_sync_done_rx,
        historical_sync_done_tx,
        vcc_intake_tx,
        shutdown_selected_chain_syncer_rx,
    );

    let (shutdown_subscriber_tx, shutdown_subscriber_rx) = tokio::sync::oneshot::channel();
    let mut subscriber = Subscriber::new(
        rpc_client.clone(),
        block_intake_tx.clone(),
        shutdown_subscriber_rx,
        block_gaps_partition.clone(),
        selected_chain_intake_tx,
        metadata_partition.get_latest_block_cursor_rtx(&tx_keyspace.read_tx())?,
        virtual_daa.clone(),
    );

    let (shutdown_ticker_tx, shutdown_ticker_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(run_ticker(
        shutdown_ticker_rx,
        scan_worker_job_done_rx,
        resolver_response_tx.clone(),
        Duration::from_secs(10),
    ));

    // Spawn workers
    let block_worker_handle = std::thread::spawn(move || {
        block_worker
            .process()
            .inspect(|_| info!("block worker has stopped"))
            .inspect_err(|err| error!("block worker stopped with error: {err}"))
    });
    let acceptance_worker_handle = std::thread::spawn(move || {
        acceptance_worker
            .process()
            .inspect(|_| info!("acceptance worker has stopped"))
            .inspect_err(|err| error!("acceptance worker stopped with error: {err}"))
    });
    let scan_worker_handle = std::thread::spawn(move || {
        scan_worker
            .worker()
            .inspect_err(|err| {
                error!("scan worker stopped with error: {err}");
            })
            .inspect(|_| info!("scan worker has stopped"))
    });

    let resolver_handle = tokio::spawn(async move { resolver.process().await });
    let selected_chain_syncer_handle =
        tokio::spawn(async move { selected_chain_syncer.process().await });
    let subscriber_handle = tokio::spawn(async move { subscriber.task().await });

    let (gap_shutdowns, gap_tasks) = {
        let tmp_rpc_client = create_rpc_client()?;
        tmp_rpc_client
            .connect(Some(ConnectOptions {
                block_async_connect: true,
                ..Default::default()
            }))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to node: {}", e))?;
        if metadata_partition
            .get_latest_block_cursor_rtx(&tx_keyspace.read_tx())?
            .is_none()
        {
            let data = tmp_rpc_client.get_block_dag_info().await?;
            let sink = data.sink;
            let pp = data.pruning_point_hash;
            let sink_blue_work = tmp_rpc_client
                .get_block(sink, false)
                .await?
                .header
                .blue_work;
            let pp_blue_work = tmp_rpc_client.get_block(pp, false).await?.header.blue_work;
            block_gaps_partition.add_gap(BlockGap {
                from_blue_work: pp_blue_work,
                from_block_hash: pp,
                to_blue_work: Some(sink_blue_work),
                to_block_hash: Some(sink),
            })?;
        }
        let shutdown_and_task = block_gaps_partition
            .get_all_gaps(&tx_keyspace.read_tx())
            .map(|gap| -> anyhow::Result<_> {
                let BlockGap {
                    from_blue_work,
                    from_block_hash,
                    to_blue_work,
                    to_block_hash,
                } = gap?;
                let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
                let mut syncer = HistoricalDataSyncer::new(
                    tmp_rpc_client.clone(),
                    Cursor {
                        blue_work: from_blue_work,
                        hash: from_block_hash,
                    },
                    Cursor {
                        blue_work: to_blue_work.unwrap(),
                        hash: to_block_hash.unwrap(),
                    },
                    block_intake_tx.clone(),
                    shutdown_rx,
                    block_gaps_partition.clone(),
                );
                Ok((
                    shutdown_tx,
                    tokio::spawn(async move { syncer.sync().await }),
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (shutdowns, tasks): (Vec<_>, Vec<_>) = shutdown_and_task.into_iter().unzip();
        (shutdowns, tasks)
    };

    let options = ConnectOptions {
        block_async_connect: false,
        connect_timeout: Some(Duration::from_millis(10_000)),
        strategy: ConnectStrategy::Retry,
        ..Default::default()
    };

    tokio::time::sleep(Duration::from_secs(5)).await; // let time to spawn everything
    info!("Connecting to Kaspa node...");
    rpc_client
        .connect(Some(options))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to node: {}", e))?;

    // Handle shutdown
    tokio::signal::ctrl_c().await?;
    info!("Termination signal received. Shutting down...");
    APP_IS_RUNNING.store(false, std::sync::atomic::Ordering::Relaxed);

    _ = shutdown_subscriber_tx
        .send(())
        .inspect_err(|_err| error!("failed to shutdown subscriber"));

    gap_shutdowns.into_iter().for_each(|shutdown| {
        let _ = shutdown
            .send(())
            .inspect_err(|_| error!("Failed to send shutdown to gaps tasks"));
    });

    _ = shutdown_block_worker_tx
        .send(())
        .inspect_err(|err| error!("failed to shutdown block worker: {}", err));
    info!("try shutdown acceptance worker");
    _ = shutdown_acceptance_worker_tx
        .send(())
        .inspect_err(|err| error!("failed to shutdown acceptance worker: {}", err));
    info!("try shutdown selected_chain_syncer");
    _ = shutdown_selected_chain_syncer_tx
        .send(())
        .inspect_err(|_err| error!("failed to shutdown chain"));
    info!("try shutdown ticker");
    _ = shutdown_ticker_tx
        .send(())
        .inspect_err(|_err| error!("failed to shutdown ticker"));
    info!("try shutdown scan worker");
    _ = resolver_response_tx
        .send(Notification::Shutdown)
        .await
        .inspect_err(|_err| error!("failed to shutdown scan worker"));
    info!("try shutdown resolver");
    _ = shutdown_resolver_tx
        .send(())
        .inspect_err(|_err| error!("failed to shutdown resolver"));

    // Await on all tasks to complete
    info!("waiting for resolver finish");
    _ = resolver_handle
        .await?
        .inspect(|_| info!("resolver has stopped")); // todo logs
    info!("waiting for syncer finish");
    _ = selected_chain_syncer_handle
        .await
        .inspect(|_| info!("selected chain syncer has stopped"))?; // todo logs
    info!("waiting for subscriber finish");
    _ = subscriber_handle
        .await
        .inspect(|_| info!("subscriber has stopped"))?; // todo logs
    let has_gaps = !gap_tasks.is_empty();
    info!("waiting for gap tasks finish");
    for task in gap_tasks {
        let _ = task.await;
    }
    if has_gaps {
        info!("all gap task have stopped");
    }

    info!("waiting for acceptance worker finish");
    _ = acceptance_worker_handle
        .join()
        .expect("failed to join acceptance worker"); // todo logs
    info!("waiting for scan worker finish");
    _ = scan_worker_handle
        .join()
        .expect("failed to join scan_worker thread"); // todo logs
    info!("waiting for block worker finish");
    _ = block_worker_handle
        .join()
        .expect("failed to join block_worker thread"); // todo logs

    info!("All tasks shut down.");

    Ok(())
}

fn create_rpc_client() -> anyhow::Result<KaspaRpcClient> {
    let encoding = WrpcEncoding::Borsh;

    let url = std::env::var("KASPA_NODE_WBORSH_URL").ok();
    let resolver = if url.is_some() {
        None
    } else {
        Some(kaspa_wrpc_client::Resolver::default())
    };

    let network_type = NetworkType::Mainnet;
    let selected_network = Some(NetworkId::new(network_type));

    let subscription_context = None;

    info!("Creating RPC client for network: {:?}", network_type);

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

pub fn init_logs<P: AsRef<Path>>(logs_dir: P) -> anyhow::Result<(WorkerGuard, WorkerGuard)> {
    let file_appender = rolling_file::BasicRollingFileAppender::new(
        logs_dir.as_ref().join("kasia-indexer.mainnet.log"),
        rolling_file::RollingConditionBasic::new()
            .max_size(1024 * 1024 * 8)
            .daily(),
        14,
    )?;

    let (non_blocking_appender, guard_file) = tracing_appender::non_blocking(file_appender);
    let file_subscriber = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(non_blocking_appender)
        .with_filter(
            EnvFilter::builder()
                .with_env_var("RUST_LOG_FILE")
                .with_default_directive(Directive::from_str("info")?)
                .from_env_lossy(),
        );
    let (non_blocking_appender, guard_stdout) = tracing_appender::non_blocking(std::io::stdout());
    let stdout_subscriber = tracing_subscriber::fmt::layer()
        .with_timer(tracing_subscriber::fmt::time::LocalTime::new(
            format_description!("[year]-[month]-[day] [hour]:[minute]:[second]"),
        ))
        .with_writer(non_blocking_appender)
        .with_filter(
            EnvFilter::builder()
                .with_default_directive(Directive::from_str("info")?)
                .from_env_lossy(),
        );

    tracing_subscriber::registry()
        .with(file_subscriber)
        .with(stdout_subscriber)
        .init();

    Ok((guard_file, guard_stdout))
}
