use fjall::Config;
use indexer_lib::database::block_gaps::BlockGap;
use indexer_lib::fifo_set::FifoSet;
use indexer_lib::historical_syncer::{Cursor, HistoricalDataSyncer};
use indexer_lib::scan_worker::Notification;
use indexer_lib::{
    acceptance_worker::{self},
    block_worker::BlockWorker,
    database::{self},
    resolver::Resolver,
    scan_worker::{self, run_ticker},
    selected_chain_syncer::SelectedChainSyncer,
    subscriber::Subscriber,
    APP_IS_RUNNING,
};
use kaspa_wrpc_client::client::{ConnectOptions, ConnectStrategy};
use kaspa_wrpc_client::prelude::{NetworkId, NetworkType, RpcApi};
use kaspa_wrpc_client::{KaspaRpcClient, WrpcEncoding};
use parking_lot::Mutex;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    let db_path = std::env::var("KASIA_INDEXER_DB_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::home_dir().unwrap().join(".kasia-indexer"));
    let config = Config::new(&db_path);
    let tx_keyspace = config.open_transactional()?;
    let reorg_lock = Arc::new(Mutex::new(()));

    // Partitions
    let metadata_partition = database::metadata::MetadataPartition::new(&tx_keyspace)?;
    let handshake_by_receiver_partition =
        database::handshake::HandshakeByReceiverPartition::new(&tx_keyspace)?;
    let tx_id_to_handshake_partition =
        database::handshake::TxIdToHandshakePartition::new(&tx_keyspace)?;
    let contextual_message_partition =
        database::contextual_message_by_sender::ContextualMessageBySenderPartition::new(
            &tx_keyspace,
        )?;
    let payment_by_receiver_partition =
        database::payment::PaymentByReceiverPartition::new(&tx_keyspace)?;
    let tx_id_to_payment_partition = database::payment::TxIdToPaymentPartition::new(&tx_keyspace)?;
    let tx_id_to_acceptance_partition =
        database::acceptance::TxIDToAcceptancePartition::new(&tx_keyspace)?;
    let skip_tx_partition = database::skip_tx::SkipTxPartition::new(&tx_keyspace)?;
    let block_compact_header_partition =
        database::block_compact_header::BlockCompactHeaderPartition::new(&tx_keyspace)?;
    let acceptance_to_tx_id_partition =
        database::acceptance::AcceptingBlockToTxIDPartition::new(&tx_keyspace)?;
    let unknown_tx_partition = database::unknown_tx::UnknownTxPartition::new(&tx_keyspace)?;
    let unknown_accepting_daa_partition =
        database::unknown_accepting_daa::UnknownAcceptingDaaPartition::new(&tx_keyspace)?;
    let pending_sender_resolution_partition =
        database::pending_sender_resolution::PendingSenderResolutionPartition::new(&tx_keyspace)?;
    let handshake_by_sender_partition =
        database::handshake::HandshakeBySenderPartition::new(&tx_keyspace)?;
    let payment_by_sender_partition =
        database::payment::PaymentBySenderPartition::new(&tx_keyspace)?;
    let block_gaps_partition = database::block_gaps::BlockGapsPartition::new(&tx_keyspace)?;

    let (block_intake_tx, block_intake_rx) = flume::bounded(255);
    let (vcc_intake_tx, vcc_intake_rx) = flume::bounded(255);
    let (shutdown_block_worker_tx, shutdown_block_worker_rx) = flume::bounded(1);
    let (shutdown_acceptance_worker_tx, shutdown_acceptance_worker_rx) = flume::bounded(1);

    let rpc_client = create_rpc_client()?;

    let mut block_worker = BlockWorker::builder()
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
        .block_compact_header_partition(block_compact_header_partition.clone())
        .build();

    let mut acceptance_worker = acceptance_worker::AcceptanceWorker::builder()
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
        workflow_core::channel::bounded(255);
    let (resolver_sender_request_tx, resolver_sender_request_rx) =
        workflow_core::channel::bounded(255);
    let (resolver_response_tx, resolver_response_rx) = workflow_core::channel::bounded(255);
    let (shutdown_resolver_tx, shutdown_resolver_rx) = tokio::sync::oneshot::channel();

    let mut resolver = Resolver::new(
        shutdown_resolver_rx,
        resolver_block_request_rx,
        resolver_sender_request_rx,
        resolver_response_tx.clone(),
        rpc_client.clone(),
    );

    let (scan_worker_job_done_tx, scan_worker_job_done_rx) = workflow_core::channel::unbounded();

    let scan_worker = scan_worker::ScanWorker::builder()
        .tick_and_resolution_rx(resolver_response_rx)
        .resolver_request_block_tx(resolver_block_request_tx)
        .resolver_request_sender_tx(resolver_sender_request_tx)
        .job_done_tx(scan_worker_job_done_tx)
        .reorg_lock(reorg_lock)
        .tx_keyspace(tx_keyspace.clone())
        .tx_id_to_acceptance_partition(tx_id_to_acceptance_partition.clone())
        .unknown_tx_partition(unknown_tx_partition.clone())
        .skip_tx_partition(skip_tx_partition.clone())
        .unknown_accepting_daa_partition(unknown_accepting_daa_partition.clone())
        .block_compact_header_partition(block_compact_header_partition.clone())
        .daa_resolution_attempt_count(5)
        .pending_sender_resolution_partition(pending_sender_resolution_partition.clone())
        .handshake_by_receiver_partition(handshake_by_receiver_partition.clone())
        .handshake_by_sender_partition(handshake_by_sender_partition.clone())
        .contextual_message_by_sender_partition(contextual_message_partition.clone())
        .payment_by_receiver_partition(payment_by_receiver_partition.clone())
        .payment_by_sender_partition(payment_by_sender_partition.clone())
        .build();

    let (selected_chain_intake_tx, selected_chain_intake_rx) = tokio::sync::mpsc::channel(128);
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
        metadata_partition.get_latest_block_cursor(&tx_keyspace.read_tx())?,
    );

    let (shutdown_ticker_tx, shutdown_ticker_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(run_ticker(
        shutdown_ticker_rx,
        scan_worker_job_done_rx,
        resolver_response_tx.clone(),
        Duration::from_secs(5),
    ));

    // Spawn workers
    let block_worker_handle = std::thread::spawn(move || block_worker.process());
    let acceptance_worker_handle = std::thread::spawn(move || acceptance_worker.process());
    let scan_worker_handle = std::thread::spawn(move || scan_worker.worker());

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
            .get_latest_block_cursor(&tx_keyspace.read_tx())?
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
        .inspect_err(|_err| error!("failed to shutdown resolver response"));
    info!("try shutdown resolver");
    _ = shutdown_resolver_tx
        .send(())
        .inspect_err(|_err| error!("failed to shutdown resolver resolver"));

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
        .expect("failed to join acceptance worker")
        .inspect(|_| info!("acceptance worker has stopped")); // todo logs
    info!("waiting for scan worker finish");
    _ = scan_worker_handle
        .join()
        .expect("failed to join scan_worker thread")
        .inspect(|_| info!("scan worker has stopped")); // todo logs
    info!("waiting for block worker finish");
    _ = block_worker_handle
        .join()
        .expect("failed to join block_worker thread")
        .inspect(|_| info!("block worker has stopped")); // todo logs

    info!("All tasks shut down.");

    Ok(())
}

fn create_rpc_client() -> anyhow::Result<KaspaRpcClient> {
    let encoding = WrpcEncoding::Borsh;

    let resolver = Some(kaspa_wrpc_client::Resolver::default());
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
    Ok(client)
}
