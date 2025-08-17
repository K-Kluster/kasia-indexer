use crate::config::get_indexer_config;
use crate::context::{IndexerContext, get_indexer_context};
use dotenv::dotenv;
use fjall::Config;
use futures_util::TryFutureExt;
use indexer_actors::block_processor::BlockProcessor;
use indexer_actors::data_source::DataSource;
use indexer_actors::metrics::{IndexerMetricsSnapshot, create_shared_metrics_from_snapshot};
use indexer_actors::periodic_processor::PeriodicProcessor;
use indexer_actors::ticker::Ticker;
use indexer_actors::util::ToHex64;
use indexer_actors::virtual_chain_processor::{CompactHeader, VirtualProcessor};
use indexer_db::headers::block_compact_headers::BlockCompactHeaderPartition;
use indexer_db::headers::block_gaps::{BlockGap, BlockGapsPartition};
use indexer_db::headers::daa_index::DaaIndexPartition;
use indexer_db::messages::contextual_message::ContextualMessageBySenderPartition;
use indexer_db::messages::handshake::{
    HandshakeByReceiverPartition, HandshakeBySenderPartition, TxIdToHandshakePartition,
};
use indexer_db::messages::payment::{
    PaymentByReceiverPartition, PaymentBySenderPartition, TxIdToPaymentPartition,
};
use indexer_db::metadata::MetadataPartition;
use indexer_db::processing::accepting_block_to_txs::AcceptingBlockToTxIDPartition;
use indexer_db::processing::pending_senders::PendingSenderResolutionPartition;
use indexer_db::processing::tx_id_to_acceptance::TxIDToAcceptancePartition;
use kaspa_rpc_core::RpcBlueWorkType;
use kaspa_wrpc_client::client::{ConnectOptions, ConnectStrategy};
use kaspa_wrpc_client::prelude::NetworkType;
use rayon::prelude::*;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use time::macros::format_description;
use tracing::level_filters::LevelFilter;
use tracing::{error, info};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};
use workflow_core::channel::Channel;

mod api;
mod config;
mod context;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ignore faillures as .env might not be present at runtime, and this use-case is tolerated
    dotenv()
        .inspect_err(|err| println!("[WARN] reading .env files is failed with err {err}"))
        .ok();

    let config = get_indexer_config()?;
    let context = get_indexer_context(&config)?;

    let _g = init_logs(&context)?;

    let config = Config::new(context.clone().db_path).max_write_buffer_size(512 * 1024 * 1024);
    let tx_keyspace = config.open_transactional()?;
    let virtual_daa = Arc::new(AtomicU64::new(0));
    // Partitions
    let metadata_partition = MetadataPartition::new(&tx_keyspace)?;
    {
        metadata_partition.0.inner().major_compact()?;
    }

    let handshake_by_receiver_partition = HandshakeByReceiverPartition::new(&tx_keyspace)?;
    let tx_id_to_handshake_partition = TxIdToHandshakePartition::new(&tx_keyspace)?;
    let contextual_message_partition = ContextualMessageBySenderPartition::new(&tx_keyspace)?;
    let payment_by_receiver_partition = PaymentByReceiverPartition::new(&tx_keyspace)?;
    let tx_id_to_payment_partition = TxIdToPaymentPartition::new(&tx_keyspace)?;
    let tx_id_to_acceptance_partition = TxIDToAcceptancePartition::new(&tx_keyspace)?;
    let block_compact_header_partition = BlockCompactHeaderPartition::new(&tx_keyspace)?;
    let acceptance_to_tx_id_partition = AcceptingBlockToTxIDPartition::new(&tx_keyspace)?;
    let pending_sender_resolution_partition = PendingSenderResolutionPartition::new(&tx_keyspace)?;
    let handshake_by_sender_partition = HandshakeBySenderPartition::new(&tx_keyspace)?;
    let payment_by_sender_partition = PaymentBySenderPartition::new(&tx_keyspace)?;
    let block_gaps_partition = BlockGapsPartition::new(&tx_keyspace)?;
    let block_daa_index_partition = DaaIndexPartition::new(&tx_keyspace)?;

    let gaps = block_gaps_partition
        .get_all_gaps()
        .collect::<Result<Vec<_>, _>>()?;
    print_gaps(&gaps);

    let metrics = create_shared_metrics_from_snapshot(IndexerMetricsSnapshot {
        handshakes_by_sender: handshake_by_sender_partition.approximate_len() as u64,
        uniq_handshakes_by_receiver: tx_id_to_handshake_partition.approximate_len() as u64,
        payments_by_sender: payment_by_sender_partition.approximate_len() as u64,
        uniq_payments_by_receiver: tx_id_to_payment_partition.approximate_len() as u64,
        contextual_messages: contextual_message_partition.len()? as u64,
        blocks_processed: block_compact_header_partition.len()? as u64,
        latest_block: metadata_partition
            .get_latest_block_cursor_rtx(&tx_keyspace.read_tx())?
            .unwrap_or_default()
            .to_hex_64(),
        latest_accepting_block: metadata_partition
            .get_latest_accepting_block_cursor()?
            .unwrap_or_default()
            .block_hash
            .to_hex_64(),
        unknown_sender_entries: pending_sender_resolution_partition.len()? as u64,
        resolved_senders: 0,
        pruned_blocks: 0,
    });
    let (block_intake_tx, block_intake_rx) = flume::bounded(4096);
    let (vcc_intake_tx, vcc_intake_rx) = flume::bounded(4096);
    let (gap_result_tx, gap_result_rx) = flume::bounded(1024);
    let (processed_block_tx, processed_block_rx) = flume::bounded(4096);
    let command_channel = {
        let (command_tx, command_rx) = workflow_core::channel::bounded(1024);
        Channel {
            sender: command_tx,
            receiver: command_rx,
        }
    };
    let (syncer_tx, syncer_rx) = flume::bounded(4);
    let (shutdown_data_source_tx, shutdown_data_source_rx) = tokio::sync::mpsc::channel(2);
    let (periodic_intake_tx, periodic_intake_rx) = workflow_core::channel::bounded(1);
    let (periodic_resp_tx, periodic_resp_rx) = workflow_core::channel::bounded(1);
    let (shutdown_ticker_tx, shutdown_ticker_rx) = tokio::sync::mpsc::channel(2);

    let mut block_processor = BlockProcessor::builder()
        .notification_rx(block_intake_rx.clone())
        .gap_result_rx(gap_result_rx)
        .gap_result_tx(gap_result_tx)
        .processed_block_tx(processed_block_tx)
        .command_tx(command_channel.sender.clone())
        .tx_keyspace(tx_keyspace.clone())
        .blocks_gap_partition(block_gaps_partition.clone())
        .runtime_handle(tokio::runtime::Handle::current())
        .metadata_partition(metadata_partition.clone())
        .block_compact_header_partition(block_compact_header_partition.clone())
        .daa_index_partition(block_daa_index_partition.clone())
        .handshake_by_receiver_partition(handshake_by_receiver_partition.clone())
        .handshake_by_sender_partition(handshake_by_sender_partition.clone())
        .tx_id_to_handshake_partition(tx_id_to_handshake_partition.clone())
        .contextual_message_by_sender_partition(contextual_message_partition.clone())
        .payment_by_receiver_partition(payment_by_receiver_partition.clone())
        .payment_by_sender_partition(payment_by_sender_partition.clone())
        .tx_id_to_payment_partition(tx_id_to_payment_partition.clone())
        .tx_id_to_acceptance_partition(tx_id_to_acceptance_partition.clone())
        .shared_metrics(metrics.clone())
        .build();
    let mut virtual_processor = VirtualProcessor::builder()
        .synced_capacity(500_000)
        .unsynced_capacity(3_000_000)
        .processed_block_tx(processed_block_rx)
        .realtime_vcc_tx(vcc_intake_rx)
        .syncer_rx(syncer_rx)
        .syncer_tx(syncer_tx)
        .command_tx(command_channel.sender.clone())
        .tx_keyspace(tx_keyspace.clone())
        .metadata_partition(metadata_partition.clone())
        .tx_id_to_acceptance_partition(tx_id_to_acceptance_partition.clone())
        .accepting_block_to_tx_id_partition(acceptance_to_tx_id_partition.clone())
        .pending_sender_resolution_partition(pending_sender_resolution_partition.clone())
        .handshake_by_receiver_partition(handshake_by_receiver_partition.clone())
        .handshake_by_sender_partition(handshake_by_sender_partition.clone())
        .contextual_message_by_sender_partition(contextual_message_partition.clone())
        .payment_by_receiver_partition(payment_by_receiver_partition.clone())
        .payment_by_sender_partition(payment_by_sender_partition.clone())
        .runtime(tokio::runtime::Handle::current())
        .build();

    let mut ticker = Ticker::new(
        Duration::from_secs(10),
        shutdown_ticker_rx,
        periodic_intake_tx,
        periodic_resp_rx,
    );

    let periodic_processor = PeriodicProcessor::builder()
        .pruning_depth(3_000_000)
        .job_trigger_rx(periodic_intake_rx)
        .resp_tx(periodic_resp_tx)
        .metrics(metrics.clone())
        .virtual_daa(virtual_daa.clone())
        .tx_keyspace(tx_keyspace.clone())
        .daa_index_partition(block_daa_index_partition.clone())
        .block_compact_header_partition(block_compact_header_partition.clone())
        .accepting_block_to_tx_id_partition(acceptance_to_tx_id_partition.clone())
        .metadata_partition(metadata_partition.clone())
        .tx_id_to_handshake_partition(tx_id_to_handshake_partition.clone())
        .tx_id_to_payment_partition(tx_id_to_payment_partition.clone())
        .payment_by_sender_partition(payment_by_sender_partition.clone())
        .handshake_by_sender_partition(handshake_by_sender_partition.clone())
        .build();

    let mut data_source = DataSource::new(
        context.rpc_client.clone(),
        block_intake_tx,
        vcc_intake_tx,
        shutdown_data_source_rx,
        virtual_daa.clone(),
        command_channel,
    );
    let limit = if gaps.is_empty() {
        1_000_000
    } else {
        3_000_000
    };
    let rtx = tx_keyspace.read_tx();
    let processed_daa_blocks = block_daa_index_partition
        .iter_lt(&rtx, u64::MAX)
        .rev()
        .take(limit)
        .map(|r| r.map(|(_daa, hash)| hash))
        .collect::<Result<Vec<_>, _>>()?;
    let processed_blocks = processed_daa_blocks
        .par_iter()
        .map(|hash| {
            block_compact_header_partition
                .get_compact_header(hash)
                .transpose()
                .unwrap()
                .map(|db_compact_header| CompactHeader {
                    block_hash: *hash,
                    blue_work: RpcBlueWorkType::from_le_bytes(db_compact_header.blue_work),
                    daa_score: db_compact_header.daa_score.into(),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let block_processor_handle = std::thread::spawn(move || block_processor.process());
    let virtual_processor_handle =
        std::thread::spawn(move || virtual_processor.process(processed_blocks));
    let periodic_processor_handle = std::thread::spawn(move || periodic_processor.process());
    let data_source_handle = tokio::spawn(async move { data_source.task().await });
    let ticker_handle = tokio::spawn(async move { ticker.process().await });

    let api_service = api::v1::Api::new(
        tx_keyspace.clone(),
        handshake_by_sender_partition,
        handshake_by_receiver_partition,
        contextual_message_partition,
        payment_by_sender_partition,
        payment_by_receiver_partition,
        tx_id_to_acceptance_partition,
        tx_id_to_handshake_partition,
        tx_id_to_payment_partition,
        metrics.clone(),
        context.clone(),
    );
    let (api_shutdown_tx, api_shutdown_rx) = tokio::sync::mpsc::channel(2);
    let api_handle = tokio::spawn(api_service.serve("0.0.0.0:8080", api_shutdown_rx));

    let options = ConnectOptions {
        block_async_connect: false,
        connect_timeout: Some(Duration::from_millis(10_000)),
        strategy: ConnectStrategy::Retry,
        ..Default::default()
    };

    tokio::time::sleep(Duration::from_secs(5)).await; // let time to spawn everything
    info!("Connecting to Kaspa node...");
    context
        .rpc_client
        .connect(Some(options))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to node: {}", e))?;
    let shutdown = Shutdown {
        api: api_shutdown_tx,
        data_source: shutdown_data_source_tx,
        ticker: shutdown_ticker_tx,
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Termination signal received. Shutting down...");
            shutdown.shutdown(None).await
        }
        r = api_handle => {
           _ = r.inspect(|_| info!("api has stopped"))
                .inspect_err(|err| error!("api has stopped with error: {}", err));
            shutdown.shutdown(Some(Exclude::Api)).await;
        },
        r = data_source_handle => {
           _ = r.inspect(|_| info!("data source has stopped")).inspect_err(|err| error!("data_source finished with err: {}", err));
             shutdown.shutdown(Some(Exclude::DataSource)).await;
        },
        r = ticker_handle => {
            _ = r.inspect(|_| info!("ticker has stopped")).inspect_err(|err| error!("ticker processing error: {}", err));
             shutdown.shutdown(Some(Exclude::Ticker)).await;
        },
    }

    info!("waiting for virtual processor finish");
    _ = virtual_processor_handle
        .join()
        .expect("failed to join virtual_processor thread")
        .inspect_err(|err| error!("virtual_processor stopped error: {}", err));

    info!("waiting for block processor finish");
    _ = block_processor_handle
        .join()
        .expect("failed to join block_processor thread")
        .inspect_err(|err| error!("block_processor stopped error: {}", err));

    info!("waiting for periodic processor finish");
    _ = periodic_processor_handle
        .join()
        .expect("failed to join periodic_processor thread")
        .inspect_err(|err| error!("periodic_processor stopped error: {}", err));

    info!("All tasks shut down.");

    Ok(())
}

pub fn init_logs(context: &IndexerContext) -> anyhow::Result<(WorkerGuard, WorkerGuard)> {
    let file_appender = rolling_file::BasicRollingFileAppender::new(
        context.log_path.join(format!(
            "kasia-indexer.{}.log",
            NetworkType::to_string(&context.network_type)
        )),
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
                .with_default_directive(LevelFilter::INFO.into())
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
                .with_env_var("RUST_LOG_FILE")
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        );

    tracing_subscriber::registry()
        .with(file_subscriber)
        .with(stdout_subscriber)
        .init();

    Ok((guard_file, guard_stdout))
}

fn print_gaps(gaps: &[BlockGap]) {
    let gaps = gaps.iter().map(format_gap).collect::<Vec<_>>();
    let gaps = gaps.join(", ");
    info!("Block Gaps found: {gaps}");
}

fn format_gap(bg: &BlockGap) -> String {
    format!(
        "BlockGap(from: {}, to: {})",
        bg.from.to_hex_64(),
        bg.to.to_hex_64()
    )
}

#[derive(Clone)]
struct Shutdown {
    api: tokio::sync::mpsc::Sender<()>,
    data_source: tokio::sync::mpsc::Sender<()>,
    ticker: tokio::sync::mpsc::Sender<()>,
}

enum Exclude {
    Api,
    DataSource,
    Ticker,
}
impl Shutdown {
    async fn shutdown(&self, exclude: Option<Exclude>) {
        match exclude {
            None => {
                _ = tokio::join!(
                    self.api
                        .send(())
                        .inspect_err(|_err| error!("failed to shutdown api")),
                    self.data_source
                        .send(())
                        .inspect_err(|_err| error!("failed to shutdown data source")),
                    self.ticker
                        .send(())
                        .inspect_err(|_err| error!("failed to shutdown ticker"))
                );
            }
            Some(Exclude::Api) => {
                _ = tokio::join!(
                    self.data_source
                        .send(())
                        .inspect_err(|_err| error!("failed to shutdown data source")),
                    self.ticker
                        .send(())
                        .inspect_err(|_err| error!("failed to shutdown ticker"))
                );
            }
            Some(Exclude::DataSource) => {
                _ = tokio::join!(
                    self.api
                        .send(())
                        .inspect_err(|_err| error!("failed to shutdown api")),
                    self.ticker
                        .send(())
                        .inspect_err(|_err| error!("failed to shutdown ticker"))
                );
            }
            Some(Exclude::Ticker) => {
                _ = tokio::join!(
                    self.api
                        .send(())
                        .inspect_err(|_err| error!("failed to shutdown api")),
                    self.data_source
                        .send(())
                        .inspect_err(|_err| error!("failed to shutdown data source")),
                );
            }
        }
    }
}
