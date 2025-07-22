use crate::APP_IS_RUNNING;
use crate::acceptance_worker::VirtualChainChangedNotificationAndBlueWork;
use crate::database::block_compact_header::BlockCompactHeaderPartition;
use crate::database::metadata::MetadataPartition;
use crate::historical_syncer::Cursor;
use anyhow::{Context, bail};
use kaspa_consensus_core::BlueWorkType;
use kaspa_rpc_core::VirtualChainChangedNotification;
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_wrpc_client::KaspaRpcClient;
use std::sync::Arc;
use std::time::Duration;
use tokio::task;
use tracing::{debug, error, info, warn};

pub enum Intake {
    VirtualChainChangedNotification(VirtualChainChangedNotification),
    Disconnect,
    Connected,
}

#[derive(Debug, Clone)]
pub enum HistoricalSyncResult {
    TargetReached {
        target: Cursor,
        reached_via: TargetReachedVia,
    },
    Interrupted {
        last_processed_block: Cursor,
    },
}

#[derive(Debug, Clone)]
pub enum TargetReachedVia {
    DirectMatch,
    BlueWorkExceeded { final_block: Cursor },
}

#[derive(Default)]
struct SyncState {
    is_synced: bool,
    is_connected: bool,
    vcc_queue: Vec<VirtualChainChangedNotificationAndBlueWork>,
    current_historical_task: Option<tokio::task::JoinHandle<()>>,
    historical_interrupt_tx: Option<tokio::sync::oneshot::Sender<()>>,
    pending_interrupted_block: Option<Cursor>,
}

impl SyncState {
    fn clear_historical_task(&mut self) {
        self.current_historical_task = None;
        self.historical_interrupt_tx = None;
    }

    fn has_active_task(&self) -> bool {
        self.current_historical_task.is_some()
    }

    fn interrupt_current_task(&mut self) {
        if let Some(interrupt_tx) = self.historical_interrupt_tx.take() {
            let _ = interrupt_tx.send(());
        }
    }
}

pub struct SelectedChainSyncer {
    rpc_client: KaspaRpcClient,
    metadata_partition: MetadataPartition,
    block_compact_header_partition: BlockCompactHeaderPartition,
    intake_rx: tokio::sync::mpsc::Receiver<Intake>,
    historical_sync_done_rx: tokio::sync::mpsc::Receiver<HistoricalSyncResult>,
    historical_sync_done_tx: tokio::sync::mpsc::Sender<HistoricalSyncResult>,
    worker_sender: flume::Sender<VirtualChainChangedNotificationAndBlueWork>,
    shutdown: tokio::sync::oneshot::Receiver<()>,

    queue_pow: u32,
}

impl SelectedChainSyncer {
    pub fn new(
        rpc_client: KaspaRpcClient,
        metadata_partition: MetadataPartition,
        block_compact_header_partition: BlockCompactHeaderPartition,
        intake_rx: tokio::sync::mpsc::Receiver<Intake>,
        historical_sync_done_rx: tokio::sync::mpsc::Receiver<HistoricalSyncResult>,
        historical_sync_done_tx: tokio::sync::mpsc::Sender<HistoricalSyncResult>,
        worker_sender: flume::Sender<VirtualChainChangedNotificationAndBlueWork>,
        shutdown: tokio::sync::oneshot::Receiver<()>,
    ) -> Self {
        Self {
            rpc_client,
            metadata_partition,
            block_compact_header_partition,
            intake_rx,
            historical_sync_done_rx,
            historical_sync_done_tx,
            worker_sender,
            shutdown,
            queue_pow: 10,
        }
    }

    pub async fn process(&mut self) -> anyhow::Result<()> {
        info!("Starting selected chain syncer");

        let mut state = SyncState::default();
        // Don't assume we're connected - wait for Connected notification

        loop {
            tokio::select! {
                biased;
                sync_result = self.historical_sync_done_rx.recv() => {
                    self.handle_sync_result(&mut state, sync_result).await?;
                }
                intake = self.intake_rx.recv() => {
                    let Some(intake) = intake else {
                        warn!("Selected chain syncer intake channel closed");
                        return self.handle_shutdown(&mut state).await
                    };
                    self.handle_intake(&mut state, intake).await?;
                }
                _ = &mut self.shutdown => {
                    return self.handle_shutdown(&mut state).await;
                }
            }
        }
    }

    // async fn initialize_sync(&self, state: &mut SyncState) -> anyhow::Result<()> {
    //     let last_cursor = self.get_last_cursor().await?;
    //     let dag_info = self.rpc_client.get_block_dag_info().await?;
    //
    //     if let Some(cursor) = last_cursor {
    //         info!(
    //             "Found last accepting block cursor: blue_work={}, hash={:?}",
    //             cursor.blue_work, cursor.hash
    //         );
    //
    //         if cursor.hash == dag_info.sink {
    //             info!("Already synced to current sink: {:?}", cursor.hash);
    //             state.is_synced = true;
    //             return Ok(());
    //         }
    //
    //         info!("Need to sync from last cursor to current sink");
    //         self.start_historical_sync(state, cursor, dag_info.sink)
    //             .await?;
    //     } else {
    //         info!("Starting sync from pruning point");
    //         let from = self.get_pruning_point_cursor(&dag_info).await?;
    //         self.start_historical_sync(state, from, dag_info.sink)
    //             .await?;
    //     }
    //
    //     Ok(())
    // }

    async fn try_initialize_sync(&self, state: &mut SyncState) -> anyhow::Result<()> {
        if !state.is_synced && !state.has_active_task() {
            let dag_info = self.rpc_client.get_block_dag_info().await?;

            let from_cursor =
                if let Some(interrupted_cursor) = state.pending_interrupted_block.take() {
                    info!(
                        "Resuming from pending interrupted block: {:?}",
                        interrupted_cursor
                    );
                    Some(interrupted_cursor)
                } else {
                    self.get_last_cursor().await?
                };

            if let Some(cursor) = from_cursor {
                self.sync_or_mark_complete(state, cursor, dag_info.sink)
                    .await?;
            } else {
                info!("No cursor after reconnection, starting from pruning point");
                let from = self.get_pruning_point_cursor(&dag_info).await?;
                self.start_historical_sync(state, from, dag_info.sink)
                    .await?;
            }
        }

        Ok(())
    }

    async fn handle_shutdown(&self, state: &mut SyncState) -> anyhow::Result<()> {
        info!("Selected chain syncer shutting down");

        state.interrupt_current_task();
        if let Some(task) = state.current_historical_task.take() {
            task.abort();
        }

        Ok(())
    }

    async fn handle_sync_result(
        &self,
        state: &mut SyncState,
        sync_result: Option<HistoricalSyncResult>,
    ) -> anyhow::Result<()> {
        match sync_result {
            Some(HistoricalSyncResult::TargetReached {
                target,
                reached_via,
            }) => {
                info!(
                    "Historical sync completed: target={:?}, reached_via={:?}",
                    target, reached_via
                );
                state.is_synced = true;
                state.clear_historical_task();
                self.process_queued_notifications(state).await?;
            }
            Some(HistoricalSyncResult::Interrupted {
                last_processed_block,
            }) => {
                state.is_synced = false;
                state.clear_historical_task();
                self.handle_interruption(state, last_processed_block)
                    .await?;
            }
            None => {
                error!("Historical sync channel closed");
                return Err(anyhow::anyhow!("Historical sync channel closed"));
            }
        }
        Ok(())
    }

    async fn handle_intake(&mut self, state: &mut SyncState, intake: Intake) -> anyhow::Result<()> {
        match intake {
            Intake::Disconnect => {
                self.handle_disconnect(state).await;
            }
            Intake::Connected => {
                self.handle_reconnect(state).await?;
            }
            Intake::VirtualChainChangedNotification(vcc) => {
                self.handle_vcc_notification(state, vcc).await?;
            }
        }
        Ok(())
    }

    async fn handle_disconnect(&self, state: &mut SyncState) {
        warn!("Disconnect received");
        state.is_connected = false;
        state.is_synced = false;
        state.interrupt_current_task();
        state.vcc_queue.clear();
    }

    async fn handle_reconnect(&self, state: &mut SyncState) -> anyhow::Result<()> {
        info!("Connected notification received");
        state.is_connected = true;

        // Keep trying to initialize sync until it succeeds
        loop {
            if !APP_IS_RUNNING.load(std::sync::atomic::Ordering::Relaxed) {
                bail!("App is stopped");
            }
            if let Err(e) = self.try_initialize_sync(state).await {
                error!(
                    "Failed to initialize sync on connection: {}, retrying in 5 seconds",
                    e
                );
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
            break;
        }

        Ok(())
    }

    async fn handle_vcc_notification(
        &mut self,
        state: &mut SyncState,
        vcc: VirtualChainChangedNotification,
    ) -> anyhow::Result<()> {
        if vcc.removed_chain_block_hashes.is_empty() && vcc.added_chain_block_hashes.is_empty() {
            return Ok(());
        }
        let last_block = vcc.added_chain_block_hashes.last().unwrap();
        let blue_work = self.get_block_blue_work(*last_block).await?;

        if state.is_synced {
            debug!("Forwarding VCC notification (synced)");
            self.worker_sender
                .send_async(VirtualChainChangedNotificationAndBlueWork {
                    vcc,
                    last_block_blue_work: blue_work,
                })
                .await?;
        } else {
            debug!("Queuing VCC notification (not synced or disconnected)");
            state
                .vcc_queue
                .push(VirtualChainChangedNotificationAndBlueWork {
                    vcc,
                    last_block_blue_work: blue_work,
                });

            // Warn about queue growth but don't limit it - we need all notifications for sync correctness
            if state.vcc_queue.len() > 2u32.pow(self.queue_pow) as usize {
                warn!(
                    "VCC queue size is large: {} notifications queued",
                    state.vcc_queue.len()
                );
                self.queue_pow += 1;
            }
        }
        Ok(())
    }

    async fn handle_interruption(
        &self,
        state: &mut SyncState,
        last_processed_block: Cursor,
    ) -> anyhow::Result<()> {
        if state.is_connected {
            let dag_info = self.rpc_client.get_block_dag_info().await?;
            self.sync_or_mark_complete(state, last_processed_block, dag_info.sink)
                .await?;
        } else {
            state.pending_interrupted_block = Some(last_processed_block);
        }
        Ok(())
    }

    async fn sync_or_mark_complete(
        &self,
        state: &mut SyncState,
        cursor: Cursor,
        sink: kaspa_rpc_core::RpcHash,
    ) -> anyhow::Result<()> {
        if cursor.hash == sink {
            info!("Already synced after reconnection/interruption");
            state.is_synced = true;
        } else {
            info!("Starting historical sync after reconnection/interruption");
            self.start_historical_sync(state, cursor, sink).await?;
        }
        Ok(())
    }

    async fn start_historical_sync(
        &self,
        state: &mut SyncState,
        from: Cursor,
        sink: kaspa_rpc_core::RpcHash,
    ) -> anyhow::Result<()> {
        let (task, interrupt_tx) = self.spawn_historical_syncer(from, sink).await?;
        state.current_historical_task = Some(task);
        state.historical_interrupt_tx = Some(interrupt_tx);
        Ok(())
    }

    async fn process_queued_notifications(&self, state: &mut SyncState) -> anyhow::Result<()> {
        if !state.vcc_queue.is_empty() {
            info!("Sending {} queued VCC notifications", state.vcc_queue.len());
            for vcc in state.vcc_queue.drain(..) {
                if let Err(e) = self.worker_sender.send_async(vcc).await {
                    error!("Failed to send queued VCC: {}", e);
                    break;
                }
            }
        }
        Ok(())
    }

    async fn get_last_cursor(&self) -> anyhow::Result<Option<Cursor>> {
        let metadata_partition = self.metadata_partition.clone();
        task::spawn_blocking(move || metadata_partition.get_latest_accepting_block_cursor()).await?
    }

    async fn get_pruning_point_cursor(
        &self,
        dag_info: &kaspa_rpc_core::GetBlockDagInfoResponse,
    ) -> anyhow::Result<Cursor> {
        let pp_block = self
            .rpc_client
            .get_block(dag_info.pruning_point_hash, false)
            .await?;
        Ok(Cursor {
            blue_work: pp_block.header.blue_work,
            hash: dag_info.pruning_point_hash,
        })
    }

    async fn spawn_historical_syncer(
        &self,
        from: Cursor,
        sink_hash: kaspa_rpc_core::RpcHash,
    ) -> anyhow::Result<(
        tokio::task::JoinHandle<()>,
        tokio::sync::oneshot::Sender<()>,
    )> {
        let sink_blue_work = self.get_block_blue_work(sink_hash).await?;
        let target = Cursor {
            blue_work: sink_blue_work,
            hash: sink_hash,
        };

        info!("Selected chain syncer: from={:?} to={:?}", from, target);

        let (interrupt_tx, interrupt_rx) = tokio::sync::oneshot::channel();
        let mut syncer = HistoricalSyncer::new(
            self.rpc_client.clone(),
            self.block_compact_header_partition.clone(),
            self.historical_sync_done_tx.clone(),
            interrupt_rx,
            self.worker_sender.clone(),
            from,
            target,
        );

        let task = tokio::spawn(async move {
            if let Err(e) = syncer.sync().await {
                error!("Historical sync failed: {}", e);
            }
        });

        Ok((task, interrupt_tx))
    }

    async fn get_block_blue_work(
        &self,
        block_hash: kaspa_rpc_core::RpcHash,
    ) -> anyhow::Result<kaspa_math::Uint192> {
        let partition = self.block_compact_header_partition.clone();
        let local_blue_work =
            task::spawn_blocking(move || partition.get_blue_work(block_hash)).await??;

        if let Some(blue_work) = local_blue_work {
            Ok(blue_work)
        } else {
            let block = self.rpc_client.get_block(block_hash, false).await?;
            Ok(block.header.blue_work)
        }
    }
}

pub struct HistoricalSyncer {
    rpc_client: KaspaRpcClient,
    block_compact_header_partition: BlockCompactHeaderPartition,
    historical_sync_done_tx: tokio::sync::mpsc::Sender<HistoricalSyncResult>,
    shutdown: tokio::sync::oneshot::Receiver<()>,
    worker_sender: flume::Sender<VirtualChainChangedNotificationAndBlueWork>,
    from: Cursor,
    to: Cursor,
}

impl HistoricalSyncer {
    pub fn new(
        rpc_client: KaspaRpcClient,
        block_compact_header_partition: BlockCompactHeaderPartition,
        historical_sync_done_tx: tokio::sync::mpsc::Sender<HistoricalSyncResult>,
        shutdown: tokio::sync::oneshot::Receiver<()>,
        worker_sender: flume::Sender<VirtualChainChangedNotificationAndBlueWork>,
        from: Cursor,
        to: Cursor,
    ) -> Self {
        Self {
            rpc_client,
            block_compact_header_partition,
            historical_sync_done_tx,
            shutdown,
            worker_sender,
            from,
            to,
        }
    }

    async fn sync(&mut self) -> anyhow::Result<()> {
        let mut current = self.from;

        loop {
            tokio::select! {
                biased;

                _ = &mut self.shutdown => {
                    info!("Historical syncer shutting down");
                    return self.handle_interruption(current).await;
                }

                // todo handle error
                vcc_result = self.rpc_client.get_virtual_chain_from_block(current.hash, true) => {
                    match self.process_vcc_result(vcc_result, &mut current).await? {
                        Some(result) => {
                            self.historical_sync_done_tx.send(result).await.context("Failed to send historical sync result")?;
                            return Ok(());
                        }
                        None => continue,
                    }
                }
            }
        }
    }

    async fn handle_interruption(&self, current: Cursor) -> anyhow::Result<()> {
        info!("Historical sync interrupted at block: {:?}", current);
        let result = HistoricalSyncResult::Interrupted {
            last_processed_block: current,
        };
        let _ = self.historical_sync_done_tx.send(result).await;
        Ok(())
    }

    async fn process_vcc_result(
        &self,
        vcc_result: Result<
            kaspa_rpc_core::GetVirtualChainFromBlockResponse,
            kaspa_rpc_core::RpcError,
        >,
        current: &mut Cursor,
    ) -> anyhow::Result<Option<HistoricalSyncResult>> {
        if !APP_IS_RUNNING.load(std::sync::atomic::Ordering::Relaxed) {
            bail!("App is stopped");
        }
        let vcc_response = match vcc_result {
            Ok(vcc) => vcc,
            Err(_) => {
                tokio::time::sleep(Duration::from_secs(1)).await;
                return Ok(None);
            }
        };

        let vcc = VirtualChainChangedNotification {
            removed_chain_block_hashes: Arc::new(vcc_response.removed_chain_block_hashes),
            added_chain_block_hashes: Arc::new(vcc_response.added_chain_block_hashes),
            accepted_transaction_ids: Arc::new(vcc_response.accepted_transaction_ids),
        };

        let last_block = *vcc.added_chain_block_hashes.last().unwrap();
        let last_blue_work = self.get_block_blue_work(last_block).await?;

        *current = Cursor {
            hash: last_block,
            blue_work: last_blue_work,
        };

        self.worker_sender
            .send_async(VirtualChainChangedNotificationAndBlueWork {
                vcc: vcc.clone(),
                last_block_blue_work: last_blue_work,
            })
            .await?;

        if self.target_reached_exactly(&vcc) {
            return Ok(Some(HistoricalSyncResult::TargetReached {
                target: self.to,
                reached_via: TargetReachedVia::DirectMatch,
            }));
        }

        if last_blue_work > self.to.blue_work {
            return Ok(Some(HistoricalSyncResult::TargetReached {
                target: self.to,
                reached_via: TargetReachedVia::BlueWorkExceeded {
                    final_block: *current,
                },
            }));
        }

        Ok(None)
    }

    fn target_reached_exactly(&self, vcc: &VirtualChainChangedNotification) -> bool {
        vcc.added_chain_block_hashes
            .iter()
            .any(|hash| hash == &self.to.hash)
            || vcc.removed_chain_block_hashes.contains(&self.to.hash)
    }

    async fn get_block_blue_work(
        &self,
        block_hash: kaspa_rpc_core::RpcHash,
    ) -> anyhow::Result<kaspa_math::Uint192> {
        let compact_header_partition = self.block_compact_header_partition.clone();
        let local_blue_work = task::spawn_blocking(move || -> anyhow::Result<_> {
            Ok(compact_header_partition
                .get_compact_header(block_hash)?
                .map(|header| BlueWorkType::from_le_bytes(header.blue_work)))
        })
        .await??;

        match local_blue_work {
            Some(blue_work) => Ok(blue_work),
            None => loop {
                if !APP_IS_RUNNING.load(std::sync::atomic::Ordering::Relaxed) {
                    bail!("App is stopped");
                }
                // TODO: Add proper error handling with HistoricalSyncResult::Failed variant
                // to avoid infinite retry loops and allow graceful failure handling
                match self.rpc_client.get_block(block_hash, false).await {
                    Ok(block) => return Ok(block.header.blue_work),
                    Err(_) => {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            },
        }
    }
}
