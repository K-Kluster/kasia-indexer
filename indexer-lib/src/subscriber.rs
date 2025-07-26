use crate::BlockOrMany;
use crate::RK_PRUNING_DEPTH;
use crate::database::headers::{BlockGap, BlockGapsPartition};
use crate::historical_syncer::{Cursor, HistoricalDataSyncer};
use crate::selected_chain_syncer::Intake;
use anyhow::Context;
use futures_util::future::FutureExt;
use kaspa_rpc_core::api::ctl::RpcState;
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_rpc_core::notify::connection::{ChannelConnection, ChannelType};
use kaspa_rpc_core::{BlockAddedNotification, Notification};
use kaspa_wrpc_client::KaspaRpcClient;
use kaspa_wrpc_client::client::ConnectOptions;
use kaspa_wrpc_client::prelude::{
    BlockAddedScope, ListenerId, Scope, VirtualChainChangedScope, VirtualDaaScoreChangedScope,
};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use tokio::task;
use tracing::{error, info, warn};
use workflow_core::channel::Channel;

pub struct Subscriber {
    /// RPC client for communicating with Kaspa node
    rpc_client: KaspaRpcClient,
    /// Channel to send processed blocks to handler
    block_handler: flume::Sender<BlockOrMany>,
    /// Shutdown signal receiver
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    // channel supplied to the notification subsystem
    // to receive the node notifications we subscribe to
    notification_channel: Channel<Notification>,
    // listener id used to manage notification scopes
    // we can have multiple IDs for different scopes
    // paired with multiple notification channels
    listener_id: Option<ListenerId>,

    last_block_cursor: Option<Cursor>,

    historical_data_syncer_shutdown_tx: Vec<tokio::sync::oneshot::Sender<()>>,

    block_gaps_partition: BlockGapsPartition,

    selected_chain_syncer: tokio::sync::mpsc::Sender<Intake>,

    virtual_daa: Arc<AtomicU64>,

    had_first_connect: bool,
}

impl Subscriber {
    pub fn new(
        rpc_client: KaspaRpcClient,
        block_handler: flume::Sender<BlockOrMany>,
        shutdown_rx: tokio::sync::oneshot::Receiver<()>,
        block_gaps_partition: BlockGapsPartition,
        selected_chain_syncer: tokio::sync::mpsc::Sender<Intake>,
        last_block_cursor: Option<Cursor>,
        virtual_daa: Arc<AtomicU64>,
    ) -> Self {
        let notification_channel = Channel::bounded(256);

        Self {
            rpc_client,
            block_handler,
            shutdown_rx,
            notification_channel,
            listener_id: None,
            last_block_cursor,
            historical_data_syncer_shutdown_tx: Vec::new(),
            block_gaps_partition,
            selected_chain_syncer,
            virtual_daa,
            had_first_connect: false,
        }
    }

    pub async fn task(&mut self) -> anyhow::Result<()> {
        let rpc_ctl_channel = self.rpc_client.rpc_ctl().multiplexer().channel();
        loop {
            tokio::select! {
            biased;
                shutdown_result = &mut self.shutdown_rx => {
                    shutdown_result
                    .inspect(|_| info!("Shutdown signal received, stopping subscriber task"))
                    .inspect_err(|e|  warn!("Shutdown receiver error: {}", e))?;
                    for shutdown in std::mem::take(&mut self.historical_data_syncer_shutdown_tx) {
                        _ = shutdown.send(()).inspect_err(|_err| error!("Error sending shutdown signal"));
                        // todo wait for their responses
                    }
                    return Ok(())
                }
                msg = rpc_ctl_channel.receiver.recv().fuse() => {
                    match msg {
                        Ok(msg) => {

                            // handle RPC channel connection and disconnection events
                            match msg {
                                RpcState::Connected => {
                                    if let Err(err) = self.handle_connect().await {
                                        error!("Error in connect handler: {err}");
                                    }
                                },
                                RpcState::Disconnected => {
                                    if let Err(err) = self.handle_disconnect().await {
                                        error!("Error in disconnect handler: {err}");
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            // this will never occur if the RpcClient is owned and
                            // properly managed. This can only occur if RpcClient is
                            // deleted while this task is still running.
                            error!("RPC CTL channel error: {err}");
                            panic!("Unexpected: RPC CTL channel closed, halting...");
                        }
                    }
                }
                notification = self.notification_channel.receiver.recv().fuse() => {
                    match notification {
                        Ok(notification) => {
                            if let Err(err) = self.handle_notification(notification).await {
                                error!("Error while handling notification: {err}");
                            }
                        }
                        Err(err) => {
                            panic!("RPC notification channel error: {err}");
                        }
                    }
                },
            }
        }
    }

    async fn handle_connect_impl(&mut self) -> anyhow::Result<()> {
        info!("Connected to {:?}", self.rpc_client.url());
        self.selected_chain_syncer.send(Intake::Connected).await?;
        // now that we have successfully connected we
        // can register for notifications
        self.register_notification_listeners().await?;
        let info = self.rpc_client.get_block_dag_info().await?;
        let sink_header = self.rpc_client.get_block(info.sink, false).await?.header;
        if !self.had_first_connect {
            let no_block_processed_before = self.last_block_cursor.is_none();
            let gaps_partition = self.block_gaps_partition.clone();
            if no_block_processed_before {
                let pp_header = self
                    .rpc_client
                    .get_block(info.pruning_point_hash, false)
                    .await?
                    .header;
                info!("No block processed before, adding gap from pruning point to sink");
                self.last_block_cursor = Some(Cursor::new(
                    pp_header.daa_score,
                    pp_header.blue_work,
                    info.pruning_point_hash,
                ));
            }
            let gaps = task::spawn_blocking(move || -> anyhow::Result<_> {
                gaps_partition
                    .get_all_gaps_since_daa(info.virtual_daa_score - RK_PRUNING_DEPTH * 2)
                    .collect::<Result<Vec<_>, _>>()
            })
            .await??;
            if !gaps.is_empty() {
                info!(
                    "Found {} gaps at startup, spawning historical syncers for gaps: {:?}",
                    gaps.len(),
                    gaps
                );
            }
            gaps.into_iter().try_for_each(
                |BlockGap {
                     from_daa_score,
                     from_blue_work,
                     from_block_hash,
                     to_blue_work,
                     to_block_hash,
                     to_daa_score,
                 }|
                 -> anyhow::Result<()> {
                    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
                    self.historical_data_syncer_shutdown_tx.push(shutdown_tx);
                    tokio::spawn({
                        let rpc_client = self.rpc_client.clone();
                        let block_handler = self.block_handler.clone();
                        let gaps_partition = self.block_gaps_partition.clone();
                        async move {
                            _ = HistoricalDataSyncer::new(
                                rpc_client,
                                Cursor::new(from_daa_score, from_blue_work, from_block_hash),
                                Cursor::new(to_daa_score, to_blue_work, to_block_hash),
                                block_handler,
                                shutdown_rx,
                                gaps_partition,
                            )
                            .sync()
                            .await
                            .inspect_err(|err| error!("Error in historical syncer: {err}"));
                        }
                    });
                    Ok(())
                },
            )?;

            self.had_first_connect = true;
        }
        if let Some(last) = self.last_block_cursor.take()
            && last.daa_score + RK_PRUNING_DEPTH * 2 > info.virtual_daa_score
        {
            let gaps_partition = self.block_gaps_partition.clone();
            task::spawn_blocking(move || {
                gaps_partition.add_gap(BlockGap {
                    from_daa_score: last.daa_score,
                    from_blue_work: last.blue_work,
                    from_block_hash: last.hash,
                    to_blue_work: sink_header.blue_work,
                    to_block_hash: info.sink,
                    to_daa_score: sink_header.daa_score,
                })
            })
            .await??;
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
            self.historical_data_syncer_shutdown_tx.push(shutdown_tx);
            tokio::spawn({
                let rpc_client = self.rpc_client.clone();
                let block_handler = self.block_handler.clone();
                let gaps_partition = self.block_gaps_partition.clone();
                async move {
                    _ = HistoricalDataSyncer::new(
                        rpc_client,
                        last,
                        Cursor::new(sink_header.daa_score, sink_header.blue_work, info.sink),
                        block_handler,
                        shutdown_rx,
                        gaps_partition,
                    )
                    .sync()
                    .await
                    .inspect_err(|err| error!("Error in historical syncer: {err}"));
                }
            });
        }

        Ok(())
    }

    async fn handle_connect(&mut self) -> anyhow::Result<()> {
        match self.handle_connect_impl().await {
            Err(err) => {
                error!("Error while connecting to node: {err}");
                // force disconnect the client if we have failed
                // to negotiate the connection to the node.
                // self.rpc_client().trigger_abort()?;
                self.rpc_client.disconnect().await?;
                tokio::time::sleep(Duration::from_secs(3)).await;
                let options = ConnectOptions {
                    block_async_connect: false,
                    ..Default::default()
                };
                self.rpc_client.connect(Some(options)).await?;
                Err(err)
            }
            Ok(_) => Ok(()),
        }
    }

    async fn register_notification_listeners(&mut self) -> anyhow::Result<()> {
        // IMPORTANT: notification scopes are managed by the node
        // for the lifetime of the RPC connection, as such they
        // are "lost" if we disconnect. For that reason we must
        // re-register all notification scopes when we connect.

        let listener_id = self
            .rpc_client
            .register_new_listener(ChannelConnection::new(
                "kasia-subscrtiber",
                self.notification_channel.sender.clone(),
                ChannelType::Persistent,
            ));
        self.listener_id = Some(listener_id);
        self.rpc_client
            .start_notify(listener_id, Scope::BlockAdded(BlockAddedScope {}))
            .await?;
        self.rpc_client
            .start_notify(
                listener_id,
                Scope::VirtualDaaScoreChanged(VirtualDaaScoreChangedScope {}),
            )
            .await?;
        self.rpc_client
            .start_notify(
                listener_id,
                Scope::VirtualChainChanged(VirtualChainChangedScope {
                    include_accepted_transaction_ids: true,
                }),
            )
            .await?;

        Ok(())
    }

    async fn handle_disconnect(&mut self) -> anyhow::Result<()> {
        info!("Disconnected from {:?}", self.rpc_client.url());
        // Unregister notifications
        self.unregister_notification_listener().await?;
        self.selected_chain_syncer.send(Intake::Disconnect).await?;

        Ok(())
    }

    async fn unregister_notification_listener(&mut self) -> anyhow::Result<()> {
        if let Some(listener_id) = self.listener_id.take() {
            self.rpc_client.unregister_listener(listener_id).await?;
        }
        Ok(())
    }

    async fn handle_notification(&mut self, notification: Notification) -> anyhow::Result<()> {
        match notification {
            Notification::BlockAdded(BlockAddedNotification { block }) => {
                let cursor = block.header.as_ref().into();
                self.block_handler
                    .send_async(BlockOrMany::Block(block))
                    .await
                    .context("block handler send failed")?;
                self.last_block_cursor = Some(cursor);
            }
            Notification::VirtualChainChanged(vcc) => {
                self.selected_chain_syncer
                    .send(Intake::VirtualChainChangedNotification(vcc))
                    .await
                    .context("block handler send failed")?;
            }
            Notification::VirtualDaaScoreChanged(daa) => {
                self.virtual_daa
                    .store(daa.virtual_daa_score, std::sync::atomic::Ordering::Relaxed);
            }
            _ => {
                warn!("unknown notification: {:?}", notification)
            }
        }
        Ok(())
    }
}
