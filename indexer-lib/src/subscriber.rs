use crate::BlockOrMany;
use crate::database::block_gaps::{BlockGap, BlockGapsPartition};
use crate::historical_syncer::{Cursor, HistoricalDataSyncer};
use crate::selected_chain_syncer::Intake;
use futures_util::future::FutureExt;
use kaspa_rpc_core::api::ctl::RpcState;
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_rpc_core::notify::connection::{ChannelConnection, ChannelType};
use kaspa_rpc_core::{BlockAddedNotification, Notification};
use kaspa_wrpc_client::KaspaRpcClient;
use kaspa_wrpc_client::client::ConnectOptions;
use kaspa_wrpc_client::prelude::{BlockAddedScope, ListenerId, Scope, VirtualChainChangedScope};
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
}

impl Subscriber {
    pub fn new(
        rpc_client: KaspaRpcClient,
        block_handler: flume::Sender<BlockOrMany>,
        shutdown_rx: tokio::sync::oneshot::Receiver<()>,
        block_gaps_partition: BlockGapsPartition,
        selected_chain_syncer: tokio::sync::mpsc::Sender<Intake>,
    ) -> Self {
        let notification_channel = Channel::bounded(256);

        Self {
            rpc_client,
            block_handler,
            shutdown_rx,
            notification_channel,
            listener_id: None,
            last_block_cursor: None,
            historical_data_syncer_shutdown_tx: Vec::new(),
            block_gaps_partition,
            selected_chain_syncer,
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
        let sink = self.rpc_client.get_sink().await?.sink;
        let blue_work = self
            .rpc_client
            .get_block(sink, false)
            .await?
            .header
            .blue_work;

        if let Some(last) = self.last_block_cursor.take() {
            let gaps_partition = self.block_gaps_partition.clone();
            task::spawn_blocking(move || {
                gaps_partition.add_gap(BlockGap {
                    from_blue_work: last.blue_work,
                    from_block_hash: last.hash,
                    to_blue_work: Some(blue_work),
                    to_block_hash: Some(sink),
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
                    HistoricalDataSyncer::new(
                        rpc_client,
                        last,
                        Cursor::new(blue_work, sink),
                        block_handler,
                        shutdown_rx,
                        gaps_partition,
                    );
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
                    .await?;
                self.last_block_cursor = Some(cursor);
            }
            Notification::VirtualChainChanged(vcc) => {
                self.selected_chain_syncer
                    .send(Intake::VirtualChainChangedNotification(vcc))
                    .await?;
            }
            _ => {
                warn!("unknown notification: {:?}", notification)
            }
        }
        Ok(())
    }
}
