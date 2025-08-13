use crate::block_processor::BlockNotification;
use crate::virtual_chain_processor::RealTimeVccNotification;
use anyhow::Context;
use futures_util::future::FutureExt;
use kaspa_consensus_core::tx::TransactionId;
use kaspa_rpc_core::api::ctl::RpcState;
use kaspa_rpc_core::api::ops::RpcApiOps;
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_rpc_core::notify::connection::{ChannelConnection, ChannelType};
use kaspa_rpc_core::{
    BlockAddedNotification, GetBlocksRequest, GetBlocksResponse, GetUtxoReturnAddressRequest,
    GetUtxoReturnAddressResponse, GetVirtualChainFromBlockRequest,
    GetVirtualChainFromBlockResponse, Notification, RpcBlock, RpcHash,
};
use kaspa_wrpc_client::KaspaRpcClient;
use kaspa_wrpc_client::client::ConnectOptions;
use kaspa_wrpc_client::prelude::{
    BlockAddedScope, ListenerId, Scope, VirtualChainChangedScope, VirtualDaaScoreChangedScope,
};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use thiserror::Error;
use tracing::{error, info, warn};
use workflow_core::channel::Channel;
use workflow_serializer::prelude::Serializable;

pub struct DataSource {
    /// RPC client for communicating with Kaspa node
    rpc_client: KaspaRpcClient,
    connected: bool,
    /// Channel to send processed blocks to handler
    block_sender: flume::Sender<BlockNotification>,
    block_sender_closed: bool,
    vcc_sender: flume::Sender<RealTimeVccNotification>,
    vcc_sender_closed: bool,
    shutting_down: bool,
    // vcc_sender
    /// Shutdown signal receiver
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    // channel supplied to the notification subsystem to receive the node
    // notifications we subscribe to
    notification_channel: Channel<Notification>,
    // listener id used to manage notification scopes we can have multiple IDs for
    // different scopes paired with multiple notification channels
    listener_id: Option<ListenerId>,
    virtual_daa: Arc<AtomicU64>,
    command_rx: workflow_core::channel::Channel<Command>,
    requests_queue: VecDeque<Request>,
}

impl DataSource {
    pub async fn task(&mut self) -> anyhow::Result<()> {
        let rpc_ctl_channel = self.rpc_client.rpc_ctl().multiplexer().channel();
        loop {
            tokio::select! {
                biased;
                shutdown_result = (&mut self.shutdown_rx).fuse() => {
                    shutdown_result
                        .inspect(|_| info!("Shutdown signal received, stopping subscriber task"))
                        .inspect_err(|e| warn!("Shutdown receiver error: {}", e))?;
                    self.handle_shutdown().await?;
                    // no need to return, we are waiting for command to mark other processor closed
                }
                msg = rpc_ctl_channel.receiver.recv().fuse() => {
                    // this will never occur if the RpcClient is owned and properly managed. This can
                    // only occur if RpcClient is deleted while this task is still running.
                    match msg.inspect_err(|e| error!("RPC CTL channel error: {}", e))? {
                        RpcState::Connected => {
                            if let Err(err) = self.handle_connect().await {
                                error!("Error in connect handler: {err}");
                            }
                        },
                        RpcState::Disconnected => {
                            if let Err(err) = self.handle_disconnect().await {
                                error!("Error in disconnect handler: {err}");
                            }
                        },
                    }
                }
                notification = self.notification_channel.receiver.recv().fuse() => {
                    _ =
                        self
                            .handle_notification(
                                notification.inspect_err(|e| error!("RPC notification channel error: {}", e))?,
                            )
                            .await
                            .inspect_err(|err| error!("Error while handling notification: {err}"));
                },
                command = self.command_rx.recv().fuse() => {
                    let con = self
                        .handle_command(command.inspect_err(|e| error!("RPC command channel error: {}", e))?)
                        .await?;
                    if !con {
                        return Ok(())
                    }
                }
            }
        }
    }

    async fn handle_connect_impl(&mut self) -> anyhow::Result<()> {
        info!("Connected to {:?}", self.rpc_client.url());
        let info = self.rpc_client.get_block_dag_info().await?;

        let sink_blue_work = self
            .rpc_client
            .get_block(info.sink, false)
            .await?
            .header
            .blue_work;
        self.connected = true;
        let pair = futures_util::future::join(
            async {
                self.block_sender
                    .send_async(BlockNotification::Connected {
                        sink: info.sink.as_bytes(),
                        pp: info.pruning_point_hash.as_bytes(),
                    })
                    .await
                    .context("block handler connect send failed")
            },
            async {
                self.vcc_sender
                    .send_async(RealTimeVccNotification::Connected {
                        sink: info.sink.as_bytes(),
                        sink_blue_work,
                        pp: info.pruning_point_hash.as_bytes(),
                    })
                    .await
                    .context("vcc handler connect send failed")
            },
        )
        .await;
        pair.0.and(pair.1)?;
        // register for notifications
        self.register_notification_listeners().await?;
        for req in self.requests_queue.drain(..) {
            self.command_rx.send(Command::Request(req)).await?;
        }
        Ok(())
    }

    async fn handle_connect(&mut self) -> anyhow::Result<()> {
        if self.shutting_down {
            _ = self
                .rpc_client
                .disconnect()
                .await
                .inspect_err(|err| error!("Error disconnecting: {err}"));
            return Ok(());
        }
        match self.handle_connect_impl().await {
            Err(err) => {
                self.connected = false;
                error!("Error while connecting to node: {err}");

                // force disconnect the client if we have failed to negotiate the connection to
                // the node. self.rpc_client().trigger_abort()?;
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
        // IMPORTANT: notification scopes are managed by the node for the lifetime of the
        // RPC connection, as such they are "lost" if we disconnect. For that reason we
        // must re-register all notification scopes when we connect.
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
        if self.shutting_down {
            _ = self
                .rpc_client
                .disconnect()
                .await
                .inspect_err(|err| error!("Error disconnecting: {err}"));
            return Ok(());
        }
        info!("Disconnected from {:?}", self.rpc_client.url());
        self.connected = false;
        let pair = futures_util::future::join(
            async {
                self.block_sender
                    .send_async(BlockNotification::Disconnected)
                    .await
                    .context("block handler disconnect send failed")
            },
            async {
                self.vcc_sender
                    .send_async(RealTimeVccNotification::Disconnected)
                    .await
                    .context("vcc handler disconnect send failed")
            },
        )
        .await;
        pair.0.and(pair.1)?;

        // Unregister notifications
        self.unregister_notification_listener().await?;
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
                if self.block_sender_closed {
                    return Ok(());
                }
                self.block_sender
                    .send_async(BlockNotification::Notification(block))
                    .await
                    .context("block handler send `Notification` failed")?;
            }
            Notification::VirtualChainChanged(vcc) => {
                if self.vcc_sender_closed {
                    return Ok(());
                }
                self.vcc_sender
                    .send_async(RealTimeVccNotification::Notification(vcc))
                    .await
                    .context("vcc send failed")?;
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

    async fn handle_shutdown(&mut self) -> anyhow::Result<()> {
        self.shutting_down = true;
        _ = self
            .rpc_client
            .disconnect()
            .await
            .inspect_err(|err| error!("Error disconnecting: {err}"));

        let pair = futures_util::future::join(
            async {
                self.block_sender
                    .send_async(BlockNotification::Shutdown)
                    .await
                    .context("block handler `Shutdown` send failed")
            },
            async {
                self.vcc_sender
                    .send_async(RealTimeVccNotification::Shutdown)
                    .await
                    .context("vcc handler `Shutdown` send failed")
            },
        )
        .await;
        pair.0.and(pair.1)?;
        Ok(())
    }

    // todo rewrite rpc client so it doesnt block notifications when waits for response
    async fn handle_command(&mut self, command: Command) -> anyhow::Result<Continue> {
        match command {
            Command::MarkBlockSenderClosed => {
                if !self.shutting_down {
                    warn!("MarkBlockSenderClosed called but not shutting down");
                }
                self.block_sender_closed = true;
                Ok(!self.vcc_sender_closed)
            }
            Command::MarkVccSenderClosed => {
                if !self.shutting_down {
                    warn!("MarkVccSenderClosed called but not shutting down");
                }
                self.vcc_sender_closed = true;
                Ok(!self.block_sender_closed)
            }
            Command::Request(request) => {
                match request {
                    Request::RequestBlocks {
                        response_channel, ..
                    } if self.shutting_down => {
                        _ = response_channel
                            .send(Err(RequestError::ShuttingDown))
                            .inspect_err(|_err| {
                                error!("Error sending response to get blocks request")
                            });
                    }
                    Request::RequestSender { .. } if self.shutting_down => {
                        // do nothing
                        // _ = response_channel
                        //     .send(Err(RequestError::ShuttingDown))
                        //     .inspect_err(|_err| error!("Error sending response to sender request"));
                    }
                    Request::RequestVirtualChain {
                        response_channel, ..
                    } if self.shutting_down => {
                        _ = response_channel
                            .send(Err(RequestError::ShuttingDown))
                            .inspect_err(|_err| {
                                error!("Error sending response to virtual chain request")
                            });
                    }
                    request if !self.connected => {
                        self.requests_queue.push_back(request);
                    }
                    Request::RequestBlocks {
                        blocks_from,
                        response_channel,
                    } => {
                        match self
                            .rpc_client
                            .rpc_client()
                            .call(
                                RpcApiOps::GetBlocks,
                                Serializable(GetBlocksRequest::new(
                                    Some(RpcHash::from_bytes(blocks_from)),
                                    true,
                                    true,
                                )),
                            )
                            .await
                        {
                            Ok(res) => {
                                let res: Serializable<GetBlocksResponse> = res;
                                let blocks = res.0.blocks;
                                _ = response_channel
                                    .send(Ok(blocks))
                                    .inspect_err(|_err| error!("sending blocks result err"));
                            }
                            Err(workflow_rpc::client::error::Error::Disconnect) => {
                                self.requests_queue.push_front(Request::RequestBlocks {
                                    blocks_from,
                                    response_channel,
                                });
                            }
                            Err(e) => {
                                _ = response_channel
                                    .send(Err(e.into()))
                                    .inspect_err(|_err| error!("sending blocks result err"));
                            }
                        }
                    }
                    Request::RequestSender { daa_score, tx_id } => {
                        match self
                            .rpc_client
                            .rpc_client()
                            .call(
                                RpcApiOps::GetUtxoReturnAddress,
                                Serializable(GetUtxoReturnAddressRequest::new(
                                    TransactionId::from_bytes(tx_id),
                                    daa_score,
                                )),
                            )
                            .await
                        {
                            Ok(res) => {
                                let res: Serializable<GetUtxoReturnAddressResponse> = res;
                                let address = res.0.return_address;
                                _ = self
                                    .vcc_sender
                                    .send_async(RealTimeVccNotification::SenderResolution {
                                        sender: address,
                                        tx_id,
                                        daa: daa_score,
                                    })
                                    .await
                                    .inspect_err(|_err| error!("sending sender result err"));
                            }
                            Err(workflow_rpc::client::error::Error::Disconnect) => {
                                self.requests_queue
                                    .push_front(Request::RequestSender { daa_score, tx_id });
                            }
                            Err(e) => {
                                warn!("Error getting sender address: {e}");
                            }
                        }
                    }
                    Request::RequestVirtualChain {
                        vc_from,
                        response_channel,
                    } => {
                        match self
                            .rpc_client
                            .rpc_client()
                            .call(
                                RpcApiOps::GetVirtualChainFromBlock,
                                Serializable(GetVirtualChainFromBlockRequest::new(
                                    RpcHash::from_bytes(vc_from),
                                    true,
                                )),
                            )
                            .await
                        {
                            Ok(res) => {
                                let res: Serializable<GetVirtualChainFromBlockResponse> = res;
                                _ = response_channel
                                    .send(Ok(res.0))
                                    .inspect_err(|_err| error!("sending vcc result err"));
                            }
                            Err(workflow_rpc::client::error::Error::Disconnect) => {
                                self.requests_queue
                                    .push_front(Request::RequestVirtualChain {
                                        vc_from,
                                        response_channel,
                                    });
                            }
                            Err(e) => {
                                _ = response_channel
                                    .send(Err(e.into()))
                                    .inspect_err(|_err| error!("sending vcc result err"));
                            }
                        }
                    }
                }
                Ok(true)
            }
        }
    }
}

type Continue = bool;

#[derive(Debug)]
pub enum Command {
    Request(Request),
    MarkBlockSenderClosed,
    MarkVccSenderClosed,
}

#[derive(Debug)]
pub enum Request {
    RequestBlocks {
        blocks_from: [u8; 32],
        response_channel: tokio::sync::oneshot::Sender<Result<Vec<RpcBlock>, RequestError>>,
    },
    RequestVirtualChain {
        vc_from: [u8; 32],
        response_channel:
            tokio::sync::oneshot::Sender<Result<GetVirtualChainFromBlockResponse, RequestError>>,
    },
    RequestSender {
        daa_score: u64,
        tx_id: [u8; 32],
    },
}

#[derive(Debug, Error)]
pub enum RequestError {
    #[error("Shutting down")]
    ShuttingDown,
    #[error("RPC error: {0}")]
    RpcError(#[from] workflow_rpc::client::error::Error),
}
