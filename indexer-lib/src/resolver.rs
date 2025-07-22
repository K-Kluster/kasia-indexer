use crate::APP_IS_RUNNING;
use anyhow::bail;
use kaspa_rpc_core::api::ops::RpcApiOps;
use kaspa_rpc_core::prelude::*;
use kaspa_rpc_core::{
    GetBlockRequest, GetBlockResponse, RpcAddress, RpcBlock, RpcHash, RpcHeader, RpcTransactionId,
};
use kaspa_wrpc_client::KaspaRpcClient;
use tracing::{debug, error, info};
use workflow_core::channel::{Receiver, Sender};
use workflow_serializer::serializer::Serializable;

#[derive(Debug, Clone, Copy)]
pub struct SenderByTxIdAndDaa {
    pub tx_id: RpcTransactionId,
    pub daa_score: u64,
}

#[derive(Debug, Clone)]
pub enum ResolverResponse {
    Block(Result<Box<RpcHeader>, RpcHash>),
    Sender(Result<(RpcAddress, SenderByTxIdAndDaa), SenderByTxIdAndDaa>),
}

pub struct Resolver {
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    block_request_rx: Receiver<RpcHash>,
    sender_request_rx: Receiver<SenderByTxIdAndDaa>,
    kaspa_rpc_client: KaspaRpcClient,
    response_tx: Sender<crate::scan_worker::Notification>,
}

impl Resolver {
    pub fn new(
        shutdown_rx: tokio::sync::oneshot::Receiver<()>,
        block_request_rx: Receiver<RpcHash>,
        sender_request_rx: Receiver<SenderByTxIdAndDaa>,
        response_tx: Sender<crate::scan_worker::Notification>,
        kaspa_rpc_client: KaspaRpcClient,
    ) -> Self {
        Self {
            shutdown_rx,
            block_request_rx,
            sender_request_rx,
            kaspa_rpc_client,
            response_tx,
        }
    }

    pub async fn process(&mut self) -> anyhow::Result<()> {
        info!("Resolver started");
        loop {
            match self.select_input().await? {
                Input::BlockRequest(hash) => {
                    debug!(%hash, "Received block resolution request");
                    match get_block_with_retries(&self.kaspa_rpc_client, hash).await {
                        Ok(block) => {
                            self.response_tx
                                .send(crate::scan_worker::Notification::ResolverResponse(
                                    ResolverResponse::Block(Ok(Box::new(block.header))),
                                ))
                                .await?;
                        }
                        Err(err) => {
                            error!(%err, "Failed to get block for hash: {hash}");
                            self.response_tx
                                .send(crate::scan_worker::Notification::ResolverResponse(
                                    ResolverResponse::Block(Err(hash)),
                                ))
                                .await?;
                        }
                    }
                }
                Input::SenderRequest { tx_id, daa_score } => {
                    debug!(%tx_id, %daa_score, "Received sender resolution request");
                    match get_utxo_return_address_with_retries(
                        &self.kaspa_rpc_client,
                        tx_id,
                        daa_score,
                    )
                    .await
                    {
                        Ok(sender) => {
                            self.response_tx
                                .send(crate::scan_worker::Notification::ResolverResponse(
                                    ResolverResponse::Sender(Ok((
                                        sender,
                                        SenderByTxIdAndDaa { tx_id, daa_score },
                                    ))),
                                ))
                                .await?;
                        }
                        Err(err) => {
                            error!(%err, "Failed to get sender for tx id: {tx_id} and daa score: {daa_score}");
                            self.response_tx
                                .send(crate::scan_worker::Notification::ResolverResponse(
                                    ResolverResponse::Sender(Err(SenderByTxIdAndDaa {
                                        tx_id,
                                        daa_score,
                                    })),
                                ))
                                .await?;
                        }
                    }
                }
                Input::Shutdown => {
                    info!("Resolver received shutdown signal, stopping.");
                    while !self.block_request_rx.is_empty() {
                        _ = self.block_request_rx.try_recv();
                    }
                    while !self.sender_request_rx.is_empty() {
                        _ = self.sender_request_rx.try_recv();
                    }
                    info!("drainin all requests is done");

                    return Ok(());
                }
            }
        }
    }

    async fn select_input(&mut self) -> anyhow::Result<Input> {
        tokio::select! {
            biased;
            _ = &mut self.shutdown_rx => {
                Ok(Input::Shutdown)
            }
            req = self.block_request_rx.recv() => {
                Ok(Input::BlockRequest(req?))
            }

            req = self.sender_request_rx.recv() => {
                let SenderByTxIdAndDaa{tx_id,daa_score} = req?;
                Ok(Input::SenderRequest{tx_id, daa_score})
            }
        }
    }
}

enum Input {
    Shutdown,
    BlockRequest(RpcHash),
    SenderRequest {
        tx_id: RpcTransactionId,
        daa_score: u64,
    },
}

async fn get_block_with_retries(
    client: &KaspaRpcClient,
    rpc_hash: RpcHash,
) -> anyhow::Result<RpcBlock> {
    loop {
        if !APP_IS_RUNNING.load(std::sync::atomic::Ordering::Relaxed) {
            bail!("App is stopped");
        }
        if !client.is_connected() {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            continue;
        }
        match client
            .rpc_client()
            .call(
                RpcApiOps::GetBlock,
                Serializable(GetBlockRequest::new(rpc_hash, false)),
            )
            .await
        {
            Ok(Serializable(GetBlockResponse { block })) => return Ok(block),
            Err(
                workflow_rpc::client::error::Error::Disconnect
                | workflow_rpc::client::error::Error::Timeout,
            ) => {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

async fn get_utxo_return_address_with_retries(
    client: &KaspaRpcClient,
    txid: RpcHash,
    accepting_block_daa_score: u64,
) -> anyhow::Result<RpcAddress> {
    loop {
        if !APP_IS_RUNNING.load(std::sync::atomic::Ordering::Relaxed) {
            bail!("App is stopped");
        }
        if !client.is_connected() {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            continue;
        }
        match client
            .rpc_client()
            .call(
                RpcApiOps::GetUtxoReturnAddress,
                Serializable(GetUtxoReturnAddressRequest::new(
                    txid,
                    accepting_block_daa_score,
                )),
            )
            .await
        {
            Ok(Serializable(GetUtxoReturnAddressResponse { return_address })) => {
                return Ok(return_address);
            }
            Err(
                workflow_rpc::client::error::Error::Disconnect
                | workflow_rpc::client::error::Error::Timeout,
            ) => {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
            Err(e) => return Err(e.into()),
        }
    }
}
