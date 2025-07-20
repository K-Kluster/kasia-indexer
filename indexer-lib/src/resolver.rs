use flume::{Receiver, Sender};
use kaspa_rpc_core::api::rpc::RpcApi;
use kaspa_rpc_core::{RpcAddress, RpcHash, RpcHeader, RpcTransactionId};
use kaspa_wrpc_client::KaspaRpcClient;
use tracing::{debug, error, info};

#[derive(Debug, Clone)]
pub enum ResolverRequest {
    BlockByHash(RpcHash),
    SenderByTxIdAndDaa(SenderByTxIdAndDaa),
}

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
    shutdown_rx: Receiver<()>,
    block_request_rx: Receiver<RpcHash>,
    sender_request_rx: Receiver<(RpcTransactionId, u64)>,
    kaspa_rpc_client: KaspaRpcClient,
    response_tx: Sender<ResolverResponse>,
}

impl Resolver {
    pub fn new(
        shutdown_rx: Receiver<()>,
        block_request_rx: Receiver<RpcHash>,
        sender_request_rx: Receiver<(RpcTransactionId, u64)>,
        response_tx: Sender<ResolverResponse>,
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

    pub async fn process(&self) -> anyhow::Result<()> {
        info!("Resolver started");
        loop {
            match self.select_input()? {
                Input::Shutdown => {
                    info!("Resolver received shutdown signal, stopping.");
                    return Ok(());
                }
                Input::BlockRequest(hash) => {
                    debug!(%hash, "Received block resolution request");
                    match self.kaspa_rpc_client.get_block(hash, false).await {
                        Ok(block) => {
                            self.response_tx
                                .send_async(ResolverResponse::Block(Ok(Box::new(block.header))))
                                .await?;
                        }
                        Err(err) => {
                            // todo there is no way to distinguish rpc error from app level ='(
                            error!(%err, "Failed to get block for hash: {hash}");
                            self.response_tx
                                .send_async(ResolverResponse::Block(Err(hash)))
                                .await?;
                        }
                    }
                }
                Input::SenderRequest { tx_id, daa_score } => {
                    debug!(%tx_id, %daa_score, "Received sender resolution request");
                    match self
                        .kaspa_rpc_client
                        .get_utxo_return_address(tx_id, daa_score)
                        .await
                    {
                        Ok(sender) => {
                            self.response_tx
                                .send_async(ResolverResponse::Sender(Ok((
                                    sender,
                                    SenderByTxIdAndDaa { tx_id, daa_score },
                                ))))
                                .await?;
                        }
                        Err(err) => {
                            // todo there is no way to distinguish rpc error from app level ='(
                            error!(%err, "Failed to get sender for tx id: {tx_id} and daa score: {daa_score}");
                            self.response_tx
                                .send_async(ResolverResponse::Sender(Err(SenderByTxIdAndDaa {
                                    tx_id,
                                    daa_score,
                                })))
                                .await?;
                        }
                    }
                }
            }
        }
    }

    fn select_input(&self) -> anyhow::Result<Input> {
        flume::Selector::new()
            .recv(&self.shutdown_rx, |_| Ok(Input::Shutdown))
            .recv(&self.block_request_rx, |res| {
                res.map(Input::BlockRequest).map_err(|e| e.into())
            })
            .recv(&self.sender_request_rx, |res| {
                res.map(|(tx_id, daa_score)| Input::SenderRequest { tx_id, daa_score })
                    .map_err(|e| e.into())
            })
            .wait()
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
