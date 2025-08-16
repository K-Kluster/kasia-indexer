use crate::block_processor::GapFillingProgress;
use crate::data_source::{Command, Request, RequestError};
use kaspa_rpc_core::RpcHash;
use tokio::sync::oneshot::error::TryRecvError;
use tracing::{error, info, warn};

/// Manages historical data synchronization from Kaspa node
pub struct BlockGapFiller {
    from_block: [u8; 32],
    /// Target sync position
    target_block: [u8; 32],
    gap_result_tx: flume::Sender<GapFillingProgress>,
    command_tx: workflow_core::channel::Sender<Command>,
    interrupt_rx: tokio::sync::oneshot::Receiver<()>,
}

impl BlockGapFiller {
    pub fn new(
        from_block: [u8; 32],
        target_block: [u8; 32],
        gap_result_tx: flume::Sender<GapFillingProgress>,
        command_tx: workflow_core::channel::Sender<Command>,
        interrupt_rx: tokio::sync::oneshot::Receiver<()>,
    ) -> Self {
        Self {
            from_block,
            target_block,
            gap_result_tx,
            command_tx,
            interrupt_rx,
        }
    }

    /// Starts the synchronization process
    pub async fn sync(mut self) -> anyhow::Result<()> {
        let mut from_block = self.from_block;
        info!(
            from = %RpcHash::from_bytes(self.from_block),
            to = %RpcHash::from_bytes(self.target_block),
            "Starting historical data synchronization",
        );

        loop {
            match self.interrupt_rx.try_recv() {
                Ok(()) => {
                    info!("Received shutdown signal, stopping sync");
                    self.gap_result_tx
                        .send_async(GapFillingProgress::Interrupted {
                            target: self.target_block,
                        })
                        .await?;
                    return Ok(());
                }
                Err(TryRecvError::Closed) => {
                    warn!("Received shutdown signal closed, stopping sync");
                    _ = self
                        .gap_result_tx
                        .send_async(GapFillingProgress::Interrupted {
                            target: self.target_block,
                        })
                        .await
                        .inspect_err(|err| error!(%err, "Error sending interrupted notification"));
                    return Ok(());
                }
                Err(TryRecvError::Empty) => {
                    // all good try sync
                }
            }
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.command_tx
                .send(Command::Request(Request::RequestBlocks {
                    blocks_from: from_block,
                    response_channel: tx,
                }))
                .await?;
            match rx.await? {
                Err(RequestError::RpcError(err)) => {
                    self.gap_result_tx
                        .send_async(GapFillingProgress::Error {
                            target: self.target_block,
                            err,
                        })
                        .await?;
                    return Ok(());
                }
                Err(RequestError::ShuttingDown) => {
                    // do nothing, we are shutting down, next iteration we must receive shutting down signal
                }
                Ok(blocks)
                    if blocks
                        .iter()
                        .any(|b| b.header.hash.as_bytes() == self.target_block) =>
                {
                    self.gap_result_tx
                        .send_async(GapFillingProgress::Finished {
                            target: self.target_block,
                            blocks,
                        })
                        .await?;
                    info!(
                        from = %RpcHash::from_bytes(self.from_block),
                        to = %RpcHash::from_bytes(self.target_block),
                        "Finished historical data synchronization"
                    );
                    return Ok(());
                }
                Ok(blocks) => {
                    from_block = blocks
                        .last()
                        .map(|block| block.header.hash.as_bytes())
                        .unwrap();
                    self.gap_result_tx
                        .send_async(GapFillingProgress::Update {
                            target: self.target_block,
                            blocks,
                        })
                        .await?;
                    continue;
                }
            }
        }
    }
}
