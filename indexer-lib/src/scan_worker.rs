use crate::database::acceptance::{AcceptingBlockToTxIDPartition, TxIDToAcceptancePartition};
use crate::database::unknown_tx::UnknownTxPartition;
use fjall::TxKeyspace;
use std::time::Duration;
use tracing::{error, info};
use workflow_core::channel::{Receiver, Sender};

#[derive(Debug, Copy, Clone)]
pub enum Notification {
    Tick,
    Shutdown,
}

pub async fn run_ticker(
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
    job_done_rx: Receiver<()>,
    tick_tx: Sender<Notification>,
    interval: Duration,
) -> anyhow::Result<()> {
    let mut t = tokio::time::interval(interval);
    let mut need_to_send = true;
    loop {
        tokio::select! {
            _ = t.tick() => {
                if need_to_send {
                    _ = tick_tx.send(Notification::Tick).await.inspect_err(|err| error!("Error sending ticker: {}", err)).ok();
                    need_to_send = false;
                }
            }
            r = job_done_rx.recv() => {
                r?;
                need_to_send = true;
            }
            r = &mut shutdown => {
                info!("Shutting down scan worker");
                r?;
                return Ok(())
            }
        }
    }
}

pub struct ScanWorker {
    tick_tx: Receiver<Notification>,
    job_done_tx: Sender<()>,

    tx_keyspace: TxKeyspace,
    tx_id_to_acceptance_partition: TxIDToAcceptancePartition,
    unknown_tx_partition: UnknownTxPartition,
}

impl ScanWorker {
    pub fn worker(&self) -> anyhow::Result<()> {
        loop {
            match self.tick_tx.recv_blocking()? {
                Notification::Tick => {
                    self.work()?;
                    self.job_done_tx.send_blocking(())?;
                }
                Notification::Shutdown => {
                    info!("Shutting down scan worker");
                    return Ok(());
                }
            }
        }
    }

    pub fn work(&self) -> anyhow::Result<()> {
        self.resolve_accepting_block()?;
        todo!()
    }

    fn resolve_accepting_block(&self) -> anyhow::Result<()> {
        let wtx = self.tx_keyspace.write_tx()?;
        let rtx = self.tx_keyspace.read_tx();
        // self.unknown_tx_partition.

        Ok(())
    }
}
