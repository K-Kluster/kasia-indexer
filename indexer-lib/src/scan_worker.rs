use crate::database::PartitionId;
use crate::database::acceptance::{
    AcceptingBlockResolutionData, AcceptingBlockToTxIDPartition, TxIDToAcceptancePartition,
};
use crate::database::skip_tx::SkipTxPartition;
use crate::database::unknown_accepting_daa::UnknownAcceptingDaaPartition;
use crate::database::unknown_tx::{UnknownTxInfo, UnknownTxPartition};
use fjall::TxKeyspace;
use kaspa_consensus_core::tx::TransactionId;
use parking_lot::Mutex;
use std::sync::Arc;
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
    reorg_lock: Arc<Mutex<()>>, // its okay to merge unknown txs before or after reorg deletion but not in parallel

    tx_keyspace: TxKeyspace,
    tx_id_to_acceptance_partition: TxIDToAcceptancePartition,
    unknown_tx_partition: UnknownTxPartition,
    skip_tx_partition: SkipTxPartition,
    unknown_accepting_daa_partition: UnknownAcceptingDaaPartition,
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
        self.resolve_unknown_tx()?;
        todo!()
    }

    fn resolve_unknown_tx(&self) -> anyhow::Result<()> {
        let _g = self.reorg_lock.lock();
        let mut wtx = self.tx_keyspace.write_tx()?;
        let rtx = self.tx_keyspace.read_tx();

        for unknown in self.unknown_tx_partition.get_all_unknown(&rtx) {
            let UnknownTxInfo {
                tx_id,
                accepting_block_hash,
            } = unknown?;
            if self.skip_tx_partition.should_skip(&rtx, tx_id.as_bytes())? {
                self.unknown_tx_partition.remove_unknown(&mut wtx, tx_id)?;
                continue;
            }
            for r in self
                .tx_id_to_acceptance_partition
                .get_by_tx_id(&rtx, tx_id.as_ref())
            {
                let (key, resolution) = r?;
                match resolution {
                    AcceptingBlockResolutionData::HandshakeKey(hk) => {
                        assert_eq!(key.partition_id, PartitionId::HandshakeBySender as u8);
                        self.tx_id_to_acceptance_partition
                            .remove(&mut wtx, key.clone());
                        self.tx_id_to_acceptance_partition.insert_handshake_wtx(
                            &mut wtx,
                            key.tx_id,
                            &hk,
                            None,
                            Some(accepting_block_hash.as_bytes()),
                        );
                        self.unknown_accepting_daa_partition
                            .mark_handshake_unknown_daa(
                                &mut wtx,
                                accepting_block_hash,
                                tx_id,
                                &hk,
                            )?;
                    }
                    AcceptingBlockResolutionData::ContextualMessageKey(cmk) => {
                        assert_eq!(
                            key.partition_id,
                            PartitionId::ContextualMessageBySender as u8
                        );
                        self.tx_id_to_acceptance_partition
                            .remove(&mut wtx, key.clone());
                        self.tx_id_to_acceptance_partition
                            .insert_contextual_message_wtx(
                                &mut wtx,
                                key.tx_id,
                                &cmk,
                                None,
                                Some(accepting_block_hash.as_bytes()),
                            );
                        self.unknown_accepting_daa_partition
                            .mark_contextual_message_unknown_daa(
                                &mut wtx,
                                accepting_block_hash,
                                tx_id,
                                &cmk,
                            )?;
                    }
                    AcceptingBlockResolutionData::PaymentKey(pmk) => {
                        assert_eq!(key.partition_id, PartitionId::PaymentBySender as u8);
                        self.tx_id_to_acceptance_partition
                            .remove(&mut wtx, key.clone());
                        self.tx_id_to_acceptance_partition.insert_payment_wtx(
                            &mut wtx,
                            key.tx_id,
                            &pmk,
                            None,
                            Some(accepting_block_hash.as_bytes()),
                        );
                        self.unknown_accepting_daa_partition
                            .mark_payment_unknown_daa(
                                &mut wtx,
                                accepting_block_hash,
                                tx_id,
                                &pmk,
                            )?;
                    }
                    AcceptingBlockResolutionData::None => {
                        // todo warning
                    }
                }
                self.unknown_tx_partition.remove_unknown(&mut wtx, tx_id)?;
            }
            // todo warn that tx is still unknown
        }

        Ok(())
    }
}
