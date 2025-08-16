use crate::metrics::SharedMetrics;
use crate::util::ToHex64;
use fjall::TxKeyspace;
use indexer_db::headers::block_compact_headers::BlockCompactHeaderPartition;
use indexer_db::headers::daa_index::DaaIndexPartition;
use indexer_db::messages::handshake::{HandshakeBySenderPartition, TxIdToHandshakePartition};
use indexer_db::messages::payment::{PaymentBySenderPartition, TxIdToPaymentPartition};
use indexer_db::metadata::MetadataPartition;
use indexer_db::processing::accepting_block_to_txs::AcceptingBlockToTxIDPartition;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, error, info, trace};
use workflow_core::channel::{Receiver, Sender};

pub enum Intake {
    DoJob,
    Shutdown,
}

pub enum Response {
    JobDone,
    Stopped,
}

#[derive(bon::Builder)]
pub struct PeriodicProcessor {
    pruning_depth: u64,
    job_trigger_rx: Receiver<Intake>,
    resp_tx: Sender<Response>,
    metrics: SharedMetrics,
    virtual_daa: Arc<AtomicU64>,

    tx_keyspace: TxKeyspace,
    daa_index_partition: DaaIndexPartition,
    block_compact_header_partition: BlockCompactHeaderPartition,
    accepting_block_to_tx_id_partition: AcceptingBlockToTxIDPartition,
    metadata_partition: MetadataPartition,
    tx_id_to_handshake_partition: TxIdToHandshakePartition,
    tx_id_to_payment_partition: TxIdToPaymentPartition,
    payment_by_sender_partition: PaymentBySenderPartition,
    handshake_by_sender_partition: HandshakeBySenderPartition,
}

impl PeriodicProcessor {
    pub fn process(&self) -> anyhow::Result<()> {
        info!("Periodic processor started");
        loop {
            match self.job_trigger_rx.recv_blocking()? {
                Intake::Shutdown => {
                    info!("Periodic processor received shutdown signal");
                    return Ok(());
                }
                Intake::DoJob => {
                    debug!("Periodic processor executing job");
                    self.do_job()?;
                    self.resp_tx.send_blocking(Response::JobDone)?;
                    trace!("Periodic job completed");
                }
            }
        }
    }

    fn do_job(&self) -> anyhow::Result<()> {
        debug!("Starting periodic maintenance job");
        self.compact_metadata()?;
        let pruned_count = self.prune()?;
        self.update_metrics(pruned_count)?;
        if pruned_count > 0 {
            info!(
                pruned_blocks = pruned_count,
                "Periodic maintenance completed"
            );
        } else {
            trace!("Periodic maintenance completed, no blocks pruned");
        }
        Ok(())
    }

    fn prune(&self) -> anyhow::Result<u64> {
        let current_daa = self.virtual_daa.load(Ordering::Relaxed);
        let prune_threshold = current_daa.saturating_sub(self.pruning_depth);
        debug!(current_daa, prune_threshold, "Starting block pruning");

        let read_tx = self.tx_keyspace.read_tx();
        let mut pruned = 0;
        for r in self.daa_index_partition.iter_lt(&read_tx, prune_threshold) {
            let (daa, hash) = r?;
            trace!(hash = %hash.to_hex_64(), daa_score = daa, "Pruning block");
            self.block_compact_header_partition.remove(&hash)?;
            self.daa_index_partition.delete(daa, &hash)?;
            self.accepting_block_to_tx_id_partition.remove(&hash)?;
            pruned += 1;
        }
        if pruned > 0 {
            debug!(pruned_blocks = pruned, "Block pruning completed");
        }
        Ok(pruned)
    }

    fn compact_metadata(&self) -> anyhow::Result<()> {
        let disk_space = self.metadata_partition.0.inner().disk_space();
        if disk_space > 1024 * 1024 {
            debug!(
                disk_space_mb = disk_space / (1024 * 1024),
                "Compacting metadata partition"
            );
            self.metadata_partition.0.inner().major_compact()?;
            trace!("Metadata compaction completed");
        }
        Ok(())
    }

    fn update_metrics(&self, pruned_blocks: u64) -> anyhow::Result<()> {
        self.metrics.increment_pruned_blocks(pruned_blocks);
        self.metrics
            .set_handshakes_by_receiver(self.tx_id_to_handshake_partition.approximate_len() as u64); // todo use len at startup and atomic for update
        self.metrics
            .set_handshakes_by_sender(self.handshake_by_sender_partition.approximate_len() as u64);
        self.metrics
            .set_payments_by_receiver(self.tx_id_to_payment_partition.approximate_len() as u64); // todo use len at startup and atomic for update
        self.metrics
            .set_payments_by_sender(self.payment_by_sender_partition.approximate_len() as u64);
        self.metrics.set_latest_block(
            self.metadata_partition
                .get_latest_block_cursor()?
                .unwrap_or_default(),
        );
        self.metrics.set_latest_accepting_block(
            self.metadata_partition
                .get_latest_accepting_block_cursor()?
                .unwrap_or_default()
                .block_hash,
        );

        Ok(())
    }
}

impl Drop for PeriodicProcessor {
    fn drop(&mut self) {
        _ = self
            .resp_tx
            .send_blocking(Response::Stopped)
            .inspect_err(|_err| error!("periodic processor sending `stopped` failed"));
    }
}
