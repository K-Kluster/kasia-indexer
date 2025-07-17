use crate::database::metadata::MetadataPartition;
use crate::historical_syncer::Cursor;
use fjall::{TxKeyspace, WriteTransaction};
use kaspa_consensus_core::BlueWorkType;
use kaspa_rpc_core::{
    RpcAcceptedTransactionIds, RpcBlock, RpcHash, RpcTransactionId, VirtualChainChangedNotification,
};
use tracing::{info, trace};

pub struct VirtualChainChangedNotificationAndBlueWork {
    pub vcc: VirtualChainChangedNotification,
    pub last_block_blue_work: BlueWorkType,
}

pub struct AcceptanceWorker {
    vcc_rx: flume::Receiver<VirtualChainChangedNotificationAndBlueWork>,
    shutdown: flume::Receiver<()>,
    tx_keyspace: TxKeyspace,

    metadata_partition: MetadataPartition,
}

impl AcceptanceWorker {
    pub fn process(&mut self) -> anyhow::Result<()> {
        info!("Acceptance worker started");
        loop {
            match self.select_input()? {
                VccOrShutdown::Shutdown(_) => {
                    info!(
                        "Acceptance worker received shutdown signal, draining notifications first"
                    );
                    let rx = std::mem::replace(&mut self.vcc_rx, flume::bounded(0).1);
                    rx.drain().try_for_each(|vcc| -> anyhow::Result<()> {
                        self.handle_vcc(&vcc)?;
                        Ok(())
                    })?;
                    info!("Draining is done, stopping acceptance worker");
                    return Ok(());
                }
                VccOrShutdown::Vcc(vcc) => {
                    self.handle_vcc(&vcc)?;
                }
            }
        }
    }

    fn select_input(&self) -> anyhow::Result<VccOrShutdown> {
        trace!("Waiting for new vcc or shutdown signal");
        Ok(flume::Selector::new()
            .recv(&self.vcc_rx, |r| r.map(VccOrShutdown::from))
            .recv(&self.shutdown, |r| r.map(VccOrShutdown::from))
            .wait()?)
    }

    fn handle_vcc(
        &self,
        VirtualChainChangedNotificationAndBlueWork {
            vcc,
            last_block_blue_work,
        }: &VirtualChainChangedNotificationAndBlueWork,
    ) -> anyhow::Result<()> {
        if vcc.added_chain_block_hashes.is_empty() {
            // todo is it possible that vcc only has removals??
            return Ok(());
        }
        let mut wtx = self.tx_keyspace.write_tx()?;
        vcc.removed_chain_block_hashes
            .iter()
            .try_for_each(|hash| -> anyhow::Result<()> {
                self.handle_chain_block_removal(&mut wtx, hash)?;
                Ok(())
            })?;
        vcc.accepted_transaction_ids.iter().try_for_each(
            |RpcAcceptedTransactionIds {
                 accepting_block_hash,
                 accepted_transaction_ids,
             }|
             -> anyhow::Result<()> {
                self.handle_accepted_block(
                    &mut wtx,
                    accepting_block_hash,
                    accepted_transaction_ids,
                )?;
                Ok(())
            },
        )?;
        let last_block = vcc.added_chain_block_hashes.last().unwrap();
        self.metadata_partition.set_latest_accepting_block_cursor(
            &mut wtx,
            Cursor {
                blue_work: *last_block_blue_work,
                hash: *last_block,
            },
        )?;
        wtx.commit()??;

        Ok(())
    }

    fn handle_chain_block_removal(
        &self,
        wtx: &mut WriteTransaction,
        removed_block_hash: &RpcHash,
    ) -> anyhow::Result<()> {
        todo!("handle chain_block_removal")
    }

    fn handle_accepted_block(
        &self,
        wtx: &mut WriteTransaction,
        accepting_block_hash: &RpcHash,
        tx_id_s: &[RpcTransactionId],
    ) -> anyhow::Result<()> {
        todo!("handle accepted_block")
    }
}

enum VccOrShutdown {
    Vcc(VirtualChainChangedNotificationAndBlueWork),
    Shutdown(()),
}

impl From<VirtualChainChangedNotificationAndBlueWork> for VccOrShutdown {
    fn from(other: VirtualChainChangedNotificationAndBlueWork) -> Self {
        Self::Vcc(other)
    }
}

impl From<()> for VccOrShutdown {
    fn from(value: ()) -> Self {
        Self::Shutdown(value)
    }
}
