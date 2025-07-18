use crate::database::PartitionId;
use crate::database::acceptance::{
    AcceptingBlockResolutionData, AcceptingBlockToTxIDPartition, TxIDToAcceptancePartition,
};
use crate::database::metadata::MetadataPartition;
use crate::database::skip_tx::SkipTxPartition;
use crate::database::unknown_accepting_daa::UnknownAcceptingDaaPartition;
use crate::database::unknown_tx::UnknownTxPartition;
use crate::historical_syncer::Cursor;
use fjall::{ReadTransaction, TxKeyspace, WriteTransaction};
use itertools::process_results;
use kaspa_consensus_core::BlueWorkType;
use kaspa_consensus_core::tx::TransactionId;
use kaspa_rpc_core::{
    RpcAcceptedTransactionIds, RpcHash, RpcTransactionId, VirtualChainChangedNotification,
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
    skip_tx_partition: SkipTxPartition,
    tx_id_to_acceptance_partition: TxIDToAcceptancePartition,
    acceptance_to_tx_id_partition: AcceptingBlockToTxIDPartition,
    unknown_tx_partition: UnknownTxPartition,
    unknown_accepting_daa_partition: UnknownAcceptingDaaPartition,
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
        let rtx = self.tx_keyspace.read_tx();
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
                    &rtx,
                    accepting_block_hash,
                    accepted_transaction_ids,
                )?;
                Ok(())
            },
        )?;
        let last_block = vcc.added_chain_block_hashes.last().unwrap();
        // todo add info log
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
        _wtx: &mut WriteTransaction,
        _removed_block_hash: &RpcHash,
    ) -> anyhow::Result<()> {
        todo!("handle chain_block_removal")
    }

    fn handle_accepted_block(
        &self,
        wtx: &mut WriteTransaction,
        rtx: &ReadTransaction,
        accepting_block_hash: &RpcHash,
        tx_id_s: &[RpcTransactionId],
    ) -> anyhow::Result<()> {
        let filtered = process_results(
            tx_id_s.iter().map(|tx_id| {
                self.skip_tx_partition
                    .should_skip_wtx(wtx, tx_id.as_bytes())
                    .map(|skip| (tx_id, skip))
            }),
            |iter| {
                iter.filter(|(_, skip)| !*skip)
                    .map(|(x, _)| x.as_bytes())
                    .collect::<Vec<_>>()
            },
        )?;

        for tx_id in &filtered {
            let mut is_required = false;
            for r in self.tx_id_to_acceptance_partition.get_by_tx_id(rtx, tx_id) {
                let (key, resolution) = r?;
                is_required = true;
                match resolution {
                    AcceptingBlockResolutionData::HandshakeKey(hk) => {
                        assert_eq!(key.partition_id, PartitionId::HandshakeBySender as u8);
                        self.tx_id_to_acceptance_partition.remove(wtx, key.clone());
                        self.tx_id_to_acceptance_partition.insert_handshake_wtx(
                            wtx,
                            key.tx_id,
                            &hk,
                            None,
                            Some(accepting_block_hash.as_bytes()),
                        );
                        self.unknown_accepting_daa_partition
                            .mark_handshake_unknown_daa(
                                wtx,
                                *accepting_block_hash,
                                TransactionId::from_bytes(*tx_id),
                                &hk,
                            )?;
                    }
                    AcceptingBlockResolutionData::ContextualMessageKey(cmk) => {
                        assert_eq!(
                            key.partition_id,
                            PartitionId::ContextualMessageBySender as u8
                        );
                        self.tx_id_to_acceptance_partition.remove(wtx, key.clone());
                        self.tx_id_to_acceptance_partition
                            .insert_contextual_message_wtx(
                                wtx,
                                key.tx_id,
                                &cmk,
                                None,
                                Some(accepting_block_hash.as_bytes()),
                            );
                        self.unknown_accepting_daa_partition
                            .mark_contextual_message_unknown_daa(
                                wtx,
                                *accepting_block_hash,
                                TransactionId::from_bytes(*tx_id),
                                &cmk,
                            )?;
                    }
                    AcceptingBlockResolutionData::PaymentKey(pmk) => {
                        assert_eq!(key.partition_id, PartitionId::PaymentBySender as u8);
                        self.tx_id_to_acceptance_partition.remove(wtx, key.clone());
                        self.tx_id_to_acceptance_partition.insert_payment_wtx(
                            wtx,
                            key.tx_id,
                            &pmk,
                            None,
                            Some(accepting_block_hash.as_bytes()),
                        );
                        self.unknown_accepting_daa_partition
                            .mark_payment_unknown_daa(
                                wtx,
                                *accepting_block_hash,
                                TransactionId::from_bytes(*tx_id),
                                &pmk,
                            )?;
                    }
                    AcceptingBlockResolutionData::None => {
                        // todo warning
                        continue;
                    }
                }
            }
            if !is_required {
                // todo add debug log
                self.unknown_tx_partition
                    .mark_unknown(wtx, tx_id, *accepting_block_hash)?;
            }
        }
        self.acceptance_to_tx_id_partition
            .insert_wtx(wtx, accepting_block_hash, &filtered);

        Ok(())
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
