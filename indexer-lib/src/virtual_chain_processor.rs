use crate::database::PartitionId;
use crate::database::headers::block_compact_headers::BlockCompactHeaderPartition;
use crate::database::metadata::MetadataPartition;
use crate::database::processing::acceptance::{
    AcceptanceTxKey, AcceptingBlockResolutionData, AcceptingBlockToTxIDPartition,
    TxIDToAcceptancePartition,
};
use crate::database::processing::pending_sender_resolution::PendingSenderResolutionPartition;
use crate::database::processing::skipped_transactions::SkipTxPartition;
use crate::database::processing::unknown_daa_scores::{
    ResolutionEntries, UnknownAcceptingDaaPartition,
};
use crate::database::processing::unknown_transactions::UnknownTxPartition;
use crate::historical_syncer::Cursor;
use fjall::{ReadTransaction, TxKeyspace, WriteTransaction};
use itertools::process_results;
use kaspa_consensus_core::BlueWorkType;
use kaspa_rpc_core::{
    RpcAcceptedTransactionIds, RpcHash, RpcTransactionId, VirtualChainChangedNotification,
};
use parking_lot::Mutex;
use std::sync::Arc;
use tracing::{debug, info, trace, warn};

pub struct VirtualChainChangedNotificationAndBlueWork {
    pub vcc: VirtualChainChangedNotification,
    pub last_block_blue_work: BlueWorkType,
}

#[derive(bon::Builder)]
pub struct VirtualChainProcessor {
    daa_resolution_attempt_count: u8,
    reorg_log: Arc<Mutex<()>>, // during reorg we should not merge unknown tx into other partitions
    vcc_rx: flume::Receiver<VirtualChainChangedNotificationAndBlueWork>,
    shutdown: flume::Receiver<()>,
    tx_keyspace: TxKeyspace,

    metadata_partition: MetadataPartition,
    skip_tx_partition: SkipTxPartition,
    tx_id_to_acceptance_partition: TxIDToAcceptancePartition,
    acceptance_to_tx_id_partition: AcceptingBlockToTxIDPartition,
    unknown_tx_partition: UnknownTxPartition,
    unknown_accepting_daa_partition: UnknownAcceptingDaaPartition,

    block_compact_header_partition: BlockCompactHeaderPartition,

    pending_sender_resolution_partition: PendingSenderResolutionPartition,
}

impl VirtualChainProcessor {
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
            info!(added = %vcc.added_chain_block_hashes.len(), removed = %vcc.removed_chain_block_hashes.len(), "Handling VCC notification");
            // todo is it possible that vcc only has removals??
            return Ok(());
        }
        let rtx = self.tx_keyspace.read_tx();
        let mut wtx = self.tx_keyspace.write_tx()?;
        vcc.removed_chain_block_hashes
            .iter()
            .try_for_each(|hash| -> anyhow::Result<()> {
                debug!(%hash, "Handling chain block removal");
                self.handle_chain_block_removal(&mut wtx, &rtx, hash)?;
                Ok(())
            })?;
        vcc.accepted_transaction_ids.iter().try_for_each(
            |RpcAcceptedTransactionIds {
                 accepting_block_hash,
                 accepted_transaction_ids,
             }|
             -> anyhow::Result<()> {
                debug!(%accepting_block_hash, tx_count = %accepted_transaction_ids.len(), "Handling accepted block");
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
        debug!(hash = %last_block, "Updating latest accepting block cursor");
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
        rtx: &ReadTransaction,
        removed_block_hash: &RpcHash,
    ) -> anyhow::Result<()> {
        let _lock = self.reorg_log.lock();
        let Some(tx_id_s) = self
            .acceptance_to_tx_id_partition
            .remove_wtx(wtx, removed_block_hash)?
        else {
            return Ok(());
        };
        let daa = self
            .block_compact_header_partition
            .get_daa_score_rtx(rtx, removed_block_hash)?;
        debug!(block_hash = %removed_block_hash, tx_count = %tx_id_s.as_tx_ids().len(), "Processing block removal");
        for tx_id in tx_id_s.as_tx_ids() {
            for r in self.tx_id_to_acceptance_partition.get_by_tx_id(rtx, tx_id) {
                let (key, value) = r?;
                let partition_id = key.partition_id;
                self.tx_id_to_acceptance_partition.remove(wtx, key.clone());
                self.tx_id_to_acceptance_partition.insert_wtx(
                    wtx,
                    &AcceptanceTxKey {
                        tx_id: *tx_id,
                        accepted_at_daa: Default::default(),
                        accepted_by_block_hash: Default::default(),
                        partition_id,
                    },
                    value,
                );

                self.unknown_accepting_daa_partition
                    .remove_by_accepting_block_hash(wtx, *removed_block_hash)?;
                self.unknown_tx_partition
                    .remove_by_accepting_block_hash(wtx, removed_block_hash)?;
                if let Some(daa) = daa {
                    self.pending_sender_resolution_partition
                        .remove_pending(wtx, daa, tx_id)?;
                }
            }
        }
        // todo consider update of metadata partition
        Ok(())
    }

    fn handle_accepted_block(
        &self,
        wtx: &mut WriteTransaction,
        rtx: &ReadTransaction,
        accepting_block_hash: &RpcHash,
        tx_id_s: &[RpcTransactionId],
    ) -> anyhow::Result<()> {
        let _lock = self.reorg_log.lock(); // rename lock

        let accepting_daa = self
            .block_compact_header_partition
            .get_daa_score_rtx(rtx, accepting_block_hash)?;
        let filtered = process_results(
            tx_id_s.iter().map(|tx_id| {
                self.skip_tx_partition
                    .should_skip(rtx, tx_id.as_ref())
                    .map(|skip| (tx_id, skip))
            }),
            |iter| {
                iter.filter(|(_, skip)| !*skip)
                    .map(|(x, _)| x.as_bytes())
                    .collect::<Vec<_>>()
            },
        )?;

        let mut entries = ResolutionEntries::new(self.daa_resolution_attempt_count);
        let mut unknown_tx_ids = Vec::with_capacity(filtered.len());
        for tx_id in &filtered {
            let mut is_required = false;
            for r in self.tx_id_to_acceptance_partition.get_by_tx_id(rtx, tx_id) {
                let (key, resolution) = r?;
                assert_eq!(*tx_id, key.tx_id);
                is_required = true;
                match resolution {
                    AcceptingBlockResolutionData::HandshakeKey(hk) => {
                        assert_eq!(key.partition_id, PartitionId::HandshakeBySender as u8);
                        self.tx_id_to_acceptance_partition.remove(wtx, key.clone());
                        self.tx_id_to_acceptance_partition.insert_handshake_wtx(
                            wtx,
                            key.tx_id,
                            &hk,
                            accepting_daa,
                            Some(accepting_block_hash.as_bytes()),
                        );
                        if let Some(daa) = accepting_daa {
                            self.pending_sender_resolution_partition
                                .mark_handshake_pending(wtx, daa, key.tx_id, &hk)?
                        } else {
                            entries.push_handshake(*tx_id, &hk);
                        }
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
                                accepting_daa,
                                Some(accepting_block_hash.as_bytes()),
                            );
                        if let Some(daa) = accepting_daa {
                            self.pending_sender_resolution_partition
                                .mark_contextual_message_pending(wtx, daa, key.tx_id, &cmk)?
                        } else {
                            entries.push_contextual_message(key.tx_id, &cmk);
                        }
                    }
                    AcceptingBlockResolutionData::PaymentKey(pmk) => {
                        assert_eq!(key.partition_id, PartitionId::PaymentBySender as u8);
                        self.tx_id_to_acceptance_partition.remove(wtx, key.clone());
                        self.tx_id_to_acceptance_partition.insert_payment_wtx(
                            wtx,
                            key.tx_id,
                            &pmk,
                            accepting_daa,
                            Some(accepting_block_hash.as_bytes()),
                        );
                        if let Some(daa) = accepting_daa {
                            self.pending_sender_resolution_partition
                                .mark_payment_pending(wtx, daa, key.tx_id, &pmk)?
                        } else {
                            entries.push_payment(key.tx_id, &pmk);
                        }
                    }
                    AcceptingBlockResolutionData::None => {
                        warn!(tx_id = %RpcTransactionId::from_bytes(key.tx_id), "No resolution data found for transaction");
                    }
                }
            }
            if !is_required {
                debug!(tx_id = %RpcTransactionId::from_bytes(*tx_id), %accepting_block_hash, "Marking transaction as unknown");
                unknown_tx_ids.push(*tx_id);
            }
        }
        if accepting_daa.is_none() {
            self.unknown_accepting_daa_partition.insert_wtx(
                wtx,
                *accepting_block_hash,
                &entries,
            )?;
        } else {
            assert!(entries.is_empty());
        }

        self.acceptance_to_tx_id_partition
            .insert_wtx(wtx, accepting_block_hash, &filtered);
        self.unknown_tx_partition
            .insert_wtx(wtx, accepting_block_hash, unknown_tx_ids.as_slice());
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
