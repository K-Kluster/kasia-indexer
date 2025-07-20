use crate::database::PartitionId;
use crate::database::acceptance::{AcceptingBlockResolutionData, TxIDToAcceptancePartition};
use crate::database::block_compact_header::BlockCompactHeaderPartition;
use crate::database::pending_sender_resolution::PendingSenderResolutionPartition;
use crate::database::resolution_keys::DaaResolutionLikeKey;
use crate::database::skip_tx::SkipTxPartition;
use crate::database::unknown_accepting_daa::{ResolutionEntries, UnknownAcceptingDaaPartition};
use crate::database::unknown_tx::{UnknownTxPartition, UnknownTxUpdateAction};
use crate::resolver::{ResolverRequest, ResolverResponse, SenderByTxIdAndDaa};
use fjall::TxKeyspace;
use kaspa_rpc_core::{RpcAddress, RpcHash, RpcHeader, RpcTransactionId};
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use workflow_core::channel::{Receiver, Sender};

#[derive(Debug, Clone)]
pub enum Notification {
    Tick,
    Shutdown,
    ResolverResponse(ResolverResponse),
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
    tick_and_resolution_rx: Receiver<Notification>,
    job_done_tx: Sender<()>,
    resolver_request_tx: Sender<ResolverRequest>,
    reorg_lock: Arc<Mutex<()>>, // its okay to merge unknown txs before or after reorg deletion but not in parallel

    tx_keyspace: TxKeyspace,
    tx_id_to_acceptance_partition: TxIDToAcceptancePartition,
    unknown_tx_partition: UnknownTxPartition,
    skip_tx_partition: SkipTxPartition,
    unknown_accepting_daa_partition: UnknownAcceptingDaaPartition,
    block_compact_header_partition: BlockCompactHeaderPartition,
    daa_resolution_attempt_count: u8,
    pending_sender_resolution_partition: PendingSenderResolutionPartition,
}

impl ScanWorker {
    pub fn worker(&self) -> anyhow::Result<()> {
        loop {
            match self.tick_and_resolution_rx.recv_blocking()? {
                Notification::Tick => {
                    self.tick_work()?;
                    self.job_done_tx.send_blocking(())?;
                }
                Notification::ResolverResponse(ResolverResponse::Block(r)) => {
                    self.handle_daa_resolution(r)?;
                }
                Notification::ResolverResponse(ResolverResponse::Sender(r)) => {
                    self.handle_sender_resolution(r)?;
                }
                Notification::Shutdown => {
                    info!("Shutting down scan worker");
                    return Ok(());
                }
            }
        }
    }

    pub fn tick_work(&self) -> anyhow::Result<()> {
        self.resolve_unknown_tx()?;
        self.unknown_daa()?;
        self.unknown_sender()?;
        Ok(())
    }

    pub fn handle_daa_resolution(&self, r: Result<Box<RpcHeader>, RpcHash>) -> anyhow::Result<()> {
        let mut wtx = self.tx_keyspace.write_tx()?;
        match r {
            Ok(header) => {
                // todo logs
                self.block_compact_header_partition.insert_compact_header(
                    &header.hash,
                    header.blue_work,
                    header.daa_score,
                )?;
                self.unknown_accepting_daa_partition
                    .remove_by_accepting_block_hash(&mut wtx, header.hash)?;
            }
            Err(hash) => {
                // todo logs
                self.unknown_accepting_daa_partition
                    .decrement_attempt_counts_by_block_hash(&mut wtx, hash)?;
            }
        }
        wtx.commit()??;
        Ok(())
    }

    pub fn handle_sender_resolution(
        &self,
        r: Result<(RpcAddress, RpcTransactionId), SenderByTxIdAndDaa>,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn resolve_unknown_tx(&self) -> anyhow::Result<()> {
        let _g = self.reorg_lock.lock();
        let rtx = self.tx_keyspace.read_tx();
        let mut wtx = self.tx_keyspace.write_tx()?;

        // Process unknown transactions by accepting block hash (following new pattern)
        for unknown_block_result in self.unknown_tx_partition.get_all_unknown(&rtx) {
            let (accepting_block_hash, like_tx_ids) = unknown_block_result?;
            let mut processed_any = false;

            let mut extended_daa_requests =
                ResolutionEntries::new(self.daa_resolution_attempt_count);

            // Process each transaction in this accepting block
            let mut remaining_tx_ids = Vec::new();
            for tx_id in like_tx_ids.as_tx_ids() {
                // Check if we should skip this transaction
                if self.skip_tx_partition.should_skip(&rtx, tx_id)? {
                    processed_any = true;
                    continue; // Skip this tx but don't add to remaining
                }

                // Look for resolution data for this transaction
                let mut found_resolution = false;

                for r in self.tx_id_to_acceptance_partition.get_by_tx_id(&rtx, tx_id) {
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
                            extended_daa_requests.push_handshake(*tx_id, &hk);
                            found_resolution = true;
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
                            extended_daa_requests.push_contextual_message(*tx_id, &cmk);
                            found_resolution = true;
                            processed_any = true;
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

                            extended_daa_requests.push_payment(*tx_id, &pmk);
                            processed_any = true;
                        }
                        AcceptingBlockResolutionData::None => {
                            warn!(tx_id = %RpcTransactionId::from_bytes(key.tx_id), "No resolution data found for transaction");
                        }
                    }
                }

                if !found_resolution {
                    debug!(tx_id = %RpcTransactionId::from_bytes(*tx_id), "Keeping transaction in unknown state");
                    // Keep this transaction in unknown state
                    remaining_tx_ids.push(*tx_id);
                }
            }
            // Update the entry for this accepting block hash using the new explicit enum
            if processed_any {
                self.unknown_tx_partition.update_by_accepting_block_hash(
                    &mut wtx,
                    &accepting_block_hash,
                    move |_current| {
                        if remaining_tx_ids.is_empty() {
                            UnknownTxUpdateAction::Delete
                        } else {
                            warn!(
                                "{} unknown transactions in accepting block {}",
                                remaining_tx_ids.len(),
                                accepting_block_hash
                            );
                            UnknownTxUpdateAction::Update(std::mem::take(&mut remaining_tx_ids))
                        }
                    },
                )?;
            }
            if !extended_daa_requests.is_empty() {
                debug!(count = %extended_daa_requests.len(), %accepting_block_hash, "Extending DAA requests");
                self.unknown_accepting_daa_partition
                    .extend_by_accepting_block_hash(
                        &mut wtx,
                        &accepting_block_hash,
                        extended_daa_requests,
                    )?;
            }
        }
        wtx.commit()??;
        Ok(())
    }

    fn unknown_daa(&self) -> anyhow::Result<()> {
        let _lock = self.reorg_lock.lock();
        let rtx = self.tx_keyspace.read_tx();
        let mut wtx = self.tx_keyspace.write_tx()?;
        for block in self
            .unknown_accepting_daa_partition
            .get_all_unknown_accepting_blocks(&rtx)
        {
            let block = block?;
            match self
                .block_compact_header_partition
                .get_daa_score_rtx(&rtx, &block)?
            {
                None => {
                    warn!(block_hash = %block, "DAA score still not available for block");
                    let _ = self
                        .resolver_request_tx
                        .send_blocking(ResolverRequest::BlockByHash(block))
                        .inspect_err(|err| error!(""));
                }
                Some(daa) => {
                    info!(block_hash = %block, daa_score = %daa, "DAA score resolved for block");
                    let entries = self
                        .unknown_accepting_daa_partition
                        .remove_by_accepting_block_hash(&mut wtx, block)?;
                    if let Some(entries) = entries {
                        let entries = entries.as_entry_slice()?;
                        let count = entries.len();
                        if count > 0 {
                            info!(%count, block_hash = %block, "Moving transactions to pending sender resolution queue");
                        }
                        entries.iter().try_for_each(|entry| -> anyhow::Result<()> {
                            match entry.get_resolution_key()? {
                                DaaResolutionLikeKey::HandshakeKey(hk) => self
                                    .pending_sender_resolution_partition
                                    .mark_handshake_pending(&mut wtx, daa, hk.tx_id, &hk)?,
                                DaaResolutionLikeKey::ContextualMessageKey(cmk) => self
                                    .pending_sender_resolution_partition
                                    .mark_contextual_message_pending(
                                        &mut wtx, daa, cmk.tx_id, &cmk,
                                    )?,
                                DaaResolutionLikeKey::PaymentKey(pmk) => self
                                    .pending_sender_resolution_partition
                                    .mark_payment_pending(&mut wtx, daa, pmk.tx_id, &pmk)?,
                            }
                            Ok(())
                        })?;
                    }
                }
            }
        }
        wtx.commit()??;
        Ok(())
    }

    fn unknown_sender(&self) -> anyhow::Result<()> {
        // todo send request to resolution task
        Ok(())
    }
}
