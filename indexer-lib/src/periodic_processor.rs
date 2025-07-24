use crate::APP_IS_RUNNING;
use crate::database::PartitionId;
use crate::database::headers::BlockCompactHeaderPartition;
use crate::database::messages::{
    ContextualMessageBySenderPartition, HandshakeByReceiverPartition, HandshakeBySenderPartition,
    HandshakeKeyByReceiver, HandshakeKeyBySender, PaymentByReceiverPartition,
    PaymentBySenderPartition, PaymentKeyByReceiver, PaymentKeyBySender, TxIdToHandshakePartition,
    TxIdToPaymentPartition,
};
use crate::database::metadata::MetadataPartition;
use crate::database::processing::{
    AcceptingBlockResolutionData, PendingResolutionKey, PendingSenderResolutionPartition,
    ResolutionEntries, SkipTxByBlockPartition, SkipTxPartition, TxIDToAcceptancePartition,
    UnknownAcceptingDaaPartition, UnknownTxPartition, UnknownTxUpdateAction,
};
use crate::database::resolution_keys::{DaaResolutionLikeKey, SenderResolutionLikeKey};
use crate::metrics::SharedMetrics;
use crate::resolver::{ResolverResponse, SenderByTxIdAndDaa};
use anyhow::Context;
use fjall::TxKeyspace;
use kaspa_rpc_core::{RpcAddress, RpcHash, RpcHeader, RpcTransactionId};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use std::time::Instant;
use tracing::{debug, error, info, trace, warn};
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
                info!("Shutting down scan ticker");
                r?;
                return Ok(())
            }
        }
    }
}

#[derive(bon::Builder)]
pub struct PeriodicProcessor {
    tick_and_resolution_rx: Receiver<Notification>,
    job_done_tx: Sender<()>,
    resolver_request_sender_tx: Sender<SenderByTxIdAndDaa>,
    resolver_request_block_tx: Sender<RpcHash>,
    reorg_lock: Arc<Mutex<()>>, // its okay to merge unknown txs before or after reorg deletion but not in parallel

    tx_keyspace: TxKeyspace,
    tx_id_to_acceptance_partition: TxIDToAcceptancePartition,
    unknown_tx_partition: UnknownTxPartition,
    skip_tx_partition: SkipTxPartition,
    skip_tx_by_block_partition: SkipTxByBlockPartition,
    unknown_accepting_daa_partition: UnknownAcceptingDaaPartition,
    block_compact_header_partition: BlockCompactHeaderPartition,
    daa_resolution_attempt_count: u8,
    pending_sender_resolution_partition: PendingSenderResolutionPartition,

    handshake_by_receiver_partition: HandshakeByReceiverPartition,
    handshake_by_sender_partition: HandshakeBySenderPartition,
    tx_id_to_handshake_partition: TxIdToHandshakePartition,
    contextual_message_by_sender_partition: ContextualMessageBySenderPartition,
    payment_by_receiver_partition: PaymentByReceiverPartition,
    payment_by_sender_partition: PaymentBySenderPartition,
    tx_id_to_payment_partition: TxIdToPaymentPartition,
    metadata_partition: MetadataPartition,
    metrics: SharedMetrics,
    metrics_snapshot_interval: Duration,
    #[builder(default = Instant::now())]
    last_metrics_snapshot_time: Instant,
    resolver_requests_in_progress: Arc<AtomicU64>,
    /// Cached latest accepting block hash for pruning when header isn't available yet
    cached_latest_accepting_block: Option<kaspa_rpc_core::RpcHash>,
}

impl PeriodicProcessor {
    pub fn worker(&mut self) -> anyhow::Result<()> {
        while APP_IS_RUNNING.load(Ordering::Relaxed) {
            match self.tick_and_resolution_rx.recv_blocking()? {
                Notification::ResolverResponse(ResolverResponse::Block(r)) => {
                    self.handle_daa_resolution(r)?;
                }
                Notification::ResolverResponse(ResolverResponse::Sender(r)) => {
                    self.handle_sender_resolution(r)?;
                }
                Notification::Tick => {
                    self.tick_work()?;
                    self.job_done_tx.send_blocking(())?;
                }
                Notification::Shutdown => {
                    info!("Shutting down scan worker");
                    return Ok(());
                }
            }
        }
        info!("Scan worker shut down");
        Ok(())
    }

    pub fn tick_work(&mut self) -> anyhow::Result<()> {
        self.resolve_unknown_tx()?;
        self.unknown_daa()?;
        self.unknown_sender()?;
        self.prune_skip_transactions()?;
        self.update_metrics()?;
        Ok(())
    }

    fn update_metrics(&mut self) -> anyhow::Result<()> {
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
                .unwrap_or_default()
                .hash,
        );
        self.metrics.set_latest_accepting_block(
            self.metadata_partition
                .get_latest_accepting_block_cursor()?
                .unwrap_or_default()
                .hash,
        );

        if self.metadata_partition.0.inner().disk_space() > 1024 * 1024 {
            self.metadata_partition.0.inner().major_compact()?;
        }

        if self.last_metrics_snapshot_time.elapsed() > self.metrics_snapshot_interval {
            info!("{}", self.metrics.snapshot());
            info!(
                "requests in progress: {}",
                self.resolver_requests_in_progress.load(Ordering::Relaxed)
            );
            self.last_metrics_snapshot_time = Instant::now();
        }

        Ok(())
    }

    pub fn handle_daa_resolution(
        &mut self,
        r: Result<Box<RpcHeader>, RpcHash>,
    ) -> anyhow::Result<()> {
        let _lock = self.reorg_lock.lock();
        let mut wtx = self.tx_keyspace.write_tx()?;
        match r {
            Err(hash) => {
                warn!(block_hash = %hash, "Failed to resolve DAA score for block, decrementing attempt count");
                self.unknown_accepting_daa_partition
                    .decrement_attempt_counts_by_block_hash(&mut wtx, hash)?;
            }
            Ok(header) => {
                self.metrics.set_latest_accepting_block(header.hash);
                self.metrics.increment_daa_resolved();
                let pending_daa = self
                    .unknown_accepting_daa_partition
                    .remove_by_accepting_block_hash(&mut wtx, header.hash)?;
                let Some(pending) = pending_daa else {
                    return Ok(());
                };
                debug!(block_hash = %header.hash, daa_score = %header.daa_score, "Successfully resolved DAA score for block");
                self.block_compact_header_partition.insert_compact_header(
                    &header.hash,
                    header.blue_work,
                    header.daa_score,
                )?;
                let accepting_daa = header.daa_score;
                let accepting_block_hash = header.hash;
                for entry in pending.as_entry_slice()? {
                    let resolution = entry.get_resolution_key()?;
                    let wtx = &mut wtx;
                    match resolution {
                        DaaResolutionLikeKey::HandshakeKey(hk) => {
                            self.tx_id_to_acceptance_partition
                                .remove_by_tx_id(wtx, entry.tx_id)?;
                            self.tx_id_to_acceptance_partition.insert_handshake_wtx(
                                wtx,
                                entry.tx_id,
                                &hk,
                                Some(accepting_daa),
                                Some(accepting_block_hash.as_bytes()),
                            );
                            self.pending_sender_resolution_partition
                                .mark_handshake_pending(wtx, accepting_daa, entry.tx_id, &hk)?
                        }
                        DaaResolutionLikeKey::ContextualMessageKey(cmk) => {
                            self.tx_id_to_acceptance_partition
                                .remove_by_tx_id(wtx, entry.tx_id)?;
                            self.tx_id_to_acceptance_partition
                                .insert_contextual_message_wtx(
                                    wtx,
                                    entry.tx_id,
                                    &cmk,
                                    Some(accepting_daa),
                                    Some(accepting_block_hash.as_bytes()),
                                );
                            self.pending_sender_resolution_partition
                                .mark_contextual_message_pending(
                                    wtx,
                                    accepting_daa,
                                    entry.tx_id,
                                    &cmk,
                                )?
                        }
                        DaaResolutionLikeKey::PaymentKey(pmk) => {
                            self.tx_id_to_acceptance_partition
                                .remove_by_tx_id(wtx, entry.tx_id)?;
                            self.tx_id_to_acceptance_partition.insert_payment_wtx(
                                wtx,
                                entry.tx_id,
                                &pmk,
                                Some(accepting_daa),
                                Some(accepting_block_hash.as_bytes()),
                            );
                            self.pending_sender_resolution_partition
                                .mark_payment_pending(wtx, accepting_daa, entry.tx_id, &pmk)?
                        }
                    }
                }
            }
        }

        wtx.commit()?
            .context("failed to commit, conflict handle_daa_resolution")?;
        Ok(())
    }

    pub fn handle_sender_resolution(
        &mut self,
        r: Result<(RpcAddress, SenderByTxIdAndDaa), SenderByTxIdAndDaa>,
    ) -> anyhow::Result<()> {
        let _lock = self.reorg_lock.lock();
        let mut wtx = self.tx_keyspace.write_tx()?;
        let rtx = self.tx_keyspace.read_tx();
        match r {
            Ok((address, SenderByTxIdAndDaa { tx_id, daa_score })) => {
                self.metrics.increment_senders_resolved();
                let sender = (&address).try_into()?;
                self.tx_id_to_acceptance_partition.resolve_by_raw_keys(
                    &mut wtx,
                    self.tx_id_to_acceptance_partition
                        .raw_keys_by_txid(&rtx, tx_id.as_bytes()),
                )?;
                trace!(%tx_id, %daa_score, "Successfully resolved sender for transaction");
                for (_partition_id, key) in self
                    .pending_sender_resolution_partition
                    .remove_pending(&mut wtx, daa_score, tx_id.as_ref())?
                {
                    match key {
                        SenderResolutionLikeKey::HandshakeKey(hk) => {
                            self.handshake_by_sender_partition.insert_wtx(
                                &mut wtx,
                                &HandshakeKeyBySender {
                                    sender,
                                    block_time: hk.block_time,
                                    block_hash: hk.block_hash,
                                    receiver: hk.receiver,
                                    version: hk.version,
                                    tx_id: hk.tx_id,
                                },
                            )?;
                            self.handshake_by_receiver_partition.insert_wtx(
                                &mut wtx,
                                &HandshakeKeyByReceiver {
                                    receiver: hk.receiver,
                                    block_time: hk.block_time,
                                    block_hash: hk.block_hash,
                                    version: hk.version,
                                    tx_id: hk.tx_id,
                                },
                                Some(sender),
                            )
                        }
                        SenderResolutionLikeKey::ContextualMessageKey(cmk) => {
                            self.contextual_message_by_sender_partition.update_sender(
                                &mut wtx,
                                Default::default(),
                                sender,
                                &cmk.alias,
                                cmk.block_time,
                                cmk.block_hash,
                                cmk.version,
                                cmk.tx_id,
                            )?;
                        }
                        SenderResolutionLikeKey::PaymentKey(pmk) => {
                            self.payment_by_sender_partition.insert_wtx(
                                &mut wtx,
                                &PaymentKeyBySender {
                                    sender,
                                    block_time: pmk.block_time,
                                    block_hash: pmk.block_hash,
                                    receiver: pmk.receiver,
                                    version: pmk.version,
                                    tx_id: pmk.tx_id,
                                },
                            );

                            self.payment_by_receiver_partition.insert_wtx(
                                &mut wtx,
                                &PaymentKeyByReceiver {
                                    receiver: pmk.receiver,
                                    block_time: pmk.block_time,
                                    block_hash: pmk.block_hash,
                                    version: pmk.version,
                                    tx_id: pmk.tx_id,
                                },
                                Some(sender),
                            );
                        }
                    }
                }
            }
            Err(SenderByTxIdAndDaa { tx_id, daa_score }) => {
                warn!(%tx_id, %daa_score, "Failed to resolve sender for transaction, decrementing attempt count");
                self.pending_sender_resolution_partition
                    .decrement_attempt_counts_by_transaction(&mut wtx, daa_score, tx_id)?;
            }
        }
        wtx.commit()?
            .context("failed to commit, conflict sender_resolution")?;
        Ok(())
    }

    fn resolve_unknown_tx(&self) -> anyhow::Result<()> {
        let _g = self.reorg_lock.lock();
        let rtx = self.tx_keyspace.read_tx();
        let mut wtx = self.tx_keyspace.write_tx()?;

        let mut total_unknown_tx_count = 0;

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
                            processed_any = true;
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
                            found_resolution = true;
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
                let remaining_count = remaining_tx_ids.len();
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
                total_unknown_tx_count += remaining_count as u64;
            } else {
                // If no processing occurred, count all existing transactions as unknown
                total_unknown_tx_count += like_tx_ids.as_tx_ids().len() as u64;
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
        wtx.commit()?
            .context("failed to commit, conflict resolve_unknown_tx")?;

        self.metrics.set_unknown_tx_entries(total_unknown_tx_count);
        Ok(())
    }

    fn unknown_daa(&mut self) -> anyhow::Result<()> {
        let _lock = self.reorg_lock.lock();
        let rtx = self.tx_keyspace.read_tx();
        let mut wtx = self.tx_keyspace.write_tx()?;
        let mut count = 0;
        for block in self
            .unknown_accepting_daa_partition
            .get_all_unknown_accepting_blocks(&rtx)
        {
            let block = block?;
            count += 1;
            match self
                .block_compact_header_partition
                .get_daa_score_rtx(&rtx, &block)?
            {
                None => {
                    debug!(block_hash = %block, "DAA score still not available for block");
                    let _ = self.resolver_request_block_tx.try_send(block).inspect(|_| {
                        self.resolver_requests_in_progress
                            .fetch_add(1, Ordering::Relaxed);
                    });
                }
                Some(daa) => {
                    debug!(block_hash = %block, daa_score = %daa, "DAA score resolved for block");
                    let entries = self
                        .unknown_accepting_daa_partition
                        .remove_by_accepting_block_hash(&mut wtx, block)?;
                    if let Some(entries) = entries {
                        let entries = entries.as_entry_slice()?;
                        let count = entries.len();
                        if count > 0 {
                            debug!(%count, block_hash = %block, "Moving transactions to pending sender resolution queue");
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
        wtx.commit()?
            .context("failed to commit, conflict unknown_daa")?;
        self.metrics.set_unknown_daa_entries(count);
        Ok(())
    }

    fn unknown_sender(&mut self) -> anyhow::Result<()> {
        let rtx = self.tx_keyspace.read_tx();
        let mut count = 0;
        for pending in self
            .pending_sender_resolution_partition
            .get_all_pending_keys(&rtx)
        {
            let PendingResolutionKey {
                accepting_daa_score,
                tx_id,
                ..
            } = pending?;
            count += 1;
            _ = self
                .resolver_request_sender_tx
                .try_send(SenderByTxIdAndDaa {
                    tx_id: RpcTransactionId::from_bytes(tx_id),
                    daa_score: u64::from_be_bytes(accepting_daa_score),
                })
                .inspect(|_| {
                    self.resolver_requests_in_progress
                        .fetch_add(1, Ordering::Relaxed);
                });
        }
        self.metrics.set_unknown_sender_entries(count);
        Ok(())
    }

    /// Prune skipped transactions based on DAA score threshold.
    /// Uses 3x finality depth (1.296M blocks ≈ 36 hours) to prevent unbounded growth
    /// while maintaining all data needed for historical/real-time sync coordination.
    /// Caches latest accepting block for cases where header isn't available during sync.
    fn prune_skip_transactions(&mut self) -> anyhow::Result<()> {
        // Get latest accepting block cursor
        let Some(latest_cursor) = self
            .metadata_partition
            .get_latest_accepting_block_cursor()?
        else {
            // No latest accepting block yet, nothing to prune
            return Ok(());
        };

        // Try to get DAA score from block compact header partition
        let current_daa = match self
            .block_compact_header_partition
            .get_daa_score(latest_cursor.hash)?
        {
            Some(daa) => {
                // Update cache only after successfully getting header
                self.cached_latest_accepting_block = Some(latest_cursor.hash);
                daa
            }
            None => {
                // If current header not available, try using cached block from previous tick
                if let Some(cached_hash) = self.cached_latest_accepting_block {
                    match self
                        .block_compact_header_partition
                        .get_daa_score(cached_hash)?
                    {
                        Some(daa) => {
                            debug!(cached_hash = %cached_hash, "Using cached block hash for pruning");
                            daa
                        }
                        None => {
                            debug!(
                                "DAA score not available for both current and cached blocks, skipping pruning"
                            );
                            return Ok(());
                        }
                    }
                } else {
                    debug!("DAA score not available and no cached block, skipping pruning");
                    return Ok(());
                }
            }
        };

        // Finality depth is 432K blocks (12 hours), use 3x for pruning (36 hours)
        // This provides 1.5x the pruning period to handle sync scenarios
        const FINALITY_DEPTH: u64 = 432_000;
        const PRUNING_DEPTH: u64 = FINALITY_DEPTH * 3; // 1.296M blocks ≈ 36 hours

        let prune_before_daa = current_daa.saturating_sub(PRUNING_DEPTH);
        let prune_before_daa_bytes = prune_before_daa.to_be_bytes();

        debug!(current_daa = %current_daa, prune_before_daa = %prune_before_daa, "Starting skip transaction pruning");

        let rtx = self.tx_keyspace.read_tx();
        let mut wtx = self.tx_keyspace.write_tx()?;

        let mut pruned_count = 0;
        let mut pruned_blocks = 0;

        // Delete entries during iteration to avoid allocations
        for entry_result in self
            .skip_tx_by_block_partition
            .get_entries_to_prune(&rtx, prune_before_daa_bytes)
        {
            let (raw_key, tx_ids_view) = entry_result.context("Failed to get entry to prune")?;

            // Remove individual skip transactions from the main skip partition
            for tx_id in tx_ids_view.as_tx_ids() {
                self.skip_tx_partition.remove_skip(&mut wtx, *tx_id);
            }

            pruned_count += tx_ids_view.as_tx_ids().len();
            pruned_blocks += 1;

            // Remove the block entry from the skip-by-block partition
            self.skip_tx_by_block_partition
                .remove_by_raw_key(&mut wtx, raw_key);
        }

        if pruned_count > 0 {
            info!(
                pruned_blocks = %pruned_blocks,
                pruned_tx_count = %pruned_count,
                prune_before_daa = %prune_before_daa,
                "Pruned old skipped transactions"
            );
        }

        wtx.commit()?
            .context("failed to commit, conflict prune_skip_transactions")?;

        Ok(())
    }
}
