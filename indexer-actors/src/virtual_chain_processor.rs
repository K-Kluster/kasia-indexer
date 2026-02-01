pub mod message;

use crate::data_source::{Command, Request};
use crate::util::ToHex64;
use crate::virtual_chain_syncer::{NotificationAck, VirtualChainSyncer};
use fjall::{TxKeyspace, WriteTransaction};
use indexer_db::headers::daa_index::DaaIndexPartition;
use indexer_db::messages::contextual_message::{
    ContextualMessageBySenderKey, ContextualMessageBySenderPartition,
};
use indexer_db::messages::handshake::{
    HandshakeByReceiverPartition, HandshakeBySenderPartition, HandshakeKeyByReceiver,
    HandshakeKeyBySender,
};
use indexer_db::messages::payment::{
    PaymentByReceiverPartition, PaymentBySenderPartition, PaymentKeyByReceiver, PaymentKeyBySender,
};
use indexer_db::messages::self_stash::{SelfStashByOwnerPartition, SelfStashKeyByOwner};
use indexer_db::metadata::{Cursor as DbCursor, MetadataPartition};
use indexer_db::processing::acceptance_gaps::{
    AcceptanceGap as DbAcceptanceGap, AcceptanceGapsPartition,
};
use indexer_db::processing::accepting_block_to_txs::AcceptingBlockToTxIDPartition;
use indexer_db::processing::pending_senders::{
    PendingResolutionKey, PendingSenderResolutionPartition,
};
use indexer_db::processing::tx_id_to_acceptance::{
    Action, LookupOutput, TxIDToAcceptancePartition,
};
use indexer_db::{AddressPayload, PartitionId, TryFromBytes};
use kaspa_consensus_core::BlueWorkType;
use kaspa_rpc_core::{
    GetVirtualChainFromBlockResponse, RpcAcceptedTransactionIds, RpcAddress, RpcHash,
    VirtualChainChangedNotification,
};
pub use message::*;
use std::collections::VecDeque;
use std::time::Instant;
use tracing::{debug, error, info, info_span, trace, warn};
use workflow_core::channel::Sender;

#[derive(bon::Builder)]
pub struct VirtualProcessor {
    synced_capacity: usize,
    processed_block_tx: flume::Receiver<CompactHeader>,
    realtime_vcc_tx: flume::Receiver<RealTimeVccNotification>,

    syncer_rx: flume::Receiver<SyncVccNotification>,
    syncer_tx: flume::Sender<SyncVccNotification>,
    command_tx: workflow_core::channel::Sender<Command>,

    tx_keyspace: TxKeyspace,
    metadata_partition: MetadataPartition,
    acceptance_gaps_partition: AcceptanceGapsPartition,
    daa_index_partition: DaaIndexPartition,
    tx_id_to_acceptance_partition: TxIDToAcceptancePartition,
    accepting_block_to_tx_id_partition: AcceptingBlockToTxIDPartition,
    pending_sender_resolution_partition: PendingSenderResolutionPartition,

    handshake_by_receiver_partition: HandshakeByReceiverPartition,
    handshake_by_sender_partition: HandshakeBySenderPartition,

    contextual_message_by_sender_partition: ContextualMessageBySenderPartition,
    self_stash_by_owner_partition: SelfStashByOwnerPartition,

    payment_by_receiver_partition: PaymentByReceiverPartition,
    payment_by_sender_partition: PaymentBySenderPartition,

    runtime: tokio::runtime::Handle,
}

struct State {
    shared_state: StateShared,
    sync_state: SyncState,
}

impl State {
    fn new(processed_blocks: Vec<CompactHeader>, asc_order: bool) -> Self {
        Self {
            shared_state: StateShared::new(processed_blocks, asc_order),
            sync_state: SyncState::default(),
        }
    }
}

struct StateShared {
    shutting_down: bool,
    processed_blocks: ringmap::RingMap<[u8; 32], (DaaScore, BlueWorkType)>, // when we get synced keep only blocks in ~10 mins interval. realloc it
    realtime_queue_vcc: VecDeque<VirtualChainChangedNotification>, // perform realloc when sync is finished if queue is too big
    processed_time_or_warn: std::time::Instant,
    /// initialized only once on connect, used in case of recovery when we missed past pp data
    pruning_point_at_startup: Option<CompactHeader>,
}

impl StateShared {
    fn new(processed_blocks: Vec<CompactHeader>, asc_order: bool) -> Self {
        let mut latest_known_block = (Default::default(), Default::default());
        let mapper = |CompactHeader {
                          blue_work,
                          block_hash,
                          daa_score,
                      }| {
            if blue_work > latest_known_block.1 {
                latest_known_block = (block_hash, blue_work);
            }
            (block_hash, (daa_score, blue_work))
        };
        let processed_blocks = if asc_order {
            ringmap::RingMap::from_iter(processed_blocks.into_iter().map(mapper))
        } else {
            ringmap::RingMap::from_iter(processed_blocks.into_iter().rev().map(mapper))
        };

        Self {
            shutting_down: false,
            processed_blocks,
            realtime_queue_vcc: VecDeque::new(),
            processed_time_or_warn: Instant::now(),
            pruning_point_at_startup: None,
        }
    }
}

#[derive(Default)]
enum SyncState {
    #[default]
    Initial,
    Synced {
        last_syncer_id: u64,
        last_accepting_block: ([u8; 32], BlueWorkType),
    },
    Syncing {
        last_accepting_block: ([u8; 32], BlueWorkType),
        syncer: workflow_core::channel::Sender<NotificationAck>,
        syncer_id: u64,
        target_block: ([u8; 32], BlueWorkType),
        sync_queue: Option<GetVirtualChainFromBlockResponse>, // when we get disconnect, push to that queue until resyncer finished the job. then move it to first queue, in case of another disconnect - remove it completely
    },
}

impl VirtualProcessor {
    pub fn process(
        &mut self,
        processed_blocks: Vec<CompactHeader>,
        asc_order: bool,
    ) -> anyhow::Result<()> {
        info!("Virtual chain processor started");
        let state = &mut State::new(processed_blocks, asc_order);
        loop {
            match self.select_input()? {
                ProcessedBlockOrVccOrSyncer::Vcc(RealTimeVccNotification::Connected {
                    sink,
                    sink_blue_work,
                    pp_header,
                }) => {
                    info!(
                        sink = %sink.to_hex_64(),
                        pp = %pp_header.block_hash.to_hex_64(),
                        "Received VCC connection notification"
                    );
                    self.handle_connect(state, sink, sink_blue_work, pp_header)?;
                }
                ProcessedBlockOrVccOrSyncer::Vcc(RealTimeVccNotification::Disconnected) => {
                    info!("Received VCC disconnection notification");
                    state.shared_state.realtime_queue_vcc.clear();
                }
                ProcessedBlockOrVccOrSyncer::Syncer(SyncVccNotification::VirtualChain {
                    syncer_id,
                    virtual_chain,
                }) => {
                    debug!(syncer_id, "Received syncer virtual chain notification");
                    self.handle_syncer_vc(state, syncer_id, virtual_chain)?;
                    // todo: process real time queue if get synced
                }
                ProcessedBlockOrVccOrSyncer::Syncer(SyncVccNotification::Pruned {
                    syncer_id,
                    from,
                    err,
                }) => {
                    warn!(
                        syncer_id,
                        from = %from.to_hex_64(),
                        error = %err,
                        "Syncer reported pruned data"
                    );
                    let cont = self.handle_syncer_pruned(state, syncer_id, from)?;
                    if cont {
                        continue;
                    } else {
                        return Ok(());
                    }
                }
                ProcessedBlockOrVccOrSyncer::Vcc(RealTimeVccNotification::Shutdown) => {
                    info!("Received VCC shutdown notification");
                    let cont = self.handle_shutdown(state)?;
                    if cont {
                        continue;
                    } else {
                        return Ok(());
                    }
                }
                ProcessedBlockOrVccOrSyncer::Syncer(SyncVccNotification::Stopped { syncer_id }) => {
                    info!(syncer_id, "Syncer stopped notification received");
                    let cont = self.handle_syncer_stopped(state, syncer_id)?;
                    if cont {
                        continue;
                    } else {
                        info!("shutting down virtual chain processor");
                        return Ok(());
                    }
                }
                ProcessedBlockOrVccOrSyncer::Block(ch) => {
                    // trace!(hash = %ch.block_hash.to_hex_64(), daa_score = ch.daa_score, "Processing block header");
                    self.handle_processed_block(state, ch)?;
                    // todo: process real time queue if get synced
                }
                ProcessedBlockOrVccOrSyncer::Vcc(RealTimeVccNotification::Notification(vcc)) => {
                    self.handle_realtime_vcc(state, vcc)?;
                }
                ProcessedBlockOrVccOrSyncer::Vcc(RealTimeVccNotification::SenderResolution {
                    sender,
                    tx_id,
                    daa,
                }) => {
                    self.handle_sender_address(sender, tx_id, daa)?;
                }
            }
        }
    }

    fn select_input(&self) -> anyhow::Result<ProcessedBlockOrVccOrSyncer> {
        Ok(flume::Selector::new()
            .recv(&self.processed_block_tx, |r| {
                r.map(ProcessedBlockOrVccOrSyncer::from)
            })
            .recv(&self.realtime_vcc_tx, |r| {
                r.map(ProcessedBlockOrVccOrSyncer::from)
            })
            .recv(&self.syncer_rx, |r| {
                r.map(ProcessedBlockOrVccOrSyncer::from)
            })
            .wait()?)
    }

    fn handle_connect(
        &self,
        state: &mut State,
        sink: [u8; 32],
        sink_blue_work: BlueWorkType,
        pp_header: CompactHeader,
    ) -> anyhow::Result<()> {
        state.shared_state.pruning_point_at_startup = Some(pp_header);
        debug!("Handling virtual chain connection, requesting all pending senders");
        match &mut state.sync_state {
            SyncState::Initial => {
                self.pending_sender_resolution_partition
                    .get_all_pending()
                    .try_for_each(|r| -> anyhow::Result<()> {
                        let key = r?;
                        self.command_tx.send_blocking(Command::Request(
                            Request::RequestSender {
                                daa_score: key.accepting_daa_score.get(),
                                tx_id: key.tx_id,
                            },
                        ))?;
                        Ok(())
                    })?;

                let last_accepting_block = self.last_accepting_block_db()?;
                debug!(last_accepting_block = ?last_accepting_block, "Checked last accepting block from database");
                match last_accepting_block {
                    None => {
                        let pp_hash = pp_header.block_hash;
                        info!(pp = %pp_hash.to_hex_64(), "No last accepting block, starting sync from pruning point");
                        let syncer = self.spawn_syncer(0, pp_hash);
                        state.sync_state = SyncState::Syncing {
                            last_accepting_block: (pp_hash, Default::default()),
                            syncer,
                            syncer_id: 0,
                            target_block: (sink, sink_blue_work),
                            sync_queue: None,
                        };
                        Ok(())
                    }
                    Some(Cursor {
                        blue_work,
                        block_hash,
                        ..
                    }) => {
                        let syncer = self.spawn_syncer(0, block_hash);
                        state.sync_state = SyncState::Syncing {
                            last_accepting_block: (block_hash, blue_work),
                            syncer,
                            syncer_id: 0,
                            target_block: (sink, sink_blue_work),
                            sync_queue: None,
                        };
                        Ok(())
                    }
                }
            }
            SyncState::Syncing {
                target_block: (target_block, target_blue_work),
                ..
            } => {
                *target_block = sink;
                *target_blue_work = sink_blue_work;
                Ok(())
            }
            SyncState::Synced {
                last_syncer_id,
                last_accepting_block: (last_accepting_block, last_accepting_blue_work),
            } => {
                if last_accepting_block == &sink || *last_accepting_blue_work > sink_blue_work {
                    // log, do nothing, we are synced
                    Ok(())
                } else {
                    // that branch is possible only if we get disconnected right before synced state
                    let syncer = self.spawn_syncer(*last_syncer_id + 1, *last_accepting_block);
                    state.sync_state = SyncState::Syncing {
                        last_accepting_block: (*last_accepting_block, *last_accepting_blue_work),
                        syncer,
                        syncer_id: *last_syncer_id + 1,
                        target_block: (sink, sink_blue_work),
                        sync_queue: None,
                    };
                    Ok(())
                }
            }
        }
    }

    fn handle_shutdown(&self, state: &mut State) -> anyhow::Result<Continue> {
        state.shared_state.shutting_down = true;
        match &mut state.sync_state {
            SyncState::Initial => Ok(false),
            SyncState::Synced { .. } => Ok(false),
            SyncState::Syncing { syncer, .. } => {
                syncer.send_blocking(NotificationAck::Stop)?;
                Ok(true)
            }
        }
    }

    fn handle_syncer_vc(
        &self,
        state: &mut State,
        notification_syncer_id: u64,
        vcc: GetVirtualChainFromBlockResponse,
    ) -> anyhow::Result<()> {
        let SyncState::Syncing {
            syncer,
            syncer_id,
            target_block: (target_block, target_blue_work),
            sync_queue,
            last_accepting_block,
        } = &mut state.sync_state
        else {
            unreachable!()
        };
        assert_eq!(notification_syncer_id, *syncer_id);
        if vcc.removed_chain_block_hashes.is_empty() && vcc.added_chain_block_hashes.is_empty() {
            syncer.send_blocking(NotificationAck::Continue)?;
            return Ok(());
        }
        if vcc.added_chain_block_hashes.iter().any(|hash| {
            !state
                .shared_state
                .processed_blocks
                .contains_key(&hash.as_bytes())
        }) {
            if state
                .shared_state
                .processed_time_or_warn
                .elapsed()
                .as_secs()
                > 120
            {
                state.shared_state.processed_time_or_warn = Instant::now();
                warn!("We don't process syncer vcc for a long time");
                // todo force request required blocks
            }
            assert!(sync_queue.is_none());
            sync_queue.replace(vcc);
            return Ok(());
        }

        let last = vcc.added_chain_block_hashes.last().unwrap().as_bytes();
        if &last == target_block
            || state.shared_state.processed_blocks.get(&last).unwrap().1 > *target_blue_work
        {
            // todo shrink queues
            syncer.send_blocking(NotificationAck::Stop)?;
            let last_accepting_block = self.handle_vc_resp(&state.shared_state, vcc)?;
            state.sync_state = SyncState::Synced {
                last_syncer_id: *syncer_id,
                last_accepting_block,
            };
        } else {
            let last = self.handle_vc_resp(&state.shared_state, vcc)?;
            state.shared_state.processed_time_or_warn = Instant::now();
            *last_accepting_block = last;
            syncer.send_blocking(NotificationAck::Continue)?;
        }

        Ok(())
    }

    fn handle_realtime_vcc(
        &self,
        state: &mut State,
        vcc: VirtualChainChangedNotification,
    ) -> anyhow::Result<()> {
        match &mut state.sync_state {
            SyncState::Initial => unreachable!(),
            SyncState::Synced {
                last_accepting_block: (last_accepting_block, last_accepting_blue_work),
                ..
            } => {
                if vcc.removed_chain_block_hashes.is_empty()
                    && vcc.added_chain_block_hashes.is_empty()
                {
                    return Ok(());
                }
                if !state.shared_state.realtime_queue_vcc.is_empty()
                    || vcc.added_chain_block_hashes.iter().any(|hash| {
                        !state
                            .shared_state
                            .processed_blocks
                            .contains_key(&hash.as_bytes())
                    })
                {
                    if state
                        .shared_state
                        .processed_time_or_warn
                        .elapsed()
                        .as_secs()
                        > 120
                    {
                        state.shared_state.processed_time_or_warn = Instant::now();
                        warn!("We don't process real time vcc for a long time");
                        // todo force request required blocks
                    }
                    state.shared_state.realtime_queue_vcc.push_back(vcc);
                } else {
                    let (last_block, last_blue_work) =
                        self.handle_vcc(&state.shared_state, &vcc)?;
                    debug!(last_block = %last_block.to_hex_64(),"Realtime vcc is handled");
                    state.shared_state.processed_time_or_warn = Instant::now();
                    *last_accepting_block = last_block;
                    *last_accepting_blue_work = last_blue_work;
                }
            }
            SyncState::Syncing { .. } => {
                trace!("Queue realtime vcc because we are syncing");
                state.shared_state.realtime_queue_vcc.push_back(vcc);
            }
        }
        Ok(())
    }

    fn last_accepting_block_db(&self) -> anyhow::Result<Option<Cursor>> {
        self.metadata_partition
            .get_latest_accepting_block_cursor()
            .map(|opt| opt.map(Into::into))
    }

    fn handle_syncer_stopped(&self, state: &mut State, syncer_id: u64) -> anyhow::Result<Continue> {
        match &mut state.sync_state {
            SyncState::Initial => {
                unreachable!()
            }
            SyncState::Synced { last_syncer_id, .. }
                if syncer_id == *last_syncer_id && state.shared_state.shutting_down =>
            {
                Ok(false)
            }
            SyncState::Syncing {
                syncer_id: current_syncer_id,
                ..
            } if syncer_id == *current_syncer_id => {
                if state.shared_state.shutting_down {
                    Ok(false)
                } else {
                    anyhow::bail!("Syncer {} stopped but we are still syncing", syncer_id)
                }
            }
            // ignore previous syncers
            _ => Ok(true),
        }
    }

    /// If: vcc is syncing, syncer is noted as in progress by state, app isn't shutting down
    ///     pruning point is known
    /// Then, try to recover:
    ///     record the acceptance gap, clear acceptance data in that range,
    ///     and start a new vcc syncer from pp
    /// TODO: same as block processor: this isn't reliable as pp might move on the next block filling task
    fn handle_syncer_pruned(
        &self,
        state: &mut State,
        syncer_id: u64,
        from: [u8; 32],
    ) -> anyhow::Result<Continue> {
        let (current_syncer_id, (target_hash, target_blue_work)) = match &state.sync_state {
            SyncState::Syncing {
                syncer_id,
                target_block,
                ..
            } => (*syncer_id, *target_block),
            _ => return Ok(true),
        };
        if syncer_id != current_syncer_id {
            return Ok(true);
        }
        if state.shared_state.shutting_down {
            return Ok(false);
        }
        let Some(pruning_point_header) = state.shared_state.pruning_point_at_startup else {
            anyhow::bail!("Cannot recover from pruned sync: pruning point header missing");
        };

        let latest_cursor = self
            .metadata_partition
            .get_latest_accepting_block_cursor()?;
        let Some(cursor) = latest_cursor else {
            anyhow::bail!("Cannot recover from pruned sync: latest accepting cursor missing");
        };
        let from_daa = cursor.daa_score.get();
        let pruning_point = pruning_point_header.block_hash;
        let to_daa = pruning_point_header.daa_score;
        if to_daa < from_daa {
            anyhow::bail!(
                "Cannot recover from pruned sync: invalid gap range (from_daa {from_daa} > to_daa {to_daa})"
            );
        }

        self.record_acceptance_gap(cursor.block_hash, from_daa, pruning_point, to_daa)?;

        let PurgeAcceptanceGapReport {
            block_count,
            txs_count,
        } = self.purge_acceptance_gap_data(from_daa, to_daa)?;
        info!(
            from_daa,
            to_daa, block_count, txs_count, "Acceptance recovery cleanup completed"
        );

        let new_syncer_id = syncer_id + 1;
        let syncer = self.spawn_syncer(new_syncer_id, pruning_point_header.block_hash);
        state.shared_state.realtime_queue_vcc.clear();
        state.shared_state.processed_time_or_warn = Instant::now();
        state.sync_state = SyncState::Syncing {
            last_accepting_block: (pruning_point_header.block_hash, Default::default()),
            syncer,
            syncer_id: new_syncer_id,
            target_block: (target_hash, target_blue_work),
            sync_queue: None,
        };
        info!(
            from = %from.to_hex_64(),
            pp = %pruning_point_header.block_hash.to_hex_64(),
            "Restarted syncer from pruning point after error"
        );
        Ok(true)
    }

    fn record_acceptance_gap(
        &self,
        from_hash: [u8; 32],
        from_daa: u64,
        to_hash: [u8; 32],
        to_daa: u64,
    ) -> anyhow::Result<()> {
        loop {
            let mut wtx = self.tx_keyspace.write_tx()?;
            self.acceptance_gaps_partition.add_gap_wtx(
                &mut wtx,
                DbAcceptanceGap {
                    from_daa,
                    to_daa,
                    from_block_hash: from_hash,
                    to_block_hash: to_hash,
                },
            );
            self.metadata_partition
                .remove_latest_accepting_block_cursor(&mut wtx)?;
            if wtx.commit()?.is_ok() {
                break;
            } else {
                debug!("Conflict detected while persisting acceptance gap");
            }
        }
        Ok(())
    }

    // TODO: I'm not convinced by the introduced complexity here
    // I assume the number of data here should be low
    fn purge_acceptance_gap_data(
        &self,
        from_daa: u64,
        to_daa: u64,
    ) -> anyhow::Result<PurgeAcceptanceGapReport> {
        if from_daa > to_daa {
            anyhow::bail!("Invalid acceptance gap range (from_daa {from_daa} > to_daa {to_daa})");
        }
        let rtx = self.tx_keyspace.read_tx();
        let mut blocks = Vec::new();
        let upper = to_daa.saturating_add(1);
        for key in self.daa_index_partition.iter_lt(&rtx, upper) {
            let key = key?;
            let daa = key.daa_score.get();
            if daa < from_daa {
                continue;
            }
            blocks.push((daa, key.block_hash));
        }
        drop(rtx);

        if blocks.is_empty() {
            return Ok(PurgeAcceptanceGapReport {
                block_count: 0,
                txs_count: 0,
            });
        }

        let mut cleared_blocks = 0u64;
        let mut cleared_txs = 0u64;
        const BATCH: usize = 256;
        for chunk in blocks.chunks(BATCH) {
            const MAX_RETRY_CIRCUIT_BREAKER: u8 = u8::MAX;
            let mut iteration_count = 0;
            loop {
                let mut wtx = self.tx_keyspace.write_tx()?;
                let mut chunk_blocks = 0u64;
                let mut chunk_txs = 0u64;
                for (daa, block_hash) in chunk {
                    let Some(tracked_tx_ids) = self
                        .accepting_block_to_tx_id_partition
                        .remove_wtx(&mut wtx, block_hash)?
                    else {
                        continue;
                    };
                    chunk_blocks += 1;
                    for tx_id in tracked_tx_ids.as_ref() {
                        let Some(key) = self.tx_id_to_acceptance_partition.key_by_tx_id(tx_id)?
                        else {
                            continue;
                        };
                        let _ = self
                            .tx_id_to_acceptance_partition
                            .clear_acceptance_wtx(&mut wtx, &key)?;
                        self.pending_sender_resolution_partition.remove_wtx(
                            &mut wtx,
                            &PendingResolutionKey {
                                accepting_daa_score: (*daa).into(),
                                tx_id: *tx_id,
                            },
                        );
                        chunk_txs += 1;
                    }
                }
                if wtx.commit()?.is_ok() {
                    cleared_blocks += chunk_blocks;
                    cleared_txs += chunk_txs;
                    break;
                } else {
                    debug!("Conflict detected while clearing acceptance window");
                }

                if iteration_count == MAX_RETRY_CIRCUIT_BREAKER {
                    // not sure how to handle this.
                    // throwing or not: will leave dangling data
                    anyhow::bail!(
                        "Failed to clear acceptance window after {} tries.",
                        MAX_RETRY_CIRCUIT_BREAKER
                    );
                }

                iteration_count += 1;
            }
        }

        Ok(PurgeAcceptanceGapReport {
            block_count: cleared_blocks,
            txs_count: cleared_txs,
        })
    }

    fn spawn_syncer(&self, syncer_id: u64, from: [u8; 32]) -> Sender<NotificationAck> {
        let (ack_tx, ack_rx) = workflow_core::channel::bounded(1);
        let syncer = VirtualChainSyncer::new(
            syncer_id,
            from,
            self.syncer_tx.clone(),
            ack_rx,
            self.command_tx.clone(),
        );
        self.runtime.spawn(async move {
            _ = syncer
                .process()
                .await
                .inspect_err(|err| error!("Error in syncer: {err}"));
        });
        ack_tx
    }

    fn handle_vcc(
        &self,
        state: &StateShared,
        vcc: &VirtualChainChangedNotification,
    ) -> anyhow::Result<([u8; 32], BlueWorkType)> {
        let resp = loop {
            let mut wtx = self.tx_keyspace.write_tx()?;
            for block in vcc.removed_chain_block_hashes.as_slice() {
                self.handle_vcc_removal(&mut wtx, block, state)?;
            }
            for block in vcc.accepted_transaction_ids.as_slice() {
                self.handle_vcc_addition(&mut wtx, block, state)?;
            }
            let last_block = vcc.added_chain_block_hashes.last().unwrap().as_bytes();
            let (last_daa, last_blue_work) = state.processed_blocks.get(&last_block).unwrap();
            self.metadata_partition.set_latest_accepting_block_cursor(
                &mut wtx,
                Cursor {
                    blue_work: *last_blue_work,
                    block_hash: last_block,
                    daa_score: *last_daa,
                }
                .into(),
            )?;
            if wtx.commit()?.is_ok() {
                break (last_block, *last_blue_work);
            } else {
                warn!("conflict detected, retry handling realtime vcc")
            }
        };
        Ok(resp)
    }
    fn handle_vc_resp(
        &self,
        state: &StateShared,
        vcc: GetVirtualChainFromBlockResponse,
    ) -> anyhow::Result<([u8; 32], BlueWorkType)> {
        let resp = loop {
            let mut wtx = self.tx_keyspace.write_tx()?;
            for block in vcc.removed_chain_block_hashes.as_slice() {
                self.handle_vcc_removal(&mut wtx, block, state)?;
            }
            for block in vcc.accepted_transaction_ids.as_slice() {
                self.handle_vcc_addition(&mut wtx, block, state)?;
            }
            let last_block = vcc.added_chain_block_hashes.last().unwrap().as_bytes();
            let (last_daa, last_blue_work) = state.processed_blocks.get(&last_block).unwrap();
            self.metadata_partition.set_latest_accepting_block_cursor(
                &mut wtx,
                Cursor {
                    blue_work: *last_blue_work,
                    block_hash: last_block,
                    daa_score: *last_daa,
                }
                .into(),
            )?;
            if wtx.commit()?.is_ok() {
                break (last_block, *last_blue_work);
            } else {
                warn!("conflict detected, retry handling sync vc")
            }
        };
        Ok(resp)
    }

    fn handle_vcc_removal(
        &self,
        wtx: &mut WriteTransaction,
        block: &RpcHash,
        state_shared: &StateShared,
    ) -> anyhow::Result<()> {
        let block = block.as_bytes();
        let Some(tracked_tx_ids) = self
            .accepting_block_to_tx_id_partition
            .remove_wtx(wtx, &block)?
        else {
            warn!(
                "Block {} not found in accepting_block_to_tx_id_partition for removal",
                block.to_hex_64()
            );
            return Ok(());
        };
        let daa = state_shared.processed_blocks.get(&block).unwrap().0;

        for tx_id in tracked_tx_ids.as_ref() {
            let key = self
                .tx_id_to_acceptance_partition
                .key_by_tx_id(tx_id)?
                .expect("Key must exists");
            let lookup_results = self
                .tx_id_to_acceptance_partition
                .update_acceptance_wtx(wtx, &key, block, daa)?;
            if let LookupOutput::KeysExistsWithEntries = lookup_results {
                self.pending_sender_resolution_partition.remove_wtx(
                    wtx,
                    &PendingResolutionKey {
                        accepting_daa_score: daa.into(),
                        tx_id: *tx_id,
                    },
                )
            }
        }
        Ok(())
    }

    fn handle_vcc_addition(
        &self,
        wtx: &mut WriteTransaction,
        block: &RpcAcceptedTransactionIds,
        state: &StateShared,
    ) -> anyhow::Result<()> {
        let mut tracked_tx_ids = Vec::new();
        let block_hash = block.accepting_block_hash.as_bytes();
        let daa = state.processed_blocks.get(&block_hash).unwrap().0;
        for tx in &block.accepted_transaction_ids {
            let Some(key) = self
                .tx_id_to_acceptance_partition
                .key_by_tx_id(tx.as_ref())?
            else {
                continue;
            };
            let lookup_results = self
                .tx_id_to_acceptance_partition
                .update_acceptance_wtx(wtx, &key, block_hash, daa)?;
            match lookup_results {
                LookupOutput::KeyDoesNotExist => {}
                LookupOutput::KeyExistsNoEntries => {
                    tracked_tx_ids.push(key.tx_id);
                }
                LookupOutput::KeysExistsWithEntries => {
                    tracked_tx_ids.push(key.tx_id);
                    debug!("request sender for: {} {}", daa, tx);
                    self.command_tx
                        .send_blocking(Command::Request(Request::RequestSender {
                            daa_score: daa,
                            tx_id: tx.as_bytes(),
                        }))?;
                    self.pending_sender_resolution_partition.insert_wtx(
                        wtx,
                        &PendingResolutionKey {
                            accepting_daa_score: daa.into(),
                            tx_id: tx.as_bytes(),
                        },
                    )
                }
            }
        }
        self.accepting_block_to_tx_id_partition
            .insert_wtx(wtx, &block_hash, &tracked_tx_ids);

        Ok(())
    }

    fn handle_processed_block(
        &self,
        state: &mut State,
        compact_header: CompactHeader,
    ) -> anyhow::Result<()> {
        match &mut state.sync_state {
            SyncState::Initial => {
                state.shared_state.processed_blocks.insert(
                    compact_header.block_hash,
                    (compact_header.daa_score, compact_header.blue_work),
                );
            }
            SyncState::Synced {
                last_accepting_block,
                ..
            } => {
                let need_to_delete = (state.shared_state.processed_blocks.len() + 1)
                    .saturating_sub(self.synced_capacity);
                (0..need_to_delete).for_each(|_| {
                    state.shared_state.processed_blocks.pop_front();
                });
                state.shared_state.processed_blocks.insert(
                    compact_header.block_hash,
                    (compact_header.daa_score, compact_header.blue_work),
                );
                while let Some(vcc) = state.shared_state.realtime_queue_vcc.pop_front() {
                    if vcc.removed_chain_block_hashes.is_empty()
                        && vcc.added_chain_block_hashes.is_empty()
                    {
                        continue;
                    }
                    if vcc.added_chain_block_hashes.iter().any(|hash| {
                        !state
                            .shared_state
                            .processed_blocks
                            .contains_key(&hash.as_bytes())
                    }) {
                        state.shared_state.realtime_queue_vcc.push_front(vcc);
                        break;
                    }
                    let (last_block, last_blue_work) =
                        self.handle_vcc(&state.shared_state, &vcc)?;
                    *last_accepting_block = (last_block, last_blue_work);
                }
            }
            SyncState::Syncing {
                last_accepting_block,
                syncer,
                syncer_id,
                target_block: (target_block, target_blue_work),
                sync_queue,
            } => {
                let need_to_delete = (state.shared_state.processed_blocks.len() + 1)
                    .saturating_sub(self.synced_capacity);
                (0..need_to_delete).for_each(|_| {
                    state.shared_state.processed_blocks.pop_front();
                });
                state.shared_state.processed_blocks.insert(
                    compact_header.block_hash,
                    (compact_header.daa_score, compact_header.blue_work),
                );
                let Some(vcc) = sync_queue.take() else {
                    return Ok(());
                };
                if vcc.added_chain_block_hashes.iter().any(|hash| {
                    !state
                        .shared_state
                        .processed_blocks
                        .contains_key(&hash.as_bytes())
                }) {
                    assert!(sync_queue.is_none());
                    sync_queue.replace(vcc);
                    return Ok(());
                }
                let last = vcc.added_chain_block_hashes.last().unwrap().as_bytes();
                if &last == target_block
                    || state.shared_state.processed_blocks.get(&last).unwrap().1 > *target_blue_work
                {
                    // todo shrink queues
                    syncer.send_blocking(NotificationAck::Stop)?;
                    let last_accepting_block = self.handle_vc_resp(&state.shared_state, vcc)?;
                    state.sync_state = SyncState::Synced {
                        last_syncer_id: *syncer_id,
                        last_accepting_block,
                    };
                } else {
                    let last = self.handle_vc_resp(&state.shared_state, vcc)?;
                    *last_accepting_block = last;
                    syncer.send_blocking(NotificationAck::Continue)?;
                }
            }
        }
        Ok(())
    }

    fn handle_sender_address(
        &self,
        sender: RpcAddress,
        tx_id: [u8; 32],
        daa: DaaScore,
    ) -> anyhow::Result<()> {
        let _span = info_span!("handle sender address", %sender).entered();
        let sender = AddressPayload::try_from(&sender)?;
        let key = self
            .tx_id_to_acceptance_partition
            .key_by_tx_id(&tx_id)?
            .expect("Key must exists");
        loop {
            let mut wtx = self.tx_keyspace.write_tx()?;
            self.pending_sender_resolution_partition.remove_wtx(
                &mut wtx,
                &PendingResolutionKey {
                    accepting_daa_score: daa.into(),
                    tx_id,
                },
            );
            self.tx_id_to_acceptance_partition.resolve_entries_wtx(
                &mut wtx,
                &key,
                |partition_id| match partition_id {
                    PartitionId::Metadata
                    | PartitionId::BlockCompactHeaders
                    | PartitionId::BlockDaaIndex
                    | PartitionId::BlockGaps
                    | PartitionId::TxIdToHandshake
                    | PartitionId::TxIdToPayment
                    | PartitionId::AcceptingBlockToTxIds
                    | PartitionId::TxIdToAcceptance
                    | PartitionId::PendingSenders
                    | PartitionId::TxIDToSelfStash => {
                        panic!("Unexpected partition id")
                    }
                    PartitionId::HandshakeByReceiver => size_of::<HandshakeKeyByReceiver>(),
                    PartitionId::HandshakeBySender => size_of::<HandshakeKeyBySender>(),
                    PartitionId::ContextualMessageBySender => {
                        size_of::<ContextualMessageBySenderKey>()
                    }
                    PartitionId::PaymentByReceiver => size_of::<PaymentKeyByReceiver>(),
                    PartitionId::PaymentBySender => size_of::<PaymentKeyBySender>(),
                    PartitionId::SelfStashByOwner => size_of::<SelfStashKeyByOwner>(),
                },
                |wtx, entry| match entry.partition_id {
                    PartitionId::Metadata
                    | PartitionId::BlockCompactHeaders
                    | PartitionId::BlockDaaIndex
                    | PartitionId::BlockGaps
                    | PartitionId::TxIdToHandshake
                    | PartitionId::TxIdToPayment
                    | PartitionId::AcceptingBlockToTxIds
                    | PartitionId::TxIdToAcceptance
                    | PartitionId::PendingSenders
                    | PartitionId::TxIDToSelfStash => {
                        panic!("Unexpected partition id")
                    }
                    PartitionId::HandshakeByReceiver => {
                        if !matches!(entry.action, Action::UpdateValueSender) {
                            panic!("Unexpected action")
                        }
                        self.handshake_by_receiver_partition.insert_wtx(
                            wtx,
                            HandshakeKeyByReceiver::try_ref_from_bytes(entry.key)
                                .map_err(|_| anyhow::anyhow!("Key conversion error"))?,
                            Some(sender),
                        )?;
                        Ok(())
                    }
                    PartitionId::HandshakeBySender => {
                        if !matches!(entry.action, Action::InsertByKeySender) {
                            panic!("Unexpected action")
                        }
                        let mut key = HandshakeKeyBySender::try_read_from_bytes(entry.key)
                            .map_err(|_| anyhow::anyhow!("Key conversion error"))?;
                        key.sender = sender;
                        self.handshake_by_sender_partition.insert_wtx(wtx, &key);
                        Ok(())
                    }
                    PartitionId::ContextualMessageBySender => {
                        if !matches!(entry.action, Action::InsertByKeySender) {
                            panic!("Unexpected action")
                        }
                        let mut key = ContextualMessageBySenderKey::try_read_from_bytes(entry.key)
                            .map_err(|_| anyhow::anyhow!("Key conversion error"))?;
                        key.sender = sender;
                        self.contextual_message_by_sender_partition
                            .insert(wtx, &key);
                        Ok(())
                    }
                    PartitionId::PaymentByReceiver => {
                        if !matches!(entry.action, Action::UpdateValueSender) {
                            panic!("Unexpected action")
                        }
                        self.payment_by_receiver_partition.insert_wtx(
                            wtx,
                            PaymentKeyByReceiver::try_ref_from_bytes(entry.key)
                                .map_err(|_| anyhow::anyhow!("Key conversion error"))?,
                            Some(sender),
                        )
                    }
                    PartitionId::PaymentBySender => {
                        if !matches!(entry.action, Action::InsertByKeySender) {
                            panic!("Unexpected action")
                        }
                        let mut key = PaymentKeyBySender::try_read_from_bytes(entry.key)
                            .map_err(|_| anyhow::anyhow!("Key conversion error"))?;
                        key.sender = sender;
                        self.payment_by_sender_partition.insert_wtx(wtx, &key);
                        Ok(())
                    }
                    PartitionId::SelfStashByOwner => {
                        if !matches!(entry.action, Action::InsertByKeySender) {
                            panic!("Unexpected action")
                        }
                        let mut key = SelfStashKeyByOwner::try_read_from_bytes(entry.key)
                            .map_err(|_| anyhow::anyhow!("Key conversion error"))?;
                        key.owner = sender;
                        self.self_stash_by_owner_partition.insert_wtx(wtx, &key);
                        Ok(())
                    }
                },
            )?;

            if wtx.commit()?.is_ok() {
                return Ok(());
            } else {
                warn!("Conflict detected, retry handling sender update")
            }
        }
    }
}

impl Drop for VirtualProcessor {
    fn drop(&mut self) {
        _ = self
            .command_tx
            .send_blocking(Command::MarkVccSenderClosed)
            .inspect_err(|_| error!("Error sending command to mark vcc sender closed"));
    }
}

enum ProcessedBlockOrVccOrSyncer {
    Block(CompactHeader),
    Vcc(RealTimeVccNotification),
    Syncer(SyncVccNotification),
}

impl From<CompactHeader> for ProcessedBlockOrVccOrSyncer {
    fn from(value: CompactHeader) -> Self {
        Self::Block(value)
    }
}

impl From<RealTimeVccNotification> for ProcessedBlockOrVccOrSyncer {
    fn from(value: RealTimeVccNotification) -> Self {
        Self::Vcc(value)
    }
}

impl From<SyncVccNotification> for ProcessedBlockOrVccOrSyncer {
    fn from(value: SyncVccNotification) -> Self {
        Self::Syncer(value)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd)]
struct Cursor {
    pub blue_work: BlueWorkType,
    pub block_hash: [u8; 32],
    pub daa_score: u64,
}

impl From<DbCursor> for Cursor {
    fn from(value: DbCursor) -> Self {
        Cursor {
            blue_work: BlueWorkType::from_be_bytes(value.blue_work),
            block_hash: value.block_hash,
            daa_score: value.daa_score.into(),
        }
    }
}

impl From<Cursor> for DbCursor {
    fn from(value: Cursor) -> Self {
        DbCursor {
            blue_work: value.blue_work.to_be_bytes(),
            block_hash: value.block_hash,
            daa_score: value.daa_score.into(),
        }
    }
}

struct PurgeAcceptanceGapReport {
    pub block_count: u64,
    pub txs_count: u64,
}

type DaaScore = u64;
type Continue = bool;

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::type_name;

    #[test]
    fn print_key_sizes() {
        fn print_size<K>() {
            println!("{}:{}", type_name::<K>(), size_of::<K>());
        }

        print_size::<HandshakeKeyByReceiver>();
        print_size::<HandshakeKeyBySender>();
        print_size::<ContextualMessageBySenderKey>();
        print_size::<PaymentKeyByReceiver>();
        print_size::<PaymentKeyBySender>();
        print_size::<SelfStashKeyByOwner>();
    }
}
