pub mod message;

use crate::data_source::Command;
use crate::virtual_chain_syncer::{NotificationAck, VirtualChainSyncer};
use fjall::{TxKeyspace, WriteTransaction};
use indexer_db::metadata::{Cursor as DbCursor, MetadataPartition};
use kaspa_consensus_core::BlueWorkType;
use kaspa_rpc_core::{
    GetVirtualChainFromBlockResponse, RpcAcceptedTransactionIds, RpcHash,
    VirtualChainChangedNotification,
};
pub use message::*;
use std::collections::VecDeque;
use tracing::error;
use workflow_core::channel::Sender;

pub struct VirtualProcessor {
    synced_capacity: usize,
    unsynced_capacity: usize,
    processed_block_tx: flume::Receiver<CompactHeader>,
    realtime_vcc_tx: flume::Receiver<RealTimeVccNotification>,

    syncer_rx: flume::Receiver<SyncVccNotification>,
    syncer_tx: flume::Sender<SyncVccNotification>,
    command_tx: workflow_core::channel::Sender<Command>,

    tx_keyspace: TxKeyspace,
    metadata_partition: MetadataPartition,
    runtime: tokio::runtime::Handle,
}

struct State {
    shared_state: StateShared,
    sync_state: SyncState,
}

impl State {
    fn new() -> Self {
        Self {
            shared_state: StateShared::new(),
            sync_state: SyncState::default(),
        }
    }
}

struct StateShared {
    shutting_down: bool,
    processed_blocks: ringmap::RingMap<[u8; 32], (DaaScore, BlueWorkType)>, // when we get synced keep only blocks in ~10 mins interval. realloc it
    realtime_queue_vcc: VecDeque<VirtualChainChangedNotification>, // perform realloc when sync is finished if queue is too big
}

impl StateShared {
    fn new() -> Self {
        Self {
            shutting_down: false,
            processed_blocks: ringmap::RingMap::new(),
            realtime_queue_vcc: VecDeque::new(),
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
    pub fn process(&mut self) -> anyhow::Result<()> {
        let state = &mut State::new();
        loop {
            match self.select_input()? {
                ProcessedBlockOrVccOrSyncer::Vcc(RealTimeVccNotification::Connected {
                    sink,
                    sink_blue_work,
                    pp,
                }) => {
                    self.handle_connect(state, sink, sink_blue_work, pp)?;
                }
                ProcessedBlockOrVccOrSyncer::Vcc(RealTimeVccNotification::Disconnected) => {
                    state.shared_state.realtime_queue_vcc.clear();
                }
                ProcessedBlockOrVccOrSyncer::Syncer(SyncVccNotification::VirtualChain {
                    syncer_id,
                    virtual_chain,
                }) => {
                    self.handle_syncer_vc(state, syncer_id, virtual_chain)?;
                    // todo: process real time queue if get synced
                }
                ProcessedBlockOrVccOrSyncer::Vcc(RealTimeVccNotification::Shutdown) => {
                    let cont = self.handle_shutdown(state)?;
                    if cont {
                        continue;
                    } else {
                        return Ok(());
                    }
                }
                ProcessedBlockOrVccOrSyncer::Syncer(SyncVccNotification::Stopped { syncer_id }) => {
                    let cont = self.handle_syncer_stopped(state, syncer_id)?;
                    if cont {
                        continue;
                    } else {
                        return Ok(());
                    }
                }
                ProcessedBlockOrVccOrSyncer::Block(ch) => {
                    self.handle_processed_block(state, ch)?;
                    // todo: process real time queue if get synced
                }
                ProcessedBlockOrVccOrSyncer::Vcc(RealTimeVccNotification::Notification(vcc)) => {
                    self.handle_realtime_vcc(state, vcc)?;
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

    // fn handle_processed_block(&mut self, block: [u8; 32], daa_score: DaaScore, synced: bool) {
    // let capacity_target = if synced {
    //     self.synced_capacity
    // } else {
    //     self.unsynced_capacity
    // };
    // let need_to_delete = (self.processed_blocks.len() + 1).saturating_sub(capacity_target);
    // (0..need_to_delete).for_each(|_| {
    //     self.processed_blocks.pop_front();
    // });
    // self.processed_blocks.push_back(block, daa_score);
    // }

    fn handle_connect(
        &self,
        state: &mut State,
        sink: [u8; 32],
        sink_blue_work: BlueWorkType,
        pp: [u8; 32],
    ) -> anyhow::Result<()> {
        match &mut state.sync_state {
            SyncState::Initial => {
                let last_accepting_block = self.last_accepting_block_db()?;
                match last_accepting_block {
                    None => {
                        let syncer = self.spawn_syncer(0, pp);
                        state.sync_state = SyncState::Syncing {
                            last_accepting_block: (pp, Default::default()),
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
                    state.shared_state.realtime_queue_vcc.push_back(vcc);
                } else {
                    let (last_block, last_blue_work) =
                        self.handle_vcc(&state.shared_state, &vcc)?;
                    *last_accepting_block = last_block;
                    *last_accepting_blue_work = last_blue_work;
                }
            }
            SyncState::Syncing { .. } => {
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
            SyncState::Synced { last_syncer_id, .. } if syncer_id == *last_syncer_id => Ok(false),
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
                .inspect_err(|_| error!("Error in syncer"));
        });
        ack_tx
    }

    fn handle_vcc(
        &self,
        state: &StateShared,
        vcc: &VirtualChainChangedNotification,
    ) -> anyhow::Result<([u8; 32], BlueWorkType)> {
        let mut wtx = self.tx_keyspace.write_tx()?;
        for block in vcc.removed_chain_block_hashes.as_slice() {
            self.handle_vcc_removal(&mut wtx, block)?;
        }
        for block in vcc.accepted_transaction_ids.as_slice() {
            self.handle_vcc_addition(&mut wtx, block)?;
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
        wtx.commit()??;
        Ok((last_block, *last_blue_work))
    }
    fn handle_vc_resp(
        &self,
        state: &StateShared,
        vcc: GetVirtualChainFromBlockResponse,
    ) -> anyhow::Result<([u8; 32], BlueWorkType)> {
        let mut wtx = self.tx_keyspace.write_tx()?;
        for block in vcc.removed_chain_block_hashes.as_slice() {
            self.handle_vcc_removal(&mut wtx, block)?;
        }
        for block in vcc.accepted_transaction_ids.as_slice() {
            self.handle_vcc_addition(&mut wtx, block)?;
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
        wtx.commit()??;
        Ok((last_block, *last_blue_work))
    }

    fn handle_vcc_removal(
        &self,
        _wtx: &mut WriteTransaction,
        _block: &RpcHash,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn handle_vcc_addition(
        &self,
        _wtx: &mut WriteTransaction,
        _block: &RpcAcceptedTransactionIds,
    ) -> anyhow::Result<()> {
        todo!()
    }

    fn handle_processed_block(
        &self,
        state: &mut State,
        compact_header: CompactHeader,
    ) -> anyhow::Result<()> {
        match &mut state.sync_state {
            SyncState::Initial => {
                state.shared_state.processed_blocks.push_back(
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
                state.shared_state.processed_blocks.push_back(
                    compact_header.block_hash,
                    (compact_header.daa_score, compact_header.blue_work),
                );
                while let Some(vcc) = state.shared_state.realtime_queue_vcc.pop_front() {
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
                    .saturating_sub(self.unsynced_capacity);
                (0..need_to_delete).for_each(|_| {
                    state.shared_state.processed_blocks.pop_front();
                });
                state.shared_state.processed_blocks.push_back(
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

type DaaScore = u64;
type Continue = bool;
