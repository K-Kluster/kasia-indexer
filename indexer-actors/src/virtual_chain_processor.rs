pub mod message;

use crate::data_source::Command;
use crate::virtual_chain_syncer::{NotificationAck, VirtualChainSyncer};
use indexer_db::metadata::{Cursor as DbCursor, MetadataPartition};
use kaspa_consensus_core::BlueWorkType;
use kaspa_rpc_core::{GetVirtualChainFromBlockResponse, VirtualChainChangedNotification};
pub use message::*;
use std::collections::VecDeque;
use tokio::runtime::Runtime;
use tracing::error;
use workflow_core::channel::Sender;

pub struct VirtualProcessor {
    synced_capacity: usize,
    unsynced_capacity: usize,
    processed_block_tx: flume::Receiver<([u8; 32], DaaScore)>,
    realtime_vcc_tx: flume::Receiver<RealTimeVccNotification>,

    syncer_rx: flume::Receiver<SyncVccNotification>,
    syncer_tx: flume::Sender<SyncVccNotification>,
    command_tx: workflow_core::channel::Sender<Command>,

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
    processed_blocks: ringmap::RingMap<[u8; 32], DaaScore>, // when we get synced keep only blocks in ~10 mins interval. realloc it
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
                    // do nothing
                }
                ProcessedBlockOrVccOrSyncer::Syncer(SyncVccNotification::VirtualChain {
                    syncer_id,
                    virtual_chain,
                }) => {
                    self.handle_syncer_vc(state, syncer_id, virtual_chain)?;
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
                // ProcessedBlockOrVccOrSyncer::Block { hash, daa_score } => {
                //     self.handle_processed_block(hash, daa_score, synced);
                // }
                // ProcessedBlockOrVccOrSyncer::Vcc(RealTimeVccNotification::Notification(vcc)) => {
                //     self.handle_realtime_vcc(vcc, synced)
                // }
                _ => {
                    todo!()
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
        if vcc.added_chain_block_hashes.iter().all(|hash| {
            state
                .shared_state
                .processed_blocks
                .contains_key(&hash.as_bytes())
        }) {
            let last = vcc.added_chain_block_hashes.last().unwrap().as_bytes();
            if &last == target_block {
                // todo shrink queues
                syncer.send_blocking(NotificationAck::Stop)?;
                state.sync_state = SyncState::Synced {
                    last_syncer_id: *syncer_id,
                    last_accepting_block: *last_accepting_block,
                };
                todo!("handle sync vcc");
                return Ok(());
            };
            let blue_work = self.get_blue_work(&last)?.unwrap();
            if blue_work > target_blue_work.to_be_bytes() {
                // todo shrink queues
                syncer.send_blocking(NotificationAck::Stop)?;
                state.sync_state = SyncState::Synced {
                    last_syncer_id: *syncer_id,
                    last_accepting_block: *last_accepting_block,
                };
                todo!("handle sync vcc");
                return Ok(());
            }
            todo!("handle sync vcc");
            syncer.send_blocking(NotificationAck::Continue)?;
        } else {
            assert!(sync_queue.is_none());
            sync_queue.replace(vcc);
        }
        Ok(())
    }

    // fn handle_realtime_vcc(
    //     &mut self,
    //     VirtualChainChangedNotification {
    //         removed_chain_block_hashes,
    //         added_chain_block_hashes,
    //         accepted_transaction_ids,
    //     }: VirtualChainChangedNotification,
    //     synced: bool,
    // ) -> anyhow::Result<()> {
    //     todo!()
    // }

    fn last_accepting_block_db(&self) -> anyhow::Result<Option<Cursor>> {
        self.metadata_partition
            .get_latest_accepting_block_cursor()
            .map(|opt| opt.map(Into::into))
    }

    fn get_blue_work(&self, _block: &[u8; 32]) -> anyhow::Result<Option<[u8; 24]>> {
        todo!()
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
    Block { hash: [u8; 32], daa_score: u64 },
    Vcc(RealTimeVccNotification),
    Syncer(SyncVccNotification),
}

impl From<([u8; 32], u64)> for ProcessedBlockOrVccOrSyncer {
    fn from((hash, daa_score): ([u8; 32], u64)) -> Self {
        Self::Block { hash, daa_score }
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

type DaaScore = u64;
type Continue = bool;
