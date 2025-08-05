mod message;

use crate::BlockGap;
use crate::block_gap_filler::BlockGapFiller;
use crate::data_source::Command;
use fjall::{TxKeyspace, WriteTransaction};
use indexer_db::headers::block_compact_headers::BlockCompactHeaderPartition;
use indexer_db::headers::block_gaps::BlockGapsPartition;
use indexer_db::headers::daa_index::DaaIndexPartition;
use indexer_db::metadata::MetadataPartition;
use kaspa_rpc_core::{RpcBlock, RpcHeader, RpcTransaction};
pub use message::*;
use std::collections::HashMap;
use tracing::{error, info, trace};

pub struct BlockProcessor {
    notification_rx: flume::Receiver<BlockNotification>,
    gap_result_rx: flume::Receiver<GapFillingProgress>,
    gap_result_tx: flume::Sender<GapFillingProgress>,
    processed_block_tx: flume::Sender<[u8; 32]>,
    command_tx: workflow_core::channel::Sender<Command>,
    tx_keyspace: TxKeyspace,
    blocks_gap_partition: BlockGapsPartition,
    runtime_handle: tokio::runtime::Handle,
    metadata_partition: MetadataPartition,
    block_compact_header_partition: BlockCompactHeaderPartition,
    daa_index_partition: DaaIndexPartition,
}

impl BlockProcessor {
    pub fn process(&mut self) -> anyhow::Result<()> {
        info!("Block worker started");
        let mut has_first_connect = false;
        let mut gaps_fillers = HashMap::new();
        let mut is_shutdown = false;
        let mut last_processed_block = None;
        let mut gaps_filling_in_progress = 0;
        loop {
            match self.select_input()? {
                NotificationOrGapResult::Notification(BlockNotification::Connected {
                    sink,
                    pp,
                }) => {
                    if !has_first_connect {
                        has_first_connect = true;
                        let gaps = self.handle_first_connect(sink, pp)?;
                        gaps_fillers = gaps
                            .into_iter()
                            .map(|gap| {
                                let (interrupt_tx, interrupt_rx) = tokio::sync::oneshot::channel();
                                self.runtime_handle.spawn({
                                    let filler = BlockGapFiller::new(
                                        gap.from_block,
                                        gap.to_block,
                                        self.gap_result_tx.clone(),
                                        self.command_tx.clone(),
                                        interrupt_rx,
                                    );
                                    async move {
                                        _ = filler.sync().await.inspect_err(
                                            |err| error!(%err, "Error in block gap filler"),
                                        );
                                    }
                                });
                                (gap.to_block, interrupt_tx)
                            })
                            .collect();
                        gaps_filling_in_progress = gaps_fillers.len();
                    } else if let Some(last_processed_block) = last_processed_block.take() {
                        self.insert_gap(last_processed_block, sink)?;
                        let (interrupt_tx, interrupt_rx) = tokio::sync::oneshot::channel();
                        self.runtime_handle.spawn({
                            let filler = BlockGapFiller::new(
                                last_processed_block,
                                sink,
                                self.gap_result_tx.clone(),
                                self.command_tx.clone(),
                                interrupt_rx,
                            );
                            async move {
                                _ = filler
                                    .sync()
                                    .await
                                    .inspect_err(|err| error!(%err, "Error in block gap filler"));
                            }
                        });
                        gaps_fillers.insert(sink, interrupt_tx);
                        gaps_filling_in_progress += 1;
                    }
                }
                NotificationOrGapResult::Notification(BlockNotification::Disconnected) => {
                    std::mem::take(&mut gaps_fillers)
                        .into_iter()
                        .for_each(|(_to, interrupt_tx)| {
                            let _ = interrupt_tx.send(()).inspect_err(|_err| {
                                error!("Error sending interrupt to block gap filler")
                            });
                        })
                }
                NotificationOrGapResult::Notification(BlockNotification::Shutdown) => {
                    is_shutdown = true;
                    std::mem::take(&mut gaps_fillers)
                        .into_iter()
                        .for_each(|(_to, interrupt_tx)| {
                            let _ = interrupt_tx.send(()).inspect_err(|_err| {
                                error!("Error sending interrupt to block gap filler")
                            });
                        })
                }
                NotificationOrGapResult::Notification(BlockNotification::Notification(block)) => {
                    let mut wtx = self.tx_keyspace.write_tx()?;
                    self.handle_block(&mut wtx, &block)?;
                    let hash = block.header.hash.as_bytes();
                    last_processed_block = Some(hash);
                    self.update_block(&mut wtx, hash);
                    wtx.commit()??;
                    self.processed_block_tx.send(hash)?;
                }
                NotificationOrGapResult::GapFilling(GapFillingProgress::Interrupted { to }) => {
                    gaps_fillers.remove(&to);
                    gaps_filling_in_progress -= 1;
                }
                NotificationOrGapResult::GapFilling(GapFillingProgress::Error { to, err }) => {
                    error!(%err, "Error in block gap filler");
                    gaps_fillers.remove(&to);
                    gaps_filling_in_progress -= 1;
                }
                NotificationOrGapResult::GapFilling(GapFillingProgress::Update { to, blocks }) => {
                    blocks.iter().try_for_each(|block| -> anyhow::Result<()> {
                        let mut wtx = self.tx_keyspace.write_tx()?;
                        self.handle_block(&mut wtx, block)?;
                        self.blocks_gap_partition.update_gap_wtx(
                            &mut wtx,
                            indexer_db::headers::block_gaps::BlockGap {
                                from: block.header.hash.as_bytes(),
                                to,
                            },
                        );
                        wtx.commit()??;
                        self.processed_block_tx.send(block.header.hash.as_bytes())?;
                        Ok(())
                    })?;
                }
                NotificationOrGapResult::GapFilling(GapFillingProgress::Finished {
                    to,
                    blocks,
                }) => {
                    let last_index = blocks.len() - 1; // todo is it possible that blocks are empty?
                    blocks.iter().enumerate().try_for_each(
                        |(idx, block)| -> anyhow::Result<()> {
                            if idx != last_index {
                                let mut wtx = self.tx_keyspace.write_tx()?;
                                self.handle_block(&mut wtx, block)?;
                                self.blocks_gap_partition.update_gap_wtx(
                                    &mut wtx,
                                    indexer_db::headers::block_gaps::BlockGap {
                                        from: block.header.hash.as_bytes(),
                                        to,
                                    },
                                );
                                wtx.commit()??;
                                self.processed_block_tx.send(block.header.hash.as_bytes())?;
                            } else {
                                let mut wtx = self.tx_keyspace.write_tx()?;
                                self.handle_block(&mut wtx, block)?;
                                self.blocks_gap_partition.remove_gap_wtx(&mut wtx, &to);
                                wtx.commit()??;
                                self.processed_block_tx.send(block.header.hash.as_bytes())?;
                            }
                            Ok(())
                        },
                    )?;
                    gaps_fillers.remove(&to);
                    gaps_filling_in_progress -= 1;
                }
            }
            if is_shutdown && gaps_filling_in_progress == 0 {
                return Ok(());
            }
        }
    }

    pub fn handle_block(
        &mut self,
        wtx: &mut WriteTransaction,
        block: &RpcBlock,
    ) -> anyhow::Result<()> {
        self.block_compact_header_partition.insert_compact_header(
            block.header.hash.as_ref(),
            block.header.blue_work.to_le_bytes(),
            block.header.daa_score,
        )?;
        self.daa_index_partition
            .insert(block.header.daa_score, block.header.hash.as_ref())?;
        for tx in &block.transactions {
            self.handle_transaction(wtx, &block.header, tx)?;
        }
        Ok(())
    }

    fn handle_transaction(
        &self,
        _wtx: &mut WriteTransaction,
        _block_header: &RpcHeader,
        _tx: &RpcTransaction,
    ) -> anyhow::Result<()> {
        todo!("Handle transaction");
        // todo we mustn't overwrite already processed tx. use fetch_update
    }

    fn select_input(&self) -> anyhow::Result<NotificationOrGapResult> {
        trace!("Waiting for new blocks or shutdown signal");
        Ok(flume::Selector::new()
            .recv(&self.notification_rx, |r| {
                r.map(NotificationOrGapResult::from)
            })
            .recv(&self.gap_result_rx, |r| {
                r.map(NotificationOrGapResult::from)
            })
            .wait()?)
    }

    fn handle_first_connect(
        &mut self,
        sink: [u8; 32],
        pp: [u8; 32],
    ) -> anyhow::Result<Vec<BlockGap>> {
        let gap = match self.last_real_time_block()? {
            None => Some(BlockGap {
                from_block: pp,
                to_block: sink,
            }),
            Some(block) if block != sink => Some(BlockGap {
                from_block: block,
                to_block: sink,
            }),
            Some(_block) => None,
        };
        let mut gaps = self
            .blocks_gap_partition
            .get_all_gaps()
            .map(|gap| {
                gap.map(|gap| BlockGap {
                    from_block: gap.from,
                    to_block: gap.to,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(gap) = gap {
            self.insert_gap(gap.from_block, gap.to_block)?;
            gaps.push(gap);
        }
        Ok(gaps)
    }

    fn last_real_time_block(&self) -> anyhow::Result<Option<[u8; 32]>> {
        self.metadata_partition.get_latest_block_cursor()
    }

    fn insert_gap(&self, from: [u8; 32], to: [u8; 32]) -> anyhow::Result<()> {
        self.blocks_gap_partition
            .add_gap(indexer_db::headers::block_gaps::BlockGap { from, to })
    }

    fn update_block(&self, wtx: &mut WriteTransaction, hash: [u8; 32]) {
        self.metadata_partition.set_latest_block_cursor(wtx, hash)
    }
}

#[derive(Debug)]
enum NotificationOrGapResult {
    Notification(BlockNotification),
    GapFilling(GapFillingProgress),
}

impl From<BlockNotification> for NotificationOrGapResult {
    fn from(value: BlockNotification) -> Self {
        Self::Notification(value)
    }
}

impl From<GapFillingProgress> for NotificationOrGapResult {
    fn from(value: GapFillingProgress) -> Self {
        Self::GapFilling(value)
    }
}
