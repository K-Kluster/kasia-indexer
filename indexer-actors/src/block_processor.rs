mod message;

use crate::BlockGap;
use crate::block_gap_filler::BlockGapFiller;
use crate::data_source::Command;
use crate::metrics::SharedMetrics;
use crate::util::{ToHex, ToHex64};
use crate::virtual_chain_processor::CompactHeader;
use fjall::{TxKeyspace, WriteTransaction};
use indexer_db::headers::block_compact_headers::BlockCompactHeaderPartition;
use indexer_db::headers::block_gaps::BlockGapsPartition;
use indexer_db::headers::daa_index::DaaIndexPartition;
use indexer_db::messages::contextual_message::{
    ContextualMessageBySenderKey, ContextualMessageBySenderPartition,
    TxIdToContextualMessagePartition,
};
use indexer_db::messages::handshake::{
    HandshakeByReceiverPartition, HandshakeBySenderPartition, HandshakeKeyByReceiver,
    HandshakeKeyBySender, TxIdToHandshakePartition,
};
use indexer_db::messages::payment::{
    PaymentByReceiverPartition, PaymentBySenderPartition, PaymentKeyByReceiver, PaymentKeyBySender,
    TxIdToPaymentPartition,
};
use indexer_db::metadata::MetadataPartition;
use indexer_db::processing::tx_id_to_acceptance::{
    AcceptanceKey, Action, InsertionEntry, TxIDToAcceptancePartition,
};
use indexer_db::{AddressPayload, IntoBytes, PartitionId};
use kaspa_consensus_core::tx::Transaction;
use kaspa_rpc_core::{RpcBlock, RpcHeader, RpcTransaction, RpcTransactionId};
pub use message::*;
use protocol::operation::deserializer::parse_sealed_operation;
use protocol::operation::{
    SealedContextualMessageV1, SealedHandshakeV2, SealedMessageOrSealedHandshakeVNone,
    SealedOperation, SealedPaymentV1,
};
use std::collections::HashMap;
use tracing::{debug, error, info, info_span, trace, warn};

#[derive(bon::Builder)]
pub struct BlockProcessor {
    notification_rx: flume::Receiver<BlockNotification>,
    gap_result_rx: flume::Receiver<GapFillingProgress>,
    gap_result_tx: flume::Sender<GapFillingProgress>,
    processed_block_tx: flume::Sender<CompactHeader>,
    command_tx: workflow_core::channel::Sender<Command>,
    tx_keyspace: TxKeyspace,
    blocks_gap_partition: BlockGapsPartition,
    runtime_handle: tokio::runtime::Handle,
    metadata_partition: MetadataPartition,
    block_compact_header_partition: BlockCompactHeaderPartition,
    daa_index_partition: DaaIndexPartition,
    handshake_by_receiver_partition: HandshakeByReceiverPartition,
    handshake_by_sender_partition: HandshakeBySenderPartition,
    tx_id_to_handshake_partition: TxIdToHandshakePartition,
    contextual_message_by_sender_partition: ContextualMessageBySenderPartition,
    tx_id_to_contextual_message_partition: TxIdToContextualMessagePartition,
    payment_by_receiver_partition: PaymentByReceiverPartition,
    payment_by_sender_partition: PaymentBySenderPartition,
    tx_id_to_payment_partition: TxIdToPaymentPartition,
    tx_id_to_acceptance_partition: TxIDToAcceptancePartition,
    shared_metrics: SharedMetrics,
    #[builder(default)]
    gaps_filling_in_progress: usize,
}

impl BlockProcessor {
    pub fn process(&mut self) -> anyhow::Result<()> {
        info!("Block worker started");
        let mut has_first_connect = false;
        let mut gaps_fillers = HashMap::new();
        let mut is_shutdown = false;
        let mut last_processed_block: Option<[u8; 32]> = None;
        // let mut gaps_filling_in_progress = 0;
        loop {
            match self.select_input()? {
                NotificationOrGapResult::Notification(BlockNotification::Connected {
                    sink,
                    pp,
                }) => {
                    info!(sink = %sink.to_hex_64(), pp = %pp.to_hex_64(), "Received connection notification");
                    if !has_first_connect {
                        has_first_connect = true;
                        info!("Handling first connection");
                        let gaps = self.handle_first_connect(sink, pp)?;
                        info!(gap_count = gaps.len(), "Found gaps to fill");
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
                        self.gaps_filling_in_progress = gaps_fillers.len();
                        info!(self.gaps_filling_in_progress, "Started gap filling tasks");
                    } else if let Some(last_processed_block) = last_processed_block.take() {
                        info!(from = %last_processed_block.to_hex_64(), to = %sink.to_hex_64(), "Creating gap for reconnection");
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
                        self.gaps_filling_in_progress += 1;
                        info!(self.gaps_filling_in_progress, "New block gap added");
                    }
                }
                NotificationOrGapResult::Notification(BlockNotification::Disconnected) => {
                    info!("Received disconnection notification, stopping gap fillers");
                    std::mem::take(&mut gaps_fillers)
                        .into_iter()
                        .for_each(|(_to, interrupt_tx)| {
                            info!("send interruption signal");
                            let _ = interrupt_tx.send(()).inspect_err(|_err| {
                                error!("Error sending interrupt to block gap filler")
                            });
                        })
                }
                NotificationOrGapResult::Notification(BlockNotification::Shutdown) => {
                    info!("Received shutdown notification");
                    is_shutdown = true;
                    std::mem::take(&mut gaps_fillers)
                        .into_iter()
                        .for_each(|(_to, interrupt_tx)| {
                            info!("send interruption signal");
                            let _ = interrupt_tx.send(()).inspect_err(|_err| {
                                error!("Error sending interrupt to block gap filler")
                            });
                        })
                }
                NotificationOrGapResult::Notification(BlockNotification::Notification(block)) => {
                    let hash = block.header.hash.as_bytes();
                    let _span =
                        info_span!("Real-time Block notification", block = %hash.to_hex_64())
                            .entered();
                    debug!(hash = %hash.to_hex_64(), daa_score = block.header.daa_score, tx_count = block.transactions.len(), "Processing block notification");
                    loop {
                        let mut wtx = self.tx_keyspace.write_tx()?;
                        self.handle_block(&mut wtx, &block)?;
                        last_processed_block = Some(hash);
                        self.update_block(&mut wtx, hash);
                        if wtx.commit()?.is_ok() {
                            break;
                        } else {
                            warn!("conflict detected, retry handling block")
                        }
                    }

                    if !is_shutdown {
                        _ = self
                            .processed_block_tx
                            .send(block.header.as_ref().into())
                            .inspect_err(|_err| warn!("Error sending block notification"));
                    }
                    trace!(hash = %hash.to_hex_64(), "Block processed successfully");
                }
                NotificationOrGapResult::GapFilling(GapFillingProgress::Interrupted {
                    target: to,
                }) => {
                    info!(to = %to.to_hex_64(), "Gap filler interrupted");
                    gaps_fillers.remove(&to);
                    self.gaps_filling_in_progress -= 1;
                }
                NotificationOrGapResult::GapFilling(GapFillingProgress::Error {
                    target: to,
                    err,
                }) => {
                    error!(to = %to.to_hex_64(), %err, "Error in block gap filler");
                    gaps_fillers.remove(&to);
                    self.gaps_filling_in_progress -= 1;
                }
                NotificationOrGapResult::GapFilling(GapFillingProgress::Update {
                    target: to,
                    blocks,
                }) => {
                    let _span = info_span!("Gap Block Update", block = %to.to_hex_64()).entered();
                    debug!(to = %to.to_hex_64(), block_count = blocks.len(), "Processing gap filling update");
                    blocks.iter().try_for_each(|block| -> anyhow::Result<()> {
                        loop {
                            let mut wtx = self.tx_keyspace.write_tx()?;
                            self.handle_block(&mut wtx, block)?;
                            self.blocks_gap_partition.update_gap_wtx(
                                &mut wtx,
                                indexer_db::headers::block_gaps::BlockGap {
                                    from: block.header.hash.as_bytes(),
                                    to,
                                },
                            );
                            if wtx.commit()?.is_ok() {
                                break;
                            } else {
                                warn!("conflict detected, retry handling block")
                            }
                        }

                        if !is_shutdown {
                            _ = self
                                .processed_block_tx
                                .send(block.header.as_ref().into())
                                .inspect_err(|_err| warn!("Error sending block notification"));
                        }
                        Ok(())
                    })?;
                }
                NotificationOrGapResult::GapFilling(GapFillingProgress::Finished {
                    target: to,
                    blocks,
                }) => {
                    let last_index = blocks.len() - 1; // todo is it possible that blocks are empty?
                    blocks.iter().enumerate().try_for_each(
                        |(idx, block)| -> anyhow::Result<()> {
                            if idx != last_index {
                                let _span = info_span!("Gap Block Update", block = %to.to_hex_64())
                                    .entered();
                                loop {
                                    let mut wtx = self.tx_keyspace.write_tx()?;
                                    self.handle_block(&mut wtx, block)?;
                                    self.blocks_gap_partition.update_gap_wtx(
                                        &mut wtx,
                                        indexer_db::headers::block_gaps::BlockGap {
                                            from: block.header.hash.as_bytes(),
                                            to,
                                        },
                                    );
                                    if wtx.commit()?.is_ok() {
                                        break;
                                    } else {
                                        warn!("conflict detected, retry handling block")
                                    }
                                }
                                if !is_shutdown {
                                    _ = self
                                        .processed_block_tx
                                        .send(block.header.as_ref().into())
                                        .inspect_err(|_err| {
                                            warn!("Error sending block notification")
                                        });
                                }
                            } else {
                                let _span =
                                    info_span!("Gap Block Finishing", block = %to.to_hex_64())
                                        .entered();
                                loop {
                                    let mut wtx = self.tx_keyspace.write_tx()?;
                                    self.handle_block(&mut wtx, block)?;
                                    self.blocks_gap_partition.remove_gap_wtx(&mut wtx, &to);
                                    if wtx.commit()?.is_ok() {
                                        break;
                                    } else {
                                        warn!("conflict detected, retry handling block")
                                    }
                                }
                                if !is_shutdown {
                                    _ = self
                                        .processed_block_tx
                                        .send(block.header.as_ref().into())
                                        .inspect_err(|_err| {
                                            warn!("Error sending block notification")
                                        });
                                }
                            }
                            Ok(())
                        },
                    )?;
                    gaps_fillers.remove(&to);
                    self.gaps_filling_in_progress -= 1;
                    info!(
                        self.gaps_filling_in_progress,
                        "Gap filling finished successfully"
                    );
                }
            }
            if is_shutdown && self.gaps_filling_in_progress == 0 {
                info!("Block worker stopped");
                return Ok(());
            }
        }
    }

    pub fn handle_block(
        &mut self,
        wtx: &mut WriteTransaction,
        block: &RpcBlock,
    ) -> anyhow::Result<()> {
        let already_processed = self.block_compact_header_partition.insert_compact_header(
            block.header.hash.as_ref(),
            block.header.blue_work.to_le_bytes(),
            block.header.daa_score,
        )?;
        if already_processed {
            debug!(hash = %block.header.hash.as_bytes().to_hex_64(), "Skipping already processed block");
            return Ok(());
        }
        self.daa_index_partition
            .insert(block.header.daa_score, block.header.hash.as_ref())?;
        debug!(hash = %block.header.hash.as_bytes().to_hex_64(), tx_count = block.transactions.len(), "Processing block transactions");
        for tx in &block.transactions {
            self.handle_transaction(wtx, &block.header, tx)?;
        }
        self.shared_metrics.increment_blocks_processed();
        trace!(hash = %block.header.hash.as_bytes().to_hex_64(), "Block handled successfully");
        Ok(())
    }

    fn handle_transaction(
        &self,
        wtx: &mut WriteTransaction,
        block_header: &RpcHeader,
        tx: &RpcTransaction,
    ) -> anyhow::Result<()> {
        let tx_id = match &tx.verbose_data {
            Some(data) => data.transaction_id,
            None => Transaction::try_from(tx.clone())?.id(),
        };

        // todo handle the case when tx has many operations
        let Some(op) = parse_sealed_operation(&tx.payload).inspect(|op| {
            trace!(%tx_id, kind = op.op_type_name(), "Parsed sealed operation");
        }) else {
            trace!(%tx_id, "No valid sealed operation found, skipping transaction");
            return Ok(());
        };

        let (amount, receiver) = tx
            .outputs
            .first()
            .map(|o| AddressPayload::try_from(&o.script_public_key).map(|addr| (o.value, addr)))
            .transpose()?
            .unwrap_or_default();
        debug!(receiver=?receiver, "Handling transaction");
        let sender_outpoint = tx.inputs.first().unwrap().previous_outpoint;
        let sender = if sender_outpoint.index == 0
            && let Some(acceptance_key) = self
                .tx_id_to_acceptance_partition
                .key_by_tx_id(sender_outpoint.transaction_id.as_ref())?
        {
            Some(acceptance_key.receiver)
        } else {
            None
        };
        match (op, sender) {
            (SealedOperation::SealedMessageOrSealedHandshakeVNone(hk), None) => {
                let by_receiver_key =
                    self.handle_handshake(wtx, block_header, tx_id, hk, receiver, None)?;
                let by_sender_key = HandshakeKeyBySender {
                    sender: sender.unwrap_or_default(),
                    block_time: by_receiver_key.block_time,
                    block_hash: by_receiver_key.block_hash,
                    receiver,
                    version: by_receiver_key.version,
                    tx_id: by_receiver_key.tx_id,
                };
                self.tx_id_to_acceptance_partition.insert_wtx(
                    wtx,
                    &AcceptanceKey {
                        tx_id: tx_id.as_bytes(),
                        receiver,
                    },
                    &[
                        InsertionEntry {
                            partition_id: PartitionId::HandshakeBySender,
                            action: Action::InsertByKeySender,
                            partition_key: &|| by_sender_key.as_bytes(),
                        },
                        InsertionEntry {
                            partition_id: PartitionId::HandshakeByReceiver,
                            action: Action::UpdateValueSender,
                            partition_key: &|| by_receiver_key.as_bytes(),
                        },
                    ],
                )?;
            }
            (SealedOperation::SealedHandshakeV2(hk), None) => {
                let by_receiver_key =
                    self.handle_handshake_v2(wtx, block_header, tx_id, hk, receiver, None)?;
                let by_sender_key = HandshakeKeyBySender {
                    sender: sender.unwrap_or_default(),
                    block_time: by_receiver_key.block_time,
                    block_hash: by_receiver_key.block_hash,
                    receiver,
                    version: by_receiver_key.version,
                    tx_id: by_receiver_key.tx_id,
                };
                self.tx_id_to_acceptance_partition.insert_wtx(
                    wtx,
                    &AcceptanceKey {
                        tx_id: tx_id.as_bytes(),
                        receiver,
                    },
                    &[
                        InsertionEntry {
                            partition_id: PartitionId::HandshakeBySender,
                            action: Action::InsertByKeySender,
                            partition_key: &|| by_sender_key.as_bytes(),
                        },
                        InsertionEntry {
                            partition_id: PartitionId::HandshakeByReceiver,
                            action: Action::UpdateValueSender,
                            partition_key: &|| by_receiver_key.as_bytes(),
                        },
                    ],
                )?;
            }
            (SealedOperation::ContextualMessageV1(cm), None) => {
                let key =
                    self.handle_contextual_message(wtx, sender, block_header, tx_id, cm, receiver);
                self.tx_id_to_acceptance_partition.insert_wtx(
                    wtx,
                    &AcceptanceKey {
                        tx_id: tx_id.as_bytes(),
                        receiver,
                    },
                    &[InsertionEntry {
                        partition_id: PartitionId::ContextualMessageBySender,
                        action: Action::InsertByKeySender,
                        partition_key: &|| key.as_bytes(),
                    }],
                )?;
            }
            (SealedOperation::PaymentV1(pm), None) => {
                let by_receiver_key =
                    self.handle_payment(wtx, block_header, tx_id, receiver, amount, pm, None)?;
                let by_sender_key = PaymentKeyBySender {
                    sender: sender.unwrap_or_default(),
                    block_time: by_receiver_key.block_time,
                    block_hash: by_receiver_key.block_hash,
                    receiver,
                    version: by_receiver_key.version,
                    tx_id: by_receiver_key.tx_id,
                };
                self.tx_id_to_acceptance_partition.insert_wtx(
                    wtx,
                    &AcceptanceKey {
                        tx_id: tx_id.as_bytes(),
                        receiver,
                    },
                    &[
                        InsertionEntry {
                            partition_id: PartitionId::PaymentByReceiver,
                            action: Action::UpdateValueSender,
                            partition_key: &|| by_receiver_key.as_bytes(),
                        },
                        InsertionEntry {
                            partition_id: PartitionId::PaymentBySender,
                            action: Action::InsertByKeySender,
                            partition_key: &|| by_sender_key.as_bytes(),
                        },
                    ],
                )?;
            }
            (SealedOperation::SealedMessageOrSealedHandshakeVNone(hk), Some(sender)) => {
                self.handle_handshake(wtx, block_header, tx_id, hk, receiver, Some(sender))?;
                self.tx_id_to_acceptance_partition.insert_wtx(
                    wtx,
                    &AcceptanceKey {
                        tx_id: tx_id.as_bytes(),
                        receiver,
                    },
                    &[],
                )?;
            }
            (SealedOperation::SealedHandshakeV2(hk), Some(sender)) => {
                self.handle_handshake_v2(wtx, block_header, tx_id, hk, receiver, Some(sender))?;
                self.tx_id_to_acceptance_partition.insert_wtx(
                    wtx,
                    &AcceptanceKey {
                        tx_id: tx_id.as_bytes(),
                        receiver,
                    },
                    &[],
                )?;
            }
            (SealedOperation::ContextualMessageV1(cm), Some(sender)) => {
                self.handle_contextual_message(
                    wtx,
                    Some(sender),
                    block_header,
                    tx_id,
                    cm,
                    receiver,
                );
                self.tx_id_to_acceptance_partition.insert_wtx(
                    wtx,
                    &AcceptanceKey {
                        tx_id: tx_id.as_bytes(),
                        receiver,
                    },
                    &[],
                )?;
            }
            (SealedOperation::PaymentV1(pm), Some(sender)) => {
                self.handle_payment(wtx, block_header, tx_id, receiver, amount, pm, Some(sender))?;
                self.tx_id_to_acceptance_partition.insert_wtx(
                    wtx,
                    &AcceptanceKey {
                        tx_id: tx_id.as_bytes(),
                        receiver,
                    },
                    &[],
                )?;
            }
        }

        Ok(())
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

    fn handle_handshake(
        &self,
        wtx: &mut WriteTransaction,
        block: &RpcHeader,
        tx_id: RpcTransactionId,
        op: SealedMessageOrSealedHandshakeVNone,
        receiver: AddressPayload,
        sender: Option<AddressPayload>,
    ) -> anyhow::Result<HandshakeKeyByReceiver> {
        debug!(%tx_id, sender = ?sender, receiver = ?receiver, "Handling handshake transaction");
        self.tx_id_to_handshake_partition
            .insert_wtx(wtx, tx_id.as_ref(), op.sealed_hex);
        let hs_key = HandshakeKeyByReceiver {
            receiver,
            block_time: block.timestamp.into(),
            block_hash: block.hash.as_bytes(),
            version: 0,
            tx_id: tx_id.as_bytes(),
        };
        self.handshake_by_receiver_partition
            .insert_wtx(wtx, &hs_key, sender)?;

        if let Some(sender) = sender {
            trace!(sender = ?sender, "Inserting handshake by sender");
            let by_sender_key = HandshakeKeyBySender {
                sender,
                block_time: hs_key.block_time,
                block_hash: hs_key.block_hash,
                receiver,
                version: hs_key.version,
                tx_id: hs_key.tx_id,
            };
            self.handshake_by_sender_partition
                .insert_wtx(wtx, &by_sender_key);
        } else {
            trace!("No sender resolved for handshake");
        }
        Ok(hs_key)
    }

    fn handle_handshake_v2(
        &self,
        wtx: &mut WriteTransaction,
        block: &RpcHeader,
        tx_id: RpcTransactionId,
        op: SealedHandshakeV2,
        receiver: AddressPayload,
        sender: Option<AddressPayload>,
    ) -> anyhow::Result<HandshakeKeyByReceiver> {
        debug!(%tx_id, sender = ?sender, receiver = ?receiver, "Handling handshake transaction");
        self.tx_id_to_handshake_partition
            .insert_wtx(wtx, tx_id.as_ref(), op.sealed_hex);
        let hs_key = HandshakeKeyByReceiver {
            receiver,
            block_time: block.timestamp.into(),
            block_hash: block.hash.as_bytes(),
            version: 0,
            tx_id: tx_id.as_bytes(),
        };
        self.handshake_by_receiver_partition
            .insert_wtx(wtx, &hs_key, sender)?;

        if let Some(sender) = sender {
            trace!(sender = ?sender, "Inserting handshake by sender");
            let by_sender_key = HandshakeKeyBySender {
                sender,
                block_time: hs_key.block_time,
                block_hash: hs_key.block_hash,
                receiver,
                version: hs_key.version,
                tx_id: hs_key.tx_id,
            };
            self.handshake_by_sender_partition
                .insert_wtx(wtx, &by_sender_key);
        } else {
            trace!("No sender resolved for handshake");
        }
        Ok(hs_key)
    }

    fn handle_contextual_message(
        &self,
        wtx: &mut WriteTransaction,
        sender: Option<AddressPayload>,
        header: &RpcHeader,
        tx_id: RpcTransactionId,
        cm: SealedContextualMessageV1,
        receiver: AddressPayload,
    ) -> ContextualMessageBySenderKey {
        debug!(%tx_id, sender = ?sender, receiver = ?receiver, alias = %cm.alias.to_hex(), "Handling contextual message");
        let mut alias = [0u8; 16];
        let len = cm.alias.len().min(16);
        alias[..len].copy_from_slice(&cm.alias[..len]);
        self.tx_id_to_contextual_message_partition
            .insert_wtx(wtx, tx_id.as_ref(), cm.sealed_hex);
        let cmk = ContextualMessageBySenderKey {
            sender: sender.unwrap_or_default(),
            alias,
            block_time: header.timestamp.into(),
            block_hash: header.hash.as_bytes(),
            receiver,
            version: 1,
            tx_id: tx_id.as_bytes(),
        };
        if sender.is_some() {
            self.contextual_message_by_sender_partition
                .insert(wtx, &cmk);
        }
        cmk
    }

    fn handle_payment(
        &self,
        wtx: &mut WriteTransaction,
        header: &RpcHeader,
        tx_id: RpcTransactionId,
        receiver: AddressPayload,
        amount: u64,
        pm: SealedPaymentV1,
        sender: Option<AddressPayload>,
    ) -> anyhow::Result<PaymentKeyByReceiver> {
        debug!(%tx_id, sender = ?sender, receiver = ?receiver, amount, "Handling payment transaction");
        self.tx_id_to_payment_partition
            .insert_wtx(wtx, tx_id.as_ref(), amount, pm.sealed_hex)?;
        let pm_key = PaymentKeyByReceiver {
            receiver,
            block_time: header.timestamp.into(),
            block_hash: header.hash.as_bytes(),
            version: 0,
            tx_id: tx_id.as_bytes(),
        };
        self.payment_by_receiver_partition
            .insert_wtx(wtx, &pm_key, sender)?;
        if let Some(sender) = sender {
            trace!(sender = ?sender, "Inserting payment by sender");
            let by_sender_key = PaymentKeyBySender {
                sender,
                block_time: pm_key.block_time,
                block_hash: pm_key.block_hash,
                receiver,
                version: pm_key.version,
                tx_id: pm_key.tx_id,
            };
            self.payment_by_sender_partition
                .insert_wtx(wtx, &by_sender_key);
        } else {
            trace!("No sender resolved for payment");
        }
        Ok(pm_key)
    }
}

impl Drop for BlockProcessor {
    fn drop(&mut self) {
        debug!(self.gaps_filling_in_progress, "Dropping block processor");
        _ = self
            .command_tx
            .send_blocking(Command::MarkBlockSenderClosed)
            .inspect_err(|_| error!("Error sending command to mark block sender closed"));
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
