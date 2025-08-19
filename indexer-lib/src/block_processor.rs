use crate::BlockOrMany;
use crate::database::headers::{BlockCompactHeaderPartition, DaaIndexPartition};
use crate::database::messages::self_stashes::{SelfStashByOwnerPartition, SelfStashKeyByOwner};
use crate::database::messages::{
    AddressPayload, ContextualMessageBySenderPartition, HandshakeByReceiverPartition,
    HandshakeKeyByReceiver, PaymentByReceiverPartition, PaymentKeyByReceiver,
    TxIdToHandshakePartition, TxIdToPaymentPartition,
};
use crate::database::metadata::MetadataPartition;
use crate::database::processing::{
    SkipTxByBlockPartition, SkipTxPartition, TxIDToAcceptancePartition,
};
use crate::database::resolution_keys::{
    ContextualMessageKeyForResolution, HandshakeKeyForResolution, PaymentKeyForResolution,
    SelfStashKeyForResolution,
};
use crate::fifo_set::FifoSet;
use crate::historical_syncer::Cursor;
use crate::metrics::SharedMetrics;
use fjall::{TxKeyspace, WriteTransaction};
use kaspa_consensus_core::tx::{Transaction, TransactionId};
use kaspa_rpc_core::{RpcBlock, RpcHash, RpcTransaction};
use protocol::operation::SealedSelfStashV1;
use protocol::operation::{
    SealedContextualMessageV1, SealedMessageOrSealedHandshakeVNone, SealedOperation,
    SealedPaymentV1, deserializer::parse_sealed_operation,
};
use tracing::{debug, info, trace};

#[derive(bon::Builder)]
pub struct BlockProcessor {
    processed_blocks: FifoSet<RpcHash>,
    processed_txs: FifoSet<TransactionId>,
    intake: flume::Receiver<BlockOrMany>,
    shutdown: flume::Receiver<()>,

    tx_keyspace: TxKeyspace,

    metadata_partition: MetadataPartition,

    handshake_by_receiver_partition: HandshakeByReceiverPartition,
    tx_id_to_handshake_partition: TxIdToHandshakePartition,

    contextual_message_partition: ContextualMessageBySenderPartition,

    payment_by_receiver_partition: PaymentByReceiverPartition,
    tx_id_to_payment_partition: TxIdToPaymentPartition,

    self_stash_by_owner_partition: SelfStashByOwnerPartition,

    tx_id_to_acceptance_partition: TxIDToAcceptancePartition,

    skip_tx_partition: SkipTxPartition,
    skip_tx_by_block_partition: SkipTxByBlockPartition,
    block_compact_header_partition: BlockCompactHeaderPartition,
    block_daa_index: DaaIndexPartition,
    metrics: SharedMetrics,
}

impl BlockProcessor {
    pub fn process(&mut self) -> anyhow::Result<()> {
        info!("Block worker started");
        loop {
            match self.select_input()? {
                BlocksOrShutdown::Shutdown(_) => {
                    info!("Block worker received shutdown signal, draining notifications first");
                    let rx = std::mem::replace(&mut self.intake, flume::unbounded().1);
                    rx.drain().try_for_each(|blocks| -> anyhow::Result<()> {
                        self.handle_blocks(&blocks)?;
                        Ok(())
                    })?;
                    info!("Draining is done, stopping block worker");
                    return Ok(());
                }
                BlocksOrShutdown::Blocks(blocks) => {
                    self.handle_blocks(&blocks)?;
                }
            }
        }
    }

    fn select_input(&self) -> anyhow::Result<BlocksOrShutdown> {
        trace!("Waiting for new blocks or shutdown signal");
        Ok(flume::Selector::new()
            .recv(&self.intake, |r| r.map(BlocksOrShutdown::from))
            .recv(&self.shutdown, |r| r.map(BlocksOrShutdown::from))
            .wait()?)
    }

    fn handle_blocks(&mut self, blocks: &[RpcBlock]) -> anyhow::Result<()> {
        debug!("Received {} blocks for processing", blocks.len());
        for block in blocks {
            let hash = &block.header.hash;
            if self.processed_blocks.contains(hash) {
                debug!(%hash, "Skipping already processed block");
                continue;
            }
            self.block_compact_header_partition.insert_compact_header(
                hash,
                block.header.blue_work,
                block.header.daa_score,
            )?;
            self.block_daa_index.insert(block.header.daa_score, hash)?;
            let mut wtx = self.tx_keyspace.write_tx()?;
            debug!(%hash, "Processing block with {} transactions", block.transactions.len());

            let mut skipped_tx_ids = Vec::with_capacity(block.transactions.len());
            for tx in &block.transactions {
                if let Some(skipped_tx_id) = self.handle_transaction(&mut wtx, block, tx)? {
                    skipped_tx_ids.push(skipped_tx_id);
                }
            }

            // Add skipped transactions to the block-organized partition
            if !skipped_tx_ids.is_empty() {
                debug!(%hash, skipped_count = skipped_tx_ids.len(), "Adding skipped transactions to block partition");
                self.skip_tx_by_block_partition.add_skip_for_block(
                    &mut wtx,
                    block.header.daa_score,
                    *block.header.hash.as_ref(),
                    &skipped_tx_ids,
                );
            }

            self.metadata_partition.set_latest_block_cursor(
                &mut wtx,
                Cursor {
                    daa_score: block.header.daa_score,
                    blue_work: block.header.blue_work,
                    hash: block.header.hash,
                },
            )?;

            wtx.commit()??;
            self.processed_blocks.insert(*hash);
            self.metrics.increment_blocks_processed();
        }
        Ok(())
    }

    fn handle_transaction(
        &mut self,
        wtx: &mut WriteTransaction,
        block: &RpcBlock,
        tx: &RpcTransaction,
    ) -> anyhow::Result<Option<[u8; 32]>> {
        let tx_id = match &tx.verbose_data {
            Some(data) => data.transaction_id,
            None => Transaction::try_from(tx.clone())?.id(),
        };
        if self.processed_txs.contains(&tx_id) {
            debug!(%tx_id, "Skipping already processed transaction");
            return Ok(None);
        }

        trace!(%tx_id, "Processing transaction");
        let skipped_tx_id = match parse_sealed_operation(&tx.payload).inspect(|op| {
            trace!(%tx_id, kind = op.op_type_name(), "Parsed sealed operation");
        }) {
            Some(SealedOperation::SealedMessageOrSealedHandshakeVNone(op)) => {
                self.handle_handshake(wtx, block, tx, &tx_id, op)?;
                None
            }
            Some(SealedOperation::ContextualMessageV1(op)) => {
                self.handle_contextual_message(wtx, block, &tx_id, op)?;
                None
            }
            Some(SealedOperation::PaymentV1(op)) => {
                self.handle_payment(wtx, block, tx, &tx_id, op)?;
                None
            }
            Some(SealedOperation::SelfStashV1(op)) => {
                self.handle_self_stash(wtx, block, tx, &tx_id, op);
                None
            }
            None => {
                debug!(%tx_id, "No valid sealed operation found, skipping");
                self.skip_tx_partition.mark_skip(wtx, tx_id.as_bytes());
                Some(tx_id.as_bytes())
            }
        };
        self.processed_txs.insert(tx_id);

        Ok(skipped_tx_id)
    }
    fn handle_handshake(
        &mut self,
        wtx: &mut WriteTransaction,
        block: &RpcBlock,
        tx: &RpcTransaction,
        tx_id: &TransactionId,
        op: SealedMessageOrSealedHandshakeVNone,
    ) -> anyhow::Result<()> {
        debug!(%tx_id, "Handling HandshakeVNone");
        self.tx_id_to_handshake_partition
            .insert_wtx(wtx, tx_id.as_ref(), op.sealed_hex);

        let receiver = tx
            .outputs
            .first()
            .map(|o| AddressPayload::try_from(&o.script_public_key))
            .transpose()?
            .unwrap_or_default();
        debug!(receiver=?receiver, "Inserting handshake by receiver");

        self.handshake_by_receiver_partition.insert_wtx(
            wtx,
            &HandshakeKeyByReceiver {
                receiver,
                block_time: block.header.timestamp.to_be_bytes(),
                block_hash: block.header.hash.as_bytes(),
                version: 0,
                tx_id: tx_id.as_bytes(),
            },
            None,
        );

        let hk_for_resolution = HandshakeKeyForResolution {
            block_time: block.header.timestamp.to_be_bytes(),
            block_hash: block.header.hash.as_bytes(),
            receiver,
            version: 0,
            tx_id: tx_id.as_bytes(),
            attempt_count: 10,
        };
        self.tx_id_to_acceptance_partition.insert_handshake_wtx(
            wtx,
            tx_id.as_bytes(),
            &hk_for_resolution,
            None,
            None,
        );

        Ok(())
    }

    fn handle_contextual_message(
        &mut self,
        wtx: &mut WriteTransaction,
        block: &RpcBlock,
        tx_id: &TransactionId,
        op: SealedContextualMessageV1,
    ) -> anyhow::Result<()> {
        debug!(%tx_id, "Handling ContextualMessageV1");
        debug!(alias=?op.alias, "Inserting contextual message");
        self.contextual_message_partition.insert(
            wtx,
            Default::default(),
            op.alias,
            block.header.timestamp,
            block.header.hash.as_bytes(),
            1,
            tx_id.as_bytes(),
            op.sealed_hex,
        )?;

        let mut alias = [0u8; 16];
        alias[..op.alias.len().min(16)].copy_from_slice(op.alias);
        let cmk_for_resolution = ContextualMessageKeyForResolution {
            alias,
            block_time: block.header.timestamp.to_be_bytes(),
            block_hash: block.header.hash.as_bytes(),
            version: 1,
            tx_id: tx_id.as_bytes(),
            attempt_count: 10,
        };
        self.tx_id_to_acceptance_partition
            .insert_contextual_message_wtx(wtx, tx_id.as_bytes(), &cmk_for_resolution, None, None);
        self.metrics.increment_contextual_messages_count();
        Ok(())
    }

    fn handle_payment(
        &mut self,
        wtx: &mut WriteTransaction,
        block: &RpcBlock,
        tx: &RpcTransaction,
        tx_id: &TransactionId,
        op: SealedPaymentV1,
    ) -> anyhow::Result<()> {
        debug!(%tx_id, "Handling PaymentV1");
        let (amount, receiver) = tx
            .outputs
            .first()
            .map(|o| AddressPayload::try_from(&o.script_public_key).map(|addr| (o.value, addr)))
            .transpose()?
            .unwrap_or_default();

        debug!(receiver=?receiver, amount, "Inserting payment by receiver");
        self.tx_id_to_payment_partition
            .insert_wtx(wtx, tx_id.as_ref(), amount, op.sealed_hex)?;
        self.payment_by_receiver_partition.insert_wtx(
            wtx,
            &PaymentKeyByReceiver {
                receiver,
                block_time: block.header.timestamp.to_be_bytes(),
                block_hash: block.header.hash.as_bytes(),
                version: 1,
                tx_id: tx_id.as_bytes(),
            },
            None,
        );
        let pk_for_resolution = PaymentKeyForResolution {
            block_time: block.header.timestamp.to_be_bytes(),
            block_hash: block.header.hash.as_bytes(),
            receiver,
            version: 1,
            tx_id: tx_id.as_bytes(),
            attempt_count: 10,
        };
        self.tx_id_to_acceptance_partition.insert_payment_wtx(
            wtx,
            tx_id.as_bytes(),
            &pk_for_resolution,
            None,
            None,
        );

        Ok(())
    }

    fn handle_self_stash(
        &mut self,
        wtx: &mut WriteTransaction,
        block: &RpcBlock,
        _tx: &RpcTransaction,
        tx_id: &TransactionId,
        op: SealedSelfStashV1,
    ) {
        debug!(%tx_id, "Handling SelfStashV1");

        debug!(scope=?op.key, op.sealed_hex, "Inserting SelfStash by owner");

        // pad with zeros
        let mut fixed_scope = [0u8; 255];
        match op.key {
            Some(scope) => {
                let scope_length = scope.len().min(255);
                fixed_scope[..scope_length].copy_from_slice(&scope[..scope_length]); // cap at 255,
            }
            None => (),
        };

        self.self_stash_by_owner_partition.insert_wtx(
            wtx,
            &SelfStashKeyByOwner {
                owner: AddressPayload::default(),
                scope: fixed_scope,
                block_time: block.header.timestamp.to_be_bytes(),
                block_hash: block.header.hash.as_bytes(),
                version: 1,
                tx_id: tx_id.as_bytes(),
            },
            op.sealed_hex,
        );

        let self_stash_for_resolution = SelfStashKeyForResolution {
            scope: fixed_scope,
            block_time: block.header.timestamp.to_be_bytes(),
            block_hash: block.header.hash.as_bytes(),
            version: 1,
            tx_id: tx_id.as_bytes(),
            attempt_count: 10,
        };
        self.tx_id_to_acceptance_partition.insert_self_stash_wtx(
            wtx,
            tx_id.as_bytes(),
            &self_stash_for_resolution,
            None,
            None,
        );
    }
}

enum BlocksOrShutdown {
    Blocks(BlockOrMany),
    Shutdown(()),
}

impl From<BlockOrMany> for BlocksOrShutdown {
    fn from(other: BlockOrMany) -> Self {
        Self::Blocks(other)
    }
}

impl From<()> for BlocksOrShutdown {
    fn from(value: ()) -> Self {
        Self::Shutdown(value)
    }
}
