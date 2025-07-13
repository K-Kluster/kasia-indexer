use crate::{
    BlockOrMany,
    database::{
        acceptance::{AcceptanceToTxIDPartition, TxIDToAcceptancePartition},
        contextual_message_by_sender::ContextualMessageBySenderPartition,
        handshake::{
            AddressPayload, HandshakeByReceiverPartition, HandshakeKeyByReceiver,
            TxIdToHandshakePartition,
        },
        metadata::MetadataPartition,
        payment::{PaymentByReceiverPartition, PaymentKeyByReceiver, TxIdToPaymentPartition},
        resolution_keys::{
            ContextualMessageKeyForResolution, HandshakeKeyForResolution, PaymentKeyForResolution,
        },
        skip_tx::SkipTxPartition,
    },
    fifo_set::FifoSet,
    historical_syncer::Cursor,
};
use fjall::{TxKeyspace, WriteTransaction};
use kaspa_consensus_core::tx::{Transaction, TransactionId};
use kaspa_rpc_core::{RpcBlock, RpcHash, RpcTransaction};
use protocol::operation::{
    SealedContextualMessageV1, SealedMessageOrSealedHandshakeVNone, SealedOperation,
    SealedPaymentV1, deserializer::parse_sealed_operation,
};
use tracing::{debug, info, trace};

pub struct BlockWorker {
    processed_blocks: FifoSet<RpcHash>,
    intake: flume::Receiver<BlockOrMany>,
    shutdown: flume::Receiver<()>,

    tx_keyspace: TxKeyspace,

    metadata_partition: MetadataPartition,

    handshake_by_receiver_partition: HandshakeByReceiverPartition,
    tx_id_to_handshake_partition: TxIdToHandshakePartition,

    contextual_message_partition: ContextualMessageBySenderPartition,

    payment_by_receiver_partition: PaymentByReceiverPartition,
    tx_id_to_payment_partition: TxIdToPaymentPartition,

    acceptance_to_tx_id_partition: AcceptanceToTxIDPartition,
    tx_id_to_acceptance_partition: TxIDToAcceptancePartition,

    skip_tx_partition: SkipTxPartition,
}

impl BlockWorker {
    pub fn process(&mut self) -> anyhow::Result<()> {
        info!("Block worker started");
        loop {
            match self.select_input()? {
                BlocksOrShutdown::Shutdown(_) => {
                    info!("Block worker shutting down");
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
        info!("Received {} blocks for processing", blocks.len());
        for block in blocks {
            let hash = &block.header.hash;
            if self.processed_blocks.contains(hash) {
                debug!(%hash, "Skipping already processed block");
                continue;
            }

            let mut wtx = self.tx_keyspace.write_tx()?;
            debug!(%hash, "Processing block with {} transactions", block.transactions.len());
            for tx in &block.transactions {
                self.handle_transaction(&mut wtx, block, tx)?;
            }

            self.metadata_partition.set_latest_block_cursor(
                &mut wtx,
                Cursor {
                    blue_work: block.header.blue_work,
                    hash: block.header.hash,
                },
            )?;

            wtx.commit()??;
        }
        Ok(())
    }

    fn handle_transaction(
        &mut self,
        wtx: &mut WriteTransaction,
        block: &RpcBlock,
        tx: &RpcTransaction,
    ) -> anyhow::Result<()> {
        let tx = Transaction::try_from(tx.clone())?;
        let tx_id = tx.id();
        trace!(%tx_id, "Processing transaction");
        match parse_sealed_operation(&tx.payload).inspect(|op| {
            trace!(%tx_id, kind = op.op_type_name(), "Parsed sealed operation");
        }) {
            Some(SealedOperation::SealedMessageOrSealedHandshakeVNone(op)) => {
                self.handle_handshake(wtx, block, &tx, &tx_id, op)?;
            }
            Some(SealedOperation::ContextualMessageV1(op)) => {
                self.handle_contextual_message(wtx, block, &tx_id, op)?;
            }
            Some(SealedOperation::PaymentV1(op)) => {
                self.handle_payment(wtx, block, &tx, &tx_id, op)?;
            }
            None => {
                trace!(%tx_id, "No valid sealed operation found, skipping");
                self.skip_tx_partition.mark_skip(wtx, tx_id.as_bytes());
            }
        }

        Ok(())
    }
    fn handle_handshake(
        &mut self,
        wtx: &mut WriteTransaction,
        block: &RpcBlock,
        tx: &Transaction,
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

        self.tx_id_to_acceptance_partition.insert_handshake_wtx(
            wtx,
            tx_id.as_bytes(),
            &HandshakeKeyForResolution {
                block_time: block.header.timestamp.to_be_bytes(),
                block_hash: block.header.hash.as_bytes(),
                receiver,
                version: 0,
                tx_id: tx_id.as_bytes(),
                attempt_count: 0,
            },
        );

        self.acceptance_to_tx_id_partition
            .insert_wtx(wtx, tx_id.as_bytes());

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

        self.tx_id_to_acceptance_partition
            .insert_contextual_message_wtx(
                wtx,
                tx_id.as_bytes(),
                &ContextualMessageKeyForResolution {
                    alias,
                    block_time: block.header.timestamp.to_be_bytes(),
                    block_hash: block.header.hash.as_bytes(),
                    version: 1,
                    tx_id: tx_id.as_bytes(),
                    attempt_count: 0,
                },
            );

        self.acceptance_to_tx_id_partition
            .insert_wtx(wtx, tx_id.as_bytes());

        Ok(())
    }

    fn handle_payment(
        &mut self,
        wtx: &mut WriteTransaction,
        block: &RpcBlock,
        tx: &Transaction,
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

        self.tx_id_to_acceptance_partition.insert_payment_wtx(
            wtx,
            tx_id.as_bytes(),
            &PaymentKeyForResolution {
                block_time: block.header.timestamp.to_be_bytes(),
                block_hash: block.header.hash.as_bytes(),
                receiver,
                version: 1,
                tx_id: tx_id.as_bytes(),
                attempt_count: 0,
            },
        );

        self.acceptance_to_tx_id_partition
            .insert_wtx(wtx, tx_id.as_bytes());

        Ok(())
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
