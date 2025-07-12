use crate::database::resolution_keys::{
    ContextualMessageKeyForResolution, HandshakeKeyForResolution,
};
use crate::database::skip_tx::SkipTxPartition;
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
    },
    fifo_set::FifoSet,
    historical_syncer::Cursor,
};
use fjall::TxKeyspace;
use kaspa_consensus_core::tx::Transaction;
use kaspa_rpc_core::RpcHash;
use protocol::operation::SealedOperation;
use protocol::operation::deserializer::parse_sealed_operation;
use tracing::info;

pub struct BlockWorker {
    processed_blocks: FifoSet<RpcHash>,
    intake: flume::Receiver<BlockOrMany>,
    shutdown: flume::Receiver<()>,

    tx_keyspace: TxKeyspace,

    metadata_partition: MetadataPartition,

    handshake_by_receiver_partition: HandshakeByReceiverPartition,
    tx_id_to_handshake_partition: TxIdToHandshakePartition,

    contextual_message_partition: ContextualMessageBySenderPartition,

    acceptance_to_tx_id_partition: AcceptanceToTxIDPartition,
    tx_id_to_acceptance_partition: TxIDToAcceptancePartition,

    skip_tx_partition: SkipTxPartition,
}

impl BlockWorker {
    pub fn process(&mut self) -> anyhow::Result<()> {
        loop {
            let ch = flume::Selector::new()
                .recv(&self.intake, |r| r.map(BlocksOrShutdown::from))
                .recv(&self.shutdown, |r| r.map(BlocksOrShutdown::from));
            match ch.wait()? {
                BlocksOrShutdown::Shutdown(_) => {
                    info!("Block worker shutting down");
                    return Ok(());
                }
                BlocksOrShutdown::Blocks(blocks) => {
                    blocks.iter().try_for_each(|b| -> anyhow::Result<()> {
                        if self.processed_blocks.contains(&b.header.hash) {
                            return Ok(());
                        }
                        let mut wtx = self.tx_keyspace.write_tx()?;
                        b.transactions
                            .iter()
                            .map(|tx| {
                                (
                                    Transaction::try_from(tx.clone()), // todo can id be calculated without conversion??
                                    parse_sealed_operation(&tx.payload),
                                )
                            })
                            .try_for_each(|(tx, op)| -> anyhow::Result<()> {
                                let tx = tx?;
                                let tx_id = tx.id();
                                let Some(op) = op else {
                                    self.skip_tx_partition.mark_skip(&mut wtx, tx_id.as_bytes());
                                    return Ok(());
                                };
                                match op {
                                    SealedOperation::SealedMessageOrSealedHandshakeVNone(op) => {
                                        self.tx_id_to_handshake_partition.insert_wtx(
                                            &mut wtx,
                                            tx_id.as_ref(),
                                            op.sealed_hex,
                                        );
                                        let receiver = tx
                                            .outputs
                                            .first()
                                            .map(|output| {
                                                AddressPayload::try_from(&output.script_public_key)
                                            })
                                            .transpose()?
                                            .unwrap_or_default();
                                        self.handshake_by_receiver_partition.insert_wtx(
                                            &mut wtx,
                                            &HandshakeKeyByReceiver {
                                                receiver,
                                                block_time: b.header.timestamp.to_be_bytes(),
                                                block_hash: b.header.hash.as_bytes(),
                                                version: 0,
                                                tx_id: tx_id.as_bytes(),
                                            },
                                            None,
                                        );
                                        self.tx_id_to_acceptance_partition.insert_handshake_wtx(
                                            &mut wtx,
                                            tx_id.as_bytes(),
                                            &HandshakeKeyForResolution {
                                                block_time: b.header.timestamp.to_be_bytes(),
                                                block_hash: b.header.hash.as_bytes(),
                                                receiver,
                                                version: 0,
                                                tx_id: tx_id.as_bytes(),
                                                attempt_count: 0,
                                            },
                                        );
                                        self.acceptance_to_tx_id_partition
                                            .insert_wtx(&mut wtx, tx_id.as_bytes());
                                    }
                                    SealedOperation::ContextualMessageV1(op) => {
                                        self.contextual_message_partition.insert(
                                            &mut wtx,
                                            Default::default(),
                                            &op.alias,
                                            b.header.timestamp,
                                            b.header.hash.as_bytes(),
                                            1,
                                            tx_id.as_bytes(),
                                            &op.sealed_hex,
                                        )?;
                                        self.tx_id_to_acceptance_partition
                                            .insert_contextual_message_wtx(
                                                &mut wtx,
                                                tx_id.as_bytes(),
                                                &ContextualMessageKeyForResolution {
                                                    alias: {
                                                        let mut alias = [0u8; 16];
                                                        alias.copy_from_slice(
                                                            &op.alias[..op.alias.len().min(16)],
                                                        );
                                                        alias
                                                    },
                                                    block_time: b.header.timestamp.to_be_bytes(),
                                                    block_hash: b.header.hash.as_bytes(),
                                                    version: 1,
                                                    tx_id: tx_id.as_bytes(),
                                                    attempt_count: 0,
                                                },
                                            );
                                        self.acceptance_to_tx_id_partition
                                            .insert_wtx(&mut wtx, tx_id.as_bytes());
                                    }
                                    SealedOperation::PaymentV1(_) => {
                                        todo!()
                                    }
                                }
                                Ok(())
                            })?;
                        self.metadata_partition.set_latest_block_cursor(
                            &mut wtx,
                            Cursor {
                                blue_work: b.header.blue_work,
                                hash: b.header.hash,
                            },
                        )?;
                        wtx.commit()??;
                        Ok(())
                    })?;
                }
            }
        }
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
