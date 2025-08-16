use crate::data_source::{Command, Request};
use crate::virtual_chain_processor::SyncVccNotification;
use futures_util::FutureExt;
use tracing::error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationAck {
    Continue,
    Stop,
}

pub struct VirtualChainSyncer {
    syncer_id: u64,
    from: [u8; 32],
    vcc_tx: flume::Sender<SyncVccNotification>,
    commands_tx: workflow_core::channel::Sender<Command>,
    ack_rx: workflow_core::channel::Receiver<NotificationAck>,
}

impl VirtualChainSyncer {
    pub fn new(
        syncer_id: u64,
        from: [u8; 32],
        vcc_tx: flume::Sender<SyncVccNotification>,
        ack_rx: workflow_core::channel::Receiver<NotificationAck>,
        commands_tx: workflow_core::channel::Sender<Command>,
    ) -> Self {
        Self {
            syncer_id,
            from,
            vcc_tx,
            commands_tx,
            ack_rx,
        }
    }

    pub async fn process(&self) -> anyhow::Result<()> {
        let mut from = self.from;
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();

        self.commands_tx
            .send(Command::Request(Request::RequestVirtualChain {
                vc_from: from,
                response_channel: resp_tx,
            }))
            .await?;
        tokio::select! {
            r = resp_rx => {
                let r = r??;
                from = r.added_chain_block_hashes.last().map(|h| h.as_bytes()).unwrap_or(from);
                self.vcc_tx.send(SyncVccNotification::VirtualChain {syncer_id: self.syncer_id,virtual_chain: r})?;
            },
            r = self.ack_rx.recv().fuse() => {
                let r = r?;
                match r {
                    NotificationAck::Continue => unreachable!(),
                    NotificationAck::Stop => {
                        return Ok(())
                    }
                }
            }
        }
        loop {
            match self.ack_rx.recv().await? {
                NotificationAck::Stop => return Ok(()),
                NotificationAck::Continue => {
                    let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                    self.commands_tx
                        .send(Command::Request(Request::RequestVirtualChain {
                            vc_from: from,
                            response_channel: resp_tx,
                        }))
                        .await?;
                    tokio::select! {
                        r = resp_rx => {
                            let r = r??;
                            from = r.added_chain_block_hashes.last().map(|h| h.as_bytes()).unwrap_or(from);
                            self.vcc_tx.send(SyncVccNotification::VirtualChain {syncer_id: self.syncer_id,virtual_chain: r})?;
                        },
                        r = self.ack_rx.recv().fuse() => {
                            let r = r?;
                            match r {
                                NotificationAck::Continue => unreachable!(),
                                NotificationAck::Stop => {
                                    return Ok(())
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

impl Drop for VirtualChainSyncer {
    fn drop(&mut self) {
        _ = self
            .vcc_tx
            .send(SyncVccNotification::Stopped {
                syncer_id: self.syncer_id,
            })
            .inspect_err(|_| {
                error!("Error sending `Stopped` to mark VirtualChainSyncer sender closed")
            });
    }
}
