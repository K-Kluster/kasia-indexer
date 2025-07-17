use kaspa_rpc_core::VirtualChainChangedNotification;
use tracing::{info, trace};

pub struct AcceptanceWorker {
    vcc_rx: flume::Receiver<VirtualChainChangedNotification>,
    shutdown: flume::Receiver<()>,
}

impl AcceptanceWorker {
    pub fn process(&mut self) -> anyhow::Result<()> {
        info!("Acceptance worker started");
        loop {
            match self.select_input()? {
                VccOrShutdown::Shutdown(_) => {
                    info!(
                        "Acceptance worker received shutdown signal, draining notifications first"
                    );
                    let rx = std::mem::replace(&mut self.vcc_rx, flume::bounded(0).1);
                    rx.drain().try_for_each(|vcc| -> anyhow::Result<()> {
                        self.handle_vcc(&vcc)?;
                        Ok(())
                    })?;
                    info!("Draining is done, stopping acceptance worker");
                    return Ok(());
                }
                VccOrShutdown::Vcc(vcc) => {
                    self.handle_vcc(&vcc)?;
                }
            }
        }
    }

    fn select_input(&self) -> anyhow::Result<VccOrShutdown> {
        trace!("Waiting for new vcc or shutdown signal");
        Ok(flume::Selector::new()
            .recv(&self.vcc_rx, |r| r.map(VccOrShutdown::from))
            .recv(&self.shutdown, |r| r.map(VccOrShutdown::from))
            .wait()?)
    }

    fn handle_vcc(&self, vcc: &VirtualChainChangedNotification) -> anyhow::Result<()> {
        // vcc.removed_chain_block_hashes.iter().try_for_each(|hash| {
        //     self.handle_chain_block_removal(hash)?;
        // })?;

        Ok(())
    }
}

enum VccOrShutdown {
    Vcc(VirtualChainChangedNotification),
    Shutdown(()),
}

impl From<VirtualChainChangedNotification> for VccOrShutdown {
    fn from(other: VirtualChainChangedNotification) -> Self {
        Self::Vcc(other)
    }
}

impl From<()> for VccOrShutdown {
    fn from(value: ()) -> Self {
        Self::Shutdown(value)
    }
}
