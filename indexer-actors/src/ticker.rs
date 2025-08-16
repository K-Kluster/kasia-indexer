use crate::periodic_processor::{Intake, Response};
use anyhow::bail;
use futures_util::FutureExt;
use std::time::Duration;
use tracing::{error, info};
use workflow_core::channel::{Receiver, Sender};

pub struct Ticker {
    interval: Duration,
    shutdown: tokio::sync::mpsc::Receiver<()>,
    job_trigger_tx: Sender<Intake>,
    resp_rx: Receiver<Response>,
}

impl Ticker {
    pub fn new(
        interval: Duration,
        shutdown: tokio::sync::mpsc::Receiver<()>,
        job_trigger_tx: Sender<Intake>,
        resp_rx: Receiver<Response>,
    ) -> Self {
        Self {
            interval,
            shutdown,
            job_trigger_tx,
            resp_rx,
        }
    }
    pub async fn process(&mut self) -> anyhow::Result<()> {
        let mut need_to_send = true;
        let mut shutting_down = false;
        let mut t = tokio::time::interval(self.interval);
        loop {
            tokio::select! {
                _ = t.tick() => {
                    if need_to_send {
                        _ = self.job_trigger_tx.send(Intake::DoJob).await.inspect_err(|err| error!("Error sending `DoJob`: {}", err));
                        need_to_send = false;
                    }
                }
                r = self.resp_rx.recv().fuse() => {
                    let r = r?;
                    match r {
                        Response::JobDone => {
                            need_to_send = true;
                        }
                        Response::Stopped => {
                            if shutting_down {
                                info!("Ticker is shutting down");
                                return Ok(())
                            } else {
                                bail!("Periodic processor stopped unexpectedly")
                            }
                        }
                    }
                }
                _ = self.shutdown.recv().fuse() => {
                    info!("Shutdown received");
                    shutting_down = true;
                }
            }
        }
    }
}
