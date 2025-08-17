use crate::periodic_processor::{Intake, Response};
use anyhow::bail;
use futures_util::FutureExt;
use std::time::Duration;
use tracing::{debug, error, info, trace};
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
        info!(interval_secs = self.interval.as_secs(), "Ticker started");
        let mut need_to_send = true;
        let mut t = tokio::time::interval(self.interval);
        loop {
            tokio::select! {
                biased;
                _ = self.shutdown.recv().fuse() => {
                    info!("Ticker shutdown signal received");
                    debug!("Sending shutdown signal to periodic processor");
                    _ = self.job_trigger_tx.send(Intake::Shutdown).await.inspect_err(|err| error!("Error sending shutdown signal: {}", err));
                    loop {
                        if let Response::Stopped = self.resp_rx.recv().await? {
                            info!("Ticker is shutting down");
                            return Ok(())
                        }
                    }
                }
                _ = t.tick().fuse() => {
                    if need_to_send {
                        debug!("Ticker interval elapsed, triggering periodic job");
                        _ = self.job_trigger_tx.send(Intake::DoJob).await.inspect_err(|err| error!("Error sending `DoJob`: {}", err));
                        need_to_send = false;
                    } else {
                        trace!("Ticker interval elapsed, but job already in progress");
                    }
                }
                r = self.resp_rx.recv().fuse() => {
                    let r = r?;
                    match r {
                        Response::JobDone => {
                            trace!("Periodic job completed, ready for next interval");
                            need_to_send = true;
                        }
                        Response::Stopped => {
                            error!("Periodic processor stopped unexpectedly");
                            bail!("Periodic processor stopped unexpectedly")
                        }
                    }
                }
            }
        }
    }
}
