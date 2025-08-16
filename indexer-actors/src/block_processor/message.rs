use kaspa_rpc_core::RpcBlock;
use std::sync::Arc;

#[derive(Debug)]
pub enum BlockNotification {
    Connected { sink: [u8; 32], pp: [u8; 32] },
    Disconnected,
    Shutdown,
    Notification(Arc<RpcBlock>),
}

#[derive(Debug)]
pub enum GapFillingProgress {
    Update {
        to: [u8; 32],
        blocks: Vec<RpcBlock>,
    },
    Interrupted {
        to: [u8; 32],
    },
    Finished {
        to: [u8; 32],
        blocks: Vec<RpcBlock>,
    },
    Error {
        to: [u8; 32],
        err: workflow_rpc::client::error::Error,
    },
}
