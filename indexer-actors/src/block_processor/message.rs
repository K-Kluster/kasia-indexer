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
        target: [u8; 32],
        blocks: Vec<RpcBlock>,
    },
    Interrupted {
        target: [u8; 32],
    },
    Finished {
        target: [u8; 32],
        blocks: Vec<RpcBlock>,
    },
    Error {
        target: [u8; 32],
        err: workflow_rpc::client::error::Error,
    },
}
