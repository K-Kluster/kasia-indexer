use kaspa_rpc_core::RpcBlock;
use std::ops::Deref;
use std::slice;
use std::sync::Arc;

pub mod fifo_set;
pub mod historical_syncer;
pub mod subscriber;

pub mod database;

pub mod acceptance_worker;
pub mod block_worker;

pub mod selected_chain_syncer;

pub mod scan_worker;

pub enum BlockOrMany {
    Many(Vec<RpcBlock>),
    Block(Arc<RpcBlock>),
}

impl Deref for BlockOrMany {
    type Target = [RpcBlock];

    fn deref(&self) -> &Self::Target {
        match self {
            BlockOrMany::Many(b) => b.as_slice(),
            BlockOrMany::Block(b) => {
                let ptr = Arc::as_ptr(b);
                unsafe { slice::from_raw_parts(ptr, 1) }
            }
        }
    }
}
