use kaspa_rpc_core::RpcBlock;
use std::ops::Deref;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

pub static APP_IS_RUNNING: AtomicBool = AtomicBool::new(true);

pub mod fifo_set;
pub mod historical_syncer;
pub mod subscriber;

pub mod database;
pub mod metrics;

pub mod block_processor;
pub mod periodic_processor;
pub mod virtual_chain_processor;

pub mod selected_chain_syncer;

pub mod resolver;

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
