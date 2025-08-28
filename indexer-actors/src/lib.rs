pub mod block_gap_filler;
pub mod block_processor;
pub mod data_source;
pub mod fifo_set;
pub mod metrics;
pub mod periodic_processor;
pub mod ticker;
pub mod virtual_chain_processor;
pub mod virtual_chain_syncer;

pub mod util;

#[derive(Debug, Clone, Copy)]
pub struct BlockGap {
    pub from_block: [u8; 32],
    pub to_block: [u8; 32],
}

#[derive(Debug, Clone, Copy)]
pub struct InterruptedGapSync {
    pub from_block: [u8; 32],
    pub interrupted_at: [u8; 32],
    pub to_block: [u8; 32],
}
