pub mod block_gap_filler;
pub mod block_processor;
pub mod data_source;
pub mod fifo_set;
pub mod virtual_chain_processor;
pub mod virtual_chain_syncer;

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
