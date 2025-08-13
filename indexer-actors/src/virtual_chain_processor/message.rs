use crate::virtual_chain_processor::DaaScore;
use kaspa_consensus_core::BlueWorkType;
use kaspa_rpc_core::{
    GetVirtualChainFromBlockResponse, RpcAddress, RpcHeader, VirtualChainChangedNotification,
};

#[derive(Debug)]
pub enum RealTimeVccNotification {
    Connected {
        sink: [u8; 32],
        sink_blue_work: BlueWorkType,
        pp: [u8; 32],
    },
    Disconnected,
    Shutdown,
    Notification(VirtualChainChangedNotification),
    // todo rename enum because of it
    SenderResolution {
        sender: RpcAddress,
        tx_id: [u8; 32],
        daa: DaaScore,
    },
}

pub enum SyncVccNotification {
    VirtualChain {
        syncer_id: u64,
        virtual_chain: GetVirtualChainFromBlockResponse,
    },
    Stopped {
        syncer_id: u64,
    },
}

#[derive(Clone, Debug, Copy, PartialEq, Eq, PartialOrd)]
pub struct CompactHeader {
    pub block_hash: [u8; 32],
    pub blue_work: BlueWorkType,
    pub daa_score: u64,
}

impl From<&RpcHeader> for CompactHeader {
    fn from(value: &RpcHeader) -> Self {
        Self {
            block_hash: value.hash.as_bytes(),
            blue_work: value.blue_work,
            daa_score: value.daa_score,
        }
    }
}
