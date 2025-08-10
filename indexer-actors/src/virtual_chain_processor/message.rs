use kaspa_consensus_core::BlueWorkType;
use kaspa_rpc_core::{GetVirtualChainFromBlockResponse, VirtualChainChangedNotification};

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
