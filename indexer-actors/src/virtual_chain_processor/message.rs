use kaspa_consensus_core::BlueWorkType;
use kaspa_rpc_core::VirtualChainChangedNotification;

#[derive(Debug)]
pub enum VccNotification {
    Connected {
        sink: [u8; 32],
        sink_blue_work: BlueWorkType,
        pp: [u8; 32],
    },
    Disconnected,
    Shutdown,
    Notification(VirtualChainChangedNotification),
}
