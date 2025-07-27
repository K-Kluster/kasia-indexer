use indexer_lib::database::messages::AddressPayload;
use kaspa_addresses::{Address, Version};
use kaspa_rpc_core::{RpcAddress, RpcNetworkType};

pub mod v1;

fn to_rpc_address(
    address_payload: &AddressPayload,
    network: RpcNetworkType,
) -> anyhow::Result<RpcAddress> {
    let version = Version::try_from(u8::MAX - address_payload.inverse_version)?;
    let address = match version {
        Version::PubKey | Version::ScriptHash => {
            Address::new(network.into(), version, &address_payload.payload[0..32])
        }
        Version::PubKeyECDSA => {
            Address::new(network.into(), version, address_payload.payload.as_slice())
        }
    };

    Ok(address)
}
