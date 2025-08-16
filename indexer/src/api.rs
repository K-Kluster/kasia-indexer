use indexer_db::{AddressPayload, EMPTY_VERSION};
use kaspa_addresses::{Address, Version};
use kaspa_rpc_core::{RpcAddress, RpcNetworkType};

pub mod v1;

pub fn to_rpc_address(
    address_payload: &AddressPayload,
    network: RpcNetworkType,
) -> anyhow::Result<Option<RpcAddress>> {
    // Return None if the AddressPayload has EMPTY_VERSION (unknown address)
    if address_payload.inverse_version == EMPTY_VERSION {
        return Ok(None);
    }

    let version = Version::try_from(u8::MAX - address_payload.inverse_version)?;
    let address = match version {
        Version::PubKey | Version::ScriptHash => {
            Address::new(network.into(), version, &address_payload.payload[0..32])
        }
        Version::PubKeyECDSA => {
            Address::new(network.into(), version, address_payload.payload.as_slice())
        }
    };

    Ok(Some(address))
}
