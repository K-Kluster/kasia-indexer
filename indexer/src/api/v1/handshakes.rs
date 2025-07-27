use crate::api::to_rpc_address;
use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use indexer_lib::database::messages::handshakes::{
    AddressPayload, HandshakeByReceiverPartition, HandshakeBySenderPartition,
};
use indexer_lib::database::messages::TxIdToHandshakePartition;
use indexer_lib::database::processing::TxIDToAcceptancePartition;
use kaspa_rpc_core::{RpcAddress, RpcNetworkType};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

#[derive(Clone)]
pub struct HandshakeApi {
    tx_keyspace: fjall::TxKeyspace,
    handshake_by_sender_partition: HandshakeBySenderPartition,
    handshake_by_receiver_partition: HandshakeByReceiverPartition,
    tx_id_to_acceptance_partition: TxIDToAcceptancePartition,
    tx_id_to_handshake_partition: TxIdToHandshakePartition,
}

impl HandshakeApi {
    pub fn new(
        tx_keyspace: fjall::TxKeyspace,
        handshake_by_sender_partition: HandshakeBySenderPartition,
        handshake_by_receiver_partition: HandshakeByReceiverPartition,
        tx_id_to_acceptance_partition: TxIDToAcceptancePartition,
        tx_id_to_handshake_partition: TxIdToHandshakePartition,
    ) -> Self {
        Self {
            tx_keyspace,
            handshake_by_sender_partition,
            handshake_by_receiver_partition,
            tx_id_to_acceptance_partition,
            tx_id_to_handshake_partition,
        }
    }

    pub fn router(self) -> Router<Self> {
        Router::new()
            .route("/by-sender", get(get_handshakes_by_sender))
            .route("/by-receiver", get(get_handshakes_by_receiver))
            .with_state(self)
    }
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct HandshakePaginationParams {
    pub limit: Option<usize>,
    pub daa_score: Option<u64>,
    pub address: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct HandshakeResponse {
    pub tx_id: String,
    pub sender: String,
    pub receiver: String,
    pub block_time: u64,
    pub accepting_block: Option<String>,
    pub accepting_daa_score: Option<u64>,
    pub message_payload: String,
}

#[utoipa::path(
    get,
    path = "/handshakes/by-sender",
    params(HandshakePaginationParams),
    responses(
        (status = 200, description = "Get handshakes by sender", body = [HandshakeResponse])
    )
)]
async fn get_handshakes_by_sender(
    State(state): State<HandshakeApi>,
    Query(params): Query<HandshakePaginationParams>,
) -> Json<Vec<HandshakeResponse>> {
    let limit = params.limit.unwrap_or(10).min(50);
    let cursor = params.daa_score.unwrap_or(0);
    let sender = RpcAddress::try_from(params.address).unwrap();
    let sender = AddressPayload::try_from(&sender).unwrap();

    let mut handshakes = vec![];

    let rtx = state.tx_keyspace.read_tx();

    for handshake in state
        .handshake_by_sender_partition
        .iter_by_sender_from_block_time_rtx(&rtx, sender, cursor)
    {
        let handshake = handshake.unwrap();
        let block_time = u64::from_be_bytes(handshake.block_time);

        let tx_id = faster_hex::hex_string(&handshake.tx_id);
        let sender = to_rpc_address(&handshake.sender, RpcNetworkType::Mainnet)
            .unwrap()
            .to_string();
        let receiver = to_rpc_address(&handshake.receiver, RpcNetworkType::Mainnet)
            .unwrap()
            .to_string();

        let acceptance = state
            .tx_id_to_acceptance_partition
            .get_by_tx_id(&rtx, &handshake.tx_id)
            .flatten()
            .next();

        let (accepting_block, accepting_daa_score) = if let Some((key, _)) = acceptance {
            (
                Some(faster_hex::hex_string(&key.accepted_by_block_hash)),
                Some(u64::from_be_bytes(key.accepted_at_daa)),
            )
        } else {
            (None, None)
        };
        let message_payload = state
            .tx_id_to_handshake_partition
            .get_rtx(&rtx, handshake.tx_id)
            .unwrap()
            .map(|bts| faster_hex::hex_string(bts.as_ref()))
            .unwrap();
        handshakes.push(HandshakeResponse {
            tx_id,
            sender,
            receiver,
            block_time,
            accepting_block,
            accepting_daa_score,
            message_payload,
        });

        if handshakes.len() >= limit {
            break;
        }
    }

    Json(handshakes)
}

#[utoipa::path(
    get,
    path = "/handshakes/by-receiver",
    params(HandshakePaginationParams),
    responses(
        (status = 200, description = "Get handshakes by receiver", body = [HandshakeResponse])
    )
)]
async fn get_handshakes_by_receiver(
    State(state): State<HandshakeApi>,
    Query(params): Query<HandshakePaginationParams>,
) -> Json<Vec<HandshakeResponse>> {
    let receiver = RpcAddress::try_from(params.address).unwrap();
    let receiver = AddressPayload::try_from(&receiver).unwrap();
    let limit = params.limit.unwrap_or(10).min(50);
    let cursor = params.daa_score.unwrap_or(0);

    let mut handshakes = vec![];

    let rtx = state.tx_keyspace.read_tx();

    for handshake_result in state
        .handshake_by_receiver_partition
        .iter_by_receiver_from_block_time_rtx(&rtx, receiver, cursor)
        .take(limit)
    {
        let (handshake, sender_payload) = handshake_result.unwrap();
        let block_time = u64::from_be_bytes(handshake.block_time);

        let tx_id = faster_hex::hex_string(&handshake.tx_id);
        let sender = to_rpc_address(&sender_payload, RpcNetworkType::Mainnet)
            .unwrap()
            .to_string();
        let receiver = to_rpc_address(&handshake.receiver, RpcNetworkType::Mainnet)
            .unwrap()
            .to_string();

        let acceptance = state
            .tx_id_to_acceptance_partition
            .get_by_tx_id(&rtx, &handshake.tx_id)
            .flatten()
            .next();
        let message_payload = state
            .tx_id_to_handshake_partition
            .get_rtx(&rtx, handshake.tx_id)
            .unwrap()
            .map(|bts| faster_hex::hex_string(bts.as_ref()))
            .unwrap();
        let (accepting_block, accepting_daa_score) = if let Some((key, _)) = acceptance {
            (
                Some(faster_hex::hex_string(&key.accepted_by_block_hash)),
                Some(u64::from_be_bytes(key.accepted_at_daa)),
            )
        } else {
            (None, None)
        };

        handshakes.push(HandshakeResponse {
            tx_id,
            sender,
            receiver,
            block_time,
            accepting_block,
            accepting_daa_score,
            message_payload,
        });
    }

    Json(handshakes)
}
