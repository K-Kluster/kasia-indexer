use crate::api::to_rpc_address;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use indexer_lib::database::messages::TxIdToHandshakePartition;
use indexer_lib::database::messages::handshakes::{
    AddressPayload, HandshakeByReceiverPartition, HandshakeBySenderPartition,
};
use indexer_lib::database::processing::TxIDToAcceptancePartition;
use kaspa_rpc_core::{RpcAddress, RpcNetworkType};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use tokio::task::spawn_blocking;
use anyhow::{bail};

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

    pub fn router() -> Router<Self> {
        Router::new()
            .route("/by-sender", get(get_handshakes_by_sender))
            .route("/by-receiver", get(get_handshakes_by_receiver))
    }
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct HandshakePaginationParams {
    pub limit: Option<usize>,
    pub block_time: Option<u64>,
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

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
}

#[utoipa::path(
    get,
    path = "/handshakes/by-sender",
    params(HandshakePaginationParams),
    responses(
        (status = 200, description = "Get handshakes by sender", body = [HandshakeResponse]),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
async fn get_handshakes_by_sender(
    State(state): State<HandshakeApi>,
    Query(params): Query<HandshakePaginationParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(10).min(50);
    let cursor = params.block_time.unwrap_or(0);

    let sender_rpc = match RpcAddress::try_from(params.address) {
        Ok(addr) => addr,
        Err(e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Invalid address: {e}"),
                }),
            ));
        }
    };
    let sender = match AddressPayload::try_from(&sender_rpc) {
        Ok(payload) => payload,
        Err(e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Invalid address payload: {e}"),
                }),
            ));
        }
    };

    let result = spawn_blocking(move || {
        let mut handshakes = vec![];

        let rtx = state.tx_keyspace.read_tx();

        for handshake in state
            .handshake_by_sender_partition
            .iter_by_sender_from_block_time_rtx(&rtx, sender, cursor)
            .take(limit)
        {
            let handshake = match handshake {
                Ok(hs) => hs,
                Err(e) => bail!("Database error: {}", e),
            };
            let block_time = u64::from_be_bytes(handshake.block_time);

            let tx_id = faster_hex::hex_string(&handshake.tx_id);
            let sender_str = match to_rpc_address(&handshake.sender, RpcNetworkType::Mainnet) {
                Ok(Some(addr)) => addr.to_string(),
                Ok(None) => bail!("Database consistency error: sender address has EMPTY_VERSION"),
                Err(e) => bail!("Address conversion error: {}", e),
            };
            let receiver_str = match to_rpc_address(&handshake.receiver, RpcNetworkType::Mainnet) {
                Ok(Some(addr)) => addr.to_string(),
                Ok(None) => bail!("Database consistency error: receiver address has EMPTY_VERSION"),
                Err(e) => bail!("Address conversion error: {}", e),
            };

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
            let payload_bytes = match state
                .tx_id_to_handshake_partition
                .get_rtx(&rtx, handshake.tx_id)
            {
                Ok(Some(bts)) => bts,
                Ok(None) => bail!("Missing handshake payload"),
                Err(e) => bail!("Database error: {}", e),
            };
            let message_payload = faster_hex::hex_string(payload_bytes.as_ref());

            handshakes.push(HandshakeResponse {
                tx_id,
                sender: sender_str,
                receiver: receiver_str,
                block_time,
                accepting_block,
                accepting_daa_score,
                message_payload,
            });
        }

        Ok(handshakes)
    }).await;

    match result {
        Ok(Ok(handshakes)) => Ok(Json(handshakes)),
        Ok(Err(e)) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: e.to_string() }),
        )),
        Err(join_err) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Task error: {join_err}") }),
        )),
    }
}

#[utoipa::path(
    get,
    path = "/handshakes/by-receiver",
    params(HandshakePaginationParams),
    responses(
        (status = 200, description = "Get handshakes by receiver", body = [HandshakeResponse]),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
async fn get_handshakes_by_receiver(
    State(state): State<HandshakeApi>,
    Query(params): Query<HandshakePaginationParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(10).min(50);
    let cursor = params.block_time.unwrap_or(0);

    let receiver_rpc = match RpcAddress::try_from(params.address.clone()) {
        Ok(addr) => addr,
        Err(e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Invalid address: {e}"),
                }),
            ));
        }
    };
    let receiver = match AddressPayload::try_from(&receiver_rpc) {
        Ok(payload) => payload,
        Err(e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Invalid address payload: {e}"),
                }),
            ));
        }
    };

    let result = spawn_blocking(move || {
        let mut handshakes = vec![];

        let rtx = state.tx_keyspace.read_tx();

        for handshake_result in state
            .handshake_by_receiver_partition
            .iter_by_receiver_from_block_time_rtx(&rtx, receiver, cursor)
            .take(limit)
        {
            let (handshake, sender_payload) = match handshake_result {
                Ok(res) => res,
                Err(e) => bail!("Database error: {}", e),
            };
            let block_time = u64::from_be_bytes(handshake.block_time);

            let tx_id = faster_hex::hex_string(&handshake.tx_id);
            let sender_str = match to_rpc_address(&sender_payload, RpcNetworkType::Mainnet) {
                Ok(Some(addr)) => addr.to_string(),
                Ok(None) => bail!("Database consistency error: sender address has EMPTY_VERSION"),
                Err(e) => bail!("Address conversion error: {}", e),
            };
            let receiver_str = match to_rpc_address(&handshake.receiver, RpcNetworkType::Mainnet) {
                Ok(Some(addr)) => addr.to_string(),
                Ok(None) => bail!("Database consistency error: receiver address has EMPTY_VERSION"),
                Err(e) => bail!("Address conversion error: {}", e),
            };

            let acceptance = state
                .tx_id_to_acceptance_partition
                .get_by_tx_id(&rtx, &handshake.tx_id)
                .flatten()
                .next();
            let payload_bytes = match state
                .tx_id_to_handshake_partition
                .get_rtx(&rtx, handshake.tx_id)
            {
                Ok(Some(bts)) => bts,
                Ok(None) => bail!("Missing handshake payload"),
                Err(e) => bail!("Database error: {}", e),
            };
            let message_payload = faster_hex::hex_string(payload_bytes.as_ref());
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
                sender: sender_str,
                receiver: receiver_str,
                block_time,
                accepting_block,
                accepting_daa_score,
                message_payload,
            });
        }

        Ok(handshakes)
    }).await;

    match result {
        Ok(Ok(handshakes)) => Ok(Json(handshakes)),
        Ok(Err(e)) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: e.to_string() }),
        )),
        Err(join_err) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: format!("Task error: {join_err}") }),
        )),
    }
}