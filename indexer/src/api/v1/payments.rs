use crate::api::to_rpc_address;
use crate::context::IndexerContext;
use anyhow::bail;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use indexer_db::AddressPayload;
use indexer_db::messages::payment::{
    PaymentByReceiverPartition, PaymentBySenderPartition, TxIdToPaymentPartition,
};
use indexer_db::processing::tx_id_to_acceptance::TxIDToAcceptancePartition;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tokio::task::spawn_blocking;
use utoipa::{IntoParams, ToSchema};

#[derive(Clone)]
pub struct PaymentApi {
    tx_keyspace: fjall::TxKeyspace,
    payment_by_sender_partition: PaymentBySenderPartition,
    payment_by_receiver_partition: PaymentByReceiverPartition,
    tx_id_to_payment_partition: TxIdToPaymentPartition,
    tx_id_to_acceptance_partition: TxIDToAcceptancePartition,
    context: IndexerContext,
}

impl PaymentApi {
    pub fn new(
        tx_keyspace: fjall::TxKeyspace,
        payment_by_sender_partition: PaymentBySenderPartition,
        payment_by_receiver_partition: PaymentByReceiverPartition,
        tx_id_to_payment_partition: TxIdToPaymentPartition,
        tx_id_to_acceptance_partition: TxIDToAcceptancePartition,
        context: IndexerContext,
    ) -> Self {
        Self {
            tx_keyspace,
            payment_by_sender_partition,
            payment_by_receiver_partition,
            tx_id_to_payment_partition,
            tx_id_to_acceptance_partition,
            context,
        }
    }

    pub fn router() -> Router<Self> {
        Router::new()
            .route("/by-sender", get(get_payments_by_sender))
            .route("/by-receiver", get(get_payments_by_receiver))
    }
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct PaymentPaginationParams {
    pub limit: Option<usize>,
    pub block_time: Option<u64>,
    pub address: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PaymentResponse {
    pub tx_id: String,
    pub sender: Option<String>,
    pub receiver: String,
    pub block_time: u64,
    pub amount: u64,
    pub message: String, // sealed_hex
    pub accepting_block: Option<String>,
    pub accepting_daa_score: Option<u64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
}

#[utoipa::path(
    get,
    path = "/payments/by-sender",
    params(PaymentPaginationParams),
    responses(
        (status = 200, description = "Get payments by sender", body = [PaymentResponse]),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
async fn get_payments_by_sender(
    State(state): State<PaymentApi>,
    Query(params): Query<PaymentPaginationParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(10).min(50);
    let cursor = params.block_time.unwrap_or(0);

    let sender_rpc = match kaspa_rpc_core::RpcAddress::try_from(params.address.as_str()) {
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

    let address_clone = params.address.clone();

    let result = spawn_blocking(move || {
        let rtx = state.tx_keyspace.read_tx();

        let mut seen_tx_ids = HashSet::new();

        state
            .payment_by_sender_partition
            .get_by_sender_from_block_time(&rtx, &sender, cursor)
            .process_results(|iter| iter.filter(|payment| seen_tx_ids.insert(payment.tx_id)).take(limit).map(|key| {
                let payment_data = match state.tx_id_to_payment_partition.get_rtx(&rtx, &key.tx_id) {
                    Ok(Some(data)) => data,
                    Ok(None) => bail!("Payment data inconsistency: tx_id {} found in payment_by_sender but not in tx_id_to_payment", faster_hex::hex_string(&key.tx_id)),
                    Err(e) => bail!("Failed to get payment data: {}", e),
                };

                let (accepting_block, accepting_daa_score) = match state
                    .tx_id_to_acceptance_partition
                    .acceptance_by_tx_id_rtx(&rtx, &key.tx_id)?
                {
                    Some(acceptance) => (
                        Some(faster_hex::hex_string(
                            &acceptance.header.accepting_block_hash,
                        )),
                        Some(acceptance.header.accepting_daa.into()),
                    ),
                    None => (None, None),
                };

                let receiver_address = match to_rpc_address(&key.receiver, state.context.network_type) {
                    Ok(Some(addr)) => addr.to_string(),
                    Ok(None) => bail!("Database consistency error: receiver address has EMPTY_VERSION"),
                    Err(e) => bail!("Failed to convert receiver address: {}", e),
                };

                Ok(PaymentResponse {
                    tx_id: faster_hex::hex_string(&key.tx_id),
                    sender: Some(address_clone.clone()),
                    receiver: receiver_address,
                    block_time: key.block_time.into(),
                    amount: payment_data.amount(),
                    message: faster_hex::hex_string(payment_data.sealed_hex()),
                    accepting_block,
                    accepting_daa_score,
                })
            }).collect::<Result<Vec<_>, _>>()
        ).flatten()
    }).await;

    match result {
        Ok(Ok(payments)) => Ok(Json(payments)),
        Ok(Err(e)) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
        Err(join_err) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Task error: {join_err}"),
            }),
        )),
    }
}

#[utoipa::path(
    get,
    path = "/payments/by-receiver",
    params(PaymentPaginationParams),
    responses(
        (status = 200, description = "Get payments by receiver", body = [PaymentResponse]),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
async fn get_payments_by_receiver(
    State(state): State<PaymentApi>,
    Query(params): Query<PaymentPaginationParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(10).min(50);
    let cursor = params.block_time.unwrap_or(0);

    let receiver_rpc = match kaspa_rpc_core::RpcAddress::try_from(params.address) {
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
        let rtx = state.tx_keyspace.read_tx();

        let mut seen_tx_ids = HashSet::new();

        state
            .payment_by_receiver_partition
            .get_by_receiver_from_block_time(&rtx, &receiver, cursor)
            .process_results(|iter| iter.filter(|payment| seen_tx_ids.insert(payment.0.tx_id)).take(limit).map(|(key, sender_payload)| {

                let payment_data = match state.tx_id_to_payment_partition.get_rtx(&rtx, &key.tx_id) {
                    Ok(Some(data)) => data,
                    Ok(None) => bail!("Payment data inconsistency: tx_id {} found in payment_by_receiver but not in tx_id_to_payment", faster_hex::hex_string(&key.tx_id)),
                    Err(e) => bail!("Failed to get payment data: {}", e),
                };

                let (accepting_block, accepting_daa_score) = match state
                    .tx_id_to_acceptance_partition
                    .acceptance_by_tx_id_rtx(&rtx, &key.tx_id)?
                {
                    Some(acceptance) => (
                        Some(faster_hex::hex_string(
                            &acceptance.header.accepting_block_hash,
                        )),
                        Some(acceptance.header.accepting_daa.into()),
                    ),
                    None => (None, None),
                };

                let sender_address = match to_rpc_address(&sender_payload, state.context.network_type) {
                    Ok(opt_addr) => opt_addr.map(|addr| addr.to_string()),
                    Err(e) => bail!("Failed to convert sender address: {}", e),
                };

                let receiver_address = match to_rpc_address(&key.receiver, state.context.network_type) {
                    Ok(Some(addr)) => addr.to_string(),
                    Ok(None) => bail!("Database consistency error: receiver address has EMPTY_VERSION"),
                    Err(e) => bail!("Failed to convert receiver address: {}", e),
                };

                Ok(PaymentResponse {
                    tx_id: faster_hex::hex_string(&key.tx_id),
                    sender: sender_address,
                    receiver: receiver_address,
                    block_time: key.block_time.into(),
                    amount: payment_data.amount(),
                    message: faster_hex::hex_string(payment_data.sealed_hex()),
                    accepting_block,
                    accepting_daa_score,
                })
            }).collect::<Result<Vec<_>, _>>()
        ).flatten()
    }).await;

    match result {
        Ok(Ok(payments)) => Ok(Json(payments)),
        Ok(Err(e)) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )),
        Err(join_err) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Task error: {join_err}"),
            }),
        )),
    }
}
