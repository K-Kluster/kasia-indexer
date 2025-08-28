use crate::api::to_rpc_address;
use crate::context::IndexerContext;
use anyhow::bail;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use indexer_db::AddressPayload;
use indexer_db::messages::contextual_message::{
    ContextualMessageBySenderPartition, TxIdToContextualMessagePartition,
};
use indexer_db::processing::tx_id_to_acceptance::TxIDToAcceptancePartition;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::task::spawn_blocking;
use utoipa::{IntoParams, ToSchema};

#[derive(Clone)]
pub struct ContextualMessageApi {
    tx_keyspace: fjall::TxKeyspace,
    contextual_message_by_sender_partition: ContextualMessageBySenderPartition,
    tx_id_to_contextual_message_partition: TxIdToContextualMessagePartition,
    tx_id_to_acceptance_partition: TxIDToAcceptancePartition,
    context: IndexerContext,
}

impl ContextualMessageApi {
    pub fn new(
        tx_keyspace: fjall::TxKeyspace,
        contextual_message_by_sender_partition: ContextualMessageBySenderPartition,
        tx_id_to_acceptance_partition: TxIDToAcceptancePartition,
        tx_id_to_contextual_message_partition: TxIdToContextualMessagePartition,
        context: IndexerContext,
    ) -> Self {
        Self {
            tx_keyspace,
            contextual_message_by_sender_partition,
            tx_id_to_contextual_message_partition,
            tx_id_to_acceptance_partition,
            context,
        }
    }

    pub fn router() -> Router<Self> {
        Router::new().route("/by-sender", get(get_contextual_messages_by_sender))
    }
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct ContextualMessagePaginationParams {
    pub limit: Option<usize>,
    pub block_time: Option<u64>,
    pub address: String,
    pub alias: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ContextualMessageResponse {
    pub tx_id: String,
    pub sender: String,
    pub alias: String,
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
    path = "/contextual-messages/by-sender",
    params(ContextualMessagePaginationParams),
    responses(
        (status = 200, description = "Get contextual messages by sender", body = [ContextualMessageResponse]),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
async fn get_contextual_messages_by_sender(
    State(state): State<ContextualMessageApi>,
    Query(params): Query<ContextualMessagePaginationParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(10).min(50);
    let cursor = params.block_time.unwrap_or(0);

    let sender_rpc = match kaspa_rpc_core::RpcAddress::try_from(params.address) {
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

    // Decode alias hex (max 32 hex chars = 16 bytes)
    if params.alias.len() > 32 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Alias hex length cannot exceed 32 characters".to_string(),
            }),
        ));
    }

    let mut alias_bytes = [0u8; 16];
    match faster_hex::hex_decode(
        params.alias.as_bytes(),
        &mut alias_bytes[..params.alias.len() / 2],
    ) {
        Ok(_) => (),
        Err(e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Invalid alias hex: {e}"),
                }),
            ));
        }
    };

    let alias = params.alias;

    let result = spawn_blocking(move || {
        let rtx = state.tx_keyspace.read_tx();

        let mut seen_tx_ids = std::collections::HashSet::new();

        state
            .contextual_message_by_sender_partition
            .get_by_sender_alias_from_block_time(&rtx, &sender, &alias_bytes, cursor)
            .process_results(|iter| {
                iter.filter(|message| seen_tx_ids.insert(message.tx_id))
                    .take(limit)
                    .map(|message_key| {
                        let block_time = message_key.block_time.into();

                        let sender_str =
                            match to_rpc_address(&message_key.sender, state.context.network_type) {
                                Ok(Some(addr)) => addr.to_string(),
                                Ok(None) => String::new(),
                                Err(e) => bail!("Address conversion error: {}", e),
                            };

                        let acceptance = state
                            .tx_id_to_acceptance_partition
                            .acceptance_by_tx_id_rtx(&rtx, &message_key.tx_id)?;

                        let (accepting_block, accepting_daa_score) =
                            if let Some(acceptance) = acceptance {
                                (
                                    Some(faster_hex::hex_string(
                                        &acceptance.header.accepting_block_hash,
                                    )),
                                    Some(acceptance.header.accepting_daa.into()),
                                )
                            } else {
                                (None, None)
                            };
                        let sealed_hex = state
                            .tx_id_to_contextual_message_partition
                            .get_rtx(&rtx, &message_key.tx_id)?
                            .expect("Message not found");
                        let message_payload = faster_hex::hex_string(sealed_hex.as_ref());

                        Ok(ContextualMessageResponse {
                            tx_id: faster_hex::hex_string(&message_key.tx_id),
                            sender: sender_str,
                            alias: alias.clone(), // todo use byteview
                            block_time,
                            accepting_block,
                            accepting_daa_score,
                            message_payload,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .flatten()
    })
    .await;

    match result {
        Ok(Ok(messages)) => Ok(Json(messages)),
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
