use crate::api::to_rpc_address;
use crate::context::IndexerContext;
use anyhow::bail;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use indexer_lib::database::messages::AddressPayload;
use indexer_lib::database::messages::self_stashes::SelfStashByOwnerPartition;
use indexer_lib::database::processing::TxIDToAcceptancePartition;
use serde::{Deserialize, Serialize};
use tokio::task::spawn_blocking;
use utoipa::{IntoParams, ToSchema};

#[derive(Clone)]
pub struct SelfStashApi {
    tx_keyspace: fjall::TxKeyspace,
    self_stash_by_owner_partition: SelfStashByOwnerPartition,
    tx_id_to_acceptance_partition: TxIDToAcceptancePartition,
    context: IndexerContext,
}

impl SelfStashApi {
    pub fn new(
        tx_keyspace: fjall::TxKeyspace,
        self_stash_by_owner_partition: SelfStashByOwnerPartition,
        tx_id_to_acceptance_partition: TxIDToAcceptancePartition,
        context: IndexerContext,
    ) -> Self {
        Self {
            tx_keyspace,
            self_stash_by_owner_partition,
            tx_id_to_acceptance_partition,
            context,
        }
    }

    pub fn router() -> Router<Self> {
        Router::new().route("/by-owner", get(get_self_stash_by_owner))
    }
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct SelfStashPaginationParams {
    pub limit: Option<usize>,
    pub block_time: Option<u64>,
    pub scope: Option<String>,
    pub owner: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SelfStashResponse {
    pub tx_id: String,
    pub owner: String,
    pub scope: Option<String>,
    pub block_time: u64,
    pub accepting_block: Option<String>,
    pub accepting_daa_score: Option<u64>,
    pub stashed_data: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
}

#[utoipa::path(
    get,
    path = "/self-stash/by-owner",
    params(SelfStashPaginationParams),
    responses(
        (status = 200, description = "Get Self stash by owner", body = [SelfStashResponse]),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    )
)]
async fn get_self_stash_by_owner(
    State(state): State<SelfStashApi>,
    Query(params): Query<SelfStashPaginationParams>,
) -> impl IntoResponse {
    let limit = params.limit.unwrap_or(10).min(50);
    let cursor = params.block_time.unwrap_or(0);

    let owner_rpc = match kaspa_rpc_core::RpcAddress::try_from(params.owner) {
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
    let owner = match AddressPayload::try_from(&owner_rpc) {
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

    // Decode scope hex if provided (max 510 hex chars = 255 bytes)
    if params.scope.clone().unwrap_or_default().len() > 255 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Alias hex length cannot exceed 32 characters".to_string(),
            }),
        ));
    }

    let mut scope_bytes = [0u8; 255];
    match faster_hex::hex_decode(
        params.scope.clone().unwrap_or_default().as_bytes(),
        &mut scope_bytes[..params.scope.unwrap_or_default().len() / 2],
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

    let result = spawn_blocking(move || {
        let mut messages = vec![];
        let rtx = state.tx_keyspace.read_tx();

        for self_stash_result in state
            .self_stash_by_owner_partition
            .iter_by_owner_and_scope_from_block_time_rtx(&rtx, Some(&scope_bytes), owner, cursor)
            .take(limit)
        {
            let (self_stash_key, self_stash_data) = match self_stash_result {
                Ok(res) => res,
                Err(e) => bail!("Database error: {}", e),
            };

            let block_time = u64::from_be_bytes(self_stash_key.block_time);
            let tx_id = faster_hex::hex_string(&self_stash_key.tx_id);

            let sender_str = match to_rpc_address(&self_stash_key.owner, state.context.network_type)
            {
                Ok(Some(addr)) => addr.to_string(),
                Ok(None) => String::new(),
                Err(e) => bail!("Address conversion error: {}", e),
            };

            let acceptance = state
                .tx_id_to_acceptance_partition
                .get_by_tx_id(&rtx, &self_stash_key.tx_id)
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

            let stashed_data_str = faster_hex::hex_string(self_stash_data.as_ref());

            let scope = if self_stash_key.scope.iter().rev().any(|&b| b != 0) {
                Some(faster_hex::hex_string(&self_stash_key.scope))
            } else {
                None
            };

            messages.push(SelfStashResponse {
                tx_id,
                owner: sender_str,
                scope,
                stashed_data: stashed_data_str,
                block_time,
                accepting_block,
                accepting_daa_score,
            });
        }

        Ok(messages)
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
