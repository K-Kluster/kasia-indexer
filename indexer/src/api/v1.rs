use crate::api::v1::handshakes::HandshakeApi;
use crate::api::v1::contextual_messages::ContextualMessageApi;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use futures_util::FutureExt;
use indexer_lib::database::messages::handshakes::{
    HandshakeByReceiverPartition, HandshakeBySenderPartition,
};
use indexer_lib::database::messages::contextual_messages::ContextualMessageBySenderPartition;
use indexer_lib::database::messages::TxIdToHandshakePartition;
use indexer_lib::database::processing::TxIDToAcceptancePartition;
use indexer_lib::metrics::{SharedMetrics, IndexerMetricsSnapshot};
use std::net::SocketAddr;
use tower_http::cors::CorsLayer;
use tracing::error;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;
pub mod handshakes;
pub mod contextual_messages;

#[derive(OpenApi)]
#[openapi(
    paths(
        handshakes::get_handshakes_by_sender,
        handshakes::get_handshakes_by_receiver,
        contextual_messages::get_contextual_messages_by_sender,
        get_metrics,
    ),
    components(
        schemas(handshakes::HandshakeResponse, contextual_messages::ContextualMessageResponse, IndexerMetricsSnapshot)
    ),
    tags(
        (name = "Kasia Indexer API", description = "Kasia Indexer API")
    )
)]
pub struct ApiDoc;

#[derive(Clone)]
pub struct Api {
    handshake_api: HandshakeApi,
    contextual_message_api: ContextualMessageApi,
    metrics: SharedMetrics,
}

impl Api {
    pub fn new(
        tx_keyspace: fjall::TxKeyspace,
        handshake_by_sender_partition: HandshakeBySenderPartition,
        handshake_by_receiver_partition: HandshakeByReceiverPartition,
        contextual_message_by_sender_partition: ContextualMessageBySenderPartition,
        tx_id_to_acceptance_partition: TxIDToAcceptancePartition,
        tx_id_to_handshake_partition: TxIdToHandshakePartition,
        metrics: SharedMetrics,
    ) -> Self {
        let handshake_api = HandshakeApi::new(
            tx_keyspace.clone(),
            handshake_by_sender_partition,
            handshake_by_receiver_partition,
            tx_id_to_acceptance_partition.clone(),
            tx_id_to_handshake_partition,
        );
        let contextual_message_api = ContextualMessageApi::new(
            tx_keyspace,
            contextual_message_by_sender_partition,
            tx_id_to_acceptance_partition,
        );
        Self { 
            handshake_api,
            contextual_message_api,
            metrics,
        }
    }

    pub async fn serve(
        self,
        bind_address: &str,
        shutdown: tokio::sync::oneshot::Receiver<()>,
    ) -> anyhow::Result<()> {
        let addr: SocketAddr = bind_address.parse()?;
        let app = self.router();
        let listener = tokio::net::TcpListener::bind(addr).await?;
        tracing::info!("Starting API server on {}", addr);
        axum::serve(listener, app.into_make_service())
            .with_graceful_shutdown(shutdown.map(|v| {
                _ = v.inspect_err(|_err| error!("shutdown receive error"));
            }))
            .await?;
        Ok(())
    }

    fn router(&self) -> Router {
        Router::new()
            .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
            .nest("/handshakes", HandshakeApi::router().with_state(self.handshake_api.clone()))
            .nest("/contextual-messages", ContextualMessageApi::router().with_state(self.contextual_message_api.clone()))
            .route("/metrics", get(get_metrics).with_state(self.metrics.clone()))
            .layer(CorsLayer::permissive())
    }
}

#[utoipa::path(
    get,
    path = "/metrics",
    responses(
        (status = 200, description = "Get system metrics", body = IndexerMetricsSnapshot)
    )
)]
async fn get_metrics(State(metrics): State<SharedMetrics>) -> impl IntoResponse {
    Json(metrics.snapshot())
}
