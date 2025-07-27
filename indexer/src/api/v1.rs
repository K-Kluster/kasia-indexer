use crate::api::v1::handshakes::HandshakeApi;
use axum::Router;
use futures_util::FutureExt;
use indexer_lib::database::messages::handshakes::{
    HandshakeByReceiverPartition, HandshakeBySenderPartition,
};
use indexer_lib::database::messages::TxIdToHandshakePartition;
use indexer_lib::database::processing::TxIDToAcceptancePartition;
use std::net::SocketAddr;
use tower_http::cors::CorsLayer;
use tracing::error;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;
pub mod handshakes;

#[derive(OpenApi)]
#[openapi(
    paths(
        handshakes::get_handshakes_by_sender,
        handshakes::get_handshakes_by_receiver,
    ),
    components(
        schemas(handshakes::HandshakeResponse)
    ),
    tags(
        (name = "Kasia Indexer API", description = "Kasia Indexer API")
    )
)]
pub struct ApiDoc;

#[derive(Clone)]
pub struct Api {
    handshake_api: HandshakeApi,
}

impl Api {
    pub fn new(
        tx_keyspace: fjall::TxKeyspace,
        handshake_by_sender_partition: HandshakeBySenderPartition,
        handshake_by_receiver_partition: HandshakeByReceiverPartition,
        tx_id_to_acceptance_partition: TxIDToAcceptancePartition,
        tx_id_to_handshake_partition: TxIdToHandshakePartition,
    ) -> Self {
        let handshake_api = HandshakeApi::new(
            tx_keyspace,
            handshake_by_sender_partition,
            handshake_by_receiver_partition,
            tx_id_to_acceptance_partition,
            tx_id_to_handshake_partition,
        );
        Self { handshake_api }
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
            .nest("/handshakes", self.handshake_api.clone().router())
            .layer(CorsLayer::permissive())
            .with_state(self.handshake_api.clone())
    }
}
