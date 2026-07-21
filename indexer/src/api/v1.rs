use crate::api::v1::contextual_messages::ContextualMessageApi;
use crate::api::v1::handshakes::HandshakeApi;
use crate::api::v1::payments::PaymentApi;
use crate::api::v1::push::PushApi;
use crate::api::v1::self_stash::SelfStashApi;
use crate::context::IndexerContext;
use axum::Router;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::routing::get;
use indexer_actors::metrics::{IndexerMetricsSnapshot, SharedMetrics};
use indexer_db::messages::contextual_message::{
    ContextualMessageBySenderPartition, TxIdToContextualMessagePartition,
};
use indexer_db::messages::handshake::{
    HandshakeByReceiverPartition, HandshakeBySenderPartition, TxIdToHandshakePartition,
};
use indexer_db::messages::payment::{
    PaymentByReceiverPartition, PaymentBySenderPartition, TxIdToPaymentPartition,
};
use indexer_db::messages::self_stash::{SelfStashByOwnerPartition, TxIdToSelfStashPartition};
use indexer_db::processing::tx_id_to_acceptance::TxIDToAcceptancePartition;
use std::net::SocketAddr;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

pub mod contextual_messages;
pub mod handshakes;
pub mod payments;
mod prometheus;
pub mod push;
pub mod self_stash;

#[derive(OpenApi)]
#[openapi(
    paths(
        handshakes::get_handshakes_by_sender,
        handshakes::get_handshakes_by_receiver,
        contextual_messages::get_contextual_messages_by_sender,
        payments::get_payments_by_sender,
        payments::get_payments_by_receiver,
        self_stash::get_self_stash_by_owner,
        push::create_challenge,
        push::register_device,
        push::update_registration,
        push::unregister_device,
        get_metrics,
    ),
    components(
        schemas(handshakes::HandshakeResponse, contextual_messages::ContextualMessageResponse, payments::PaymentResponse, self_stash::SelfStashResponse, push::PushRegistrationRequest, push::PushUpdateRequest, push::PushUnregisterRequest, push::PushAuthRequest, push::PushChallengeResponse, push::PushResponse, push::ErrorResponse, IndexerMetricsSnapshot)
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
    payment_api: PaymentApi,
    self_stash_api: SelfStashApi,
    push_api: PushApi,
    metrics: SharedMetrics,
}

const PUSH_REQUEST_BODY_MAX_BYTES: usize = 64 * 1024;

impl Api {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tx_keyspace: fjall::TxKeyspace,
        handshake_by_sender_partition: HandshakeBySenderPartition,
        handshake_by_receiver_partition: HandshakeByReceiverPartition,
        contextual_message_by_sender_partition: ContextualMessageBySenderPartition,
        tx_id_to_contextual_message_partition: TxIdToContextualMessagePartition,
        payment_by_sender_partition: PaymentBySenderPartition,
        payment_by_receiver_partition: PaymentByReceiverPartition,
        tx_id_to_acceptance_partition: TxIDToAcceptancePartition,
        tx_id_to_handshake_partition: TxIdToHandshakePartition,
        tx_id_to_payment_partition: TxIdToPaymentPartition,
        self_stash_by_owner_partition: SelfStashByOwnerPartition,
        tx_id_to_self_stash_partition: TxIdToSelfStashPartition,
        metrics: SharedMetrics,
        push_api: PushApi,
        context: IndexerContext,
    ) -> Self {
        let handshake_api = HandshakeApi::new(
            tx_keyspace.clone(),
            handshake_by_sender_partition,
            handshake_by_receiver_partition,
            tx_id_to_acceptance_partition.clone(),
            tx_id_to_handshake_partition,
            metrics.clone(),
            context.clone(),
        );

        let contextual_message_api = ContextualMessageApi::new(
            tx_keyspace.clone(),
            contextual_message_by_sender_partition,
            tx_id_to_acceptance_partition.clone(),
            tx_id_to_contextual_message_partition,
            metrics.clone(),
            context.clone(),
        );

        let payment_api = PaymentApi::new(
            tx_keyspace.clone(),
            payment_by_sender_partition,
            payment_by_receiver_partition,
            tx_id_to_payment_partition,
            tx_id_to_acceptance_partition.clone(),
            metrics.clone(),
            context.clone(),
        );

        let self_stash_api = SelfStashApi::new(
            tx_keyspace,
            self_stash_by_owner_partition,
            tx_id_to_acceptance_partition,
            tx_id_to_self_stash_partition,
            metrics.clone(),
            context,
        );

        Self {
            handshake_api,
            contextual_message_api,
            payment_api,
            self_stash_api,
            push_api,
            metrics,
        }
    }

    pub async fn serve(
        self,
        bind_address: &str,
        mut shutdown: tokio::sync::mpsc::Receiver<()>,
    ) -> anyhow::Result<()> {
        let addr: SocketAddr = bind_address.parse()?;
        let app = self.router();
        let listener = tokio::net::TcpListener::bind(addr).await?;
        tracing::info!("Starting API server on {}", addr);
        axum::serve(listener, app.into_make_service())
            .with_graceful_shutdown(async move {
                shutdown.recv().await;
            })
            .await?;
        Ok(())
    }

    fn router(&self) -> Router {
        let push_router = PushApi::router()
            .with_state(self.push_api.clone())
            .layer(DefaultBodyLimit::max(PUSH_REQUEST_BODY_MAX_BYTES));

        Router::new()
            .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
            .nest(
                "/handshakes",
                HandshakeApi::router().with_state(self.handshake_api.clone()),
            )
            .nest(
                "/contextual-messages",
                ContextualMessageApi::router().with_state(self.contextual_message_api.clone()),
            )
            .nest(
                "/payments",
                PaymentApi::router().with_state(self.payment_api.clone()),
            )
            .nest(
                "/self-stash",
                SelfStashApi::router().with_state(self.self_stash_api.clone()),
            )
            .nest("/v1/push", push_router)
            .route(
                "/metrics",
                get(get_metrics).with_state(self.metrics.clone()),
            )
    }
}

#[utoipa::path(
    get,
    path = "/metrics",
    responses(
        (status = 200, description = "Get Prometheus metrics", content_type = "text/plain", body = String)
    )
)]
async fn get_metrics(State(metrics): State<SharedMetrics>) -> impl IntoResponse {
    (
        [(CONTENT_TYPE, prometheus::CONTENT_TYPE)],
        prometheus::render(&metrics.snapshot()),
    )
}
