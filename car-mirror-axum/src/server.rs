use crate::{extract::dag_cbor::DagCbor, AppResult};
use anyhow::Result;
use axum::{
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Router,
};
use car_mirror::{
    cache::InMemoryCache,
    common::Config,
    messages::{PullRequest, PushResponse},
};
use futures::TryStreamExt;
use libipld::Cid;
use std::str::FromStr;
use tokio_util::io::StreamReader;
use tower_http::cors::{Any, CorsLayer};
use wnfs_common::BlockStore;

/// TODO(matheus23): docs
pub async fn serve(store: impl BlockStore + Clone + 'static) -> Result<()> {
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3344").await?;
    let addr = listener.local_addr()?;
    println!("Listening on {addr}");
    axum::serve(listener, app(store)).await?;
    Ok(())
}

/// TODO(matheus23): docs
pub fn app(store: impl BlockStore + Clone + 'static) -> Router {
    let cors = CorsLayer::new()
        .allow_methods(Any)
        .allow_headers(Any)
        .allow_origin(Any);

    Router::new()
        .nest("/dag", dag_router(store))
        .layer(cors)
        .fallback(not_found)
}

/// TODO(matheus23): docs
pub fn dag_router(store: impl BlockStore + Clone + 'static) -> Router {
    Router::new()
        .route("/pull/:cid", get(car_mirror_pull))
        .route("/pull/:cid", post(car_mirror_pull))
        .route("/push/:cid", post(car_mirror_push))
        .with_state(ServerState::new(store))
}

/// TODO(matheus23): docs
#[derive(Debug, Clone)]
pub struct ServerState<B: BlockStore + Clone + 'static> {
    store: B,
    cache: InMemoryCache,
}

impl<B: BlockStore + Clone + 'static> ServerState<B> {
    /// TODO(matheus23): docs
    pub fn new(store: B) -> ServerState<B> {
        Self {
            store,
            cache: InMemoryCache::new(100_000),
        }
    }
}

/// TODO(matheus23): docs
#[tracing::instrument(skip(state))]
pub async fn car_mirror_push<B: BlockStore + Clone + 'static>(
    State(state): State<ServerState<B>>,
    Path(cid_string): Path<String>,
    body: Body,
) -> AppResult<(StatusCode, DagCbor<PushResponse>)>
where {
    tracing::info!("Handling request");
    let cid = Cid::from_str(&cid_string)?;

    let body_stream = body.into_data_stream();

    let reader = StreamReader::new(
        body_stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
    );

    let response = car_mirror::push::response_streaming(
        cid,
        reader,
        &Config::default(),
        &state.store,
        &state.cache,
    )
    .await?;

    if response.indicates_finished() {
        Ok((StatusCode::OK, DagCbor(response)))
    } else {
        Ok((StatusCode::ACCEPTED, DagCbor(response)))
    }
}

/// TODO(matheus23): docs
#[tracing::instrument(skip(state))]
pub async fn car_mirror_pull<B: BlockStore + Clone + 'static>(
    State(state): State<ServerState<B>>,
    Path(cid_string): Path<String>,
    DagCbor(request): DagCbor<PullRequest>,
) -> AppResult<(StatusCode, Body)> {
    tracing::info!("Handling request");
    let cid = Cid::from_str(&cid_string)?;

    let car_chunks = car_mirror::pull::response_streaming(
        cid,
        request,
        state.store.clone(),
        state.cache.clone(),
    )
    .await?;

    Ok((StatusCode::OK, Body::from_stream(car_chunks)))
}

/// TODO(matheus23): docs
#[axum_macros::debug_handler]
async fn not_found() -> (StatusCode, &'static str) {
    tracing::info!("Hit 404");
    (StatusCode::NOT_FOUND, "404 Not Found")
}
