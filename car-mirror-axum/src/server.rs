use crate::{extract::dag_cbor::DagCbor, AppResult};
use axum::{
    body::{Body, HttpBody},
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
use tower_http::{
    cors::{Any, CorsLayer},
    trace::{DefaultMakeSpan, TraceLayer},
};
use wnfs_common::BlockStore;

/// TODO(matheus23): docs
pub fn app(store: impl BlockStore + Clone + 'static) -> Router {
    let cors = CorsLayer::new()
        .allow_methods(Any)
        .allow_headers(Any)
        .allow_origin(Any);

    Router::new()
        .nest("/dag", dag_router(store))
        .layer(cors)
        .layer(
            TraceLayer::new_for_http().make_span_with(DefaultMakeSpan::new().include_headers(true)),
        )
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
#[tracing::instrument(skip(state), err, ret)]
pub async fn car_mirror_push<B: BlockStore + Clone + 'static>(
    State(state): State<ServerState<B>>,
    Path(cid_string): Path<String>,
    body: Body,
) -> AppResult<(StatusCode, DagCbor<PushResponse>)>
where {
    let cid = Cid::from_str(&cid_string)?;

    let content_length = body.size_hint().exact();
    let body_stream = body.into_data_stream();

    tracing::info!(content_length, "Parsed content length hint");

    let mut reader = StreamReader::new(
        body_stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
    );

    let response = car_mirror::push::response_streaming(
        cid,
        &mut reader,
        &Config::default(),
        &state.store,
        &state.cache,
    )
    .await?;

    if content_length.is_some() {
        tracing::info!("Draining request");
        // If the client provided a `Content-Length` value, then
        // we know the client didn't stream the request.
        // In that case, it's common that the client doesn't support
        // getting a response before it finished finished sending,
        // because the socket closes early, before the client manages
        // to read the response.
        tokio::io::copy(&mut reader, &mut tokio::io::sink()).await?;
    }

    if response.indicates_finished() {
        Ok((StatusCode::OK, DagCbor(response)))
    } else {
        Ok((StatusCode::ACCEPTED, DagCbor(response)))
    }
}

/// TODO(matheus23): docs
#[tracing::instrument(skip(state), err, ret)]
pub async fn car_mirror_pull<B: BlockStore + Clone + 'static>(
    State(state): State<ServerState<B>>,
    Path(cid_string): Path<String>,
    pull_request: Option<DagCbor<PullRequest>>,
) -> AppResult<(StatusCode, Body)> {
    let cid = Cid::from_str(&cid_string)?;

    let DagCbor(request) = pull_request.unwrap_or_else(|| {
        DagCbor(PullRequest {
            resources: vec![cid],
            bloom_hash_count: 3,
            bloom_bytes: vec![],
        })
    });

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
