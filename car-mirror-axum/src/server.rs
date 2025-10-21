use crate::{extract::dag_cbor::DagCbor, AppResult};
use anyhow::Result;
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
use std::str::FromStr;
use tokio_util::io::StreamReader;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::{DefaultMakeSpan, TraceLayer},
};
use wnfs_common::{BlockStore, Cid};

/// Serve a basic car mirror server that serves the routes from `app`
/// with given blockstore at `127.0.0.1:3344`.
///
/// When the server is ready to accept connections, it will print a
/// message to the console: "Listening on 127.0.0.1.3344".
///
/// This is a simple function mostly useful for tests. If you want to
/// customize its function, copy its source and create a modified copy
/// as needed.
///
/// This is not intended for production usage, for multiple reasons:
/// - There is no rate-limiting on the requests, so such a service would
///   be susceptible to DoS attacks.
/// - The `push` route should usually only be available behind
///   authorization or perhaps be heavily rate-limited, otherwise it
///   can cause unbounded memory or disk growth remotely.
pub async fn serve(store: impl BlockStore + Clone + 'static) -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3344").await?;
    let addr = listener.local_addr()?;
    println!("Listening on {addr}");
    axum::serve(listener, app(store)).await?;
    Ok(())
}

/// This will serve the routes from `dag_router` nested under `/dag`, but with
/// tracing and cors headers.
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

/// Returns a router for car mirror requests with the
/// given blockstore as well as a new 10MB cache as state.
///
/// This serves following routes:
/// - `GET /pull/:cid` for pull requests (GET is generally not recommended here)
/// - `POST /pull/:cid` for pull requests
/// - `POST /push/:cid` for push requests
pub fn dag_router(store: impl BlockStore + Clone + 'static) -> Router {
    Router::new()
        .route("/pull/:cid", get(car_mirror_pull))
        .route("/pull/:cid", post(car_mirror_pull))
        .route("/push/:cid", post(car_mirror_push))
        .with_state(ServerState::new(store))
}

/// The server state used for a basic car mirror server.
///
/// Stores a block store and a car mirror operations cache.
#[derive(Debug, Clone)]
pub struct ServerState<B: BlockStore + Clone + 'static> {
    store: B,
    cache: InMemoryCache,
}

impl<B: BlockStore + Clone + 'static> ServerState<B> {
    /// Initialize the server state with given blockstore and
    /// a roughly 10MB car mirror operations cache.
    pub fn new(store: B) -> ServerState<B> {
        Self {
            store,
            cache: InMemoryCache::new(100_000),
        }
    }
}

/// Handle a POST request for car mirror pushes.
///
/// This will consume the incoming body as a car file stream.
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

/// Handle an incoming GET or POST request for a car mirror pull.
///
/// The response body will contain a stream of car file chunks.
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

#[axum_macros::debug_handler]
async fn not_found() -> (StatusCode, &'static str) {
    tracing::info!("Hit 404");
    (StatusCode::NOT_FOUND, "404 Not Found")
}
