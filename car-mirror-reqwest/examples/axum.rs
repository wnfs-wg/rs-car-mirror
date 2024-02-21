use anyhow::Result;
use axum::{
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use car_mirror::{
    cache::{InMemoryCache, NoCache},
    common::Config,
    messages::{PullRequest, PushResponse},
};
use car_mirror_reqwest::RequestBuilderExt;
use futures::TryStreamExt;
use libipld::Cid;
use reqwest::Client;
use std::{future::IntoFuture, str::FromStr};
use tokio_util::io::StreamReader;
use wnfs_common::{BlockStore, MemoryBlockStore, CODEC_RAW};

#[tokio::main]
async fn main() -> Result<()> {
    // Say, you have a webserver running like so:
    let app = Router::new()
        .route("/dag/pull/:cid", get(car_mirror_pull))
        .route("/dag/push/:cid", post(car_mirror_push))
        .with_state(ServerState::new());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3344").await?;
    tokio::spawn(axum::serve(listener, app).into_future());

    // You can issue requests from your client like so:
    let store = MemoryBlockStore::new();
    let data = b"Hello, world!".to_vec();
    let root = store.put_block(data, CODEC_RAW).await?;

    let config = &Config::default();

    let client = Client::new();
    client
        .post(format!("http://localhost:3344/dag/push/{root}"))
        .run_car_mirror_push(root, &store, &NoCache) // rounds of push protocol
        .await?;

    let store = MemoryBlockStore::new(); // clear out data
    client
        .get(format!("http://localhost:3344/dag/pull/{root}"))
        .run_car_mirror_pull(root, config, &store, &NoCache) // rounds of pull protocol
        .await?;

    assert!(store.has_block(&root).await?);

    Ok(())
}

// Server details:

#[derive(Debug, Clone)]
struct ServerState {
    store: MemoryBlockStore,
    cache: InMemoryCache,
}

impl ServerState {
    fn new() -> Self {
        Self {
            store: MemoryBlockStore::new(),
            cache: InMemoryCache::new(100_000),
        }
    }
}

#[axum_macros::debug_handler]
async fn car_mirror_push(
    State(state): State<ServerState>,
    Path(cid_string): Path<String>,
    body: Body,
) -> Result<(StatusCode, Json<PushResponse>), AppError>
where {
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
        Ok((StatusCode::OK, Json(response)))
    } else {
        Ok((StatusCode::ACCEPTED, Json(response)))
    }
}

#[axum_macros::debug_handler]
async fn car_mirror_pull(
    State(state): State<ServerState>,
    Path(cid_string): Path<String>,
    Json(request): Json<PullRequest>,
) -> Result<(StatusCode, Body), AppError> {
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

// Basic anyhow error handling:

struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}
