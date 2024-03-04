use crate::Error;
use anyhow::Result;
use car_mirror::{cache::Cache, common::Config, messages::PushResponse};
use futures::{Future, TryStreamExt};
use libipld::Cid;
use reqwest::{Body, Response, StatusCode};
use std::{collections::TryReserveError, convert::Infallible};
use tokio_util::io::StreamReader;
use wnfs_common::BlockStore;

/// Extension methods on `RequestBuilder`s for sending car mirror protocol requests.
pub trait RequestBuilderExt {
    /// Initiate a car mirror push request to send some data to the
    /// server via HTTP.
    ///
    /// Repeats this request until the car mirror protocol is finished.
    ///
    /// The `root` is the CID of the DAG root that should be made fully
    /// present on the server side at the end of the protocol.
    ///
    /// The full DAG under `root` needs to be available in the local
    /// blockstore `store`.
    ///
    /// `cache` is used to reduce costly `store` lookups and computations
    /// regarding which blocks still need to be transferred.
    ///
    /// This will call `try_clone()` and `send()` on this
    /// request builder, so it must not have a `body` set yet.
    /// There is no need to set a body, this function will do so automatically.
    ///
    /// `store` and `cache` need to be references to `Clone`-able types
    /// which don't borrow data, because of the way request streaming
    /// lifetimes work with `reqwest`.
    /// Usually blockstores and caches satisfy these conditions due to
    /// using atomic reference counters.
    fn run_car_mirror_push(
        &self,
        root: Cid,
        store: &(impl BlockStore + Clone + 'static),
        cache: &(impl Cache + Clone + 'static),
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Initiate a car mirror pull request to load some data from
    /// a server via HTTP.
    ///
    /// Repeats the request in rounds, until the car mirror protocol is finished.
    ///
    /// The `root` is the CID of the DAG root that should be made fully
    /// present on the client side at the end of the protocol.
    ///
    /// At the end of the protocol, `store` will contain all blocks under `root`.
    ///
    /// `cache` is used to reduce costly `store` lookups and computations
    /// regarding which blocks still need to be transferred.
    ///
    /// This will call `try_clone()` and `send()` on this
    /// request builder, so it must not have a `body` set yet.
    /// There is no need to set a body, this function will do so automatically.
    fn run_car_mirror_pull(
        &self,
        root: Cid,
        config: &Config,
        store: &impl BlockStore,
        cache: &impl Cache,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

impl RequestBuilderExt for reqwest_middleware::RequestBuilder {
    async fn run_car_mirror_push(
        &self,
        root: Cid,
        store: &(impl BlockStore + Clone + 'static),
        cache: &(impl Cache + Clone + 'static),
    ) -> Result<(), Error> {
        push_with(root, store, cache, |body| {
            send_middleware_reqwest(self, body)
        })
        .await
    }

    async fn run_car_mirror_pull(
        &self,
        root: Cid,
        config: &Config,
        store: &impl BlockStore,
        cache: &impl Cache,
    ) -> Result<(), Error> {
        pull_with(root, config, store, cache, |body| {
            send_middleware_reqwest(self, body)
        })
        .await
    }
}

async fn send_middleware_reqwest(
    builder: &reqwest_middleware::RequestBuilder,
    body: reqwest::Body,
) -> Result<Response, Error> {
    Ok(builder
        .try_clone()
        .ok_or(Error::RequestBuilderBodyAlreadySet)?
        .header("Content-Type", "application/vnd.ipld.dag-cbor")
        .body(body)
        .send()
        .await?)
}

impl RequestBuilderExt for reqwest::RequestBuilder {
    async fn run_car_mirror_push(
        &self,
        root: Cid,
        store: &(impl BlockStore + Clone + 'static),
        cache: &(impl Cache + Clone + 'static),
    ) -> Result<(), Error> {
        push_with(root, store, cache, |body| send_reqwest(self, body)).await
    }

    async fn run_car_mirror_pull(
        &self,
        root: Cid,
        config: &Config,
        store: &impl BlockStore,
        cache: &impl Cache,
    ) -> Result<(), Error> {
        pull_with(root, config, store, cache, |body| send_reqwest(self, body)).await
    }
}

async fn send_reqwest(
    builder: &reqwest::RequestBuilder,
    body: reqwest::Body,
) -> Result<Response, Error> {
    Ok(builder
        .try_clone()
        .ok_or(Error::RequestBuilderBodyAlreadySet)?
        .header("Content-Type", "application/vnd.ipld.dag-cbor")
        .body(body)
        .send()
        .await?)
}

/// Run (possibly multiple rounds of) the car mirror push protocol.
///
/// See `run_car_mirror_push` for a more ergonomic interface.
///
/// Unlike `run_car_mirror_push`, this allows customizing the
/// request every time it gets built, e.g. to refresh authentication tokens.
pub async fn push_with<F, Fut, E>(
    root: Cid,
    store: &(impl BlockStore + Clone + 'static),
    cache: &(impl Cache + Clone + 'static),
    mut make_request: F,
) -> Result<(), E>
where
    F: FnMut(reqwest::Body) -> Fut,
    Fut: Future<Output = Result<Response, E>>,
    E: From<Error>,
    E: From<car_mirror::Error>,
    E: From<reqwest::Error>,
    E: From<serde_ipld_dagcbor::DecodeError<Infallible>>,
{
    let mut push_state = None;

    loop {
        let car_stream =
            car_mirror::push::request_streaming(root, push_state, store.clone(), cache.clone())
                .await?;
        let reqwest_stream = Body::wrap_stream(car_stream);

        let response = make_request(reqwest_stream).await?.error_for_status()?;

        match response.status() {
            StatusCode::OK => {
                return Ok(());
            }
            StatusCode::ACCEPTED => {
                // We need to continue.
            }
            _ => {
                // Some unexpected response code
                return Err(Error::UnexpectedStatusCode { response }.into());
            }
        }

        let response_bytes = response.bytes().await?;

        let push_response = PushResponse::from_dag_cbor(&response_bytes)?;

        push_state = Some(push_response);
    }
}

/// Run (possibly multiple rounds of) the car mirror pull protocol.
///
/// See `run_car_mirror_pull` for a more ergonomic interface.
///
/// Unlike `run_car_mirror_pull`, this allows customizing the
/// request every time it gets built, e.g. to refresh authentication tokens.
///
/// **Important:** Don't forget to set the `Content-Type` header to
/// `application/vnd.ipld.dag-cbor` on your requests.
pub async fn pull_with<F, Fut, E>(
    root: Cid,
    config: &Config,
    store: &impl BlockStore,
    cache: &impl Cache,
    mut make_request: F,
) -> Result<(), E>
where
    F: FnMut(reqwest::Body) -> Fut,
    Fut: Future<Output = Result<Response, E>>,
    E: From<car_mirror::Error>,
    E: From<reqwest::Error>,
    E: From<serde_ipld_dagcbor::EncodeError<TryReserveError>>,
{
    let mut pull_request = car_mirror::pull::request(root, None, config, store, cache).await?;

    while !pull_request.indicates_finished() {
        let answer = make_request(pull_request.to_dag_cbor()?.into())
            .await?
            .error_for_status()?;

        let stream = StreamReader::new(answer.bytes_stream().map_err(std::io::Error::other));

        pull_request =
            car_mirror::pull::handle_response_streaming(root, stream, config, store, cache).await?;
    }

    Ok(())
}
