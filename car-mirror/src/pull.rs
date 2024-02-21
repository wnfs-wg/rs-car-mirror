use crate::{
    cache::Cache,
    common::{
        block_receive, block_receive_car_stream, block_send, block_send_block_stream,
        stream_car_frames, CarFile, CarStream, Config, ReceiverState,
    },
    error::Error,
    messages::PullRequest,
};
use libipld::Cid;
use tokio::io::AsyncRead;
use wnfs_common::{utils::CondSend, BlockStore};

/// Create a CAR mirror pull request.
///
/// If this is the first request that's sent for this
/// particular root CID, then set `last_response` to `None`.
///
/// On subsequent requests, set `last_response` to the
/// last successfully received response.
///
/// Before actually sending the request over the network,
/// make sure to check the `request.indicates_finished()`.
/// If true, the "client" already has all data and the request
/// doesn't need to be sent.
pub async fn request(
    root: Cid,
    last_response: Option<CarFile>,
    config: &Config,
    store: impl BlockStore,
    cache: impl Cache,
) -> Result<PullRequest, Error> {
    Ok(block_receive(root, last_response, config, store, cache)
        .await?
        .into())
}

/// On the "client" side, handle a streaming response from a pull request.
///
/// This will accept blocks as long as they're useful to get the DAG under
/// `root`, verify them, and store them in the given `store`.
pub async fn handle_response_streaming(
    root: Cid,
    stream: impl AsyncRead + Unpin + CondSend,
    config: &Config,
    store: impl BlockStore,
    cache: impl Cache,
) -> Result<PullRequest, Error> {
    Ok(block_receive_car_stream(root, stream, config, store, cache)
        .await?
        .into())
}

/// Respond to a CAR mirror pull request on the "server" side.
pub async fn response(
    root: Cid,
    request: PullRequest,
    config: &Config,
    store: impl BlockStore,
    cache: impl Cache,
) -> Result<CarFile, Error> {
    let receiver_state = Some(ReceiverState::from(request));
    block_send(root, receiver_state, config, store, cache).await
}

/// On the "server" side, respond to a pull request with a stream.
///
/// This can especially speed up cold pull requests.
pub async fn response_streaming<'a>(
    root: Cid,
    request: PullRequest,
    store: impl BlockStore + 'a,
    cache: impl Cache + 'a,
) -> Result<CarStream<'a>, Error> {
    let block_stream = block_send_block_stream(root, Some(request.into()), store, cache).await?;
    let car_stream = stream_car_frames(block_stream).await?;
    Ok(car_stream)
}

#[cfg(test)]
mod tests {
    use crate::{
        cache::{InMemoryCache, NoCache},
        common::Config,
        dag_walk::DagWalk,
        pull,
        test_utils::{setup_random_dag, store_test_unixfs, Metrics},
    };
    use anyhow::Result;
    use futures::TryStreamExt;
    use libipld::Cid;
    use std::collections::HashSet;
    use testresult::TestResult;
    use tokio_util::io::StreamReader;
    use wnfs_common::{BlockStore, MemoryBlockStore};

    pub(crate) async fn simulate_protocol(
        root: Cid,
        config: &Config,
        client_store: &impl BlockStore,
        server_store: &impl BlockStore,
    ) -> Result<Vec<Metrics>> {
        let mut metrics = Vec::new();
        let mut request = pull::request(root, None, config, client_store, &NoCache).await?;
        while !request.indicates_finished() {
            let request_bytes = serde_ipld_dagcbor::to_vec(&request)?.len();
            let response = pull::response(root, request, config, server_store, NoCache).await?;
            let response_bytes = response.bytes.len();

            metrics.push(Metrics {
                request_bytes,
                response_bytes,
            });

            request = pull::request(root, Some(response), config, client_store, &NoCache).await?;
        }

        Ok(metrics)
    }

    #[test_log::test(async_std::test)]
    async fn test_transfer() -> TestResult {
        let client_store = &MemoryBlockStore::new();
        let (root, ref server_store) = setup_random_dag(256, 10 * 1024 /* 10 KiB */).await?;

        simulate_protocol(root, &Config::default(), client_store, server_store).await?;

        // client should have all data
        let client_cids = DagWalk::breadth_first([root])
            .stream(client_store, &NoCache)
            .and_then(|item| async move { item.to_cid() })
            .try_collect::<HashSet<_>>()
            .await?;
        let server_cids = DagWalk::breadth_first([root])
            .stream(server_store, &NoCache)
            .and_then(|item| async move { item.to_cid() })
            .try_collect::<HashSet<_>>()
            .await?;

        assert_eq!(client_cids, server_cids);

        Ok(())
    }

    #[test_log::test(async_std::test)]
    async fn test_streaming_transfer() -> TestResult {
        let client_store = MemoryBlockStore::new();
        let server_store = MemoryBlockStore::new();

        let client_cache = InMemoryCache::new(100_000);
        let server_cache = InMemoryCache::new(100_000);

        let file_bytes = async_std::fs::read("../Cargo.lock").await?;
        let root = store_test_unixfs(file_bytes.clone(), &client_store).await?;
        store_test_unixfs(file_bytes[0..10_000].to_vec(), &server_store).await?;

        let config = &Config::default();

        let mut request = pull::request(root, None, config, &client_store, &client_cache).await?;

        while !request.indicates_finished() {
            let car_stream =
                pull::response_streaming(root, request, &server_store, &server_cache).await?;

            let byte_stream = StreamReader::new(
                car_stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
            );

            request = pull::handle_response_streaming(
                root,
                byte_stream,
                config,
                &client_store,
                &client_cache,
            )
            .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod proptests {
    use crate::{
        cache::NoCache,
        common::Config,
        dag_walk::DagWalk,
        pull,
        test_utils::{setup_blockstore, variable_blocksize_dag},
    };
    use futures::TryStreamExt;
    use libipld::{Cid, Ipld};
    use std::collections::HashSet;
    use test_strategy::proptest;
    use wnfs_common::MemoryBlockStore;

    #[proptest]
    fn cold_transfer_completes(#[strategy(variable_blocksize_dag())] dag: (Vec<(Cid, Ipld)>, Cid)) {
        let (blocks, root) = dag;
        async_std::task::block_on(async {
            let server_store = &setup_blockstore(blocks).await.unwrap();
            let client_store = &MemoryBlockStore::new();

            pull::tests::simulate_protocol(root, &Config::default(), client_store, server_store)
                .await
                .unwrap();

            // client should have all data
            let client_cids = DagWalk::breadth_first([root])
                .stream(client_store, &NoCache)
                .and_then(|item| async move { item.to_cid() })
                .try_collect::<HashSet<_>>()
                .await
                .unwrap();
            let server_cids = DagWalk::breadth_first([root])
                .stream(server_store, &NoCache)
                .and_then(|item| async move { item.to_cid() })
                .try_collect::<HashSet<_>>()
                .await
                .unwrap();

            assert_eq!(client_cids, server_cids);
        })
    }
}
