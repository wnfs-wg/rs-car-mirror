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
/// If true, the client already has all data and the request
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

/// TODO(matheus23): DOCS
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

/// Respond to a CAR mirror pull request.
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

/// TODO(matheus23): DOCS
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
        cache::NoCache,
        common::Config,
        dag_walk::DagWalk,
        test_utils::{setup_random_dag, Metrics},
    };
    use anyhow::Result;
    use futures::TryStreamExt;
    use libipld::Cid;
    use std::collections::HashSet;
    use testresult::TestResult;
    use wnfs_common::{BlockStore, MemoryBlockStore};

    pub(crate) async fn simulate_protocol(
        root: Cid,
        config: &Config,
        client_store: &impl BlockStore,
        server_store: &impl BlockStore,
    ) -> Result<Vec<Metrics>> {
        let mut metrics = Vec::new();
        let mut request = crate::pull::request(root, None, config, client_store, &NoCache).await?;
        while !request.indicates_finished() {
            let request_bytes = serde_ipld_dagcbor::to_vec(&request)?.len();
            let response =
                crate::pull::response(root, request, config, server_store, NoCache).await?;
            let response_bytes = response.bytes.len();

            metrics.push(Metrics {
                request_bytes,
                response_bytes,
            });

            request =
                crate::pull::request(root, Some(response), config, client_store, &NoCache).await?;
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
}

#[cfg(test)]
mod proptests {
    use crate::{
        cache::NoCache,
        common::Config,
        dag_walk::DagWalk,
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

            crate::pull::tests::simulate_protocol(
                root,
                &Config::default(),
                client_store,
                server_store,
            )
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
