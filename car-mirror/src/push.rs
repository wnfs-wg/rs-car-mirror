use crate::{
    cache::Cache,
    common::{
        block_receive, block_receive_car_stream, block_send, block_send_block_stream,
        stream_car_frames, CarFile, CarStream, Config, ReceiverState,
    },
    error::Error,
    messages::PushResponse,
};
use wnfs_common::{utils::CondSend, BlockStore, Cid};

/// Create a CAR mirror push request.
///
/// On the first request for a particular `root`, set
/// `last_response` to `None`.
///
/// For subsequent requests, set it to the last successful
/// response from a request with the same `root`.
///
/// The returned request body is a CAR file from some of the first
/// blocks below the root.
pub async fn request(
    root: Cid,
    last_response: Option<PushResponse>,
    config: &Config,
    store: impl BlockStore,
    cache: impl Cache,
) -> Result<CarFile, Error> {
    let receiver_state = last_response.map(ReceiverState::from);
    block_send(root, receiver_state, config, store, cache).await
}

/// Streaming version of `request` to create a push request.
///
/// It's recommended to run the streaming push until the "server" interrupts
/// it with an updated `PushResponse`. Then continuing with another
/// push request with updated information.
pub async fn request_streaming<'a>(
    root: Cid,
    last_response: Option<PushResponse>,
    store: impl BlockStore + 'a,
    cache: impl Cache + 'a,
) -> Result<CarStream<'a>, Error> {
    let receiver_state = last_response.map(|s| s.into());
    let block_stream = block_send_block_stream(root, receiver_state, store, cache).await?;
    let car_stream = stream_car_frames(block_stream).await?;
    Ok(car_stream)
}

/// Create a response for a CAR mirror push request.
///
/// This takes in the CAR file from the request body and stores its blocks
/// in the given `store`, if the blocks can be shown to relate
/// to the `root` CID.
///
/// Returns a response that gives the "client" information about what
/// other data remains to be fetched.
pub async fn response(
    root: Cid,
    request: CarFile,
    config: &Config,
    store: impl BlockStore,
    cache: impl Cache,
) -> Result<PushResponse, Error> {
    Ok(block_receive(root, Some(request), config, store, cache)
        .await?
        .into())
}

/// Respond to a push request on the "server" side in a streaming fashing
/// (as opposed to the `response` function).
///
/// This will read from the `request` until the server realizes it got
/// some bytes it already had. Then it'll create an updated bloom filter
/// and send a `PushResponse`, interrupting the incoming stream.
pub async fn response_streaming(
    root: Cid,
    request: impl tokio::io::AsyncRead + Unpin + CondSend,
    config: &Config,
    store: impl BlockStore,
    cache: impl Cache,
) -> Result<PushResponse, Error> {
    Ok(
        block_receive_car_stream(root, request, config, store, cache)
            .await?
            .into(),
    )
}

#[cfg(test)]
mod tests {
    use crate::{
        cache::{InMemoryCache, NoCache},
        common::Config,
        dag_walk::DagWalk,
        push,
        test_utils::{
            get_cid_at_approx_path, setup_random_dag, store_test_unixfs, total_dag_blocks,
            total_dag_bytes, Metrics, Rvg,
        },
    };
    use anyhow::Result;
    use futures::TryStreamExt;
    use proptest::collection::vec;
    use std::collections::HashSet;
    use testresult::TestResult;
    use tokio_util::io::StreamReader;
    use wnfs_common::{BlockStore, Cid, MemoryBlockStore};

    pub(crate) async fn simulate_protocol(
        root: Cid,
        config: &Config,
        client_store: &impl BlockStore,
        server_store: &impl BlockStore,
    ) -> Result<Vec<Metrics>> {
        let mut metrics = Vec::new();
        let mut request = push::request(root, None, config, client_store, &NoCache).await?;
        loop {
            let request_bytes = request.bytes.len();
            let response = push::response(root, request, config, server_store, &NoCache).await?;
            let response_bytes = serde_ipld_dagcbor::to_vec(&response)?.len();

            metrics.push(Metrics {
                request_bytes,
                response_bytes,
            });

            if response.indicates_finished() {
                break;
            }
            request = push::request(root, Some(response), config, client_store, &NoCache).await?;
        }

        Ok(metrics)
    }

    #[test_log::test(async_std::test)]
    async fn test_transfer() -> TestResult {
        let (root, ref client_store) = setup_random_dag(256, 10 * 1024 /* 10 KiB */).await?;
        let server_store = &MemoryBlockStore::new();
        simulate_protocol(root, &Config::default(), client_store, server_store).await?;

        // receiver should have all data
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

        let mut last_response = None;
        loop {
            let stream =
                push::request_streaming(root, last_response, &client_store, &client_cache).await?;

            let byte_stream = StreamReader::new(
                stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
            );

            let response =
                push::response_streaming(root, byte_stream, config, &server_store, &server_cache)
                    .await?;

            if response.indicates_finished() {
                break;
            }

            last_response = Some(response);
        }
        Ok(())
    }

    #[test_log::test(async_std::test)]
    async fn test_deduplicating_transfer() -> TestResult {
        let (root, ref client_store) = setup_random_dag(256, 10 * 1024 /* 10 KiB */).await?;
        let total_bytes = total_dag_bytes(root, client_store).await?;
        let path = Rvg::new().sample(&vec(0usize..128, 0..64));
        let second_root = get_cid_at_approx_path(path, root, client_store).await?;

        let server_store = &MemoryBlockStore::new();
        let config = &Config::default();
        let metrics1 = simulate_protocol(second_root, config, client_store, server_store).await?;
        let metrics2 = simulate_protocol(root, config, client_store, server_store).await?;

        let total_network_bytes = metrics1
            .into_iter()
            .chain(metrics2.into_iter())
            .map(|metric| metric.request_bytes + metric.response_bytes)
            .sum::<usize>();

        println!("Total DAG bytes: {total_bytes}");
        println!("Total network bytes: {total_network_bytes}");

        Ok(())
    }

    #[test_log::test(async_std::test)]
    async fn print_metrics() -> TestResult {
        const TESTS: usize = 200;
        const DAG_SIZE: u16 = 256;
        const BLOCK_PADDING: usize = 10 * 1024;

        let mut total_rounds = 0;
        let mut total_blocks = 0;
        let mut total_block_bytes = 0;
        let mut total_network_bytes = 0;
        for _ in 0..TESTS {
            let (root, ref client_store) = setup_random_dag(DAG_SIZE, BLOCK_PADDING).await?;
            let server_store = &MemoryBlockStore::new();
            let metrics =
                simulate_protocol(root, &Config::default(), client_store, server_store).await?;

            total_rounds += metrics.len();
            total_blocks += total_dag_blocks(root, client_store).await?;
            total_block_bytes += total_dag_bytes(root, client_store).await?;
            total_network_bytes += metrics
                .iter()
                .map(|metric| metric.request_bytes + metric.response_bytes)
                .sum::<usize>();
        }

        println!(
            "Average # of rounds: {}",
            total_rounds as f64 / TESTS as f64
        );
        println!(
            "Average # of blocks: {}",
            total_blocks as f64 / TESTS as f64
        );
        println!(
            "Average network overhead: {}%",
            (total_network_bytes as f64 / total_block_bytes as f64 - 1.0) * 100.0
        );

        Ok(())
    }
}

#[cfg(test)]
mod proptests {
    use crate::{
        cache::NoCache,
        common::Config,
        dag_walk::DagWalk,
        push,
        test_utils::{setup_blockstore, variable_blocksize_dag},
    };
    use futures::TryStreamExt;
    use ipld_core::ipld::Ipld;
    use std::collections::HashSet;
    use test_strategy::proptest;
    use wnfs_common::{Cid, MemoryBlockStore};

    #[proptest]
    fn cold_transfer_completes(#[strategy(variable_blocksize_dag())] dag: (Vec<(Cid, Ipld)>, Cid)) {
        let (blocks, root) = dag;
        async_std::task::block_on(async {
            let client_store = &setup_blockstore(blocks).await.unwrap();
            let server_store = &MemoryBlockStore::new();

            push::tests::simulate_protocol(root, &Config::default(), client_store, server_store)
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
