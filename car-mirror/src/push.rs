use crate::{
    common::{block_receive, block_send, CarFile, Config, ReceiverState},
    messages::PushResponse,
};
use anyhow::Result;
use libipld_core::cid::Cid;
use wnfs_common::BlockStore;

/// TODO(matheus23) update docs
///
/// Send a subsequent car mirror push request, following up on
/// a response retrieved from an initial `client_initiate_push` request.
///
/// Make sure to call `response.indicates_finished()` before initiating
/// a follow-up `client_push` request.
///
/// The return value is another CAR file with more blocks from the DAG below the root.
pub async fn request(
    root: Cid,
    last_response: Option<PushResponse>,
    config: &Config,
    store: &impl BlockStore,
) -> Result<CarFile> {
    let receiver_state = last_response.map(ReceiverState::from_push_response);
    block_send(root, receiver_state, config, store).await
}

/// TODO(matheus23) update docs
///
/// This handles a car mirror push request on the server side.
///
/// The root is the root CID of the DAG that is pushed, the request is a CAR file
/// with some blocks from the cold call.
///
/// Returns a response to answer the client's request with.
pub async fn response(
    root: Cid,
    request: CarFile,
    config: &Config,
    store: &impl BlockStore,
) -> Result<PushResponse> {
    Ok(block_receive(root, Some(request), config, store)
        .await?
        .into_push_response())
}

#[cfg(test)]
mod tests {
    use crate::{
        common::Config,
        dag_walk::DagWalk,
        test_utils::{
            get_cid_at_approx_path, setup_random_dag, total_dag_blocks, total_dag_bytes, Metrics,
            Rvg,
        },
    };
    use anyhow::Result;
    use futures::TryStreamExt;
    use libipld::Cid;
    use proptest::collection::vec;
    use wnfs_common::MemoryBlockStore;

    async fn simulate_protocol(
        root: Cid,
        config: &Config,
        client_store: &MemoryBlockStore,
        server_store: &MemoryBlockStore,
    ) -> Result<Vec<Metrics>> {
        let mut metrics = Vec::new();
        let mut request = crate::push::request(root, None, config, client_store).await?;
        loop {
            let request_bytes = request.bytes.len();
            let response = crate::push::response(root, request, config, server_store).await?;
            let response_bytes = serde_ipld_dagcbor::to_vec(&response)?.len();

            metrics.push(Metrics {
                request_bytes,
                response_bytes,
            });

            if response.indicates_finished() {
                break;
            }
            request = crate::push::request(root, Some(response), config, client_store).await?;
        }

        Ok(metrics)
    }

    #[async_std::test]
    async fn test_transfer() -> Result<()> {
        const BLOCK_PADDING: usize = 10 * 1024;
        let (root, ref client_store) = setup_random_dag::<BLOCK_PADDING>(256).await?;
        let server_store = &MemoryBlockStore::new();
        simulate_protocol(root, &Config::default(), client_store, server_store).await?;

        // receiver should have all data
        let client_cids = DagWalk::breadth_first([root])
            .stream(client_store)
            .map_ok(|(cid, _)| cid)
            .try_collect::<Vec<_>>()
            .await?;
        let server_cids = DagWalk::breadth_first([root])
            .stream(server_store)
            .map_ok(|(cid, _)| cid)
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(client_cids, server_cids);

        Ok(())
    }

    #[async_std::test]
    async fn test_deduplicating_transfer() -> Result<()> {
        const BLOCK_PADDING: usize = 10 * 1024;
        let (root, ref client_store) = setup_random_dag::<BLOCK_PADDING>(256).await?;
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

    #[async_std::test]
    async fn print_metrics() -> Result<()> {
        const TESTS: usize = 200;
        const DAG_SIZE: u16 = 256;
        const BLOCK_PADDING: usize = 10 * 1024;

        let mut total_rounds = 0;
        let mut total_blocks = 0;
        let mut total_block_bytes = 0;
        let mut total_network_bytes = 0;
        for _ in 0..TESTS {
            let (root, ref client_store) = setup_random_dag::<BLOCK_PADDING>(DAG_SIZE).await?;
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
    use super::*;
}
