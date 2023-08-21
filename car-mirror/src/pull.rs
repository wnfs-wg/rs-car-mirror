use crate::{
    common::{block_receive, block_send, CarFile, Config, ReceiverState},
    messages::PullRequest,
};
use anyhow::Result;
use libipld::Cid;
use wnfs_common::BlockStore;

pub async fn request(
    root: Cid,
    last_response: Option<CarFile>,
    config: &Config,
    store: &impl BlockStore,
) -> Result<PullRequest> {
    Ok(block_receive(root, last_response, config, store)
        .await?
        .into_pull_request())
}

pub async fn response(
    root: Cid,
    request: PullRequest,
    config: &Config,
    store: &impl BlockStore,
) -> Result<CarFile> {
    let receiver_state = Some(ReceiverState::from_pull_request(request));
    block_send(root, receiver_state, config, store).await
}

#[cfg(test)]
mod tests {
    use crate::{
        common::Config,
        dag_walk::DagWalk,
        test_utils::{setup_random_dag, Metrics},
    };
    use anyhow::Result;
    use futures::TryStreamExt;
    use libipld::Cid;
    use wnfs_common::MemoryBlockStore;

    async fn simulate_protocol(
        root: Cid,
        config: &Config,
        client_store: &MemoryBlockStore,
        server_store: &MemoryBlockStore,
    ) -> Result<Vec<Metrics>> {
        let mut metrics = Vec::new();
        let mut request = crate::pull::request(root, None, config, client_store).await?;
        loop {
            let request_bytes = serde_ipld_dagcbor::to_vec(&request)?.len();
            let response = crate::pull::response(root, request, config, server_store).await?;
            let response_bytes = response.bytes.len();

            metrics.push(Metrics {
                request_bytes,
                response_bytes,
            });

            request = crate::pull::request(root, Some(response), config, client_store).await?;
            if request.indicates_finished() {
                break;
            }
        }

        Ok(metrics)
    }

    #[async_std::test]
    async fn test_transfer() -> Result<()> {
        let client_store = &MemoryBlockStore::new();
        let (root, ref server_store) = setup_random_dag(256, 10 * 1024 /* 10 KiB */).await?;

        simulate_protocol(root, &Config::default(), client_store, server_store).await?;

        // client should have all data
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
}
