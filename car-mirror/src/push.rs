use crate::{
    dag_walk::DagWalk, incremental_verification::IncrementalDagVerification, messages::PushResponse,
};
use anyhow::{bail, Result};
use bytes::Bytes;
use deterministic_bloom::runtime_size::BloomFilter;
use futures::TryStreamExt;
use iroh_car::{CarHeader, CarReader, CarWriter};
use libipld_core::cid::Cid;
use std::io::Cursor;
use wnfs_common::BlockStore;

/// Configuration values (such as byte limits) for the CAR mirror push protocol
#[derive(Clone, Debug)]
pub struct PushConfig {
    /// A client will try to send at least `send_minimum` bytes of block data
    /// in each request, except if close to the end of the protocol (when there's)
    /// not that much data left.
    pub send_minimum: usize,
    /// The maximum number of bytes per request that the server accepts.
    pub receive_maximum: usize,
    /// The maximum number of roots per request that the server will send to the client,
    /// and that the client will consume.
    pub max_roots_per_round: usize,
    /// The target false positive rate for the bloom filter that the server sends.
    pub bloom_fpr: fn(u64) -> f64,
}

impl Default for PushConfig {
    fn default() -> Self {
        Self {
            send_minimum: 128 * 1024,    // 128KiB
            receive_maximum: 512 * 1024, // 512KiB
            max_roots_per_round: 1000,   // max. ~41KB of CIDs
            bloom_fpr: |num_of_elems| 0.1 / num_of_elems as f64,
        }
    }
}

/// Initiate a car mirror push request.
///
/// The goal is to transfer the DAG below the root CID to
/// the server.
///
/// The return value is a CAR file.
pub async fn client_initiate_push(
    root: Cid,
    config: &PushConfig,
    store: &impl BlockStore,
) -> Result<Bytes> {
    let fake_response = PushResponse {
        subgraph_roots: vec![root],
        // Just putting an empty bloom here
        bloom_k: 3,
        bloom: Vec::new(),
    };
    client_push(root, fake_response, config, store).await
}

/// Send a subsequent car mirror push request, following up on
/// a response retrieved from an initial `client_initiate_push` request.
///
/// Make sure to call `response.indicates_finished()` before initiating
/// a follow-up `client_push` request.
///
/// The return value is another CAR file with more blocks from the DAG below the root.
pub async fn client_push(
    root: Cid,
    last_response: PushResponse,
    config: &PushConfig,
    store: &impl BlockStore,
) -> Result<Bytes> {
    let PushResponse {
        ref subgraph_roots,
        bloom_k,
        bloom,
    } = last_response;

    // Verify that all subgraph roots are in the relevant DAG:
    let subgraph_roots: Vec<Cid> = DagWalk::breadth_first([root])
        .stream(store)
        .try_filter_map(|(cid, _)| async move { Ok(subgraph_roots.contains(&cid).then_some(cid)) })
        .try_collect()
        .await?;

    let bloom = if bloom.is_empty() {
        BloomFilter::new_with(1, Box::new([0])) // An empty bloom that contains nothing
    } else {
        BloomFilter::new_with(bloom_k as usize, bloom.into_boxed_slice())
    };

    let mut writer = CarWriter::new(
        CarHeader::new_v1(
            // TODO(matheus23): This is stupid
            // CAR files *must* have at least one CID in them, and all of them
            // need to appear as a block in the payload.
            // It would probably make most sense to just write all subgraph roots into this,
            // but we don't know how many of the subgraph roots fit into this round yet,
            // so we're simply writing the first one in here, since we know
            // at least one block will be written (and it'll be that one).
            subgraph_roots.iter().take(1).cloned().collect(),
        ),
        Vec::new(),
    );

    writer.write_header().await?;

    let mut block_bytes = 0;
    let mut dag_walk = DagWalk::breadth_first(subgraph_roots.clone());
    while let Some((cid, block)) = dag_walk.next(store).await? {
        if bloom.contains(&cid.to_bytes()) && !subgraph_roots.contains(&cid) {
            break;
        }

        writer.write(cid, &block).await?;

        // TODO(matheus23): Count the actual bytes sent?
        block_bytes += block.len();
        if block_bytes > config.send_minimum {
            break;
        }
    }

    Ok(writer.finish().await?.into())
}

/// This handles a car mirror push request on the server side.
///
/// The root is the root CID of the DAG that is pushed, the request is a CAR file
/// with some blocks from the cold call.
///
/// Returns a response to answer the client's request with.
pub async fn server_push_response(
    root: Cid,
    request: Bytes,
    config: &PushConfig,
    store: &impl BlockStore,
) -> Result<PushResponse> {
    let mut dag_verification = IncrementalDagVerification::new([root], store).await?;

    let mut reader = CarReader::new(Cursor::new(request)).await?;
    let mut block_bytes = 0;

    while let Some((cid, vec)) = reader.next_block().await? {
        let block = Bytes::from(vec);

        block_bytes += block.len();
        if block_bytes > config.receive_maximum {
            bail!(
                "Received more than {} bytes ({block_bytes}), aborting request.",
                config.receive_maximum
            );
        }

        dag_verification
            .verify_and_store_block((cid, block), store)
            .await?;
    }

    let subgraph_roots = dag_verification
        .want_cids
        .iter()
        .take(config.max_roots_per_round)
        .cloned()
        .collect();

    let bloom_capacity = dag_verification.have_cids.len() as u64;

    let mut bloom =
        BloomFilter::new_from_fpr_po2(bloom_capacity, (config.bloom_fpr)(bloom_capacity));

    dag_verification
        .have_cids
        .iter()
        .for_each(|cid| bloom.insert(&cid.to_bytes()));

    Ok(PushResponse {
        subgraph_roots,
        bloom_k: bloom.hash_count() as u32,
        bloom: bloom.as_bytes().to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{encode, generate_dag, get_cid_at_approx_path, Rvg};
    use libipld::{Ipld, IpldCodec};
    use libipld_core::multihash::{Code, MultihashDigest};
    use proptest::{collection::vec, prelude::Rng};
    use std::collections::BTreeMap;
    use wnfs_common::MemoryBlockStore;

    #[derive(Clone, Debug)]
    struct Metrics {
        request_bytes: usize,
        response_bytes: usize,
    }

    async fn setup_random_dag<const BLOCK_PADDING: usize>(
        dag_size: u16,
    ) -> Result<(Cid, MemoryBlockStore)> {
        let (blocks, root) = Rvg::new().sample(&generate_dag(dag_size, |cids, rng| {
            let ipld = Ipld::Map(BTreeMap::from([
                (
                    "data".into(),
                    Ipld::Bytes((0..BLOCK_PADDING).map(|_| rng.gen::<u8>()).collect()),
                ),
                (
                    "links".into(),
                    Ipld::List(cids.into_iter().map(Ipld::Link).collect()),
                ),
            ]));
            let bytes = encode(&ipld);
            let cid = Cid::new_v1(IpldCodec::DagCbor.into(), Code::Blake3_256.digest(&bytes));
            (cid, bytes)
        }));

        let store = MemoryBlockStore::new();
        for (cid, bytes) in blocks.into_iter() {
            let cid_store = store.put_block(bytes, IpldCodec::DagCbor.into()).await?;
            assert_eq!(cid, cid_store);
        }

        Ok((root, store))
    }

    async fn simulate_protocol(
        root: Cid,
        config: &PushConfig,
        client_store: &MemoryBlockStore,
        server_store: &MemoryBlockStore,
    ) -> Result<Vec<Metrics>> {
        let mut metrics = Vec::new();
        let mut request = client_initiate_push(root, config, client_store).await?;
        loop {
            let request_bytes = request.len();
            let response = server_push_response(root, request, config, server_store).await?;
            let response_bytes = serde_ipld_dagcbor::to_vec(&response)?.len();

            metrics.push(Metrics {
                request_bytes,
                response_bytes,
            });

            if response.indicates_finished() {
                break;
            }
            request = client_push(root, response, config, client_store).await?;
        }

        Ok(metrics)
    }

    async fn total_dag_bytes(root: Cid, store: &impl BlockStore) -> Result<usize> {
        Ok(DagWalk::breadth_first([root])
            .stream(store)
            .map_ok(|(_, block)| block.len())
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .sum::<usize>())
    }

    async fn total_dag_blocks(root: Cid, store: &impl BlockStore) -> Result<usize> {
        Ok(DagWalk::breadth_first([root])
            .stream(store)
            .map_ok(|(_, block)| block.len())
            .try_collect::<Vec<_>>()
            .await?
            .len())
    }

    #[async_std::test]
    async fn test_transfer() -> Result<()> {
        const BLOCK_PADDING: usize = 10 * 1024;
        let (root, ref client_store) = setup_random_dag::<BLOCK_PADDING>(256).await?;
        let server_store = &MemoryBlockStore::new();
        simulate_protocol(root, &PushConfig::default(), client_store, server_store).await?;

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
        let config = &PushConfig::default();
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
                simulate_protocol(root, &PushConfig::default(), client_store, server_store).await?;

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
