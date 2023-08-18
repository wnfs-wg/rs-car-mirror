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
    pub bloom_fpr: f64,
}

impl Default for PushConfig {
    fn default() -> Self {
        Self {
            send_minimum: 128 * 1024,    // 128KiB
            receive_maximum: 512 * 1024, // 512KiB
            max_roots_per_round: 1000,   // max. ~41KB of CIDs
            bloom_fpr: 1.0 / 10_000.0,   // 0.1%
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
            // TODO(matheus23) I think the spec means to prune the whole subgraph.
            // But
            // 1. That requires the receiver to check the whole subgraph at that CID to find out whether there's a missing block at the subgraph.
            // 2. It requires the sender to go through every block under this subgraph down to the leaves to mark all of these CIDs as visited.
            // Both of these are *huge* traversals. I'd say likely not worth it. The only case I can image they're worth it, is if the DAG
            // is *heavily* using structural sharing and not tree-like.
            // Also: This fails completely if the sender is just missing a single leaf. It couldn't add the block to the bloom in that case.
            dag_walk.skip_walking((cid, block))?;
            println!("Skipped walking {cid} due to bloom");
            break;
        }

        writer.write(cid, &block).await?;
        println!("Sending {cid}");

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
        println!("Received {cid}");

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

    let mut bloom =
        BloomFilter::new_from_fpr_po2(dag_verification.have_cids.len() as u64, config.bloom_fpr);

    dag_verification
        .have_cids
        .iter()
        .for_each(|cid| bloom.insert(&cid.to_bytes()));

    Ok(PushResponse {
        subgraph_roots,
        // We ignore blooms for now
        bloom_k: bloom.hash_count() as u32,
        bloom: bloom.as_bytes().to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{encode, generate_dag, Rvg};
    use libipld::{Ipld, IpldCodec};
    use libipld_core::multihash::{Code, MultihashDigest};
    use std::collections::BTreeMap;
    use wnfs_common::MemoryBlockStore;

    #[async_std::test]
    async fn test_transfer() -> Result<()> {
        let (blocks, root) = Rvg::new().sample(&generate_dag(256, |cids| {
            let ipld = Ipld::Map(BTreeMap::from([
                ("data".into(), Ipld::Bytes(vec![0u8; 10 * 1024])),
                (
                    "links".into(),
                    Ipld::List(cids.into_iter().map(Ipld::Link).collect()),
                ),
            ]));
            let bytes = encode(&ipld);
            let cid = Cid::new_v1(IpldCodec::DagCbor.into(), Code::Blake3_256.digest(&bytes));
            (cid, bytes)
        }));

        let sender_store = &MemoryBlockStore::new();
        for (cid, bytes) in blocks.iter() {
            let cid_store = sender_store
                .put_block(bytes.clone(), IpldCodec::DagCbor.into())
                .await?;
            assert_eq!(*cid, cid_store);
        }

        let receiver_store = &MemoryBlockStore::new();
        let config = &PushConfig::default();
        let mut request = client_initiate_push(root, config, sender_store).await?;
        loop {
            println!("Sending request {} bytes", request.len());
            let response = server_push_response(root, request, config, receiver_store).await?;
            println!(
                "Response (bloom bytes: {}): {:?}",
                response.bloom.len(),
                response.subgraph_roots,
            );
            if response.indicates_finished() {
                break;
            }
            request = client_push(root, response, config, sender_store).await?;
        }

        // receiver should have all data
        let sender_cids = DagWalk::breadth_first([root])
            .stream(sender_store)
            .map_ok(|(cid, _)| cid)
            .try_collect::<Vec<_>>()
            .await?;
        let receiver_cids = DagWalk::breadth_first([root])
            .stream(receiver_store)
            .map_ok(|(cid, _)| cid)
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(sender_cids, receiver_cids);

        Ok(())
    }
}
