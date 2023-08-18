#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, missing_docs, rust_2018_idioms)]
#![deny(unreachable_pub, private_in_public)]

//! car-mirror

use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use deterministic_bloom::runtime_size::BloomFilter;
use futures::{stream::try_unfold, Stream, StreamExt, TryStreamExt};
use iroh_car::{CarHeader, CarReader, CarWriter};
use libipld::{Ipld, IpldCodec};
use libipld_core::{cid::Cid, codec::References};
use messages::PushResponse;
use std::{
    collections::{HashSet, VecDeque},
    eprintln,
    io::Cursor,
};
use wnfs_common::{BlockStore, BlockStoreError};

/// Test utilities.
#[cfg(any(test, feature = "test_utils"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test_utils")))]
pub mod test_utils;

/// Contains the data types that are sent over-the-wire and relevant serialization code.
pub mod messages;

pub struct PushConfig {
    send_minimum: usize,
    receive_maximum: usize,
    max_roots_per_round: usize,
    bloom_fpr: f64,
}

impl Default for PushConfig {
    fn default() -> Self {
        Self {
            send_minimum: 128 * 1024,    // 128KiB
            receive_maximum: 512 * 1024, // 512KiB
            max_roots_per_round: 1000,   // max. ~41KB of CIDs
            bloom_fpr: 1.0 / 1_000.0,    // 0.1%
        }
    }
}

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

pub struct DagWalk {
    pub frontier: VecDeque<Cid>,
    pub visited: HashSet<Cid>,
    pub breadth_first: bool,
}

impl DagWalk {
    pub fn breadth_first(roots: impl IntoIterator<Item = Cid>) -> Self {
        Self::new(roots, true)
    }

    pub fn depth_first(roots: impl IntoIterator<Item = Cid>) -> Self {
        Self::new(roots, false)
    }

    pub fn new(roots: impl IntoIterator<Item = Cid>, breadth_first: bool) -> Self {
        let frontier = roots.into_iter().collect();
        let visited = HashSet::new();
        Self {
            frontier,
            visited,
            breadth_first,
        }
    }

    pub async fn next(&mut self, store: &impl BlockStore) -> Result<Option<(Cid, Bytes)>> {
        let cid = loop {
            let popped = if self.breadth_first {
                self.frontier.pop_front()
            } else {
                self.frontier.pop_back()
            };

            let Some(cid) = popped else {
                return Ok(None);
            };

            // We loop until we find an unvisited block
            if self.visited.insert(cid) {
                break cid;
            }
        };

        let block = store.get_block(&cid).await?;
        for ref_cid in references(cid, &block)? {
            if !self.visited.contains(&ref_cid) {
                self.frontier.push_back(ref_cid);
            }
        }

        Ok(Some((cid, block)))
    }

    pub fn stream(
        self,
        store: &impl BlockStore,
    ) -> impl Stream<Item = Result<(Cid, Bytes)>> + Unpin + '_ {
        Box::pin(try_unfold(self, move |mut this| async move {
            let maybe_block = this.next(store).await?;
            Ok(maybe_block.map(|b| (b, this)))
        }))
    }

    pub fn is_finished(&self) -> bool {
        // We're finished if the frontier does not contain any CIDs that we have not visited yet.
        // Put differently:
        // We're not finished if there exist unvisited CIDs in the frontier.
        !self
            .frontier
            .iter()
            .any(|frontier_cid| !self.visited.contains(frontier_cid))
    }

    pub fn skip_walking(&mut self, block: (Cid, Bytes)) -> Result<()> {
        let (cid, bytes) = block;
        let refs = references(cid, bytes)?;
        self.visited.insert(cid);
        self.frontier
            .retain(|frontier_cid| !refs.contains(frontier_cid));

        Ok(())
    }
}

/// Writes a stream of blocks into a car file
pub async fn stream_into_car<W: tokio::io::AsyncWrite + Send + Unpin>(
    mut blocks: impl Stream<Item = Result<(Cid, Bytes)>> + Unpin,
    writer: &mut CarWriter<W>,
) -> Result<()> {
    while let Some(result) = blocks.next().await {
        let (cid, bytes) = result?;
        writer.write(cid, bytes).await?;
    }
    Ok(())
}

pub struct IncrementalDagVerification {
    pub want_cids: HashSet<Cid>,
    pub have_cids: HashSet<Cid>,
}

impl IncrementalDagVerification {
    pub async fn new(
        roots: impl IntoIterator<Item = Cid>,
        store: &impl BlockStore,
    ) -> Result<Self> {
        let mut want_cids = HashSet::new();
        let mut have_cids = HashSet::new();
        let mut dag_walk = DagWalk::breadth_first(roots);

        loop {
            match dag_walk.next(store).await {
                Err(e) => {
                    if let Some(BlockStoreError::CIDNotFound(not_found)) =
                        e.downcast_ref::<BlockStoreError>()
                    {
                        want_cids.insert(*not_found);
                    } else {
                        bail!(e);
                    }
                }
                Ok(Some((cid, _))) => {
                    have_cids.insert(cid);
                }
                Ok(None) => {
                    break;
                }
            }
        }

        Ok(Self {
            want_cids,
            have_cids,
        })
    }

    pub async fn verify_and_store_block(
        &mut self,
        block: (Cid, Bytes),
        store: &impl BlockStore,
    ) -> Result<()> {
        let (cid, bytes) = block;

        if !self.want_cids.contains(&cid) {
            if self.have_cids.contains(&cid) {
                eprintln!("Warn: Received {cid}, even though we already have it");
            } else {
                bail!("Unexpected block or block out of order: {cid}");
            }
        }

        let refs = references(cid, &bytes)?;
        let result_cid = store.put_block(bytes, cid.codec()).await?;

        if result_cid != cid {
            bail!("Digest mismatch in CAR file: expected {cid}, got {result_cid}");
        }

        for ref_cid in refs {
            if !self.have_cids.contains(&ref_cid) {
                self.want_cids.insert(ref_cid);
            }
        }

        self.want_cids.remove(&cid);
        self.have_cids.insert(cid);

        Ok(())
    }
}

fn references(cid: Cid, block: impl AsRef<[u8]>) -> Result<Vec<Cid>> {
    let codec: IpldCodec = cid
        .codec()
        .try_into()
        .map_err(|_| anyhow!("Unsupported codec in Cid: {cid}"))?;

    let mut refs = Vec::new();
    <Ipld as References<IpldCodec>>::references(codec, &mut Cursor::new(block), &mut refs)?;
    Ok(refs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{encode, generate_dag, Rvg};
    use futures::TryStreamExt;
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

    #[async_std::test]
    async fn test_walk_dag_breadth_first() -> Result<()> {
        let store = &MemoryBlockStore::new();

        let cid_1 = store.put_serializable(&Ipld::String("1".into())).await?;
        let cid_2 = store.put_serializable(&Ipld::String("2".into())).await?;
        let cid_3 = store.put_serializable(&Ipld::String("3".into())).await?;

        let cid_1_wrap = store
            .put_serializable(&Ipld::List(vec![Ipld::Link(cid_1)]))
            .await?;

        let cid_root = store
            .put_serializable(&Ipld::List(vec![
                Ipld::Link(cid_1_wrap),
                Ipld::Link(cid_2),
                Ipld::Link(cid_3),
            ]))
            .await?;

        let cids = DagWalk::breadth_first([cid_root])
            .stream(store)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|(cid, _block)| cid)
            .collect::<Vec<_>>();

        assert_eq!(cids, vec![cid_root, cid_1_wrap, cid_2, cid_3, cid_1]);

        Ok(())
    }
}

#[cfg(test)]
mod proptests {
    use crate::{
        test_utils::{encode, generate_dag},
        DagWalk,
    };
    use futures::TryStreamExt;
    use libipld::{
        multihash::{Code, MultihashDigest},
        Cid, Ipld, IpldCodec,
    };
    use proptest::strategy::Strategy;
    use std::collections::BTreeSet;
    use test_strategy::proptest;
    use wnfs_common::{BlockStore, MemoryBlockStore};

    fn ipld_dags() -> impl Strategy<Value = (Vec<(Cid, Ipld)>, Cid)> {
        generate_dag(256, |cids| {
            let ipld = Ipld::List(cids.into_iter().map(Ipld::Link).collect());
            let cid = Cid::new_v1(
                IpldCodec::DagCbor.into(),
                Code::Blake3_256.digest(&encode(&ipld)),
            );
            (cid, ipld)
        })
    }

    #[proptest(max_shrink_iters = 100_000)]
    fn walk_dag_never_iterates_block_twice(#[strategy(ipld_dags())] dag: (Vec<(Cid, Ipld)>, Cid)) {
        async_std::task::block_on(async {
            let (dag, root) = dag;
            let store = &MemoryBlockStore::new();
            for (cid, ipld) in dag.iter() {
                let cid_store = store
                    .put_block(encode(ipld), IpldCodec::DagCbor.into())
                    .await
                    .unwrap();
                assert_eq!(*cid, cid_store);
            }

            let mut cids = DagWalk::breadth_first([root])
                .stream(store)
                .map_ok(|(cid, _)| cid)
                .try_collect::<Vec<_>>()
                .await
                .unwrap();

            cids.sort();

            let unique_cids = cids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();

            assert_eq!(cids, unique_cids);
        });
    }
}
