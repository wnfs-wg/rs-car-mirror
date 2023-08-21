use std::collections::BTreeMap;

use crate::{common::references, dag_walk::DagWalk};
use anyhow::Result;
use bytes::Bytes;
use futures::TryStreamExt;
use libipld::{Cid, Ipld, IpldCodec};
use libipld_core::{
    codec::Encode,
    multihash::{Code, MultihashDigest},
};
use proptest::prelude::Rng;
use wnfs_common::{BlockStore, MemoryBlockStore};

#[cfg(feature = "test_utils")]
mod dag_strategy;
/// Random value generator for sampling data.
#[cfg(feature = "test_utils")]
mod rvg;
#[cfg(feature = "test_utils")]
pub use dag_strategy::*;
#[cfg(feature = "test_utils")]
pub use rvg::*;

#[derive(Clone, Debug)]
pub(crate) struct Metrics {
    pub(crate) request_bytes: usize,
    pub(crate) response_bytes: usize,
}

/// Encode some IPLD as dag-cbor
pub(crate) fn encode(ipld: &Ipld) -> Bytes {
    let mut vec = Vec::new();
    ipld.encode(IpldCodec::DagCbor, &mut vec).unwrap(); // TODO(matheus23) unwrap
    Bytes::from(vec)
}

/// Walk a root DAG along some path.
/// At each node, take the `n % numlinks`th link,
/// and only walk the path as long as there are further links.
pub(crate) async fn get_cid_at_approx_path(
    path: Vec<usize>,
    root: Cid,
    store: &impl BlockStore,
) -> Result<Cid> {
    let mut working_cid = root;
    for nth in path {
        let block = store.get_block(&working_cid).await?;
        let refs = references(working_cid, block)?;
        if refs.is_empty() {
            break;
        }

        working_cid = refs[nth % refs.len()];
    }
    Ok(working_cid)
}

pub(crate) async fn setup_random_dag<const BLOCK_PADDING: usize>(
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

pub(crate) async fn total_dag_bytes(root: Cid, store: &impl BlockStore) -> Result<usize> {
    Ok(DagWalk::breadth_first([root])
        .stream(store)
        .map_ok(|(_, block)| block.len())
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .sum::<usize>())
}

pub(crate) async fn total_dag_blocks(root: Cid, store: &impl BlockStore) -> Result<usize> {
    Ok(DagWalk::breadth_first([root])
        .stream(store)
        .map_ok(|(_, block)| block.len())
        .try_collect::<Vec<_>>()
        .await?
        .len())
}
