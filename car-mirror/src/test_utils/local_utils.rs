///! Crate-local test utilities
use super::{arb_ipld_dag, links_to_padded_ipld, setup_blockstore, Rvg};
use crate::{common::references, dag_walk::DagWalk, error::Error, traits::NoCache};
use anyhow::Result;
use futures::TryStreamExt;
use libipld::{Cid, Ipld};
use proptest::strategy::Strategy;
use wnfs_common::{BlockStore, MemoryBlockStore};

#[derive(Clone, Debug)]
pub(crate) struct Metrics {
    pub(crate) request_bytes: usize,
    pub(crate) response_bytes: usize,
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
        let refs = references(working_cid, block, Vec::new())?;
        if refs.is_empty() {
            break;
        }

        working_cid = refs[nth % refs.len()];
    }
    Ok(working_cid)
}

pub(crate) fn padded_dag_strategy(
    dag_size: u16,
    block_padding: usize,
) -> impl Strategy<Value = (Vec<(Cid, Ipld)>, Cid)> {
    arb_ipld_dag(1..dag_size, 0.5, links_to_padded_ipld(block_padding))
}

pub(crate) fn variable_blocksize_dag() -> impl Strategy<Value = (Vec<(Cid, Ipld)>, Cid)> {
    const MAX_DAG_NODES: u16 = 128; // with this proptests run ~15 sec for me
    const MAX_LINK_BYTES: usize = MAX_DAG_NODES as usize * 42; // 1 byte cbor CID tag, 1 byte multibase indicator, 40 bytes CID

    // 1 byte cbor tag for whole object,
    // 1 byte cbor tag for block padding bytes
    // up to ~3 bytes for block padding size
    // 1 bytes cbor tag for list (of cids)
    // up to ~2 bytes for list size
    const EST_OVERHEAD: usize = 1 + 1 + 3 + 1 + 2;
    const MAX_BLOCK_SIZE: usize = 256 * 1024;
    const MAX_BLOCK_PADDING: usize = MAX_BLOCK_SIZE - EST_OVERHEAD - MAX_LINK_BYTES;

    (32..MAX_BLOCK_PADDING).prop_ind_flat_map(move |block_padding| {
        arb_ipld_dag(1..MAX_DAG_NODES, 0.5, links_to_padded_ipld(block_padding))
    })
}

pub(crate) async fn setup_random_dag(
    dag_size: u16,
    block_padding: usize,
) -> Result<(Cid, MemoryBlockStore)> {
    let (blocks, root) = Rvg::new().sample(&padded_dag_strategy(dag_size, block_padding));
    let store = setup_blockstore(blocks).await?;
    Ok((root, store))
}

pub(crate) async fn total_dag_bytes(root: Cid, store: &impl BlockStore) -> Result<usize> {
    Ok(DagWalk::breadth_first([root])
        .stream(store, &NoCache)
        .try_filter_map(|cid| async move {
            let block = store
                .get_block(&cid)
                .await
                .map_err(Error::BlockStoreError)?;
            Ok(Some(block.len()))
        })
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .sum::<usize>())
}

pub(crate) async fn total_dag_blocks(root: Cid, store: &impl BlockStore) -> Result<usize> {
    Ok(DagWalk::breadth_first([root])
        .stream(store, &NoCache)
        .try_collect::<Vec<_>>()
        .await?
        .len())
}
