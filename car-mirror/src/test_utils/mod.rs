use crate::common::references;
use anyhow::Result;
use bytes::Bytes;
use libipld::{Cid, Ipld, IpldCodec};
use libipld_core::codec::Encode;
use wnfs_common::BlockStore;

#[cfg(feature = "test_utils")]
mod dag_strategy;
/// Random value generator for sampling data.
#[cfg(feature = "test_utils")]
mod rvg;
#[cfg(feature = "test_utils")]
pub use dag_strategy::*;
#[cfg(feature = "test_utils")]
pub use rvg::*;

/// Encode some IPLD as dag-cbor
pub fn encode(ipld: &Ipld) -> Bytes {
    let mut vec = Vec::new();
    ipld.encode(IpldCodec::DagCbor, &mut vec).unwrap(); // TODO(matheus23) unwrap
    Bytes::from(vec)
}

/// Walk a root DAG along some path.
/// At each node, take the `n % numlinks`th link,
/// and only walk the path as long as there are further links.
pub async fn get_cid_at_approx_path(
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
