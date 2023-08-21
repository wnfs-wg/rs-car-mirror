use anyhow::Result;
use bytes::Bytes;
use libipld::{Cid, Ipld, IpldCodec};
use libipld_core::codec::Encode;
use wnfs_common::{BlockStore, MemoryBlockStore};

/// Take a list of dag-cbor IPLD blocks and store all of them as dag-cbor in a
/// MemoryBlockStore & return it.
pub async fn setup_blockstore(blocks: Vec<(Cid, Ipld)>) -> Result<MemoryBlockStore> {
    let store = MemoryBlockStore::new();
    for (cid, ipld) in blocks.into_iter() {
        let cid_store = store
            .put_block(encode(&ipld), IpldCodec::DagCbor.into())
            .await?;
        debug_assert_eq!(cid, cid_store);
    }

    Ok(store)
}

/// Encode some IPLD as dag-cbor.
pub fn encode(ipld: &Ipld) -> Bytes {
    let mut vec = Vec::new();
    ipld.encode(IpldCodec::DagCbor, &mut vec).unwrap();
    Bytes::from(vec)
}
