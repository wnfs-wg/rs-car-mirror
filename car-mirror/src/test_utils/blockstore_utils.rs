use crate::common::references;
use anyhow::Result;
use bytes::Bytes;
use libipld::{Cid, Ipld, IpldCodec};
use std::io::Write;
use wnfs_common::{encode, BlockStore, MemoryBlockStore};

/// Take a list of dag-cbor IPLD blocks and store all of them as dag-cbor in a
/// MemoryBlockStore & return it.
pub async fn setup_blockstore(blocks: Vec<(Cid, Ipld)>) -> Result<MemoryBlockStore> {
    let store = MemoryBlockStore::new();
    setup_existing_blockstore(blocks, &store).await?;
    Ok(store)
}

/// Take a list of dag-cbor IPLD blocks and store all of them as dag-cbor in
/// the given `BlockStore`.
pub async fn setup_existing_blockstore(
    blocks: Vec<(Cid, Ipld)>,
    store: &impl BlockStore,
) -> Result<()> {
    for (cid, ipld) in blocks.into_iter() {
        let block: Bytes = encode(&ipld, IpldCodec::DagCbor)?.into();
        let cid_store = store.put_block(block, IpldCodec::DagCbor.into()).await?;
        debug_assert_eq!(cid, cid_store);
    }

    Ok(())
}

/// Print a DAG as a dot file with truncated CIDs
pub fn dag_to_dot(
    writer: &mut impl Write,
    blocks: impl IntoIterator<Item = (Cid, Ipld)>,
) -> Result<()> {
    writeln!(writer, "digraph {{")?;

    for (cid, ipld) in blocks {
        let bytes = encode(&ipld, IpldCodec::DagCbor)?;
        let refs = references(cid, bytes, Vec::new())?;
        for to_cid in refs {
            print_truncated_string(writer, cid.to_string())?;
            write!(writer, " -> ")?;
            print_truncated_string(writer, to_cid.to_string())?;
            writeln!(writer)?;
        }
    }

    writeln!(writer, "}}")?;

    Ok(())
}

fn print_truncated_string(writer: &mut impl Write, mut string: String) -> Result<()> {
    if string.len() > 20 {
        let mut string_rest = string.split_off(10);
        let string_end = string_rest.split_off(std::cmp::max(string_rest.len(), 10) - 10);
        write!(writer, "\"{string}...{string_end}\"")?;
    } else {
        write!(writer, "\"{string}\"")?;
    }

    Ok(())
}
