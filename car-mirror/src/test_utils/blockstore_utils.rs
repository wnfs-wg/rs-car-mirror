use anyhow::Result;
use ipld_core::ipld::Ipld;
use std::io::Write;
use wnfs_common::{BlockStore, Cid, MemoryBlockStore, CODEC_DAG_CBOR, CODEC_DAG_JSON, CODEC_RAW};

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
        let block: Vec<u8> = match cid.codec() {
            CODEC_DAG_CBOR => serde_ipld_dagcbor::to_vec(&ipld)?,
            CODEC_DAG_JSON => serde_json::to_vec(&ipld)?,
            CODEC_RAW => {
                let Ipld::Bytes(bytes) = ipld else {
                    anyhow::bail!("raw codec block that's not bytes");
                };
                bytes
            }
            other => {
                anyhow::bail!("Unsupported codec {other}")
            }
        };
        let cid_store = store.put_block(block, cid.codec()).await?;
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
        let mut refs = Vec::new();
        ipld.references(&mut refs);
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
