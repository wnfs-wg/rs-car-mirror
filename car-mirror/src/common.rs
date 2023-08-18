use anyhow::{anyhow, Result};
use libipld::{Ipld, IpldCodec};
use libipld_core::{cid::Cid, codec::References};
use std::io::Cursor;

/// Find all CIDs that a block references.
///
/// This will error out if
/// - the codec is not supported
/// - the block can't be parsed.
pub fn references(cid: Cid, block: impl AsRef<[u8]>) -> Result<Vec<Cid>> {
    let codec: IpldCodec = cid
        .codec()
        .try_into()
        .map_err(|_| anyhow!("Unsupported codec in Cid: {cid}"))?;

    let mut refs = Vec::new();
    <Ipld as References<IpldCodec>>::references(codec, &mut Cursor::new(block), &mut refs)?;
    Ok(refs)
}
