use crate::{common::references, dag_walk::DagWalk};
use anyhow::{bail, Result};
use bytes::Bytes;
use libipld_core::cid::Cid;
use std::{collections::HashSet, eprintln};
use wnfs_common::{BlockStore, BlockStoreError};

/// A data structure that keeps state about incremental DAG verification.
#[derive(Clone, Debug)]
pub struct IncrementalDagVerification {
    /// All the CIDs that have been discovered to be missing from the DAG.
    pub want_cids: HashSet<Cid>,
    /// All the CIDs that are available locally.
    pub have_cids: HashSet<Cid>,
}

impl IncrementalDagVerification {
    /// Initiate incremental DAG verification of given roots.
    ///
    /// This will already run a traversal to find missing subgraphs and
    /// CIDs that are already present.
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

    /// Verify that
    /// - the block actually hashes to the hash from given CID and
    /// - the block is part of the graph below the roots.
    ///
    /// And finally stores the block in the blockstore.
    ///
    /// This *may* fail, even if the block is part of the graph below the roots,
    /// if intermediate blocks between the roots and this block are missing.
    ///
    /// This *may* add the block to the blockstore, but still fail to verify, specifically
    /// if the block's bytes don't match the hash in the CID.
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
