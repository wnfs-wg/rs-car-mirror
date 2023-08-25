use crate::dag_walk::DagWalk;
use anyhow::{bail, Result};
use bytes::Bytes;
use libipld_core::cid::Cid;
use std::{collections::HashSet, matches};
use wnfs_common::{BlockStore, BlockStoreError};

/// A data structure that keeps state about incremental DAG verification.
#[derive(Clone, Debug)]
pub struct IncrementalDagVerification {
    /// All the CIDs that have been discovered to be missing from the DAG.
    pub want_cids: HashSet<Cid>,
    /// All the CIDs that are available locally.
    pub have_cids: HashSet<Cid>,
}

/// The state of a block retrieval
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockState {
    /// The block was already received/is already stored
    Have,
    /// We know we will need this block
    Want,
    /// We don't know whether we'll need this block
    Unexpected,
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
        let mut this = Self {
            want_cids: roots.into_iter().collect(),
            have_cids: HashSet::new(),
        };

        this.update_have_cids(store).await?;

        Ok(this)
    }

    async fn update_have_cids(&mut self, store: &impl BlockStore) -> Result<()> {
        let mut dag_walk = DagWalk::breadth_first(self.want_cids.iter().cloned());

        loop {
            match dag_walk.next(store).await {
                Err(e) => {
                    if let Some(BlockStoreError::CIDNotFound(not_found)) =
                        e.downcast_ref::<BlockStoreError>()
                    {
                        self.want_cids.insert(*not_found);
                    } else {
                        bail!(e);
                    }
                }
                Ok(Some((cid, _))) => {
                    self.want_cids.remove(&cid);
                    self.have_cids.insert(cid);
                }
                Ok(None) => {
                    break;
                }
            }
        }

        Ok(())
    }

    /// Check the state of a CID to find out whether
    /// - we expect it as one of the next possible blocks to receive (Want)
    /// - we have already stored it (Have)
    /// - we don't know whether we need it (Unexpected)
    pub fn block_state(&self, cid: Cid) -> BlockState {
        if self.want_cids.contains(&cid) {
            BlockState::Want
        } else if self.have_cids.contains(&cid) {
            BlockState::Have
        } else {
            BlockState::Unexpected
        }
    }

    /// Verify that
    /// - the block is part of the graph below the roots.
    /// - the block hasn't been received before
    /// - the block actually hashes to the hash from given CID and
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

        let block_state = self.block_state(cid);
        if !matches!(block_state, BlockState::Want) {
            bail!("Incremental verification failed. Block state is: {block_state:?}, expected BlockState::Want");
        }

        // TODO(matheus23): Verify hash before putting it into the blockstore.
        let result_cid = store.put_block(bytes, cid.codec()).await?;

        // TODO(matheus23): The BlockStore chooses the hashing function,
        // so it may choose a different hashing function, causing a mismatch
        if result_cid != cid {
            bail!("Digest mismatch in CAR file: expected {cid}, got {result_cid}");
        }

        self.update_have_cids(store).await?;

        Ok(())
    }
}
