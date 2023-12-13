#![allow(unknown_lints)] // Because the `instrument` macro contains some `#[allow]`s that rust 1.66 doesn't know yet.

use crate::{
    dag_walk::DagWalk,
    error::{Error, IncrementalVerificationError},
    traits::Cache,
};
use bytes::Bytes;
use libipld_core::{
    cid::Cid,
    multihash::{Code, MultihashDigest},
};
use std::{collections::HashSet, matches};
use tracing::instrument;
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
        cache: &impl Cache,
    ) -> Result<Self, Error> {
        let mut this = Self {
            want_cids: roots.into_iter().collect(),
            have_cids: HashSet::new(),
        };

        this.update_have_cids(store, cache).await?;

        Ok(this)
    }

    #[instrument(level = "trace", skip_all, fields(num_want = self.want_cids.len(), num_have = self.have_cids.len()))]
    async fn update_have_cids(
        &mut self,
        store: &impl BlockStore,
        cache: &impl Cache,
    ) -> Result<(), Error> {
        let mut dag_walk = DagWalk::breadth_first(self.want_cids.iter().cloned());

        loop {
            match dag_walk.next(store, cache).await {
                Err(Error::BlockStoreError(e)) => {
                    if let Some(BlockStoreError::CIDNotFound(not_found)) =
                        e.downcast_ref::<BlockStoreError>()
                    {
                        self.want_cids.insert(*not_found);
                    } else {
                        return Err(Error::BlockStoreError(e));
                    }
                }
                Err(e) => return Err(e),
                Ok(Some(cid)) => {
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
        cache: &impl Cache,
    ) -> Result<(), Error> {
        let (cid, bytes) = block;

        let block_state = self.block_state(cid);
        if !matches!(block_state, BlockState::Want) {
            return Err(IncrementalVerificationError::ExpectedWantedBlock {
                cid: Box::new(cid),
                block_state,
            }
            .into());
        }

        let hash_func: Code = cid
            .hash()
            .code()
            .try_into()
            .map_err(|_| Error::UnsupportedHashCode { cid })?;

        let hash = hash_func.digest(bytes.as_ref());

        if &hash != cid.hash() {
            let actual_cid = Cid::new_v1(cid.codec(), hash);
            return Err(IncrementalVerificationError::DigestMismatch {
                cid: Box::new(cid),
                actual_cid: Box::new(actual_cid),
            }
            .into());
        }

        let actual_cid = store
            .put_block(bytes, cid.codec())
            .await
            .map_err(Error::BlockStoreError)?;

        // TODO(matheus23): The BlockStore chooses the hashing function,
        // so it may choose a different hashing function, causing a mismatch
        if actual_cid != cid {
            return Err(Error::BlockStoreIncompatible {
                cid: Box::new(cid),
                actual_cid: Box::new(actual_cid),
            });
        }

        self.update_have_cids(store, cache).await?;

        Ok(())
    }
}
