use crate::{
    cache::Cache,
    common::ReceiverState,
    dag_walk::{DagWalk, TraversedItem},
    error::{Error, IncrementalVerificationError},
};
use bytes::Bytes;
use deterministic_bloom::runtime_size::BloomFilter;
use libipld_core::{
    cid::Cid,
    multihash::{Code, MultihashDigest},
};
use std::{collections::HashSet, matches};
use wnfs_common::BlockStore;

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

    /// Updates the state of incremental dag verification.
    /// This goes through all "want" blocks and what they link to,
    /// removing items that we now have and don't want anymore.
    #[tracing::instrument(level = "trace", skip_all)]
    pub async fn update_have_cids(
        &mut self,
        store: &impl BlockStore,
        cache: &impl Cache,
    ) -> Result<(), Error> {
        let mut dag_walk = DagWalk::breadth_first(self.want_cids.iter().cloned());

        while let Some(item) = dag_walk.next(store, cache).await? {
            match item {
                TraversedItem::Have(cid) => {
                    self.mark_as_have(cid);
                }
                TraversedItem::Missing(cid) => {
                    tracing::trace!(%cid, "Missing block, adding to want list");
                    self.mark_as_want(cid);
                }
            }
        }

        tracing::debug!(
            num_want = self.want_cids.len(),
            num_have = self.have_cids.len(),
            "Finished dag verification"
        );

        Ok(())
    }

    fn mark_as_want(&mut self, want: Cid) {
        if self.have_cids.contains(&want) {
            tracing::warn!(%want, "Marking a CID as wanted, that we have previously marked as having!");
            self.have_cids.remove(&want);
        }
        self.want_cids.insert(want);
    }

    fn mark_as_have(&mut self, have: Cid) {
        self.want_cids.remove(&have);
        self.have_cids.insert(have);
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

        store
            .put_block_keyed(cid, bytes)
            .await
            .map_err(Error::BlockStoreError)?;

        self.update_have_cids(store, cache).await?;

        Ok(())
    }

    /// Computes the receiver state for the current incremental dag verification state.
    /// This takes the have CIDs and turns them into
    pub fn into_receiver_state(self, bloom_fpr: fn(u64) -> f64) -> ReceiverState {
        let missing_subgraph_roots = self.want_cids.into_iter().collect();

        let bloom_capacity = self.have_cids.len() as u64;

        if bloom_capacity == 0 {
            return ReceiverState {
                missing_subgraph_roots,
                have_cids_bloom: None,
            };
        }

        if missing_subgraph_roots.is_empty() {
            // We're done. No need to compute a bloom.
            return ReceiverState {
                missing_subgraph_roots,
                have_cids_bloom: None,
            };
        }

        let target_fpr = bloom_fpr(bloom_capacity);
        let mut bloom = BloomFilter::new_from_fpr_po2(bloom_capacity, target_fpr);

        self.have_cids
            .into_iter()
            .for_each(|cid| bloom.insert(&cid.to_bytes()));

        tracing::debug!(
            inserted_elements = bloom_capacity,
            size_bits = bloom.as_bytes().len() * 8,
            hash_count = bloom.hash_count(),
            ones_count = bloom.count_ones(),
            target_fpr,
            estimated_fpr = bloom.current_false_positive_rate(),
            "built 'have cids' bloom",
        );

        ReceiverState {
            missing_subgraph_roots,
            have_cids_bloom: Some(bloom),
        }
    }
}
