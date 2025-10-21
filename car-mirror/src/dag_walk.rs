use crate::{cache::Cache, common::references, error::Error};
use bytes::Bytes;
use futures::{Stream, stream::try_unfold};
use std::collections::{HashSet, VecDeque};
use wnfs_common::{BlockStore, BlockStoreError, Cid};

/// A struct that represents an ongoing walk through the Dag.
#[derive(Clone, Debug)]
pub struct DagWalk {
    /// A queue of CIDs to visit next
    pub frontier: VecDeque<Cid>,
    /// The set of already visited CIDs. This prevents re-visiting.
    pub visited: HashSet<Cid>,
    /// Whether to do a breadth-first or depth-first traversal.
    /// This controls whether newly discovered links are appended or prepended to the frontier.
    pub breadth_first: bool,
}

/// Represents the state that a traversed block was found in.
/// If it's `Have`, then
#[derive(Debug, Clone, Copy)]
pub enum TraversedItem {
    /// The block is available locally, and further
    /// links from this block will be traversed.
    Have(Cid),
    /// The block is not available locally, so its links
    /// can't be followed.
    Missing(Cid),
}

impl TraversedItem {
    /// Return the CID of this traversed item. If the block for this CID
    /// is missing, turn this item into the appropriate error.
    pub fn to_cid(self) -> Result<Cid, Error> {
        match self {
            Self::Have(cid) => Ok(cid),
            Self::Missing(cid) => Err(Error::BlockStoreError(BlockStoreError::CIDNotFound(cid))),
        }
    }
}

impl DagWalk {
    /// Start a breadth-first traversal of given roots.
    ///
    /// Breadth-first is explained the easiest in the simple case of a tree (which is a DAG):
    /// It will visit each node in the tree layer-by-layer.
    ///
    /// So the first nodes it will visit are going to be all roots in order.
    pub fn breadth_first(roots: impl IntoIterator<Item = Cid>) -> Self {
        Self::new(roots, true)
    }

    /// Start a depth-first traversal of given roots.
    ///
    /// Depth-first will follow links immediately after discovering them, taking the fastest
    /// path towards leaves.
    ///
    /// The very first node is guaranteed to be the first root, but subsequent nodes may not be
    /// from the initial roots.
    pub fn depth_first(roots: impl IntoIterator<Item = Cid>) -> Self {
        Self::new(roots, false)
    }

    /// Start a DAG traversal of given roots. See also `breadth_first` and `depth_first`.
    pub fn new(roots: impl IntoIterator<Item = Cid>, breadth_first: bool) -> Self {
        let frontier = roots.into_iter().collect();
        let visited = HashSet::new();
        Self {
            frontier,
            visited,
            breadth_first,
        }
    }

    fn frontier_next(&mut self) -> Option<Cid> {
        loop {
            let cid = if self.breadth_first {
                self.frontier.pop_back()?
            } else {
                self.frontier.pop_front()?
            };

            // We loop until we find an unvisited block
            if self.visited.insert(cid) {
                return Some(cid);
            }
        }
    }

    /// Return the next node in the traversal.
    ///
    /// Returns `None` if no nodes are left to be visited.
    ///
    /// Returns `Some((cid, item_state))` where `cid` is the next block in
    /// the traversal, and `item_state` tells you whether the block is available
    /// in the blockstore locally. If not, its links won't be followed and the
    /// traversal will be incomplete.
    /// This is not an error! If you want this to be an error, consider using
    /// `TraversedItem::to_cid`.
    pub async fn next(
        &mut self,
        store: &impl BlockStore,
        cache: &impl Cache,
    ) -> Result<Option<TraversedItem>, Error> {
        let Some(cid) = self.frontier_next() else {
            return Ok(None);
        };

        let has_block = store
            .has_block(&cid)
            .await
            .map_err(Error::BlockStoreError)?;

        if has_block {
            let refs = cache
                .references(cid, store)
                .await
                .map_err(Error::BlockStoreError)?;

            for ref_cid in refs {
                if !self.visited.contains(&ref_cid) {
                    self.frontier.push_front(ref_cid);
                }
            }
        }

        let item = if has_block {
            TraversedItem::Have(cid)
        } else {
            TraversedItem::Missing(cid)
        };

        Ok(Some(item))
    }

    /// Turn this traversal into a stream
    pub fn stream<'a>(
        self,
        store: &'a impl BlockStore,
        cache: &'a impl Cache,
    ) -> impl Stream<Item = Result<TraversedItem, Error>> + Unpin + 'a {
        Box::pin(try_unfold(self, move |mut this| async move {
            let item = this.next(store, cache).await?;
            Ok(item.map(|b| (b, this)))
        }))
    }

    /// Turn this traversal into a stream that takes ownership of the store & cache.
    ///
    /// In most cases `store` and `cache` should be cheaply-clonable types, so giving
    /// the traversal ownership of them shouldn't be a big deal.
    ///
    /// This helps with creating streams that are `: 'static`, which is useful for
    /// anything that ends up being put into e.g. a tokio task.
    pub fn stream_owned(
        self,
        store: impl BlockStore,
        cache: impl Cache,
    ) -> impl Stream<Item = Result<TraversedItem, Error>> + Unpin {
        Box::pin(try_unfold(
            (self, store, cache),
            move |(mut this, store, cache)| async move {
                let item = this.next(&store, &cache).await?;
                Ok(item.map(|b| (b, (this, store, cache))))
            },
        ))
    }

    /// Find out whether the traversal is finished.
    ///
    /// The next call to `next` would result in `None` if this returns true.
    pub fn is_finished(&self) -> bool {
        // We're finished if the frontier does not contain any CIDs that we have not visited yet.
        // Put differently:
        // We're not finished if there exist unvisited CIDs in the frontier.
        !self
            .frontier
            .iter()
            .any(|frontier_cid| !self.visited.contains(frontier_cid))
    }

    /// Skip a node from the traversal for now.
    pub fn skip_walking(&mut self, block: (Cid, Bytes)) -> Result<(), Error> {
        let (cid, bytes) = block;
        let refs = references(cid, bytes, HashSet::new()).map_err(Error::ParsingError)?;
        self.visited.insert(cid);
        self.frontier
            .retain(|frontier_cid| !refs.contains(frontier_cid));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::NoCache;
    use futures::TryStreamExt;
    use ipld_core::ipld::Ipld;
    use testresult::TestResult;
    use wnfs_common::{CODEC_DAG_CBOR, MemoryBlockStore};

    #[test_log::test(async_std::test)]
    async fn test_walk_dag_breadth_first() -> TestResult {
        let store = &MemoryBlockStore::new();

        // cid_root ---> cid_1_wrap ---> cid_1
        //            -> cid_2
        //            -> cid_3

        let cid_1 = store
            .put_block(
                serde_ipld_dagcbor::to_vec(&Ipld::String("1".into()))?,
                CODEC_DAG_CBOR,
            )
            .await?;
        let cid_2 = store
            .put_block(
                serde_ipld_dagcbor::to_vec(&Ipld::String("2".into()))?,
                CODEC_DAG_CBOR,
            )
            .await?;
        let cid_3 = store
            .put_block(
                serde_ipld_dagcbor::to_vec(&Ipld::String("3".into()))?,
                CODEC_DAG_CBOR,
            )
            .await?;

        let cid_1_wrap = store
            .put_block(
                serde_ipld_dagcbor::to_vec(&Ipld::List(vec![Ipld::Link(cid_1)]))?,
                CODEC_DAG_CBOR,
            )
            .await?;

        let cid_root = store
            .put_block(
                serde_ipld_dagcbor::to_vec(&Ipld::List(vec![
                    Ipld::Link(cid_1_wrap),
                    Ipld::Link(cid_2),
                    Ipld::Link(cid_3),
                ]))?,
                CODEC_DAG_CBOR,
            )
            .await?;

        let cids = DagWalk::breadth_first([cid_root])
            .stream(store, &NoCache)
            .and_then(|item| async move { item.to_cid() })
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .collect::<Vec<_>>();

        assert_eq!(cids, vec![cid_root, cid_1_wrap, cid_2, cid_3, cid_1]);

        Ok(())
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use crate::{
        cache::NoCache,
        test_utils::{arb_ipld_dag, links_to_ipld},
    };
    use futures::TryStreamExt;
    use ipld_core::ipld::Ipld;
    use proptest::strategy::Strategy;
    use std::collections::BTreeSet;
    use test_strategy::proptest;
    use wnfs_common::{CODEC_DAG_CBOR, MemoryBlockStore};

    fn ipld_dags() -> impl Strategy<Value = (Vec<(Cid, Ipld)>, Cid)> {
        arb_ipld_dag(1..256, 0.5, |cids, _| links_to_ipld(cids))
    }

    #[proptest(max_shrink_iters = 100_000)]
    fn walk_dag_never_iterates_block_twice(#[strategy(ipld_dags())] dag: (Vec<(Cid, Ipld)>, Cid)) {
        async_std::task::block_on(async {
            let (dag, root) = dag;
            let store = &MemoryBlockStore::new();

            for (cid, ipld) in dag.iter() {
                let block: Bytes = serde_ipld_dagcbor::to_vec(&ipld).unwrap().into();
                let cid_store = store.put_block(block, CODEC_DAG_CBOR).await.unwrap();
                assert_eq!(*cid, cid_store);
            }

            let mut cids = DagWalk::breadth_first([root])
                .stream(store, &NoCache)
                .and_then(|item| async move { item.to_cid() })
                .try_collect::<Vec<_>>()
                .await
                .unwrap();

            cids.sort();

            let unique_cids = cids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();

            assert_eq!(cids, unique_cids);
        });
    }
}
