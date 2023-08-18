use crate::common::references;
use anyhow::Result;
use bytes::Bytes;
use futures::{stream::try_unfold, Stream};
use libipld_core::cid::Cid;
use std::collections::{HashSet, VecDeque};
use wnfs_common::BlockStore;

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

    /// Return the next node in the traversal.
    ///
    /// Returns `None` if no nodes are left to be visited.
    pub async fn next(&mut self, store: &impl BlockStore) -> Result<Option<(Cid, Bytes)>> {
        let cid = loop {
            let popped = if self.breadth_first {
                self.frontier.pop_back()
            } else {
                self.frontier.pop_front()
            };

            let Some(cid) = popped else {
                return Ok(None);
            };

            // We loop until we find an unvisited block
            if self.visited.insert(cid) {
                break cid;
            }
        };

        let block = store.get_block(&cid).await?;
        for ref_cid in references(cid, &block)? {
            if !self.visited.contains(&ref_cid) {
                self.frontier.push_front(ref_cid);
            }
        }

        Ok(Some((cid, block)))
    }

    /// Turn this traversal into a stream
    pub fn stream(
        self,
        store: &impl BlockStore,
    ) -> impl Stream<Item = Result<(Cid, Bytes)>> + Unpin + '_ {
        Box::pin(try_unfold(self, move |mut this| async move {
            let maybe_block = this.next(store).await?;
            Ok(maybe_block.map(|b| (b, this)))
        }))
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
    pub fn skip_walking(&mut self, block: (Cid, Bytes)) -> Result<()> {
        let (cid, bytes) = block;
        let refs = references(cid, bytes)?;
        self.visited.insert(cid);
        self.frontier
            .retain(|frontier_cid| !refs.contains(frontier_cid));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use libipld::Ipld;
    use wnfs_common::MemoryBlockStore;

    #[async_std::test]
    async fn test_walk_dag_breadth_first() -> Result<()> {
        let store = &MemoryBlockStore::new();

        let cid_1 = store.put_serializable(&Ipld::String("1".into())).await?;
        let cid_2 = store.put_serializable(&Ipld::String("2".into())).await?;
        let cid_3 = store.put_serializable(&Ipld::String("3".into())).await?;

        let cid_1_wrap = store
            .put_serializable(&Ipld::List(vec![Ipld::Link(cid_1)]))
            .await?;

        let cid_root = store
            .put_serializable(&Ipld::List(vec![
                Ipld::Link(cid_1_wrap),
                Ipld::Link(cid_2),
                Ipld::Link(cid_3),
            ]))
            .await?;

        let cids = DagWalk::breadth_first([cid_root])
            .stream(store)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|(cid, _block)| cid)
            .collect::<Vec<_>>();

        assert_eq!(cids, vec![cid_root, cid_1_wrap, cid_2, cid_3, cid_1]);

        Ok(())
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use crate::test_utils::{encode, generate_dag};
    use futures::TryStreamExt;
    use libipld::{
        multihash::{Code, MultihashDigest},
        Cid, Ipld, IpldCodec,
    };
    use proptest::strategy::Strategy;
    use std::collections::BTreeSet;
    use test_strategy::proptest;
    use wnfs_common::{BlockStore, MemoryBlockStore};

    fn ipld_dags() -> impl Strategy<Value = (Vec<(Cid, Ipld)>, Cid)> {
        generate_dag(256, |cids| {
            let ipld = Ipld::List(cids.into_iter().map(Ipld::Link).collect());
            let cid = Cid::new_v1(
                IpldCodec::DagCbor.into(),
                Code::Blake3_256.digest(&encode(&ipld)),
            );
            (cid, ipld)
        })
    }

    #[proptest(max_shrink_iters = 100_000)]
    fn walk_dag_never_iterates_block_twice(#[strategy(ipld_dags())] dag: (Vec<(Cid, Ipld)>, Cid)) {
        async_std::task::block_on(async {
            let (dag, root) = dag;
            let store = &MemoryBlockStore::new();
            for (cid, ipld) in dag.iter() {
                let cid_store = store
                    .put_block(encode(ipld), IpldCodec::DagCbor.into())
                    .await
                    .unwrap();
                assert_eq!(*cid, cid_store);
            }

            let mut cids = DagWalk::breadth_first([root])
                .stream(store)
                .map_ok(|(cid, _)| cid)
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
