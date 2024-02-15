use crate::common::references;
use anyhow::Result;
use futures::Future;
use libipld::{Cid, IpldCodec};
#[cfg(feature = "quick_cache")]
use wnfs_common::utils::Arc;
use wnfs_common::{
    utils::{CondSend, CondSync},
    BlockStore,
};

/// This trait abstracts caches used by the car mirror implementation.
/// An efficient cache implementation can significantly reduce the amount
/// of lookups into the blockstore.
///
/// At the moment, all caches are either memoization tables or informationally
/// monotonous, so you don't need to be careful about cache eviction.
///
/// See `InMemoryCache` for a `quick_cache`-based implementation
/// (enable the `quick-cache` feature), and `NoCache` for disabling the cache.
pub trait Cache: CondSync {
    /// This returns further references from the block referenced by given CID,
    /// if the cache is hit.
    /// Returns `None` if it's a cache miss.
    ///
    /// This isn't meant to be called directly, instead use `Cache::references`.
    fn get_references_cache(
        &self,
        cid: Cid,
    ) -> impl Future<Output = Result<Option<Vec<Cid>>>> + CondSend;

    /// Populates the references cache for given CID with given references.
    fn put_references_cache(
        &self,
        cid: Cid,
        references: Vec<Cid>,
    ) -> impl Future<Output = Result<()>> + CondSend;

    /// Find out any CIDs that are linked to from the block with given CID.
    ///
    /// This makes use of the cache via `get_references_cached`, if possible.
    /// If the cache is missed, then it will fetch the block, compute the references
    /// and automatically populate the cache using `put_references_cached`.
    fn references(
        &self,
        cid: Cid,
        store: &impl BlockStore,
    ) -> impl Future<Output = Result<Vec<Cid>>> + CondSend {
        async move {
            // raw blocks don't have further links
            let raw_codec: u64 = IpldCodec::Raw.into();
            if cid.codec() == raw_codec {
                return Ok(Vec::new());
            }

            if let Some(refs) = self.get_references_cache(cid).await? {
                return Ok(refs);
            }

            let block = store.get_block(&cid).await?;
            let refs = references(cid, block, Vec::new())?;
            self.put_references_cache(cid, refs.clone()).await?;
            Ok(refs)
        }
    }
}

impl<C: Cache> Cache for &C {
    async fn get_references_cache(&self, cid: Cid) -> Result<Option<Vec<Cid>>> {
        (**self).get_references_cache(cid).await
    }

    async fn put_references_cache(&self, cid: Cid, references: Vec<Cid>) -> Result<()> {
        (**self).put_references_cache(cid, references).await
    }
}

impl<C: Cache> Cache for Box<C> {
    async fn get_references_cache(&self, cid: Cid) -> Result<Option<Vec<Cid>>> {
        (**self).get_references_cache(cid).await
    }

    async fn put_references_cache(&self, cid: Cid, references: Vec<Cid>) -> Result<()> {
        (**self).put_references_cache(cid, references).await
    }
}

/// A [quick-cache]-based implementation of a car mirror cache.
///
/// [quick-cache]: https://github.com/arthurprs/quick-cache/
#[cfg(feature = "quick_cache")]
#[derive(Debug, Clone)]
pub struct InMemoryCache {
    references: Arc<quick_cache::sync::Cache<Cid, Vec<Cid>>>,
}

#[cfg(feature = "quick_cache")]
impl InMemoryCache {
    /// Create a new in-memory cache that approximately holds
    /// cached references for `approx_references_capacity` CIDs.
    ///
    /// Computing the expected memory requirements for the reference
    /// cache isn't easy.
    /// A block in theory have up to thousands of references.
    /// [UnixFS] chunked files will reference up to 174 chunks
    /// at a time.
    /// Each CID takes up 96 bytes of memory.
    /// In the UnixFS worst case of 175 CIDs per cache entry,
    /// you need to reserve up to `175 * 96B = 16.8KB` of space
    /// per entry. Thus, if you want your cache to not outgrow
    /// ~100MB (ignoring the internal cache structure space
    /// requirements), you can store up to `100MB / 16.8KB = 5952`
    /// entries.
    ///
    /// In practice, the fanout average will be much lower than 174.
    ///
    /// [UnixFS]: https://github.com/ipfs/specs/blob/main/UNIXFS.md#layout
    pub fn new(approx_references_capacity: usize) -> Self {
        Self {
            references: Arc::new(quick_cache::sync::Cache::new(approx_references_capacity)),
        }
    }
}

#[cfg(feature = "quick_cache")]
impl Cache for InMemoryCache {
    async fn get_references_cache(&self, cid: Cid) -> Result<Option<Vec<Cid>>> {
        Ok(self.references.get(&cid))
    }

    async fn put_references_cache(&self, cid: Cid, references: Vec<Cid>) -> Result<()> {
        self.references.insert(cid, references);
        Ok(())
    }
}

/// An implementation of `Cache` that doesn't cache at all.
#[derive(Debug)]
pub struct NoCache;

impl Cache for NoCache {
    async fn get_references_cache(&self, _: Cid) -> Result<Option<Vec<Cid>>> {
        Ok(None)
    }

    async fn put_references_cache(&self, _: Cid, _: Vec<Cid>) -> Result<()> {
        Ok(())
    }
}

#[cfg(feature = "quick_cache")]
#[cfg(test)]
mod quick_cache_tests {
    use super::{Cache, InMemoryCache};
    use libipld::{cbor::DagCborCodec, Ipld, IpldCodec};
    use testresult::TestResult;
    use wnfs_common::{encode, BlockStore, MemoryBlockStore};

    #[test_log::test(async_std::test)]
    async fn test_references_cache() -> TestResult {
        let store = &MemoryBlockStore::new();
        let cache = InMemoryCache::new(10_000);

        let hello_one_cid = store
            .put_block(b"Hello, One?".to_vec(), IpldCodec::Raw.into())
            .await?;
        let hello_two_cid = store
            .put_block(b"Hello, Two?".to_vec(), IpldCodec::Raw.into())
            .await?;
        let cid = store
            .put_block(
                encode(
                    &Ipld::List(vec![Ipld::Link(hello_one_cid), Ipld::Link(hello_two_cid)]),
                    DagCborCodec,
                )?,
                DagCborCodec.into(),
            )
            .await?;

        // Cache unpopulated initially
        assert_eq!(cache.get_references_cache(cid).await?, None);

        // This should populate the references cache
        assert_eq!(
            cache.references(cid, store).await?,
            vec![hello_one_cid, hello_two_cid]
        );

        // Cache should now contain the references
        assert_eq!(
            cache.get_references_cache(cid).await?,
            Some(vec![hello_one_cid, hello_two_cid])
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Cache, NoCache};
    use anyhow::Result;
    use libipld::{cbor::DagCborCodec, Cid, Ipld, IpldCodec};
    use std::{collections::HashMap, sync::RwLock};
    use testresult::TestResult;
    use wnfs_common::{encode, BlockStore, MemoryBlockStore};

    #[derive(Debug, Default)]
    struct HashMapCache {
        references: RwLock<HashMap<Cid, Vec<Cid>>>,
    }

    impl Cache for HashMapCache {
        async fn get_references_cache(&self, cid: Cid) -> Result<Option<Vec<Cid>>> {
            Ok(self.references.read().unwrap().get(&cid).cloned())
        }

        async fn put_references_cache(&self, cid: Cid, references: Vec<Cid>) -> Result<()> {
            self.references.write().unwrap().insert(cid, references);
            Ok(())
        }
    }

    #[test_log::test(async_std::test)]
    async fn test_references_cache() -> TestResult {
        let store = &MemoryBlockStore::new();
        let cache = HashMapCache::default();

        let hello_one_cid = store
            .put_block(b"Hello, One?".to_vec(), IpldCodec::Raw.into())
            .await?;
        let hello_two_cid = store
            .put_block(b"Hello, Two?".to_vec(), IpldCodec::Raw.into())
            .await?;
        let cid = store
            .put_block(
                encode(
                    &Ipld::List(vec![Ipld::Link(hello_one_cid), Ipld::Link(hello_two_cid)]),
                    DagCborCodec,
                )?,
                DagCborCodec.into(),
            )
            .await?;

        // Cache unpopulated initially
        assert_eq!(cache.get_references_cache(cid).await?, None);

        // This should populate the references cache
        assert_eq!(
            cache.references(cid, store).await?,
            vec![hello_one_cid, hello_two_cid]
        );

        // Cache should now contain the references
        assert_eq!(
            cache.get_references_cache(cid).await?,
            Some(vec![hello_one_cid, hello_two_cid])
        );

        Ok(())
    }

    #[test_log::test(async_std::test)]
    async fn test_no_cache_references() -> TestResult {
        let store = &MemoryBlockStore::new();
        let cache = NoCache;

        let hello_one_cid = store
            .put_block(b"Hello, One?".to_vec(), IpldCodec::Raw.into())
            .await?;
        let hello_two_cid = store
            .put_block(b"Hello, Two?".to_vec(), IpldCodec::Raw.into())
            .await?;
        let cid = store
            .put_block(
                encode(
                    &Ipld::List(vec![Ipld::Link(hello_one_cid), Ipld::Link(hello_two_cid)]),
                    DagCborCodec,
                )?,
                DagCborCodec.into(),
            )
            .await?;

        // Cache should start out unpopulated
        assert_eq!(cache.get_references_cache(cid).await?, None);

        // We should get the correct answer for our queries
        assert_eq!(
            cache.references(cid, store).await?,
            vec![hello_one_cid, hello_two_cid]
        );

        // We don't have a populated cache though
        assert_eq!(cache.get_references_cache(cid).await?, None);

        Ok(())
    }
}
