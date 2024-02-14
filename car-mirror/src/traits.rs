use crate::common::references;
use anyhow::Result;
use futures::Future;
use libipld::{Cid, IpldCodec};
#[cfg(feature = "quick_cache")]
use wnfs_common::utils::Arc;
use wnfs_common::{
    utils::{CondSend, CondSync},
    BlockStore, BlockStoreError,
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

    /// This returns whether the cache has the fact stored that a block with given
    /// CID is stored.
    ///
    /// This only returns `true` in case the block has been stored.
    /// `false` simply indicates that the cache doesn't know whether the block is
    /// stored or not (it's always a cache miss).
    ///
    /// Don't call this directly, instead, use `Cache::has_block`.
    fn get_has_block_cache(&self, cid: &Cid) -> impl Future<Output = Result<bool>> + CondSend;

    /// This populates the cache with the fact that a block with given CID is stored.
    fn put_has_block_cache(&self, cid: Cid) -> impl Future<Output = Result<()>> + CondSend;

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

    /// Find out whether a given block is stored in given blockstore or not.
    ///
    /// This cache is *only* effective on `true` values for `has_block`.
    /// Repeatedly calling `has_block` with `Cid`s of blocks that are *not*
    /// stored will cause repeated calls to given blockstore.
    ///
    /// **Make sure to always use the same `BlockStore` when calling this function.**
    ///
    /// This makes use of the caches `get_has_block_cached`, if possible.
    /// On cache misses, this will actually fetch the block from the store
    /// and if successful, populate the cache using `put_has_block_cached`.
    fn has_block(
        &self,
        cid: Cid,
        store: &impl BlockStore,
    ) -> impl Future<Output = Result<bool>> + CondSend {
        async move {
            if self.get_has_block_cache(&cid).await? {
                return Ok(true);
            }

            match store.get_block(&cid).await {
                Ok(_) => {
                    self.put_has_block_cache(cid).await?;
                    Ok(true)
                }
                Err(e) if matches!(e.downcast_ref(), Some(BlockStoreError::CIDNotFound(_))) => {
                    Ok(false)
                }
                Err(e) => Err(e),
            }
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

    async fn get_has_block_cache(&self, cid: &Cid) -> Result<bool> {
        (**self).get_has_block_cache(cid).await
    }

    async fn put_has_block_cache(&self, cid: Cid) -> Result<()> {
        (**self).put_has_block_cache(cid).await
    }
}

impl<C: Cache> Cache for Box<C> {
    async fn get_references_cache(&self, cid: Cid) -> Result<Option<Vec<Cid>>> {
        (**self).get_references_cache(cid).await
    }

    async fn put_references_cache(&self, cid: Cid, references: Vec<Cid>) -> Result<()> {
        (**self).put_references_cache(cid, references).await
    }

    async fn get_has_block_cache(&self, cid: &Cid) -> Result<bool> {
        (**self).get_has_block_cache(cid).await
    }

    async fn put_has_block_cache(&self, cid: Cid) -> Result<()> {
        (**self).put_has_block_cache(cid).await
    }
}

/// A [quick-cache]-based implementation of a car mirror cache.
///
/// [quick-cache]: https://github.com/arthurprs/quick-cache/
#[cfg(feature = "quick_cache")]
#[derive(Debug, Clone)]
pub struct InMemoryCache {
    references: Arc<quick_cache::sync::Cache<Cid, Vec<Cid>>>,
    has_blocks: Arc<quick_cache::sync::Cache<Cid, ()>>,
}

#[cfg(feature = "quick_cache")]
impl InMemoryCache {
    /// Create a new in-memory cache that approximately holds
    /// cached references for `approx_references_capacity` CIDs
    /// and `approx_has_blocks_capacity` CIDs known to be stored locally.
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
    /// On the other hand, each cache entry for the `has_blocks` cache
    /// will take a little more than 64 bytes, so for a 10MB
    /// `has_blocks` cache, you would use `10MB / 64bytes = 156_250`.
    ///
    /// [UnixFS]: https://github.com/ipfs/specs/blob/main/UNIXFS.md#layout
    pub fn new(approx_references_capacity: usize, approx_has_blocks_capacity: usize) -> Self {
        Self {
            references: Arc::new(quick_cache::sync::Cache::new(approx_references_capacity)),
            has_blocks: Arc::new(quick_cache::sync::Cache::new(approx_has_blocks_capacity)),
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

    async fn get_has_block_cache(&self, cid: &Cid) -> Result<bool> {
        Ok(self.has_blocks.get(cid).is_some())
    }

    async fn put_has_block_cache(&self, cid: Cid) -> Result<()> {
        self.has_blocks.insert(cid, ());
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

    async fn get_has_block_cache(&self, _: &Cid) -> Result<bool> {
        Ok(false)
    }

    async fn put_has_block_cache(&self, _: Cid) -> Result<()> {
        Ok(())
    }
}

#[cfg(feature = "quick_cache")]
#[cfg(test)]
mod quick_cache_tests {
    use super::{Cache, InMemoryCache};
    use libipld::{Ipld, IpldCodec};
    use testresult::TestResult;
    use wnfs_common::{BlockStore, MemoryBlockStore};

    #[test_log::test(async_std::test)]
    async fn test_has_block_cache() -> TestResult {
        let store = &MemoryBlockStore::new();
        let cache = InMemoryCache::new(10_000, 150_000);

        let cid = store
            .put_block(b"Hello, World!".to_vec(), IpldCodec::Raw.into())
            .await?;

        // Initially, the cache is unpopulated
        assert!(!cache.get_has_block_cache(&cid).await?);

        // Then, we populate that cache
        assert!(cache.has_block(cid, store).await?);

        // Now, the cache should be populated
        assert!(cache.get_has_block_cache(&cid).await?);

        Ok(())
    }

    #[test_log::test(async_std::test)]
    async fn test_references_cache() -> TestResult {
        let store = &MemoryBlockStore::new();
        let cache = InMemoryCache::new(10_000, 150_000);

        let hello_one_cid = store
            .put_block(b"Hello, One?".to_vec(), IpldCodec::Raw.into())
            .await?;
        let hello_two_cid = store
            .put_block(b"Hello, Two?".to_vec(), IpldCodec::Raw.into())
            .await?;
        let cid = store
            .put_serializable(&Ipld::List(vec![
                Ipld::Link(hello_one_cid),
                Ipld::Link(hello_two_cid),
            ]))
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
    use libipld::{Cid, Ipld, IpldCodec};
    use std::{
        collections::{HashMap, HashSet},
        sync::RwLock,
    };
    use testresult::TestResult;
    use wnfs_common::{BlockStore, MemoryBlockStore};

    #[derive(Debug, Default)]
    struct HashMapCache {
        references: RwLock<HashMap<Cid, Vec<Cid>>>,
        has_blocks: RwLock<HashSet<Cid>>,
    }

    impl Cache for HashMapCache {
        async fn get_references_cache(&self, cid: Cid) -> Result<Option<Vec<Cid>>> {
            Ok(self.references.read().unwrap().get(&cid).cloned())
        }

        async fn put_references_cache(&self, cid: Cid, references: Vec<Cid>) -> Result<()> {
            self.references.write().unwrap().insert(cid, references);
            Ok(())
        }

        async fn get_has_block_cache(&self, cid: &Cid) -> Result<bool> {
            Ok(self.has_blocks.read().unwrap().contains(cid))
        }

        async fn put_has_block_cache(&self, cid: Cid) -> Result<()> {
            self.has_blocks.write().unwrap().insert(cid);
            Ok(())
        }
    }

    #[test_log::test(async_std::test)]
    async fn test_has_block_cache() -> TestResult {
        let store = &MemoryBlockStore::new();
        let cache = HashMapCache::default();

        let cid = store
            .put_block(b"Hello, World!".to_vec(), IpldCodec::Raw.into())
            .await?;

        // Initially, the cache is unpopulated
        assert!(!cache.get_has_block_cache(&cid).await?);

        // Then, we populate that cache
        assert!(cache.has_block(cid, store).await?);

        // Now, the cache should be populated
        assert!(cache.get_has_block_cache(&cid).await?);

        Ok(())
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
            .put_serializable(&Ipld::List(vec![
                Ipld::Link(hello_one_cid),
                Ipld::Link(hello_two_cid),
            ]))
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
    async fn test_no_cache_has_block() -> TestResult {
        let store = &MemoryBlockStore::new();
        let cache = NoCache;

        let cid = store
            .put_block(b"Hello, World!".to_vec(), IpldCodec::Raw.into())
            .await?;

        let not_stored_cid = store.create_cid(b"Hi!", IpldCodec::Raw.into())?;

        // Cache should start out unpopulated
        assert!(!cache.get_has_block_cache(&cid).await?);

        // Then we "try to populate it".
        assert!(cache.has_block(cid, store).await?);

        // It should still give correct answers
        assert!(!cache.has_block(not_stored_cid, store).await?);

        // Still, it should stay unpopulated
        assert!(!cache.get_has_block_cache(&cid).await?);

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
            .put_serializable(&Ipld::List(vec![
                Ipld::Link(hello_one_cid),
                Ipld::Link(hello_two_cid),
            ]))
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
