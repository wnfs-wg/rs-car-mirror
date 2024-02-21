use crate::common::references;
use futures::Future;
use libipld::{Cid, IpldCodec};
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
    ) -> impl Future<Output = Result<Option<Vec<Cid>>, BlockStoreError>> + CondSend;

    /// Populates the references cache for given CID with given references.
    fn put_references_cache(
        &self,
        cid: Cid,
        references: Vec<Cid>,
    ) -> impl Future<Output = Result<(), BlockStoreError>> + CondSend;

    /// Find out any CIDs that are linked to from the block with given CID.
    ///
    /// This makes use of the cache via `get_references_cached`, if possible.
    /// If the cache is missed, then it will fetch the block, compute the references
    /// and automatically populate the cache using `put_references_cached`.
    fn references(
        &self,
        cid: Cid,
        store: &impl BlockStore,
    ) -> impl Future<Output = Result<Vec<Cid>, BlockStoreError>> + CondSend {
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
    async fn get_references_cache(&self, cid: Cid) -> Result<Option<Vec<Cid>>, BlockStoreError> {
        (**self).get_references_cache(cid).await
    }

    async fn put_references_cache(
        &self,
        cid: Cid,
        references: Vec<Cid>,
    ) -> Result<(), BlockStoreError> {
        (**self).put_references_cache(cid, references).await
    }
}

impl<C: Cache> Cache for Box<C> {
    async fn get_references_cache(&self, cid: Cid) -> Result<Option<Vec<Cid>>, BlockStoreError> {
        (**self).get_references_cache(cid).await
    }

    async fn put_references_cache(
        &self,
        cid: Cid,
        references: Vec<Cid>,
    ) -> Result<(), BlockStoreError> {
        (**self).put_references_cache(cid, references).await
    }
}

/// An implementation of `Cache` that doesn't cache at all.
#[derive(Debug, Clone)]
pub struct NoCache;

impl Cache for NoCache {
    async fn get_references_cache(&self, _: Cid) -> Result<Option<Vec<Cid>>, BlockStoreError> {
        Ok(None)
    }

    async fn put_references_cache(&self, _: Cid, _: Vec<Cid>) -> Result<(), BlockStoreError> {
        Ok(())
    }
}

#[cfg(feature = "quick_cache")]
pub use quick_cache::*;

#[cfg(feature = "quick_cache")]
mod quick_cache {
    use super::Cache;
    use bytes::Bytes;
    use libipld::Cid;
    use quick_cache::{sync, OptionsBuilder, Weighter};
    use wnfs_common::{
        utils::{Arc, CondSend},
        BlockStore, BlockStoreError,
    };

    /// A [quick-cache]-based implementation of a car mirror cache.
    ///
    /// [quick-cache]: https://github.com/arthurprs/quick-cache/
    #[derive(Debug, Clone)]
    pub struct InMemoryCache {
        references: Arc<sync::Cache<Cid, Vec<Cid>, ReferencesWeighter>>,
    }

    /// A wrapper struct for a `BlockStore` that attaches an in-memory cache
    /// of which blocks are available and which aren't, speeding up
    /// consecutive `has_block` and `get_block` calls.
    #[derive(Debug, Clone)]
    pub struct CacheMissing<B: BlockStore> {
        /// Access to the inner blockstore
        pub inner: B,
        has_blocks: Arc<sync::Cache<Cid, bool>>,
    }

    impl InMemoryCache {
        /// Create a new in-memory cache that approximately holds
        /// cached references for `approx_cids` CIDs.
        ///
        /// Memory requirements can be eye-balled by calculating ~100 bytes
        /// per CID in the cache.
        ///
        /// So if you want this cache to never exceed roughly ~100MB, set
        /// `approx_cids` to `1_000_000`.
        pub fn new(approx_cids: usize) -> Self {
            let max_links_per_unixfs = 175;
            let est_average_links = max_links_per_unixfs / 10;
            Self {
                references: Arc::new(sync::Cache::with_options(
                    OptionsBuilder::new()
                        .estimated_items_capacity(approx_cids / est_average_links)
                        .weight_capacity(approx_cids as u64)
                        .build()
                        .expect("Couldn't create options for quick cache?"),
                    ReferencesWeighter,
                    Default::default(),
                    Default::default(),
                )),
            }
        }
    }

    impl Cache for InMemoryCache {
        async fn get_references_cache(
            &self,
            cid: Cid,
        ) -> Result<Option<Vec<Cid>>, BlockStoreError> {
            Ok(self.references.get(&cid))
        }

        async fn put_references_cache(
            &self,
            cid: Cid,
            references: Vec<Cid>,
        ) -> Result<(), BlockStoreError> {
            self.references.insert(cid, references);
            Ok(())
        }
    }

    impl<B: BlockStore> CacheMissing<B> {
        /// Wrap an existing `BlockStore`, caching `has_block` responses.
        ///
        /// This will also intercept `get_block` requests and fail early, if the
        /// block is known to be missing.
        /// In some circumstances this can be problematic, e.g. if blocks can be
        /// added and removed to the underlying blockstore without going through
        /// the wrapped instance's `put_block` or `put_block_keyed` interfaces.
        ///
        /// In these cases a more advanced caching strategy that may answer
        /// `has_block` early from a cache with a cache TTL & eviction strategy.
        ///
        /// The additional memory requirements for this cache can be estimated
        /// using the `approx_capacity`: Each cache line is roughly ~100 bytes
        /// in size, so for a 100MB cache, set this value to `1_000_000`.
        pub fn new(approx_capacity: usize, inner: B) -> Self {
            Self {
                inner,
                has_blocks: Arc::new(sync::Cache::new(approx_capacity)),
            }
        }
    }

    impl<B: BlockStore> BlockStore for CacheMissing<B> {
        async fn get_block(&self, cid: &Cid) -> Result<Bytes, BlockStoreError> {
            match self.has_blocks.get_value_or_guard_async(cid).await {
                Ok(false) => Err(BlockStoreError::CIDNotFound(*cid)),
                Ok(true) => self.inner.get_block(cid).await,
                Err(guard) => match self.inner.get_block(cid).await {
                    Ok(block) => {
                        let _ignore_meantime_eviction = guard.insert(true);
                        Ok(block)
                    }
                    e @ Err(BlockStoreError::CIDNotFound(_)) => {
                        let _ignore_meantime_eviction = guard.insert(false);
                        e
                    }
                    Err(e) => Err(e),
                },
            }
        }

        async fn put_block_keyed(
            &self,
            cid: Cid,
            bytes: impl Into<Bytes> + CondSend,
        ) -> Result<(), BlockStoreError> {
            self.inner.put_block_keyed(cid, bytes).await?;
            self.has_blocks.insert(cid, true);
            Ok(())
        }

        async fn has_block(&self, cid: &Cid) -> Result<bool, BlockStoreError> {
            self.has_blocks
                .get_or_insert_async(cid, self.inner.has_block(cid))
                .await
        }

        async fn put_block(
            &self,
            bytes: impl Into<Bytes> + CondSend,
            codec: u64,
        ) -> Result<Cid, BlockStoreError> {
            let cid = self.inner.put_block(bytes, codec).await?;
            self.has_blocks.insert(cid, true);
            Ok(cid)
        }

        fn create_cid(&self, bytes: &[u8], codec: u64) -> Result<Cid, BlockStoreError> {
            self.inner.create_cid(bytes, codec)
        }
    }

    #[derive(Debug, Clone)]
    struct ReferencesWeighter;

    impl Weighter<Cid, Vec<Cid>> for ReferencesWeighter {
        fn weight(&self, _key: &Cid, val: &Vec<Cid>) -> u32 {
            1 + val.len() as u32
        }
    }

    #[cfg(test)]
    mod tests {
        use super::{Cache, InMemoryCache};
        use libipld::{cbor::DagCborCodec, Ipld, IpldCodec};
        use testresult::TestResult;
        use wnfs_common::{encode, BlockStore, MemoryBlockStore};

        #[test_log::test(async_std::test)]
        async fn test_references_cache() -> TestResult {
            let store = &MemoryBlockStore::new();
            let cache = InMemoryCache::new(100_000);

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
}

#[cfg(test)]
mod tests {
    use super::{Cache, NoCache};
    use anyhow::Result;
    use libipld::{cbor::DagCborCodec, Cid, Ipld, IpldCodec};
    use std::{collections::HashMap, sync::RwLock};
    use testresult::TestResult;
    use wnfs_common::{encode, BlockStore, BlockStoreError, MemoryBlockStore};

    #[derive(Debug, Default)]
    struct HashMapCache {
        references: RwLock<HashMap<Cid, Vec<Cid>>>,
    }

    impl Cache for HashMapCache {
        async fn get_references_cache(
            &self,
            cid: Cid,
        ) -> Result<Option<Vec<Cid>>, BlockStoreError> {
            Ok(self.references.read().unwrap().get(&cid).cloned())
        }

        async fn put_references_cache(
            &self,
            cid: Cid,
            references: Vec<Cid>,
        ) -> Result<(), BlockStoreError> {
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
