use crate::common::references;
use anyhow::Result;
use async_trait::async_trait;
use libipld::{Cid, IpldCodec};
use wnfs_common::{utils::CondSync, BlockStore};

/// This trait abstracts caches used by the car mirror implementation.
/// An efficient cache implementation can significantly reduce the amount
/// of lookups into the blockstore.
///
/// At the moment, all caches are conceptually memoization tables, so you don't
/// necessarily need to think about being careful about cache eviction.
///
/// See `InMemoryCache` for a `quick_cache`-based implementation
/// (enable the `quick-cache` feature), and `NoCache` for disabling the cache.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Cache: CondSync {
    /// This returns further references from the block referenced by given CID,
    /// if the cache is hit.
    /// Returns `None` if it's a cache miss.
    ///
    /// This isn't meant to be called directly, instead use `Cache::references`.
    async fn get_references_cached(&self, cid: Cid) -> Result<Option<Vec<Cid>>>;

    /// Populates the references cache for given CID with given references.
    async fn put_references_cached(&self, cid: Cid, references: Vec<Cid>) -> Result<()>;

    /// Find out any CIDs that are linked to from the block with given CID.
    ///
    /// This makes use of the cache via `get_references_cached`, if possible.
    /// If the cache is missed, then it will fetch the block, compute the references
    /// and automatically populate the cache using `put_references_cached`.
    async fn references(&self, cid: Cid, store: &impl BlockStore) -> Result<Vec<Cid>> {
        // raw blocks don't have further links
        let raw_codec: u64 = IpldCodec::Raw.into();
        if cid.codec() == raw_codec {
            return Ok(Vec::new());
        }

        if let Some(refs) = self.get_references_cached(cid).await? {
            return Ok(refs);
        }

        let block = store.get_block(&cid).await?;
        let refs = references(cid, block, Vec::new())?;
        self.put_references_cached(cid, refs.clone()).await?;
        Ok(refs)
    }
}

/// A [quick-cache]-based implementation of a car mirror cache.
///
/// [quick-cache]: https://github.com/arthurprs/quick-cache/
#[cfg(feature = "quick_cache")]
#[derive(Debug)]
pub struct InMemoryCache {
    references: quick_cache::sync::Cache<Cid, Vec<Cid>>,
}

#[cfg(feature = "quick_cache")]
impl InMemoryCache {
    /// Create a new in-memory cache that approximately holds
    /// cached references for `approx_capacity` CIDs.
    ///
    /// Computing the expected memory requirements isn't easy.
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
    pub fn new(approx_capacity: usize) -> Self {
        Self {
            references: quick_cache::sync::Cache::new(approx_capacity),
        }
    }
}

#[cfg(feature = "quick_cache")]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Cache for InMemoryCache {
    async fn get_references_cached(&self, cid: Cid) -> Result<Option<Vec<Cid>>> {
        Ok(self.references.get(&cid))
    }

    async fn put_references_cached(&self, cid: Cid, references: Vec<Cid>) -> Result<()> {
        self.references.insert(cid, references);
        Ok(())
    }
}

/// An implementation of `Cache` that doesn't cache at all.
#[derive(Debug)]
pub struct NoCache;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Cache for NoCache {
    async fn get_references_cached(&self, _: Cid) -> Result<Option<Vec<Cid>>> {
        Ok(None)
    }

    async fn put_references_cached(&self, _: Cid, _: Vec<Cid>) -> Result<()> {
        Ok(())
    }
}
