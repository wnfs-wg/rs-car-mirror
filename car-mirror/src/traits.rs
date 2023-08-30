use crate::common::references;
use anyhow::Result;
use async_trait::async_trait;
use libipld::{Cid, IpldCodec};
use wnfs_common::BlockStore;

#[async_trait(?Send)]
pub trait Cache {
    async fn get_references_cached(&self, cid: Cid) -> Result<Option<Vec<Cid>>>;

    async fn put_references_cached(&self, cid: Cid, references: Vec<Cid>) -> Result<()>;

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

#[derive(Debug)]
pub struct InMemoryCache {
    references: quick_cache::sync::Cache<Cid, Vec<Cid>>,
}

impl InMemoryCache {
    pub fn new(approx_capacity: usize) -> Self {
        Self {
            references: quick_cache::sync::Cache::new(approx_capacity),
        }
    }
}

#[async_trait(?Send)]
impl Cache for InMemoryCache {
    async fn get_references_cached(&self, cid: Cid) -> Result<Option<Vec<Cid>>> {
        Ok(self.references.get(&cid))
    }

    async fn put_references_cached(&self, cid: Cid, references: Vec<Cid>) -> Result<()> {
        self.references.insert(cid, references);
        Ok(())
    }
}

#[derive(Debug)]
pub struct NoCache();

#[async_trait(?Send)]
impl Cache for NoCache {
    async fn get_references_cached(&self, cid: Cid) -> Result<Option<Vec<Cid>>> {
        Ok(None)
    }

    async fn put_references_cached(&self, cid: Cid, references: Vec<Cid>) -> Result<()> {
        Ok(())
    }
}
