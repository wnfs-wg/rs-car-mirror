use anyhow::Result;
use bytes::Bytes;
use car_mirror::{
    cache::{CacheMissing, InMemoryCache},
    common::Config,
    pull, push,
    test_utils::{arb_ipld_dag, links_to_padded_ipld, setup_blockstore},
};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use std::time::Duration;
use wnfs_common::{BlockStore, BlockStoreError, Cid, MemoryBlockStore, utils::CondSend};

pub fn push_throttled(c: &mut Criterion) {
    let mut rvg = car_mirror::test_utils::Rvg::deterministic();

    c.bench_function("push cold, get_block throttled", |b| {
        b.iter_batched(
            || {
                let (blocks, root) = rvg.sample(&arb_ipld_dag(
                    60..64,
                    0.9, // Very highly connected
                    links_to_padded_ipld(10 * 1024),
                ));
                let store = async_std::task::block_on(setup_blockstore(blocks)).unwrap();
                (store, root)
            },
            |(client_store, root)| {
                let client_store = &CacheMissing::new(100_000, ThrottledBlockStore(client_store));
                let client_cache = &InMemoryCache::new(100_000);
                let server_store = &CacheMissing::new(100_000, ThrottledBlockStore::new());
                let server_cache = &InMemoryCache::new(100_000);
                let config = &Config::default();

                // Simulate a multi-round protocol run in-memory
                async_std::task::block_on(async move {
                    let mut last_response = None;
                    loop {
                        let request = push::request(
                            root,
                            last_response,
                            config,
                            client_store.clone(),
                            client_cache.clone(),
                        )
                        .await?;

                        let response =
                            push::response(root, request, config, server_store, server_cache)
                                .await?;

                        if response.indicates_finished() {
                            break;
                        }

                        last_response = Some(response);
                    }

                    Ok::<(), anyhow::Error>(())
                })
                .unwrap();
            },
            BatchSize::LargeInput,
        )
    });
}

pub fn pull_throttled(c: &mut Criterion) {
    let mut rvg = car_mirror::test_utils::Rvg::deterministic();

    c.bench_function("pull cold, get_block throttled", |b| {
        b.iter_batched(
            || {
                let (blocks, root) = rvg.sample(&arb_ipld_dag(
                    60..64,
                    0.9,                             // Very highly connected
                    links_to_padded_ipld(10 * 1024), // 10KiB random data added
                ));
                let store = async_std::task::block_on(setup_blockstore(blocks)).unwrap();
                (store, root)
            },
            |(server_store, root)| {
                let server_store = &CacheMissing::new(100_000, ThrottledBlockStore(server_store));
                let server_cache = &InMemoryCache::new(100_000);
                let client_store = &CacheMissing::new(100_000, ThrottledBlockStore::new());
                let client_cache = &InMemoryCache::new(100_000);
                let config = &Config::default();

                // Simulate a multi-round protocol run in-memory
                async_std::task::block_on(async move {
                    let mut request =
                        pull::request(root, None, config, client_store, client_cache).await?;
                    loop {
                        let response =
                            pull::response(root, request, config, server_store, server_cache)
                                .await?;
                        request =
                            pull::request(root, Some(response), config, client_store, client_cache)
                                .await?;

                        if request.indicates_finished() {
                            break;
                        }
                    }

                    Ok::<(), anyhow::Error>(())
                })
                .unwrap();
            },
            BatchSize::LargeInput,
        )
    });
}

#[derive(Debug, Clone)]
struct ThrottledBlockStore(MemoryBlockStore);

impl BlockStore for ThrottledBlockStore {
    async fn get_block(&self, cid: &Cid) -> Result<Bytes, BlockStoreError> {
        async_std::task::sleep(Duration::from_micros(50)).await; // Block fetching is artifically slowed by 50 microseconds
        self.0.get_block(cid).await
    }

    async fn put_block(
        &self,
        bytes: impl Into<Bytes> + CondSend,
        codec: u64,
    ) -> Result<Cid, BlockStoreError> {
        self.0.put_block(bytes, codec).await
    }

    async fn put_block_keyed(
        &self,
        cid: Cid,
        bytes: impl Into<Bytes> + CondSend,
    ) -> Result<(), BlockStoreError> {
        self.0.put_block_keyed(cid, bytes).await
    }

    async fn has_block(&self, cid: &Cid) -> Result<bool, BlockStoreError> {
        async_std::task::sleep(Duration::from_micros(50)).await; // Block fetching is artifically slowed by 50 microseconds
        self.0.has_block(cid).await
    }
}

impl ThrottledBlockStore {
    pub fn new() -> Self {
        Self(MemoryBlockStore::new())
    }
}

criterion_group!(benches, push_throttled, pull_throttled);
criterion_main!(benches);
