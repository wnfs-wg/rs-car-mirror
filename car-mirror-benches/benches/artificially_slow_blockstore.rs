use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use car_mirror::{
    common::Config,
    pull, push,
    test_utils::{arb_ipld_dag, links_to_padded_ipld, setup_blockstore},
    traits::InMemoryCache,
};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use libipld::Cid;
use std::time::Duration;
use wnfs_common::{BlockStore, MemoryBlockStore};

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
                let client_store = &ThrottledBlockStore(client_store);
                let client_cache = &InMemoryCache::new(10_000);
                let server_store = &ThrottledBlockStore::new();
                let server_cache = &InMemoryCache::new(10_000);
                let config = &Config::default();

                // Simulate a multi-round protocol run in-memory
                async_std::task::block_on(async move {
                    let mut request =
                        push::request(root, None, config, client_store, client_cache).await?;
                    loop {
                        let response =
                            push::response(root, request, config, server_store, server_cache)
                                .await?;

                        if response.indicates_finished() {
                            break;
                        }
                        request =
                            push::request(root, Some(response), config, client_store, client_cache)
                                .await?;
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
                let server_store = &ThrottledBlockStore(server_store);
                let server_cache = &InMemoryCache::new(10_000);
                let client_store = &ThrottledBlockStore::new();
                let client_cache = &InMemoryCache::new(10_000);
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

#[async_trait(?Send)]
impl BlockStore for ThrottledBlockStore {
    async fn get_block(&self, cid: &Cid) -> Result<Bytes> {
        let bytes = self.0.get_block(cid).await?;
        async_std::task::sleep(Duration::from_micros(50)).await; // Block fetching is artifically slowed by 50 microseconds
        Ok(bytes)
    }

    async fn put_block(&self, bytes: impl Into<Bytes>, codec: u64) -> Result<Cid> {
        self.0.put_block(bytes, codec).await
    }
}

impl ThrottledBlockStore {
    pub fn new() -> Self {
        Self(MemoryBlockStore::new())
    }
}

criterion_group!(benches, push_throttled, pull_throttled);
criterion_main!(benches);
