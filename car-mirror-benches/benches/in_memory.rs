use car_mirror::{
    cache::InMemoryCache,
    common::Config,
    pull, push,
    test_utils::{arb_ipld_dag, links_to_padded_ipld, setup_blockstore},
};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use wnfs_common::MemoryBlockStore;

pub fn push(c: &mut Criterion) {
    let mut rvg = car_mirror::test_utils::Rvg::deterministic();

    c.bench_function("push cold", |b| {
        b.iter_batched(
            || {
                let (blocks, root) = rvg.sample(&arb_ipld_dag(
                    250..256,
                    0.9, // Very highly connected
                    links_to_padded_ipld(10 * 1024),
                ));
                let store = async_std::task::block_on(setup_blockstore(blocks)).unwrap();
                (store, root)
            },
            |(ref client_store, root)| {
                let client_cache = &InMemoryCache::new(100_000);
                let server_store = &MemoryBlockStore::new();
                let server_cache = &InMemoryCache::new(100_000);
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

pub fn pull(c: &mut Criterion) {
    let mut rvg = car_mirror::test_utils::Rvg::deterministic();

    c.bench_function("pull cold", |b| {
        b.iter_batched(
            || {
                let (blocks, root) = rvg.sample(&arb_ipld_dag(
                    250..256,
                    0.9,                             // Very highly connected
                    links_to_padded_ipld(10 * 1024), // 10KiB random data per block
                ));
                let store = async_std::task::block_on(setup_blockstore(blocks)).unwrap();
                (store, root)
            },
            |(ref server_store, root)| {
                let server_cache = &InMemoryCache::new(100_000);
                let client_store = &MemoryBlockStore::new();
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

criterion_group!(benches, push, pull);
criterion_main!(benches);
