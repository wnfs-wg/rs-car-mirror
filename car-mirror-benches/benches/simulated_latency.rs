use car_mirror::{
    common::Config,
    pull, push,
    test_utils::{arb_ipld_dag, links_to_padded_ipld, setup_blockstore},
    traits::InMemoryCache,
};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use std::{ops::Range, time::Duration};
use wnfs_common::MemoryBlockStore;

// We artificially slow down sending & receiving data.
// This is the *one way* latency.
const LATENCY: Duration = Duration::from_millis(150);
// We also simulate limited bandwidth.
// We assume that upload & download are asymmetrical.
// Taking statistics from here: https://www.statista.com/statistics/896779/average-mobile-fixed-broadband-download-upload-speeds/
// and using mobile numbers, so 10.33 Mbps for upload and 42.07 Mbps for download.
// This gives us ~1291250 bytes per second upload and ~5258750 bytes per second download.
// Inverting this gives us ~774 nanoseconds per byte upload and ~190 nanoseconds per byte download.
const UPLOAD_DELAY_PER_BYTE: Duration = Duration::from_nanos(774);
const DOWNLOAD_DELAY_PER_BYTE: Duration = Duration::from_nanos(227);

async fn simulate_upload_latency(request_size: usize) {
    let delay = LATENCY + UPLOAD_DELAY_PER_BYTE * request_size as u32;
    async_std::task::sleep(delay).await;
}

async fn simulate_download_latency(response_size: usize) {
    let delay = LATENCY + DOWNLOAD_DELAY_PER_BYTE * response_size as u32;
    async_std::task::sleep(delay).await;
}

pub fn pull_with_simulated_latency_10kb_blocks(c: &mut Criterion) {
    // Very highly connected
    // 10KiB random data added
    // ~61 blocks on average
    pull_with_simulated_latency(c, 60..64, 0.9, 10 * 1024);
}

pub fn pull_with_simulated_latency_1kb_blocks(c: &mut Criterion) {
    // Very highly connected
    // 1KiB random data added
    // ~625 blocks on average
    pull_with_simulated_latency(c, 600..640, 0.9, 1024);
}

pub fn pull_with_simulated_latency(
    c: &mut Criterion,
    dag_size: impl Into<Range<u16>>,
    edge_probability: f64,
    block_padding: usize,
) {
    let mut rvg = car_mirror::test_utils::Rvg::deterministic();

    let dag_size = dag_size.into();

    let bench_name = format!(
        "pull with simulated latency, {block_padding} byte blocks, ~{}..{} blocks",
        dag_size.start, dag_size.end
    );

    c.bench_function(&bench_name, |b| {
        b.iter_batched(
            || {
                let (blocks, root) = rvg.sample(&arb_ipld_dag(
                    dag_size.clone(),
                    edge_probability,
                    links_to_padded_ipld(block_padding),
                ));
                let store = async_std::task::block_on(setup_blockstore(blocks)).unwrap();
                let cache = InMemoryCache::new(10_000, 150_000);
                (store, cache, root)
            },
            |(ref server_store, ref server_cache, root)| {
                let client_store = &MemoryBlockStore::new();
                let client_cache = &InMemoryCache::new(10_000, 150_000);
                let config = &Config::default();

                // Simulate a multi-round protocol run in-memory
                async_std::task::block_on(async move {
                    let mut request =
                        pull::request(root, None, config, client_store, client_cache).await?;
                    loop {
                        let request_bytes = serde_ipld_dagcbor::to_vec(&request)?.len();
                        simulate_upload_latency(request_bytes).await;

                        let response =
                            pull::response(root, request, config, server_store, server_cache)
                                .await?;
                        simulate_download_latency(response.bytes.len()).await;

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

pub fn push_with_simulated_latency_10kb_blocks(c: &mut Criterion) {
    // Very highly connected
    // 10KiB random data added
    // ~61 blocks on average
    push_with_simulated_latency(c, 60..64, 0.9, 10 * 1024);
}

pub fn push_with_simulated_latency_1kb_blocks(c: &mut Criterion) {
    // Very highly connected
    // 1KiB random data added
    // ~625 blocks on average
    push_with_simulated_latency(c, 600..640, 0.9, 1024);
}

pub fn push_with_simulated_latency(
    c: &mut Criterion,
    dag_size: impl Into<Range<u16>>,
    edge_probability: f64,
    block_padding: usize,
) {
    let mut rvg = car_mirror::test_utils::Rvg::deterministic();

    let dag_size = dag_size.into();

    let bench_name = format!(
        "push with simulated latency, {block_padding} byte blocks, ~{}..{} blocks",
        dag_size.start, dag_size.end
    );

    c.bench_function(&bench_name, |b| {
        b.iter_batched(
            || {
                let (blocks, root) = rvg.sample(&arb_ipld_dag(
                    dag_size.clone(),
                    edge_probability,
                    links_to_padded_ipld(block_padding),
                ));
                let store = async_std::task::block_on(setup_blockstore(blocks)).unwrap();
                let cache = InMemoryCache::new(10_000, 150_000);
                (store, cache, root)
            },
            |(ref client_store, ref client_cache, root)| {
                let server_store = &MemoryBlockStore::new();
                let server_cache = &InMemoryCache::new(10_000, 150_000);
                let config = &Config::default();

                // Simulate a multi-round protocol run in-memory
                async_std::task::block_on(async move {
                    let mut request =
                        push::request(root, None, config, client_store, client_cache).await?;
                    loop {
                        simulate_upload_latency(request.bytes.len()).await;

                        let response =
                            push::response(root, request, config, server_store, server_cache)
                                .await?;
                        let response_bytes = serde_ipld_dagcbor::to_vec(&response)?.len();
                        simulate_download_latency(response_bytes).await;

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

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10); // Reduced sample size due to ~2s per test
    targets = pull_with_simulated_latency_10kb_blocks, pull_with_simulated_latency_1kb_blocks, push_with_simulated_latency_10kb_blocks, push_with_simulated_latency_1kb_blocks
}
criterion_main!(benches);
