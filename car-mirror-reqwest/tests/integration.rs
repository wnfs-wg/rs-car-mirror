//! A copy of the doctest in lib.rs, because code coverage is buggy
//! with doctests.
use car_mirror::{cache::NoCache, common::Config};
use car_mirror_reqwest::RequestBuilderExt;
use reqwest::Client;
use testresult::TestResult;
use wnfs_common::{BlockStore, CODEC_RAW, MemoryBlockStore};

#[test_log::test(tokio::test)]
async fn test_car_mirror_reqwest_axum_integration() -> TestResult {
    tokio::spawn(car_mirror_axum::serve(MemoryBlockStore::new()));

    let store = MemoryBlockStore::new();
    let data = b"Hello, world!".to_vec();
    let root = store.put_block(data, CODEC_RAW).await?;

    let client = Client::new();
    client
        .post(format!("http://localhost:3344/dag/push/{root}"))
        .run_car_mirror_push(root, &store, &NoCache)
        .await?;

    let store = MemoryBlockStore::new(); // clear out data
    client
        .post(format!("http://localhost:3344/dag/pull/{root}"))
        .run_car_mirror_pull(root, &Config::default(), &store, &NoCache)
        .await?;

    assert!(store.has_block(&root).await?);
    Ok(())
}
