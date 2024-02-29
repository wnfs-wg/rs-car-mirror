use anyhow::Result;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use wnfs_common::MemoryBlockStore;

#[test_log::test(tokio::main)]
async fn main() -> Result<()> {
    tracing::info!("Starting");
    let store = MemoryBlockStore::new();

    let mut test_file = vec![0u8; 100_000_000];
    ChaCha8Rng::seed_from_u64(0).fill_bytes(&mut test_file);
    tracing::info!("Test file size: {} bytes", test_file.len());
    let test_root = wnfs_unixfs_file::builder::FileBuilder::new()
        .content_bytes(test_file)
        .build()?
        .store(&store)
        .await?;

    tracing::info!("Serving test root {test_root}");
    car_mirror_axum::serve(store).await?;
    Ok(())
}
