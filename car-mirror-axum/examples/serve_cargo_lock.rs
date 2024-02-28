use anyhow::Result;
use wnfs_common::MemoryBlockStore;

#[test_log::test(tokio::main)]
async fn main() -> Result<()> {
    tracing::info!("Starting");
    let store = MemoryBlockStore::new();

    let test_file = tokio::fs::read("./Cargo.lock").await?.repeat(10_000);
    println!("Test file size: {} bytes", test_file.len());
    let test_root = wnfs_unixfs_file::builder::FileBuilder::new()
        .content_bytes(test_file)
        .build()?
        .store(&store)
        .await?;

    println!("Serving test root {test_root}");
    car_mirror_axum::serve(store).await?;
    Ok(())
}
