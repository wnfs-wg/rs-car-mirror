use anyhow::Result;
use axum_server::tls_rustls::RustlsConfig;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::future::IntoFuture;
use wnfs_common::MemoryBlockStore;

#[test_log::test(tokio::main)]
async fn main() -> Result<()> {
    tracing::info!("Starting");

    tracing::info!("Generating self-signed certificate");
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let tls_config = RustlsConfig::from_der(
        vec![cert.serialize_der()?],
        cert.serialize_private_key_der(),
    )
    .await?;
    tracing::info!("Successfully generated self-signed configuration");

    tracing::info!("Generating test data");
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

    let addr = "0.0.0.0:3344".parse()?;
    let handle = tokio::spawn(
        axum_server_dual_protocol::bind_dual_protocol(addr, tls_config)
            .serve(car_mirror_axum::app(store).into_make_service()),
    );
    println!("Listening on {addr}");
    handle.into_future().await??;
    Ok(())
}
