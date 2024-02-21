#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, missing_docs, rust_2018_idioms)]
#![deny(unreachable_pub)]

//! # Car Mirror
//!
//! This crate provides the "no-io" protocol pieces to run the car mirror protocol.
//!
//! For more information, see the `push` and `pull` modules for further documentation
//! or take a look at the [specification].
//!
//! [specification]: https://github.com/wnfs-wg/car-mirror-spec

/// Test utilities. Enabled with the `test_utils` feature flag.
#[cfg(any(test, feature = "test_utils"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test_utils")))]
pub mod test_utils;

/// Module with local caching strategies and mechanisms that greatly enhance CAR mirror performance
pub mod cache;
/// Code that's common among the push and pull protocol sides (most of the code).
///
/// This code is less concerened about the "client" and "server" ends of the protocol, but
/// more about the "block sending" and "block receiving" end of the protocol. I.e. which
/// direction do blocks go?
/// When going from "push" to "pull" protocol, the client and server swap the "block sending"
/// and "block receiving" roles.
///
/// Consider the functions in here mostly internal, and refer to the `push` and `pull` modules instead.
pub mod common;
/// Algorithms for walking IPLD directed acyclic graphs
pub mod dag_walk;
/// Error types
mod error;
/// Algorithms for doing incremental verification of IPLD DAGs against a root hash on the receiving end.
pub mod incremental_verification;
/// Data types that are sent over-the-wire and relevant serialization code.
pub mod messages;
/// The CAR mirror pull protocol. Meant to be used qualified, i.e. `pull::request` and `pull::response`.
pub mod pull;
/// The CAR mirror push protocol. Meant to be used qualified, i.e. `push::request` and `push::response`.
///
/// This library exposes both streaming and non-streaming variants. It's recommended to use
/// the streaming variants if possible.
///
/// ## Examples
///
/// ### Test Data
///
/// We'll set up some test data to simulate the protocol like this:
///
/// ```no_run
/// use car_mirror::cache::InMemoryCache;
/// use wnfs_common::MemoryBlockStore;
/// use wnfs_unixfs_file::builder::FileBuilder;
/// use futures::TryStreamExt;
/// use tokio_util::io::StreamReader;
///
/// # #[async_std::main]
/// # async fn main() -> anyhow::Result<()> {
/// // We simulate peers having separate data stores
/// let client_store = MemoryBlockStore::new();
/// let server_store = MemoryBlockStore::new();
///
/// // Give both peers ~1MB of cache space for speeding up computations.
/// // These are available under the `quick_cache` feature.
/// // (You can also implement your own, or disable caches using `NoCache`)
/// let client_cache = InMemoryCache::new(100_000);
/// let server_cache = InMemoryCache::new(100_000);
///
/// let file_bytes = async_std::fs::read("../Cargo.lock").await?;
///
/// // Load some data onto the client
/// let root = FileBuilder::new()
///     .content_bytes(file_bytes.clone())
///     .fixed_chunker(1024) // Generate lots of small blocks
///     .degree(4)
///     .build()?
///     .store(&client_store)
///     .await?;
///
/// // The server may already have a subset of the data
/// FileBuilder::new()
///     .content_bytes(file_bytes[0..10_000].to_vec())
///     .fixed_chunker(1024) // Generate lots of small blocks
///     .degree(4)
///     .build()?
///     .store(&server_store)
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// ### With Streaming
///
/// This simulates a push protocol run between two peers locally:
///
/// ```
/// use car_mirror::{push, common::Config};
/// # use car_mirror::cache::InMemoryCache;
/// # use wnfs_common::MemoryBlockStore;
/// # use wnfs_unixfs_file::builder::FileBuilder;
/// # use futures::TryStreamExt;
/// # use tokio_util::io::StreamReader;
/// #
/// # #[async_std::main]
/// # async fn main() -> anyhow::Result<()> {
/// # let client_store = MemoryBlockStore::new();
/// # let server_store = MemoryBlockStore::new();
/// #
/// # let client_cache = InMemoryCache::new(100_000);
/// # let server_cache = InMemoryCache::new(100_000);
/// #
/// # let file_bytes = async_std::fs::read("../Cargo.lock").await?;
/// #
/// # let root = FileBuilder::new()
/// #     .content_bytes(file_bytes.clone())
/// #     .fixed_chunker(1024) // Generate lots of small blocks
/// #     .degree(4)
/// #     .build()?
/// #     .store(&client_store)
/// #     .await?;
/// #
/// # FileBuilder::new()
/// #     .content_bytes(file_bytes[0..10_000].to_vec())
/// #     .fixed_chunker(1024) // Generate lots of small blocks
/// #     .degree(4)
/// #     .build()?
/// #     .store(&server_store)
/// #     .await?;
///
/// // We set up some protocol configurations (allowed maximum block sizes etc.)
/// let config = &Config::default();
///
/// let mut last_response = None;
/// loop {
///     // The client generates a request that streams the data to the server
///     let stream = push::request_streaming(
///         root,
///         last_response,
///         &client_store,
///         &client_cache
///     ).await?;
///
///     let byte_stream = StreamReader::new(
///         stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
///     );
///
///     // The server consumes the streaming request & interrupts with new
///     // information about what blocks it already has or in case the client
///     // can stop sending altogether.
///     let response = push::response_streaming(
///         root,
///         byte_stream,
///         config,
///         &server_store,
///         &server_cache
///     ).await?;
///
///     if response.indicates_finished() {
///         break; // we're done!
///     }
///
///     last_response = Some(response);
/// }
/// # Ok(())
/// # }
/// ```
///
///
/// ### Without Streaming
///
/// This simulates a push protocol run between two peers locally, without streaming:
///
/// ```
/// use car_mirror::{push, common::Config};
/// # use car_mirror::cache::InMemoryCache;
/// # use wnfs_common::MemoryBlockStore;
/// # use wnfs_unixfs_file::builder::FileBuilder;
/// # use futures::TryStreamExt;
/// # use tokio_util::io::StreamReader;
/// #
/// # #[async_std::main]
/// # async fn main() -> anyhow::Result<()> {
/// # let client_store = MemoryBlockStore::new();
/// # let server_store = MemoryBlockStore::new();
/// #
/// # let client_cache = InMemoryCache::new(100_000);
/// # let server_cache = InMemoryCache::new(100_000);
/// #
/// # let file_bytes = async_std::fs::read("../Cargo.lock").await?;
/// #
/// # let root = FileBuilder::new()
/// #     .content_bytes(file_bytes.clone())
/// #     .fixed_chunker(1024) // Generate lots of small blocks
/// #     .degree(4)
/// #     .build()?
/// #     .store(&client_store)
/// #     .await?;
/// #
/// # FileBuilder::new()
/// #     .content_bytes(file_bytes[0..10_000].to_vec())
/// #     .fixed_chunker(1024) // Generate lots of small blocks
/// #     .degree(4)
/// #     .build()?
/// #     .store(&server_store)
/// #     .await?;
///
/// // We set up some protocol configurations (allowed maximum block sizes etc.)
/// let config = &Config::default();
///
/// let mut last_response = None;
/// loop {
///     // The client creates a CAR file for the request
///     let car_file = push::request(
///         root,
///         last_response,
///         config,
///         &client_store,
///         &client_cache
///     ).await?;
///
///     // The server consumes the car file and provides information about
///     // further blocks needed
///     let response = push::response(
///         root,
///         car_file,
///         config,
///         &server_store,
///         &server_cache
///     ).await?;
///
///     if response.indicates_finished() {
///         break; // we're done!
///     }
///
///     last_response = Some(response);
/// }
/// # Ok(())
/// # }
/// ```
pub mod push;

pub use error::*;
