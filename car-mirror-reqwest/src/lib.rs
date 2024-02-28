#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, missing_docs, rust_2018_idioms)]
#![deny(unreachable_pub)]

//! # car-mirror-reqwest
//!
//! A helper library that helps making car-mirror client requests using reqwest.
//!
//! ## Examples
//!
//! ```
//! # use anyhow::Result;
//! use car_mirror::{cache::NoCache, common::Config};
//! use car_mirror_reqwest::RequestBuilderExt;
//! use reqwest::Client;
//! use wnfs_common::{BlockStore, MemoryBlockStore, CODEC_RAW};
//!
//! # #[test_log::test(tokio::main)]
//! # async fn main() -> Result<()> {
//! // Say, you have a webserver that supports car-mirror requests running:
//! tokio::spawn(car_mirror_axum::serve(MemoryBlockStore::new()));
//!
//! // You can issue requests from your client like so:
//! let store = MemoryBlockStore::new();
//! let data = b"Hello, world!".to_vec();
//! let root = store.put_block(data, CODEC_RAW).await?;
//!
//! let client = Client::new();
//! client
//!     .post(format!("http://localhost:3344/dag/push/{root}"))
//!     .run_car_mirror_push(root, &store, &NoCache) // rounds of push protocol
//!     .await?;
//!
//! let store = MemoryBlockStore::new(); // clear out data
//! client
//!     .get(format!("http://localhost:3344/dag/pull/{root}"))
//!     .run_car_mirror_pull(root, &Config::default(), &store, &NoCache) // rounds of pull protocol
//!     .await?;
//!
//! assert!(store.has_block(&root).await?);
//! # Ok(())
//! # }
//! ```

mod error;
mod request;

pub use error::*;
pub use request::*;
