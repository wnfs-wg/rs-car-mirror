#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, missing_docs, rust_2018_idioms)]
#![deny(unreachable_pub)]

//! # car-mirror-axum
//!
//! This crate exposes a very basic car mirror server.
//! It accepts `GET /dag/pull/:cid`, `POST /dag/pull/:cid` and `POST /dag/push/:cid` requests
//! with streaming car file request and response types, respectively.
//!
//! It is roughly based on the [car-mirror-http specification](https://github.com/wnfs-wg/car-mirror-http-spec).
//!
//! It also exposes some utilities with which it's easier to build a car-mirror axum server.
//!
//! At the moment, it's recommended to only make use of the `extract` module, and mostly
//! use the rest of the library for tests or treat the rest of the code as an example
//! to copy code from for actual production use.

mod error;
pub mod extract;
mod server;

pub use error::*;
pub use server::*;
