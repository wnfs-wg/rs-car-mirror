#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, missing_docs, rust_2018_idioms)]
#![deny(unreachable_pub)]

//! # car-mirror-axum TODO docs

mod error;
pub mod extract;
mod server;

pub use error::*;
pub use server::*;
