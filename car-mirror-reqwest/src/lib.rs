#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, missing_docs, rust_2018_idioms)]
#![deny(unreachable_pub)]

//! A helper library that helps making car-mirror client requests using reqwest

mod error;
mod request;

pub use error::*;
pub use request::*;
