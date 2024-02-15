#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, missing_docs, rust_2018_idioms)]
#![deny(unreachable_pub)]

//! car-mirror

/// Test utilities. Enabled with the `test_utils` feature flag.
#[cfg(any(test, feature = "test_utils"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test_utils")))]
pub mod test_utils;

/// Module with local caching strategies and mechanisms that greatly enhance CAR mirror performance
pub mod cache;
/// Common utilities
pub mod common;
/// Algorithms for walking IPLD directed acyclic graphs
pub mod dag_walk;
/// Error types
pub mod error;
/// Algorithms for doing incremental verification of IPLD DAGs on the receiving end.
pub mod incremental_verification;
/// Data types that are sent over-the-wire and relevant serialization code.
pub mod messages;
/// The CAR mirror pull protocol. Meant to be used qualified, i.e. `pull::request` and `pull::response`
pub mod pull;
/// The CAR mirror push protocol. Meant to be used qualified, i.e. `push::request` and `push::response`
pub mod push;
