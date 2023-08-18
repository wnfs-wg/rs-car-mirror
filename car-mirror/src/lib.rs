#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, missing_docs, rust_2018_idioms)]
#![deny(unreachable_pub, private_in_public)]

//! car-mirror

use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use deterministic_bloom::runtime_size::BloomFilter;
use futures::{stream::try_unfold, Stream, StreamExt, TryStreamExt};
use iroh_car::{CarHeader, CarReader, CarWriter};
use libipld::{Ipld, IpldCodec};
use libipld_core::{cid::Cid, codec::References};
use messages::PushResponse;
use std::{
    collections::{HashSet, VecDeque},
    eprintln,
    io::Cursor,
};
use wnfs_common::{BlockStore, BlockStoreError};

/// Test utilities.
#[cfg(any(test, feature = "test_utils"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test_utils")))]
pub mod test_utils;

/// Common utilities
pub mod common;
/// Algorithms for walking IPLD directed acyclic graphs
pub mod dag_walk;
/// Algorithms for doing incremental verification of IPLD DAGs on the receiving end.
pub mod incremental_verification;
/// Data types that are sent over-the-wire and relevant serialization code.
pub mod messages;
/// The CAR mirror push protocol
pub mod push;
