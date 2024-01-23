#![allow(unknown_lints)] // Because the `instrument` macro contains some `#[allow]`s that rust 1.66 doesn't know yet.

use bytes::Bytes;
use deterministic_bloom::runtime_size::BloomFilter;
use futures::{future, TryStreamExt};
use iroh_car::{CarHeader, CarReader, CarWriter};
use libipld::{Ipld, IpldCodec};
use libipld_core::{cid::Cid, codec::References};
use std::io::Cursor;
use tracing::{debug, instrument, trace, warn};
use wnfs_common::{
    utils::{BoxStream, CondSend},
    BlockStore,
};

use crate::{
    dag_walk::DagWalk,
    error::Error,
    incremental_verification::{BlockState, IncrementalDagVerification},
    messages::{Bloom, PullRequest, PushResponse},
    traits::Cache,
};

//--------------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------------

/// Configuration values (such as byte limits) for the CAR mirror protocol
#[derive(Clone, Debug)]
pub struct Config {
    /// The maximum number of bytes per request that a recipient should.
    pub receive_maximum: usize,
    /// The maximum number of roots per request that will be requested by the recipient
    /// to be sent by the sender.
    pub max_roots_per_round: usize,
    /// The target false positive rate for the bloom filter that the recipient sends.
    pub bloom_fpr: fn(u64) -> f64,
}

/// Some information that the block receiving end provides the block sending end
/// in order to deduplicate block transfers.
#[derive(Clone)]
pub struct ReceiverState {
    /// At least *some* of the subgraph roots that are missing for sure on the receiving end.
    pub missing_subgraph_roots: Vec<Cid>,
    /// An optional bloom filter of all CIDs below the root that the receiving end has.
    pub have_cids_bloom: Option<BloomFilter>,
}

/// Newtype around bytes that are supposed to represent a CAR file
#[derive(Debug, Clone)]
pub struct CarFile {
    /// The car file contents as bytes.
    /// (`CarFile` is cheap to clone, since `Bytes` is an `Arc` wrapper around a byte buffer.)
    pub bytes: Bytes,
}

/// A stream of blocks. This requires the underlying futures to be `Send`, except for the `wasm32` target.
pub type BlockStream<'a> = BoxStream<'a, Result<(Cid, Bytes), Error>>;

//--------------------------------------------------------------------------------------------------
// Functions
//--------------------------------------------------------------------------------------------------

/// This function is run on the block sending side of the protocol.
///
/// It's used on the client during the push protocol, or on the server
/// during the pull protocol.
///
/// It returns a `CarFile` of (a subset) of all blocks below `root`, that
/// are thought to be missing on the receiving end.
#[instrument(skip_all, fields(root, last_state))]
pub async fn block_send(
    root: Cid,
    last_state: Option<ReceiverState>,
    config: &Config,
    store: &impl BlockStore,
    cache: &impl Cache,
) -> Result<CarFile, Error> {
    let bytes = block_send_car_stream(
        root,
        last_state,
        Vec::new(),
        Some(config.receive_maximum),
        store,
        cache,
    )
    .await?;

    Ok(CarFile {
        bytes: bytes.into(),
    })
}

/// TODO(matheus23): Docs
#[instrument(skip_all, fields(root, last_state))]
pub async fn block_send_car_stream<'a, W: tokio::io::AsyncWrite + Unpin + Send>(
    root: Cid,
    last_state: Option<ReceiverState>,
    stream: W,
    send_limit: Option<usize>,
    store: &impl BlockStore,
    cache: &impl Cache,
) -> Result<W, Error> {
    let mut block_stream = block_send_block_stream(root, last_state, store, cache).await?;
    write_blocks_into_car(stream, &mut block_stream, send_limit).await
}

/// TODO(matheus23): Docs
pub async fn block_send_block_stream<'a>(
    root: Cid,
    last_state: Option<ReceiverState>,
    store: &'a impl BlockStore,
    cache: &'a impl Cache,
) -> Result<BlockStream<'a>, Error> {
    let ReceiverState {
        missing_subgraph_roots,
        have_cids_bloom,
    } = last_state.unwrap_or(ReceiverState {
        missing_subgraph_roots: vec![root],
        have_cids_bloom: None,
    });

    // Verify that all missing subgraph roots are in the relevant DAG:
    let subgraph_roots =
        verify_missing_subgraph_roots(root, &missing_subgraph_roots, store, cache).await?;

    let bloom = handle_missing_bloom(have_cids_bloom);

    let stream = stream_blocks_from_roots(subgraph_roots, bloom, store, cache);

    Ok(Box::pin(stream))
}

/// This function is run on the block receiving end of the protocol.
///
/// It's used on the client during the pull protocol and on the server
/// during the push protocol.
///
/// It takes a `CarFile`, verifies that its contents are related to the
/// `root` and returns some information to help the block sending side
/// figure out what blocks to send next.
#[instrument(skip_all, fields(root, car_bytes = last_car.as_ref().map(|car| car.bytes.len())))]
pub async fn block_receive(
    root: Cid,
    last_car: Option<CarFile>,
    config: &Config,
    store: &impl BlockStore,
    cache: &impl Cache,
) -> Result<ReceiverState, Error> {
    let mut receiver_state = match last_car {
        Some(car) => {
            if car.bytes.len() > config.receive_maximum {
                return Err(Error::TooManyBytes {
                    receive_maximum: config.receive_maximum,
                    bytes_read: car.bytes.len(),
                });
            }

            block_receive_car_stream(root, Cursor::new(car.bytes), config, store, cache).await?
        }
        None => IncrementalDagVerification::new([root], store, cache)
            .await?
            .into_receiver_state(config.bloom_fpr),
    };

    receiver_state
        .missing_subgraph_roots
        .truncate(config.max_roots_per_round);

    Ok(receiver_state)
}

/// TODO(matheus23): Docs
#[instrument(skip_all, fields(root))]
pub async fn block_receive_car_stream<R: tokio::io::AsyncRead + Unpin + CondSend>(
    root: Cid,
    reader: R,
    config: &Config,
    store: &impl BlockStore,
    cache: &impl Cache,
) -> Result<ReceiverState, Error> {
    let reader = CarReader::new(reader).await?;

    let mut stream: BlockStream<'_> = Box::pin(
        reader
            .stream()
            .map_ok(|(cid, bytes)| (cid, Bytes::from(bytes)))
            .map_err(Error::CarFileError),
    );

    block_receive_block_stream(root, &mut stream, config, store, cache).await
}

/// Consumes a stream of blocks, verifying their integrity and
/// making sure all blocks are part of the DAG.
pub async fn block_receive_block_stream(
    root: Cid,
    stream: &mut BlockStream<'_>,
    config: &Config,
    store: &impl BlockStore,
    cache: &impl Cache,
) -> Result<ReceiverState, Error> {
    let mut dag_verification = IncrementalDagVerification::new([root], store, cache).await?;

    while let Some((cid, block)) = stream.try_next().await? {
        match read_and_verify_block(&mut dag_verification, (cid, block), store, cache).await? {
            BlockState::Have => {
                // This can happen because we've just discovered a subgraph we already have.
                // Let's update the endpoint with our new receiver state.
                tracing::debug!(%cid, "Received block we already have, stopping transfer");
                break;
            }
            BlockState::Unexpected => {
                // We received a block out-of-order. This is weird, but can
                // happen due to bloom filter false positives.
                // Essentially, the sender could've skipped a block that was
                // important for us to verify that further blocks are connected
                // to the root.
                // We should update the endpoint about the skipped block.
                tracing::debug!(%cid, "Received block out of order, stopping transfer");
                break;
            }
            BlockState::Want => {
                // Perfect, we're just getting what we want. Let's continue!
            }
        }
    }

    Ok(dag_verification.into_receiver_state(config.bloom_fpr))
}

/// Find all CIDs that a block references.
///
/// This will error out if
/// - the codec is not supported
/// - the block can't be parsed.
pub fn references<E: Extend<Cid>>(
    cid: Cid,
    block: impl AsRef<[u8]>,
    mut refs: E,
) -> Result<E, anyhow::Error> {
    let codec: IpldCodec = cid
        .codec()
        .try_into()
        .map_err(|_| Error::UnsupportedCodec { cid })?;

    <Ipld as References<IpldCodec>>::references(codec, &mut Cursor::new(block), &mut refs)?;
    Ok(refs)
}

//--------------------------------------------------------------------------------------------------
// Private
//--------------------------------------------------------------------------------------------------

async fn verify_missing_subgraph_roots(
    root: Cid,
    missing_subgraph_roots: &[Cid],
    store: &impl BlockStore,
    cache: &impl Cache,
) -> Result<Vec<Cid>, Error> {
    let subgraph_roots: Vec<Cid> = DagWalk::breadth_first([root])
        .stream(store, cache)
        .try_filter_map(
            |cid| async move { Ok(missing_subgraph_roots.contains(&cid).then_some(cid)) },
        )
        .try_collect()
        .await?;

    if subgraph_roots.len() != missing_subgraph_roots.len() {
        let unrelated_roots = missing_subgraph_roots
            .iter()
            .filter(|cid| !subgraph_roots.contains(cid))
            .map(|cid| cid.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        warn!(
            unrelated_roots = %unrelated_roots,
            "got asked for DAG-unrelated blocks"
        );
    }

    Ok(subgraph_roots)
}

fn handle_missing_bloom(have_cids_bloom: Option<BloomFilter>) -> BloomFilter {
    if let Some(bloom) = &have_cids_bloom {
        debug!(
            size_bits = bloom.as_bytes().len() * 8,
            hash_count = bloom.hash_count(),
            ones_count = bloom.count_ones(),
            estimated_fpr = bloom.current_false_positive_rate(),
            "received 'have cids' bloom",
        );
    }

    have_cids_bloom.unwrap_or_else(|| BloomFilter::new_with(1, Box::new([0]))) // An empty bloom that contains nothing
}

fn stream_blocks_from_roots<'a>(
    subgraph_roots: Vec<Cid>,
    bloom: BloomFilter,
    store: &'a impl BlockStore,
    cache: &'a impl Cache,
) -> BlockStream<'a> {
    Box::pin(
        DagWalk::breadth_first(subgraph_roots.clone())
            .stream(store, cache)
            .try_filter(move |cid| {
                future::ready(!should_block_be_skipped(cid, &bloom, &subgraph_roots))
            })
            .and_then(move |cid| async move {
                let bytes = store
                    .get_block(&cid)
                    .await
                    .map_err(Error::BlockStoreError)?;
                Ok((cid, bytes))
            }),
    )
}

async fn write_blocks_into_car<W: tokio::io::AsyncWrite + Unpin + Send>(
    write: W,
    blocks: &mut BlockStream<'_>,
    size_limit: Option<usize>,
) -> Result<W, Error> {
    let mut block_bytes = 0;

    // https://github.com/wnfs-wg/car-mirror-spec/issues/6
    // CAR files *must* have at least one CID in them, and all of them
    // need to appear as a block in the payload.
    // It would probably make most sense to just write all subgraph roots into this,
    // but we don't know how many of the subgraph roots fit into this round yet,
    // so we're simply writing the first one in here, since we know
    // at least one block will be written (and it'll be that one).
    let Some((cid, block)) = blocks.try_next().await? else {
        debug!("No blocks to write.");
        return Ok(write);
    };

    let mut writer = CarWriter::new(CarHeader::new_v1(vec![cid]), write);

    block_bytes += writer.write(cid, block).await?;

    while let Some((cid, block)) = blocks.try_next().await? {
        debug!(
            cid = %cid,
            num_bytes = block.len(),
            "writing block to CAR",
        );

        // Let's be conservative, assume a 64-byte CID (usually ~40 byte)
        // and a 4-byte frame size varint (3 byte would be enough for an 8MiB frame).
        let added_bytes = 64 + 4 + block.len();

        if let Some(receive_limit) = size_limit {
            if block_bytes + added_bytes > receive_limit {
                debug!(%cid, receive_limit, block_bytes, added_bytes, "Skipping block because it would go over the receive limit");
                break;
            }
        }

        block_bytes += writer.write(cid, &block).await?;
    }

    Ok(writer.finish().await?)
}

fn should_block_be_skipped(cid: &Cid, bloom: &BloomFilter, subgraph_roots: &[Cid]) -> bool {
    bloom.contains(&cid.to_bytes()) && !subgraph_roots.contains(cid)
}

/// Takes a block and stores it iff it's one of the blocks we're currently trying to retrieve.
/// Returns the block state of the received block.
async fn read_and_verify_block(
    dag_verification: &mut IncrementalDagVerification,
    (cid, block): (Cid, Bytes),
    store: &impl BlockStore,
    cache: &impl Cache,
) -> Result<BlockState, Error> {
    match dag_verification.block_state(cid) {
        BlockState::Have => Ok(BlockState::Have),
        BlockState::Unexpected => {
            trace!(
                cid = %cid,
                "received block out of order (possibly due to bloom false positive)"
            );
            Ok(BlockState::Unexpected)
        }
        BlockState::Want => {
            dag_verification
                .verify_and_store_block((cid, block), store, cache)
                .await?;
            Ok(BlockState::Want)
        }
    }
}

//--------------------------------------------------------------------------------------------------
// Implementations
//--------------------------------------------------------------------------------------------------

impl From<PushResponse> for ReceiverState {
    fn from(push: PushResponse) -> Self {
        let PushResponse {
            subgraph_roots,
            bloom,
        } = push;

        Self {
            missing_subgraph_roots: subgraph_roots,
            have_cids_bloom: Self::bloom_deserialize(bloom),
        }
    }
}

impl From<PullRequest> for ReceiverState {
    fn from(pull: PullRequest) -> Self {
        let PullRequest { resources, bloom } = pull;

        Self {
            missing_subgraph_roots: resources,
            have_cids_bloom: Self::bloom_deserialize(bloom),
        }
    }
}

impl From<ReceiverState> for PushResponse {
    fn from(receiver_state: ReceiverState) -> PushResponse {
        let ReceiverState {
            missing_subgraph_roots,
            have_cids_bloom,
        } = receiver_state;

        let bloom = ReceiverState::bloom_serialize(have_cids_bloom);

        PushResponse {
            subgraph_roots: missing_subgraph_roots,
            bloom,
        }
    }
}

impl From<ReceiverState> for PullRequest {
    fn from(receiver_state: ReceiverState) -> PullRequest {
        let ReceiverState {
            missing_subgraph_roots,
            have_cids_bloom,
        } = receiver_state;

        let bloom = ReceiverState::bloom_serialize(have_cids_bloom);

        PullRequest {
            resources: missing_subgraph_roots,
            bloom,
        }
    }
}

impl ReceiverState {
    fn bloom_serialize(bloom: Option<BloomFilter>) -> Bloom {
        match bloom {
            Some(bloom) => Bloom {
                hash_count: bloom.hash_count() as u32,
                bytes: bloom.as_bytes().to_vec(),
            },
            None => Bloom {
                hash_count: 3,
                bytes: Vec::new(),
            },
        }
    }

    fn bloom_deserialize(bloom: Bloom) -> Option<BloomFilter> {
        if bloom.bytes.is_empty() {
            None
        } else {
            Some(BloomFilter::new_with(
                bloom.hash_count as usize,
                bloom.bytes.into_boxed_slice(),
            ))
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            receive_maximum: 2_000_000, // 2 MB
            max_roots_per_round: 1000,  // max. ~41KB of CIDs
            bloom_fpr: |num_of_elems| f64::min(0.001, 0.1 / num_of_elems as f64),
        }
    }
}

impl std::fmt::Debug for ReceiverState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let have_cids_bloom = self
            .have_cids_bloom
            .as_ref()
            .map_or("None".into(), |bloom| {
                format!(
                    "Some(BloomFilter(k_hashes = {}, {} bytes))",
                    bloom.hash_count(),
                    bloom.as_bytes().len()
                )
            });
        f.debug_struct("ReceiverState")
            .field(
                "missing_subgraph_roots.len() == ",
                &self.missing_subgraph_roots.len(),
            )
            .field("have_cids_bloom", &have_cids_bloom)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{test_utils::assert_cond_send_sync, traits::NoCache};
    use testresult::TestResult;
    use wnfs_common::MemoryBlockStore;

    #[allow(clippy::unreachable, unused)]
    fn test_assert_send() {
        assert_cond_send_sync(|| {
            block_send(
                unimplemented!(),
                unimplemented!(),
                unimplemented!(),
                unimplemented!() as &MemoryBlockStore,
                &NoCache,
            )
        });
        assert_cond_send_sync(|| {
            block_receive(
                unimplemented!(),
                unimplemented!(),
                unimplemented!(),
                unimplemented!() as &MemoryBlockStore,
                &NoCache,
            )
        })
    }

    #[test]
    fn test_receiver_state_is_not_a_huge_debug() -> TestResult {
        let state = ReceiverState {
            have_cids_bloom: Some(BloomFilter::new_from_size(4096, 1000)),
            missing_subgraph_roots: vec![Cid::default(); 1000],
        };

        let debug_print = format!("{state:#?}");

        assert!(debug_print.len() < 1000);

        Ok(())
    }
}
