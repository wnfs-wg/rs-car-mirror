#![allow(unknown_lints)] // Because the `instrument` macro contains some `#[allow]`s that rust 1.66 doesn't know yet.

use anyhow::anyhow;
use bytes::Bytes;
use deterministic_bloom::runtime_size::BloomFilter;
use futures::TryStreamExt;
use iroh_car::{CarHeader, CarReader, CarWriter};
use libipld::{Ipld, IpldCodec};
use libipld_core::{cid::Cid, codec::References};
use std::io::Cursor;
use tracing::{debug, instrument, trace, warn};
use wnfs_common::BlockStore;

use crate::{
    dag_walk::DagWalk,
    error::Error,
    incremental_verification::{BlockState, IncrementalDagVerification},
    messages::{Bloom, PullRequest, PushResponse},
};

//--------------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------------

/// Configuration values (such as byte limits) for the CAR mirror protocol
#[derive(Clone, Debug)]
pub struct Config {
    /// A client will try to send at least `send_minimum` bytes of block data
    /// in each request, except if close to the end of the protocol (when there's)
    /// not that much data left.
    pub send_minimum: usize,
    /// The maximum number of bytes per request that the server accepts.
    pub receive_maximum: usize,
    /// The maximum number of roots per request that the server will send to the client,
    /// and that the client will consume.
    pub max_roots_per_round: usize,
    /// The target false positive rate for the bloom filter that the server sends.
    pub bloom_fpr: fn(u64) -> f64,
}

/// Some information that the block receiving end provides the block sending end
/// in order to deduplicate block transfers.
#[derive(Debug, Clone)]
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
#[instrument(skip(config, store))]
pub async fn block_send(
    root: Cid,
    last_state: Option<ReceiverState>,
    config: &Config,
    store: &impl BlockStore,
) -> Result<CarFile, Error> {
    let ReceiverState {
        ref missing_subgraph_roots,
        have_cids_bloom,
    } = last_state.unwrap_or(ReceiverState {
        missing_subgraph_roots: vec![root],
        have_cids_bloom: None,
    });

    // Verify that all missing subgraph roots are in the relevant DAG:
    let subgraph_roots = verify_missing_subgraph_roots(root, missing_subgraph_roots, store).await?;

    let bloom = handle_missing_bloom(have_cids_bloom);

    let mut writer = CarWriter::new(
        CarHeader::new_v1(
            // https://github.com/wnfs-wg/car-mirror-spec/issues/6
            // CAR files *must* have at least one CID in them, and all of them
            // need to appear as a block in the payload.
            // It would probably make most sense to just write all subgraph roots into this,
            // but we don't know how many of the subgraph roots fit into this round yet,
            // so we're simply writing the first one in here, since we know
            // at least one block will be written (and it'll be that one).
            subgraph_roots.iter().take(1).cloned().collect(),
        ),
        Vec::new(),
    );

    writer
        .write_header()
        .await
        .map_err(|e| Error::CarFileError(anyhow!(e)))?;

    write_blocks_into_car(
        &mut writer,
        subgraph_roots,
        &bloom,
        config.send_minimum,
        store,
    )
    .await?;

    Ok(CarFile {
        bytes: writer
            .finish()
            .await
            .map_err(|e| Error::CarFileError(anyhow!(e)))?
            .into(),
    })
}

/// This function is run on the block receiving end of the protocol.
///
/// It's used on the client during the pull protocol and on the server
/// during the push protocol.
///
/// It takes a `CarFile`, verifies that its contents are related to the
/// `root` and returns some information to help the block sending side
/// figure out what blocks to send next.
#[instrument(skip(last_car, config, store), fields(car_bytes = last_car.as_ref().map(|car| car.bytes.len())))]
pub async fn block_receive(
    root: Cid,
    last_car: Option<CarFile>,
    config: &Config,
    store: &impl BlockStore,
) -> Result<ReceiverState, Error> {
    let mut dag_verification = IncrementalDagVerification::new([root], store).await?;

    if let Some(car) = last_car {
        let mut reader = CarReader::new(Cursor::new(car.bytes))
            .await
            .map_err(|e| Error::CarFileError(anyhow!(e)))?;

        read_and_verify_blocks(
            &mut dag_verification,
            &mut reader,
            config.receive_maximum,
            store,
        )
        .await?;
    }

    let missing_subgraph_roots = dag_verification
        .want_cids
        .iter()
        .take(config.max_roots_per_round)
        .cloned()
        .collect();

    let bloom_capacity = dag_verification.have_cids.len() as u64;

    if bloom_capacity == 0 {
        return Ok(ReceiverState {
            missing_subgraph_roots,
            have_cids_bloom: None,
        });
    }

    if missing_subgraph_roots.is_empty() {
        // We're done. No need to compute a bloom.
        return Ok(ReceiverState {
            missing_subgraph_roots,
            have_cids_bloom: None,
        });
    }

    let mut bloom =
        BloomFilter::new_from_fpr_po2(bloom_capacity, (config.bloom_fpr)(bloom_capacity));

    dag_verification
        .have_cids
        .iter()
        .for_each(|cid| bloom.insert(&cid.to_bytes()));

    debug!(
        inserted_elements = bloom_capacity,
        size_bits = bloom.as_bytes().len() * 8,
        hash_count = bloom.hash_count(),
        ones_count = bloom.count_ones(),
        estimated_fpr = bloom.current_false_positive_rate(),
        "built 'have cids' bloom",
    );

    Ok(ReceiverState {
        missing_subgraph_roots,
        have_cids_bloom: Some(bloom),
    })
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
) -> Result<E, Error> {
    let codec: IpldCodec = cid
        .codec()
        .try_into()
        .map_err(|_| Error::UnsupportedCodec { cid })?;

    <Ipld as References<IpldCodec>>::references(codec, &mut Cursor::new(block), &mut refs)
        .map_err(Error::ParsingError)?;
    Ok(refs)
}

//--------------------------------------------------------------------------------------------------
// Private Functions
//--------------------------------------------------------------------------------------------------

async fn verify_missing_subgraph_roots(
    root: Cid,
    missing_subgraph_roots: &Vec<Cid>,
    store: &impl BlockStore,
) -> Result<Vec<Cid>, Error> {
    let subgraph_roots: Vec<Cid> = DagWalk::breadth_first([root])
        .stream(store)
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

async fn write_blocks_into_car<W: tokio::io::AsyncWrite + Unpin + Send>(
    writer: &mut CarWriter<W>,
    subgraph_roots: Vec<Cid>,
    bloom: &BloomFilter,
    send_minimum: usize,
    store: &impl BlockStore,
) -> Result<(), Error> {
    let mut block_bytes = 0;
    let mut dag_walk = DagWalk::breadth_first(subgraph_roots.clone());

    while let Some(cid) = dag_walk.next(store).await? {
        let block = store
            .get_block(&cid)
            .await
            .map_err(Error::BlockStoreError)?;

        if bloom.contains(&cid.to_bytes()) && !subgraph_roots.contains(&cid) {
            debug!(
                cid = %cid,
                bloom_contains = bloom.contains(&cid.to_bytes()),
                subgraph_roots_contains = subgraph_roots.contains(&cid),
                "skipped writing block"
            );
            continue;
        }

        debug!(
            cid = %cid,
            num_bytes = block.len(),
            frontier_size = dag_walk.frontier.len(),
            "writing block to CAR",
        );

        writer
            .write(cid, &block)
            .await
            .map_err(|e| Error::CarFileError(anyhow!(e)))?;

        // TODO(matheus23): Count the actual bytes sent?
        // At the moment, this is a rough estimate. iroh-car could be improved to return the written bytes.
        block_bytes += block.len();
        if block_bytes > send_minimum {
            break;
        }
    }

    Ok(())
}

async fn read_and_verify_blocks<R: tokio::io::AsyncRead + Unpin>(
    dag_verification: &mut IncrementalDagVerification,
    reader: &mut CarReader<R>,
    receive_maximum: usize,
    store: &impl BlockStore,
) -> Result<(), Error> {
    let mut block_bytes = 0;
    while let Some((cid, vec)) = reader
        .next_block()
        .await
        .map_err(|e| Error::CarFileError(anyhow!(e)))?
    {
        let block = Bytes::from(vec);

        debug!(
            cid = %cid,
            num_bytes = block.len(),
            "reading block from CAR",
        );

        block_bytes += block.len();
        if block_bytes > receive_maximum {
            return Err(Error::TooManyBytes {
                block_bytes,
                receive_maximum,
            });
        }

        match dag_verification.block_state(cid) {
            BlockState::Have => continue,
            BlockState::Unexpected => {
                trace!(
                    cid = %cid,
                    "received block out of order (possibly due to bloom false positive)"
                );
                break;
            }
            BlockState::Want => {
                dag_verification
                    .verify_and_store_block((cid, block), store)
                    .await?;
            }
        }
    }

    Ok(())
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
            send_minimum: 128 * 1024,    // 128KiB
            receive_maximum: 512 * 1024, // 512KiB
            max_roots_per_round: 1000,   // max. ~41KB of CIDs
            bloom_fpr: |num_of_elems| f64::min(0.001, 0.1 / num_of_elems as f64),
        }
    }
}
