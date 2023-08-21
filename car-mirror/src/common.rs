use anyhow::{anyhow, bail, Result};
use bytes::Bytes;
use deterministic_bloom::runtime_size::BloomFilter;
use futures::TryStreamExt;
use iroh_car::{CarHeader, CarReader, CarWriter};
use libipld::{Ipld, IpldCodec};
use libipld_core::{cid::Cid, codec::References};
use std::io::Cursor;
use wnfs_common::BlockStore;

use crate::{
    dag_walk::DagWalk,
    incremental_verification::IncrementalDagVerification,
    messages::{PullRequest, PushResponse},
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
pub async fn block_send(
    root: Cid,
    last_state: Option<ReceiverState>,
    config: &Config,
    store: &impl BlockStore,
) -> Result<CarFile> {
    let ReceiverState {
        ref missing_subgraph_roots,
        have_cids_bloom,
    } = last_state.unwrap_or(ReceiverState {
        missing_subgraph_roots: vec![root],
        have_cids_bloom: None,
    });

    // Verify that all missing subgraph roots are in the relevant DAG:
    let subgraph_roots: Vec<Cid> = DagWalk::breadth_first([root])
        .stream(store)
        .try_filter_map(|(cid, _)| async move {
            Ok(missing_subgraph_roots.contains(&cid).then_some(cid))
        })
        .try_collect()
        .await?;

    let bloom = have_cids_bloom.unwrap_or(BloomFilter::new_with(1, Box::new([0]))); // An empty bloom that contains nothing

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

    writer.write_header().await?;

    let mut block_bytes = 0;
    let mut dag_walk = DagWalk::breadth_first(subgraph_roots.clone());
    while let Some((cid, block)) = dag_walk.next(store).await? {
        if bloom.contains(&cid.to_bytes()) && !subgraph_roots.contains(&cid) {
            break;
        }

        writer.write(cid, &block).await?;

        // TODO(matheus23): Count the actual bytes sent?
        // At the moment, this is a rough estimate. iroh-car could be improved to return the written bytes.
        block_bytes += block.len();
        if block_bytes > config.send_minimum {
            break;
        }
    }

    Ok(CarFile {
        bytes: writer.finish().await?.into(),
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
pub async fn block_receive(
    root: Cid,
    last_car: Option<CarFile>,
    config: &Config,
    store: &impl BlockStore,
) -> Result<ReceiverState> {
    let mut dag_verification = IncrementalDagVerification::new([root], store).await?;

    if let Some(car) = last_car {
        let mut reader = CarReader::new(Cursor::new(car.bytes)).await?;
        let mut block_bytes = 0;

        while let Some((cid, vec)) = reader.next_block().await? {
            let block = Bytes::from(vec);

            block_bytes += block.len();
            if block_bytes > config.receive_maximum {
                bail!(
                    "Received more than {} bytes ({block_bytes}), aborting request.",
                    config.receive_maximum
                );
            }

            dag_verification
                .verify_and_store_block((cid, block), store)
                .await?;
        }
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

    let mut bloom =
        BloomFilter::new_from_fpr_po2(bloom_capacity, (config.bloom_fpr)(bloom_capacity));

    dag_verification
        .have_cids
        .iter()
        .for_each(|cid| bloom.insert(&cid.to_bytes()));

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
pub fn references(cid: Cid, block: impl AsRef<[u8]>) -> Result<Vec<Cid>> {
    let codec: IpldCodec = cid
        .codec()
        .try_into()
        .map_err(|_| anyhow!("Unsupported codec in Cid: {cid}"))?;

    let mut refs = Vec::new();
    <Ipld as References<IpldCodec>>::references(codec, &mut Cursor::new(block), &mut refs)?;
    Ok(refs)
}

//--------------------------------------------------------------------------------------------------
// Implementations
//--------------------------------------------------------------------------------------------------

impl From<PushResponse> for ReceiverState {
    fn from(push: PushResponse) -> Self {
        let PushResponse {
            subgraph_roots,
            bloom_k,
            bloom,
        } = push;

        Self {
            missing_subgraph_roots: subgraph_roots,
            have_cids_bloom: Self::bloom_deserialize(bloom_k, bloom),
        }
    }
}

impl From<PullRequest> for ReceiverState {
    fn from(pull: PullRequest) -> Self {
        let PullRequest {
            resources,
            bloom_k,
            bloom,
        } = pull;

        Self {
            missing_subgraph_roots: resources,
            have_cids_bloom: Self::bloom_deserialize(bloom_k, bloom),
        }
    }
}

impl From<ReceiverState> for PushResponse {
    fn from(receiver_state: ReceiverState) -> PushResponse {
        let ReceiverState {
            missing_subgraph_roots,
            have_cids_bloom,
        } = receiver_state;

        let (bloom_k, bloom) = ReceiverState::bloom_serialize(have_cids_bloom);

        PushResponse {
            subgraph_roots: missing_subgraph_roots,
            bloom_k,
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

        let (bloom_k, bloom) = ReceiverState::bloom_serialize(have_cids_bloom);

        PullRequest {
            resources: missing_subgraph_roots,
            bloom_k,
            bloom,
        }
    }
}

impl ReceiverState {
    fn bloom_serialize(bloom: Option<BloomFilter>) -> (u32, Vec<u8>) {
        match bloom {
            Some(bloom) => (bloom.hash_count() as u32, bloom.as_bytes().to_vec()),
            None => (3, Vec::new()),
        }
    }

    fn bloom_deserialize(bloom_k: u32, bloom: Vec<u8>) -> Option<BloomFilter> {
        if bloom.is_empty() {
            None
        } else {
            Some(BloomFilter::new_with(
                bloom_k as usize,
                bloom.into_boxed_slice(),
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
            bloom_fpr: |num_of_elems| 0.1 / num_of_elems as f64,
        }
    }
}
