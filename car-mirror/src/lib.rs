#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, missing_docs, rust_2018_idioms)]
#![deny(unreachable_pub, private_in_public)]

//! car-mirror

use anyhow::{anyhow, bail, Result};
use async_stream::try_stream;
use bytes::Bytes;
use futures::{stream::LocalBoxStream, Stream, StreamExt, TryStreamExt};
use iroh_car::{CarHeader, CarReader, CarWriter};
use libipld::{Ipld, IpldCodec};
use libipld_core::{
    cid::Cid,
    codec::References,
    multihash::{Code, MultihashDigest},
};
use messages::PushResponse;
use std::{
    collections::{HashSet, VecDeque},
    io::Cursor,
};
use wnfs_common::BlockStore;

/// Test utilities.
#[cfg(any(test, feature = "test_utils"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test_utils")))]
pub mod test_utils;

/// Contains the data types that are sent over-the-wire and relevant serialization code.
pub mod messages;

pub struct PushSenderSession<'a, B: BlockStore> {
    last_response: PushResponse,
    send_limit: usize,
    store: &'a B,
}

impl<'a, B: BlockStore> PushSenderSession<'a, B> {
    pub fn new(root: Cid, store: &'a B) -> Self {
        Self {
            last_response: PushResponse {
                subgraph_roots: vec![root],
                // Just putting an empty bloom here initially
                bloom_k: 3,
                bloom: Vec::new(),
            },
            send_limit: 256 * 1024, // 256KiB
            store,
        }
    }

    pub fn handle_response(&mut self, response: PushResponse) -> bool {
        self.last_response = response;
        self.last_response.subgraph_roots.is_empty()
    }

    pub async fn next_request(&mut self) -> Result<Bytes> {
        let mut writer = CarWriter::new(
            CarHeader::new_v1(
                // TODO(matheus23): This is stupid
                // CAR files *must* have at least one CID in them, and all of them
                // need to appear as a block in the payload.
                // It would probably make most sense to just write all subgraph roots into this,
                // but we don't know how many of the subgraph roots fit into this round yet,
                // so we're simply writing the first one in here, since we know
                // at least one block will be written (and it'll be that one).
                self.last_response
                    .subgraph_roots
                    .iter()
                    .take(1)
                    .cloned()
                    .collect(),
            ),
            Vec::new(),
        );
        writer.write_header().await?;

        let mut block_bytes = 0;
        let mut stream =
            walk_dag_in_order_breadth_first(self.last_response.subgraph_roots.clone(), self.store);
        while let Some((cid, block)) = stream.try_next().await? {
            // TODO Eventually we'll need to turn the `LocalBoxStream` into a more configurable
            // "external iterator", and then this will be the point where we prune parts of the DAG
            // that the recipient already has.

            // TODO(matheus23): Count the actual bytes sent?
            block_bytes += block.len();
            if block_bytes > self.send_limit {
                break;
            }

            writer.write(cid, &block).await?;
        }

        Ok(writer.finish().await?.into())
    }
}

pub struct PushReceiverSession<'a, B: BlockStore> {
    accepted_roots: Vec<Cid>,
    receive_limit: usize,
    store: &'a B,
}

impl<'a, B: BlockStore> PushReceiverSession<'a, B> {
    pub fn new(root: Cid, store: &'a B) -> Self {
        Self {
            accepted_roots: vec![root],
            receive_limit: 256 * 1024, // 256KiB
            store,
        }
    }

    pub async fn handle_request(&mut self, request: Bytes) -> Result<PushResponse> {
        let mut reader = CarReader::new(Cursor::new(request)).await?;
        let mut stream = read_in_order_dag_from_car(self.accepted_roots.clone(), &mut reader);

        let mut missing_subgraphs: HashSet<_> = self.accepted_roots.iter().cloned().collect();

        let mut block_bytes = 0;
        while let Some((cid, block)) = stream.try_next().await? {
            block_bytes += block.len();
            if block_bytes > self.receive_limit {
                bail!(
                    "Received more than {} bytes ({block_bytes}), aborting request.",
                    self.receive_limit
                );
            }

            let codec: IpldCodec = cid
                .codec()
                .try_into()
                .map_err(|_| anyhow!("Unsupported codec in Cid: {cid}"))?;

            missing_subgraphs.remove(&cid);
            missing_subgraphs.extend(references(codec, &block)?);

            self.store.put_block(block, cid.codec()).await?;
        }

        let subgraph_roots: Vec<_> = missing_subgraphs.into_iter().collect();

        self.accepted_roots = subgraph_roots.clone();

        Ok(PushResponse {
            subgraph_roots,
            // We ignore blooms for now
            bloom_k: 3,
            bloom: Vec::new(),
        })
    }
}

/// walks a DAG from given root breadth-first along IPLD links
pub fn walk_dag_in_order_breadth_first(
    roots: impl IntoIterator<Item = Cid>,
    store: &impl BlockStore,
) -> LocalBoxStream<'_, Result<(Cid, Bytes)>> {
    let mut frontier: VecDeque<_> = roots.into_iter().collect();

    Box::pin(try_stream! {
        let mut visited = HashSet::new();
        while let Some(cid) = frontier.pop_front() {
            if visited.contains(&cid) {
                continue;
            }
            visited.insert(cid);
            let block = store.get_block(&cid).await?;
            let codec = IpldCodec::try_from(cid.codec())?;
            frontier.extend(references(codec, &block)?);
            yield (cid, block);
        }
    })
}

/// Writes a stream of blocks into a car file
pub async fn stream_into_car<W: tokio::io::AsyncWrite + Send + Unpin>(
    mut blocks: impl Stream<Item = Result<(Cid, Bytes)>> + Unpin,
    writer: &mut CarWriter<W>,
) -> Result<()> {
    while let Some(result) = blocks.next().await {
        let (cid, bytes) = result?;
        writer.write(cid, bytes).await?;
    }
    Ok(())
}

/// Read a directed acyclic graph from a CAR file, making sure it's read in-order and
/// only blocks reachable from the root are included.
pub fn read_in_order_dag_from_car<R: tokio::io::AsyncRead + Unpin>(
    roots: impl IntoIterator<Item = Cid>,
    reader: &mut CarReader<R>,
) -> LocalBoxStream<'_, Result<(Cid, Bytes)>> {
    let mut reachable_from_root: HashSet<_> = roots.into_iter().collect();
    Box::pin(try_stream! {
        while let Some((cid, vec)) = reader.next_block().await.map_err(|e| anyhow!(e))? {
            let block = Bytes::from(vec);

            let code: Code = cid
                .hash()
                .code()
                .try_into()
                .map_err(|_| anyhow!("Unsupported hash code in Cid: {cid}"))?;

            let codec: IpldCodec = cid
                .codec()
                .try_into()
                .map_err(|_| anyhow!("Unsupported codec in Cid: {cid}"))?;

            let digest = code.digest(&block);

            if cid.hash() != &digest {
                Err(anyhow!(
                    "Digest mismatch in CAR file: expected {:?}, got {:?}",
                    digest,
                    cid.hash()
                ))?;
            }

            if !reachable_from_root.contains(&cid) {
                Err(anyhow!("Unexpected block or block out of order: {cid}"))?;
            }

            reachable_from_root.extend(references(codec, &block)?);

            yield (cid, block);
        }
    })
}

fn references(codec: IpldCodec, block: impl AsRef<[u8]>) -> Result<Vec<Cid>> {
    let mut refs = Vec::new();
    <Ipld as References<IpldCodec>>::references(codec, &mut Cursor::new(block), &mut refs)?;
    Ok(refs)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::test_utils::{encode, generate_dag, Rvg};

    use super::*;
    use async_std::fs::File;
    use futures::{future, TryStreamExt};
    use iroh_car::CarHeader;
    use libipld_core::multihash::{Code, MultihashDigest};
    use tokio_util::compat::{FuturesAsyncReadCompatExt, FuturesAsyncWriteCompatExt};
    use wnfs_common::MemoryBlockStore;

    #[async_std::test]
    async fn test_write_into_car() -> Result<()> {
        let (blocks, root) = Rvg::new().sample(&generate_dag(256, |cids| {
            let ipld = Ipld::List(cids.into_iter().map(Ipld::Link).collect());
            let bytes = encode(&ipld);
            let cid = Cid::new_v1(IpldCodec::DagCbor.into(), Code::Blake3_256.digest(&bytes));
            (cid, bytes)
        }));

        let store = &MemoryBlockStore::new();
        for (cid, bytes) in blocks.iter() {
            let cid_store = store
                .put_block(bytes.clone(), IpldCodec::DagCbor.into())
                .await?;
            assert_eq!(*cid, cid_store);
        }

        let filename = "./my-car.car";

        let file = File::create(filename).await?;
        let mut writer = CarWriter::new(CarHeader::new_v1(vec![root]), file.compat_write());
        writer.write_header().await?;
        let block_stream = walk_dag_in_order_breadth_first([root], store);
        stream_into_car(block_stream, &mut writer).await?;
        writer.finish().await?;

        let mut reader = CarReader::new(File::open(filename).await?.compat()).await?;

        read_in_order_dag_from_car([root], &mut reader)
            .try_for_each(|(cid, _)| {
                println!("Got {cid}");
                future::ready(Ok(()))
            })
            .await?;

        Ok(())
    }

    #[async_std::test]
    async fn test_transfer() -> Result<()> {
        let (blocks, root) = Rvg::new().sample(&generate_dag(256, |cids| {
            let ipld = Ipld::Map(BTreeMap::from([
                ("data".into(), Ipld::Bytes(vec![0u8; 10 * 1024])),
                (
                    "links".into(),
                    Ipld::List(cids.into_iter().map(Ipld::Link).collect()),
                ),
            ]));
            let bytes = encode(&ipld);
            let cid = Cid::new_v1(IpldCodec::DagCbor.into(), Code::Blake3_256.digest(&bytes));
            (cid, bytes)
        }));

        let sender_store = &MemoryBlockStore::new();
        for (cid, bytes) in blocks.iter() {
            let cid_store = sender_store
                .put_block(bytes.clone(), IpldCodec::DagCbor.into())
                .await?;
            assert_eq!(*cid, cid_store);
        }

        let receiver_store = &MemoryBlockStore::new();

        let mut sender = PushSenderSession::new(root, sender_store);
        let mut receiver = PushReceiverSession::new(root, receiver_store);

        loop {
            let request = sender.next_request().await?;
            println!("Sending request {} bytes", request.len());
            let response = receiver.handle_request(request).await?;
            if sender.handle_response(response) {
                // Should be done
                break;
            }
        }

        // receiver should have all data
        let sender_cids = walk_dag_in_order_breadth_first([root], sender_store)
            .map_ok(|(cid, _)| cid)
            .try_collect::<Vec<_>>()
            .await?;
        let receiver_cids = walk_dag_in_order_breadth_first([root], receiver_store)
            .map_ok(|(cid, _)| cid)
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(sender_cids, receiver_cids);

        Ok(())
    }

    #[async_std::test]
    async fn test_walk_dag_breadth_first() -> Result<()> {
        let store = &MemoryBlockStore::new();

        let cid_1 = store.put_serializable(&Ipld::String("1".into())).await?;
        let cid_2 = store.put_serializable(&Ipld::String("2".into())).await?;
        let cid_3 = store.put_serializable(&Ipld::String("3".into())).await?;

        let cid_1_wrap = store
            .put_serializable(&Ipld::List(vec![Ipld::Link(cid_1)]))
            .await?;

        let cid_root = store
            .put_serializable(&Ipld::List(vec![
                Ipld::Link(cid_1_wrap),
                Ipld::Link(cid_2),
                Ipld::Link(cid_3),
            ]))
            .await?;

        let cids = walk_dag_in_order_breadth_first([cid_root], store)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(|(cid, _block)| cid)
            .collect::<Vec<_>>();

        assert_eq!(cids, vec![cid_root, cid_1_wrap, cid_2, cid_3, cid_1]);

        Ok(())
    }
}

#[cfg(test)]
mod proptests {
    use crate::{
        test_utils::{encode, generate_dag},
        walk_dag_in_order_breadth_first,
    };
    use futures::TryStreamExt;
    use libipld::{
        multihash::{Code, MultihashDigest},
        Cid, Ipld, IpldCodec,
    };
    use proptest::strategy::Strategy;
    use std::collections::BTreeSet;
    use test_strategy::proptest;
    use wnfs_common::{BlockStore, MemoryBlockStore};

    fn ipld_dags() -> impl Strategy<Value = (Vec<(Cid, Ipld)>, Cid)> {
        generate_dag(256, |cids| {
            let ipld = Ipld::List(cids.into_iter().map(Ipld::Link).collect());
            let cid = Cid::new_v1(
                IpldCodec::DagCbor.into(),
                Code::Blake3_256.digest(&encode(&ipld)),
            );
            (cid, ipld)
        })
    }

    #[proptest(max_shrink_iters = 100_000)]
    fn walk_dag_never_iterates_block_twice(#[strategy(ipld_dags())] dag: (Vec<(Cid, Ipld)>, Cid)) {
        async_std::task::block_on(async {
            let (dag, root) = dag;
            let store = &MemoryBlockStore::new();
            for (cid, ipld) in dag.iter() {
                let cid_store = store
                    .put_block(encode(ipld), IpldCodec::DagCbor.into())
                    .await
                    .unwrap();
                assert_eq!(*cid, cid_store);
            }

            let mut cids = walk_dag_in_order_breadth_first([root], store)
                .map_ok(|(cid, _)| cid)
                .try_collect::<Vec<_>>()
                .await
                .unwrap();

            cids.sort();

            let unique_cids = cids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();

            assert_eq!(cids, unique_cids);
        });
    }
}
