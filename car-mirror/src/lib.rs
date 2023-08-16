#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_debug_implementations, missing_docs, rust_2018_idioms)]
#![deny(unreachable_pub, private_in_public)]

//! car-mirror

use anyhow::Result;
use async_stream::try_stream;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use iroh_car::CarWriter;
use libipld::{Ipld, IpldCodec};
use libipld_core::{cid::Cid, codec::References};
use std::{
    collections::{HashSet, VecDeque},
    io::Cursor,
};
use tokio::io::AsyncWrite;
use wnfs_common::BlockStore;

/// Test utilities.
#[cfg(any(test, feature = "test_utils"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test_utils")))]
pub mod test_utils;

/// walks a DAG from given root breadth-first along IPLD links
pub fn walk_dag_in_order_breadth_first<'a>(
    root: Cid,
    store: &'a impl BlockStore,
) -> impl Stream<Item = Result<(Cid, Bytes)>> + Unpin + 'a {
    Box::pin(try_stream! {
        let mut visited = HashSet::new();
        let mut frontier = VecDeque::from([root]);
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
pub async fn stream_into_car<W: AsyncWrite + Send + Unpin>(
    mut blocks: impl Stream<Item = Result<(Cid, Bytes)>> + Unpin,
    writer: &mut CarWriter<W>,
) -> Result<()> {
    while let Some(result) = blocks.next().await {
        let (cid, bytes) = result?;
        writer.write(cid, bytes).await?;
    }
    Ok(())
}

fn references(codec: IpldCodec, block: impl AsRef<[u8]>) -> Result<Vec<Cid>> {
    let mut refs = Vec::new();
    <Ipld as References<IpldCodec>>::references(codec, &mut Cursor::new(block), &mut refs)?;
    Ok(refs)
}

#[cfg(test)]
mod tests {
    use crate::test_utils::{encode, generate_dag, Rvg};

    use super::*;
    use async_std::fs::File;
    use futures::TryStreamExt;
    use iroh_car::CarHeader;
    use libipld_core::multihash::{Code, MultihashDigest};
    use tokio_util::compat::FuturesAsyncWriteCompatExt;
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

        let file = File::create("./my-car3.car").await?;
        let mut writer = CarWriter::new(CarHeader::new_v1(vec![root]), file.compat_write());
        writer.write_header().await?;
        let block_stream = walk_dag_in_order_breadth_first(root, store);
        stream_into_car(block_stream, &mut writer).await?;
        let file = writer.finish().await?;

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

        let cids = walk_dag_in_order_breadth_first(cid_root, store)
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

            let mut cids = walk_dag_in_order_breadth_first(root, store)
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
