use std::{collections::HashSet, fmt::Debug};

use bytes::Bytes;
use libipld::{Cid, Ipld, IpldCodec};
use libipld_core::codec::Encode;
use proptest::strategy::Strategy;
use roaring_graphs::{arb_dag, DirectedAcyclicGraph, Vertex};

/// Encode some IPLD as dag-cbor
pub fn encode(ipld: &Ipld) -> Bytes {
    let mut vec = Vec::new();
    ipld.encode(IpldCodec::DagCbor, &mut vec).unwrap(); // TODO(matheus23) unwrap
    Bytes::from(vec)
}

/// A strategy for use with proptest to generate random DAGs (directed acyclic graphs).
/// The strategy generates a list of blocks of type T and their CIDs, as well as
/// the root block's CID.
pub fn generate_dag<T: Debug + Clone>(
    max_nodes: u16,
    generate_block: fn(Vec<Cid>) -> (Cid, T),
) -> impl Strategy<Value = (Vec<(Cid, T)>, Cid)> {
    arb_dag(1..max_nodes, 0.5).prop_map(move |dag| dag_to_nodes(&dag, generate_block))
}

/// Turn a directed acyclic graph into a list of nodes (with their CID) and a root CID.
/// This will select only the DAG that's reachable from the root.
pub fn dag_to_nodes<T>(
    dag: &DirectedAcyclicGraph,
    generate_node: fn(Vec<Cid>) -> (Cid, T),
) -> (Vec<(Cid, T)>, Cid) {
    let mut blocks = Vec::new();
    let mut visited = HashSet::new();
    let (cid, block) = dag_to_nodes_helper(dag, 0, generate_node, &mut blocks, &mut visited);
    blocks.push((cid, block));
    (blocks, cid)
}

fn dag_to_nodes_helper<T>(
    dag: &DirectedAcyclicGraph,
    root: Vertex,
    generate_node: fn(Vec<Cid>) -> (Cid, T),
    arr: &mut Vec<(Cid, T)>,
    visited: &mut HashSet<Vertex>,
) -> (Cid, T) {
    let mut child_blocks = Vec::new();
    for child in dag.iter_children(root) {
        if visited.contains(&child) {
            continue;
        }
        visited.insert(child);
        child_blocks.push(dag_to_nodes_helper(dag, child, generate_node, arr, visited));
    }
    let result = generate_node(child_blocks.iter().map(|(cid, _)| *cid).collect());
    arr.extend(child_blocks);
    result
}
