use libipld_core::cid::Cid;
use serde::{Deserialize, Serialize};

/// Initial message for pull requests.
///
/// Over-the-wire data type from the [specification].
///
/// [specification]: https://github.com/fission-codes/spec/blob/86fcfb07d507f1df4fdaaf49088abecbb1dda76a/car-pool/car-mirror/http.md#12-requestor-payload
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PullRequest {
    /// Requested CID roots
    #[serde(rename = "rs")]
    pub resources: Vec<Cid>,

    /// A bloom containing already stored blocks
    #[serde(flatten)]
    pub bloom: Bloom,
}

/// The response sent after the initial and subsequent push requests.
///
/// Wire data type from the [specification].
///
/// [specification]: https://github.com/fission-codes/spec/blob/86fcfb07d507f1df4fdaaf49088abecbb1dda76a/car-pool/car-mirror/http.md#23-provider-payload
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PushResponse {
    /// Incomplete subgraph roots
    #[serde(rename = "sr")]
    pub subgraph_roots: Vec<Cid>,

    /// A bloom containing already stored blocks
    #[serde(flatten)]
    pub bloom: Bloom,
}

/// The serialization format for bloom filters in CAR mirror
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bloom {
    /// Bloom filter hash count
    #[serde(rename = "bk")]
    pub hash_count: u32,

    /// Bloom filter Binary
    #[serde(rename = "bb")]
    pub bytes: Vec<u8>,
}

impl PushResponse {
    /// Whether this response indicates that the protocol is finished.
    pub fn indicates_finished(&self) -> bool {
        self.subgraph_roots.is_empty()
    }
}

impl PullRequest {
    /// Whether you need to actually send the request or not. If true, this indicates that the protocol is finished.
    pub fn indicates_finished(&self) -> bool {
        self.resources.is_empty()
    }
}
