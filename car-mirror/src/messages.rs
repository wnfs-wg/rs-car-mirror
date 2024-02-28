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
    #[serde(rename = "rs", with = "crate::serde_cid_vec")]
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
    #[serde(rename = "sr", with = "crate::serde_cid_vec")]
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
    #[serde(with = "crate::serde_bloom_bytes")]
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

#[cfg(test)]
mod test {
    use crate::{
        cache::NoCache,
        common::Config,
        incremental_verification::IncrementalDagVerification,
        messages::{PullRequest, PushResponse},
    };
    use testresult::TestResult;
    use wnfs_common::MemoryBlockStore;
    use wnfs_unixfs_file::builder::FileBuilder;

    #[test_log::test(async_std::test)]
    async fn test_encoding_format() -> TestResult {
        let store = &MemoryBlockStore::new();
        let store2 = &MemoryBlockStore::new();

        let previous_cid = FileBuilder::new()
            .content_bytes(vec![42; 500_000])
            .build()?
            .store(store)
            .await?;

        let root_cid = FileBuilder::new()
            .content_bytes(vec![42; 1_000_000])
            .build()?
            .store(store2)
            .await?;

        let mut dag = IncrementalDagVerification::new([previous_cid], store, &NoCache).await?;
        dag.want_cids.insert(root_cid);
        dag.update_have_cids(store, &NoCache).await?;

        let receiver_state = dag.into_receiver_state(Config::default().bloom_fpr);

        let pull_request: PullRequest = receiver_state.clone().into();
        let push_response: PushResponse = receiver_state.into();

        println!("{}", serde_json::to_string_pretty(&pull_request)?);
        println!("{}", serde_json::to_string_pretty(&push_response)?);

        Ok(())
    }
}
