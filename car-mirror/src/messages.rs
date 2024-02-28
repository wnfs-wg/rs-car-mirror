use std::{collections::TryReserveError, convert::Infallible};

use libipld_core::cid::Cid;
use serde::{Deserialize, Serialize};
use serde_ipld_dagcbor::{DecodeError, EncodeError};

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

    /// Bloom filter hash count
    #[serde(rename = "bk")]
    pub bloom_hash_count: u32,

    /// Bloom filter Binary
    #[serde(rename = "bb")]
    #[serde(with = "crate::serde_bloom_bytes")]
    pub bloom_bytes: Vec<u8>,
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

    /// Bloom filter hash count
    #[serde(rename = "bk")]
    pub bloom_hash_count: u32,

    /// Bloom filter Binary
    #[serde(rename = "bb")]
    #[serde(with = "crate::serde_bloom_bytes")]
    pub bloom_bytes: Vec<u8>,
}

impl PushResponse {
    /// Whether this response indicates that the protocol is finished.
    pub fn indicates_finished(&self) -> bool {
        self.subgraph_roots.is_empty()
    }

    /// Deserialize a push response from dag-cbor bytes
    pub fn from_dag_cbor(slice: impl AsRef<[u8]>) -> Result<Self, DecodeError<Infallible>> {
        serde_ipld_dagcbor::from_slice(slice.as_ref())
    }

    /// Serialize a push response into dag-cbor bytes
    pub fn to_dag_cbor(&self) -> Result<Vec<u8>, EncodeError<TryReserveError>> {
        serde_ipld_dagcbor::to_vec(self)
    }
}

impl PullRequest {
    /// Whether you need to actually send the request or not. If true, this indicates that the protocol is finished.
    pub fn indicates_finished(&self) -> bool {
        self.resources.is_empty()
    }

    /// Deserialize a pull request from dag-cbor bytes
    pub fn from_dag_cbor(slice: impl AsRef<[u8]>) -> Result<Self, DecodeError<Infallible>> {
        serde_ipld_dagcbor::from_slice(slice.as_ref())
    }

    /// Serialize a pull request into dag-cbor bytes
    pub fn to_dag_cbor(&self) -> Result<Vec<u8>, EncodeError<TryReserveError>> {
        serde_ipld_dagcbor::to_vec(self)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        cache::NoCache,
        common::{Config, ReceiverState},
        incremental_verification::IncrementalDagVerification,
        messages::{PullRequest, PushResponse},
    };
    use anyhow::Result;
    use testresult::TestResult;
    use wnfs_common::MemoryBlockStore;
    use wnfs_unixfs_file::builder::FileBuilder;

    async fn example_receiver_state() -> Result<ReceiverState> {
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

        Ok(dag.into_receiver_state(Config::default().bloom_fpr))
    }

    #[test_log::test(async_std::test)]
    async fn test_encoding_format_json_concise() -> TestResult {
        let receiver_state = example_receiver_state().await?;
        let pull_request: PullRequest = receiver_state.clone().into();
        let push_response: PushResponse = receiver_state.into();

        // In this example, if the bloom weren't encoded as base64, it'd blow past the 150 byte limit.
        // At the time of writing, these both encode into 97 characters.
        assert!(serde_json::to_string(&pull_request)?.len() < 150);
        assert!(serde_json::to_string(&push_response)?.len() < 150);

        Ok(())
    }

    #[test_log::test(async_std::test)]
    async fn test_dag_cbor_roundtrip() -> TestResult {
        let receiver_state = example_receiver_state().await?;
        let pull_request: PullRequest = receiver_state.clone().into();
        let push_response: PushResponse = receiver_state.into();

        let pull_back = PullRequest::from_dag_cbor(pull_request.to_dag_cbor()?)?;
        let push_back = PushResponse::from_dag_cbor(push_response.to_dag_cbor()?)?;

        assert_eq!(pull_request, pull_back);
        assert_eq!(push_response, push_back);

        Ok(())
    }
}
