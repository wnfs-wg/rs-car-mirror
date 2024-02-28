//! Axum extractor for DagCbor

use anyhow::Result;
use axum::{
    extract::{rejection::BytesRejection, FromRequest, Request},
    http::{
        header::{ToStrError, CONTENT_TYPE},
        HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};
use serde_ipld_dagcbor::DecodeError;
use std::{convert::Infallible, fmt::Debug};

/// TODO(matheus23): docs
#[derive(Debug, Clone)]
pub struct DagCbor<M>(pub M);

/// TODO(matheus23): docs
#[derive(Debug, thiserror::Error)]
pub enum DagCborRejection {
    /// TODO(matheus23): docs
    #[error("Missing Content-Type header on request, expected application/json or application/vnd.ipld.dag-cbor, but got nothing")]
    MissingContentType,

    /// TODO(matheus23): docs
    #[error("Incorrect mime type, expected application/vnd.ipld.dag-cbor, but got {0}")]
    UnexpectedContentType(mime::Mime),

    /// TODO(matheus23): docs
    #[error("Failed parsing Content-Type header as mime type, expected application/json or application/vnd.ipld.dag-cbor")]
    FailedToParseMime,

    /// TODO(matheus23): docs
    #[error("Unable to buffer the request body, perhaps it exceeded the 2MB limit")]
    FailedParsingRequestBytes,

    /// TODO(matheus23): docs
    #[error("Failed decoding dag-cbor: {0}")]
    FailedDecoding(#[from] DecodeError<Infallible>),
}

impl IntoResponse for DagCborRejection {
    fn into_response(self) -> Response {
        (
            match &self {
                Self::MissingContentType => StatusCode::BAD_REQUEST,
                Self::UnexpectedContentType(_) => StatusCode::BAD_REQUEST,
                Self::FailedToParseMime => StatusCode::BAD_REQUEST,
                Self::FailedParsingRequestBytes => StatusCode::PAYLOAD_TOO_LARGE,
                Self::FailedDecoding(_) => StatusCode::BAD_REQUEST,
            },
            self.to_string(),
        )
            .into_response()
    }
}

impl From<ToStrError> for DagCborRejection {
    fn from(_err: ToStrError) -> Self {
        Self::FailedToParseMime
    }
}

impl From<mime::FromStrError> for DagCborRejection {
    fn from(_err: mime::FromStrError) -> Self {
        Self::FailedToParseMime
    }
}

impl From<BytesRejection> for DagCborRejection {
    fn from(_err: BytesRejection) -> Self {
        Self::FailedParsingRequestBytes
    }
}

#[async_trait::async_trait]
impl<S, M> FromRequest<S> for DagCbor<M>
where
    M: DeserializeOwned + Debug,
    S: Send + Sync,
{
    type Rejection = DagCborRejection;

    #[tracing::instrument(skip_all, ret, err)]
    #[allow(clippy::blocks_in_conditions)]
    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let mime = req
            .headers()
            .get(CONTENT_TYPE)
            .ok_or(DagCborRejection::MissingContentType)?
            .to_str()?
            .parse::<mime::Mime>()?;

        if mime.essence_str() != "application/vnd.ipld.dag-cbor" {
            return Err(DagCborRejection::UnexpectedContentType(mime));
        }

        let bytes = Bytes::from_request(req, state).await?;
        Ok(DagCbor(serde_ipld_dagcbor::from_slice(bytes.as_ref())?))
    }
}

impl<M> IntoResponse for DagCbor<M>
where
    M: Serialize,
{
    fn into_response(self) -> Response {
        match serde_ipld_dagcbor::to_vec(&self.0) {
            Ok(bytes) => (
                [(
                    CONTENT_TYPE,
                    HeaderValue::from_static("application/vnd.ipld.dag-cbor"),
                )],
                bytes,
            )
                .into_response(),
            Err(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(
                    CONTENT_TYPE,
                    HeaderValue::from_static(mime::TEXT_PLAIN_UTF_8.as_ref()),
                )],
                format!("Failed to encode dag-cbor: {err}"),
            )
                .into_response(),
        }
    }
}
