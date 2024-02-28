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
use std::convert::Infallible;

/// TODO(matheus23): docs
#[derive(Debug, Clone)]
pub struct DagCbor<M>(pub M);

/// TODO(matheus23): docs
#[derive(Debug)]
pub enum DagCborRejection {
    /// TODO(matheus23): docs
    MissingContentType,
    /// TODO(matheus23): docs
    UnexpectedContentType(mime::Mime),
    /// TODO(matheus23): docs
    FailedToParseMime,
    /// TODO(matheus23): docs
    FailedParsingRequestBytes,
    /// TODO(matheus23): docs
    FailedDecoding(DecodeError<Infallible>),
}

impl IntoResponse for DagCborRejection {
    fn into_response(self) -> Response {
        match self {
            Self::MissingContentType => (StatusCode::BAD_REQUEST, "Missing Content-Type header on request, expected application/json or application/vnd.ipld.dag-cbor, but got nothing").into_response(),
            Self::UnexpectedContentType(mime) => (StatusCode::BAD_REQUEST, format!("Incorrect mime type, expected application/vnd.ipld.dag-cbor, but got {mime}")).into_response(),
            Self::FailedToParseMime => (StatusCode::BAD_REQUEST, "Failed parsing Content-Type header as mime type, expected application/json or application/vnd.ipld.dag-cbor").into_response(),
            Self::FailedParsingRequestBytes => (StatusCode::PAYLOAD_TOO_LARGE, "Unable to buffer the request body, perhaps it exceeded the 2MB limit").into_response(),
            Self::FailedDecoding(err) => (StatusCode::BAD_REQUEST, format!("Failed decoding dag-cbor: {err}")).into_response(),
        }
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

impl From<DecodeError<Infallible>> for DagCborRejection {
    fn from(err: DecodeError<Infallible>) -> Self {
        Self::FailedDecoding(err)
    }
}

#[async_trait::async_trait]
impl<S, M> FromRequest<S> for DagCbor<M>
where
    M: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = DagCborRejection;

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
