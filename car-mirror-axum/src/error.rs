//! Basic anyhow-based error webserver errors

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use std::fmt::Display;

/// A basic anyhow error type wrapper that returns
/// internal server errors if something goes wrong.
#[derive(Debug)]
pub struct AppError {
    status_code: StatusCode,
    error_msg: String,
}

impl Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error_msg.fmt(f)
    }
}

impl AppError {
    /// Construct a new error from a status code and an error message
    pub fn new(status_code: StatusCode, msg: impl ToString) -> Self {
        Self {
            status_code,
            error_msg: msg.to_string(),
        }
    }
}

/// Helper type alias that defaults the error type to `AppError`
pub type AppResult<T, E = AppError> = Result<T, E>;

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (self.status_code, self.error_msg).into_response()
    }
}

impl From<anyhow::Error> for AppError {
    fn from(err: anyhow::Error) -> Self {
        Self::from(&err)
    }
}

impl From<&anyhow::Error> for AppError {
    fn from(err: &anyhow::Error) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", err),
        )
    }
}

impl From<car_mirror::Error> for AppError {
    fn from(err: car_mirror::Error) -> Self {
        Self::from(&err)
    }
}

impl From<&car_mirror::Error> for AppError {
    fn from(err: &car_mirror::Error) -> Self {
        use car_mirror::Error;
        match err {
            Error::TooManyBytes { .. } => Self::new(StatusCode::PAYLOAD_TOO_LARGE, err),
            Error::BlockSizeExceeded { .. } => Self::new(StatusCode::PAYLOAD_TOO_LARGE, err),
            Error::UnsupportedCodec { .. } => Self::new(StatusCode::BAD_REQUEST, err),
            Error::UnsupportedHashCode { .. } => Self::new(StatusCode::BAD_REQUEST, err),
            Error::BlockStoreError(err) => Self::from(err),
            Error::ParsingError(_) => Self::new(StatusCode::UNPROCESSABLE_ENTITY, err),
            Error::IncrementalVerificationError(_) => Self::new(StatusCode::BAD_REQUEST, err),
            Error::CarFileError(_) => Self::new(StatusCode::BAD_REQUEST, err),
        }
    }
}

impl From<wnfs_common::BlockStoreError> for AppError {
    fn from(err: wnfs_common::BlockStoreError) -> Self {
        Self::from(&err)
    }
}

impl From<&wnfs_common::BlockStoreError> for AppError {
    fn from(err: &wnfs_common::BlockStoreError) -> Self {
        use wnfs_common::BlockStoreError;
        match err {
            BlockStoreError::MaximumBlockSizeExceeded(_) => {
                Self::new(StatusCode::PAYLOAD_TOO_LARGE, err)
            }
            BlockStoreError::CIDNotFound(_) => Self::new(StatusCode::NOT_FOUND, err),
            BlockStoreError::CIDError(_) => Self::new(StatusCode::INTERNAL_SERVER_ERROR, err),
            BlockStoreError::Custom(_) => Self::new(StatusCode::INTERNAL_SERVER_ERROR, err),
        }
    }
}

impl From<libipld::cid::Error> for AppError {
    fn from(err: libipld::cid::Error) -> Self {
        Self::from(&err)
    }
}

impl From<&libipld::cid::Error> for AppError {
    fn from(err: &libipld::cid::Error) -> Self {
        Self::new(StatusCode::BAD_REQUEST, err)
    }
}

impl From<std::io::Error> for AppError {
    fn from(err: std::io::Error) -> Self {
        if let Some(err) = err.get_ref() {
            if let Some(err) = err.downcast_ref::<car_mirror::Error>() {
                return Self::from(err);
            }

            if let Some(err) = err.downcast_ref::<wnfs_common::BlockStoreError>() {
                return Self::from(err);
            }
        }

        Self::new(StatusCode::INTERNAL_SERVER_ERROR, err)
    }
}
