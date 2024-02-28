//! Basic anyhow-based error webserver errors

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

/// TODO(matheus23): docs
#[derive(Debug)]
pub struct AppError(anyhow::Error);

/// TODO(matheus23): docs
pub type AppResult<T> = Result<T, AppError>;

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}
