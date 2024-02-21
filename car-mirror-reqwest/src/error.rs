use reqwest::Response;

/// Possible errors raised in this library
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Raised when the HTTP response code didn't end up as a 200 or 202
    #[error("Unexpected response code: {}, expected 200 or 202", response.status())]
    UnexpectedStatusCode {
        /// The response
        response: Response,
    },

    /// Raised when `RequestBuilder::try_clone` fails, usually because
    /// `RequestBuilder::body(Body::wrap_stream(...))` was called.
    ///
    /// Generally, car-mirror-reqwest will take over body creation, so there's
    /// no point in setting the body before `run_car_mirror_pull` / `run_car_mirror_push`.
    #[error("Body must not be set on request builder")]
    RequestBuilderBodyAlreadySet,

    /// reqwest errors
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),

    /// reqwest-middleware errors
    #[error(transparent)]
    ReqwestMiddlewareError(#[from] reqwest_middleware::Error),

    /// car-mirror errors
    #[error(transparent)]
    CarMirrorError(#[from] car_mirror::Error),
}
