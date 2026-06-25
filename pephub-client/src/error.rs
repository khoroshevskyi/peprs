use std::num::ParseIntError;
use thiserror::Error;

type HeaderName = &'static str;

#[derive(Debug, Error)]
/// All errors the API can throw
pub enum ApiError {
    /// Api expects certain header to be present in the results to derive some information
    #[error("Header {0} is missing")]
    MissingHeader(HeaderName),

    /// The header exists, but the value is not conform to what the Api expects.
    #[error("Header {0} is invalid")]
    InvalidHeader(HeaderName),

    /// Error in the request
    #[error("request error: {0}")]
    RequestError(#[from] Box<ureq::Error>),

    /// Error parsing some range value
    #[error("Cannot parse int")]
    ParseIntError(#[from] ParseIntError),

    /// I/O Error
    #[error("I/O error {0}")]
    IoError(#[from] std::io::Error),

    /// We tried to download chunk too many times
    #[error("Too many retries: {0}")]
    TooManyRetries(Box<ApiError>),

    /// The part file is corrupted
    #[error("Invalid part file - corrupted file")]
    InvalidResume,

    /// Error parsing YAML configuration
    #[error("YAML parse error: {0}")]
    YamlParseError(#[from] Box<serde_yaml::Error>),
}

#[derive(Debug, Error)]
/// Errors raised while loading or building the token [`crate::auth::Cache`]
pub enum CacheError {
    /// I/O Error reading or writing the token file
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The token file is not valid TOML
    #[error("failed to parse token file: {0}")]
    TomlDe(#[from] toml::de::Error),

    /// The token could not be serialized to TOML
    #[error("failed to serialize token: {0}")]
    TomlSer(#[from] toml::ser::Error),

    /// Network / HTTP error talking to PEPHub
    #[error("request error: {0}")]
    Request(#[from] Box<ureq::Error>),

    /// Failed to build the HTTP client used for login
    #[error("api error: {0}")]
    Api(#[from] ApiError),

    /// Device code not yet authorized (HTTP 401) — retryable
    #[error("authorization pending")]
    AuthorizationPending,

    /// Login did not complete (user never authorized, or final attempt failed)
    #[error("login failed: device code was not authorized")]
    LoginFailed,
}
