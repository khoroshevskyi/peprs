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
