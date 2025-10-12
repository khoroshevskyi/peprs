use thiserror::Error;

/// Main error type for the application
#[derive(Error, Debug)]
pub enum Error {
    /// IO-related errors (file not found, permission denied, etc.)
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// YAML parsing/serialization errors
    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    /// Configuration validation errors
    #[error("Configuration error: {0}")]
    Config(String),

    /// Data processing errors
    #[error("Processing error: {0}")]
    Processing(String),

    /// File format errors
    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    /// Polars error
    #[error("Polars error: {0}")]
    Polars(#[from] polars::error::PolarsError),

    /// Not found amendment error
    #[error("Amendment '{0}' not found in config")]
    AmendmentNotFound(String),
}

/// Result type alias for convenience
pub type Result<T> = std::result::Result<T, Error>;

/// Helper functions for creating specific error types
impl Error {
    pub fn config<S: Into<String>>(msg: S) -> Self {
        Error::Config(msg.into())
    }

    pub fn processing<S: Into<String>>(msg: S) -> Self {
        Error::Processing(msg.into())
    }

    pub fn invalid_format<S: Into<String>>(msg: S) -> Self {
        Error::InvalidFormat(msg.into())
    }
}
