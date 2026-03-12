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

    /// Project missing required attribute/input error
    #[error("Project missing required attribute: {0}")]
    ProjectMissingAttribute(String),

    /// JSON serialization/deserialization errors
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Zip archive errors
    #[error("Zip error: {0}")]
    Zip(#[from] zip::result::ZipError),
}

/// Result type alias for convenience
pub type Result<T> = std::result::Result<T, Error>;

/// Helper functions for creating specific error types.
impl Error {
    ///
    /// Creates a configuration error.
    ///
    /// # Arguments
    ///
    /// * `msg` - The error message.
    ///
    /// # Returns
    ///
    /// A new [`Error::Config`] variant.
    ///
    pub fn config<S: Into<String>>(msg: S) -> Self {
        Error::Config(msg.into())
    }

    ///
    /// Creates a processing error.
    ///
    /// # Arguments
    ///
    /// * `msg` - The error message.
    ///
    /// # Returns
    ///
    /// A new [`Error::Processing`] variant.
    ///
    pub fn processing<S: Into<String>>(msg: S) -> Self {
        Error::Processing(msg.into())
    }

    ///
    /// Creates an invalid format error.
    ///
    /// # Arguments
    ///
    /// * `msg` - The error message.
    ///
    /// # Returns
    ///
    /// A new [`Error::InvalidFormat`] variant.
    ///
    pub fn invalid_format<S: Into<String>>(msg: S) -> Self {
        Error::InvalidFormat(msg.into())
    }
}
