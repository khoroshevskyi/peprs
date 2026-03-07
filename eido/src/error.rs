use thiserror::Error;

#[derive(Error, Debug)]
pub enum EidoError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Schema parsing error: {0}")]
    SchemaParse(String),

    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Polars error: {0}")]
    Polars(#[from] polars::error::PolarsError),

    #[error("PEP error: {0}")]
    Pep(#[from] peprs_core::error::Error),
}