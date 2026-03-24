use std::fmt;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum EidoError {
    #[error("Schema loading error: {0}")]
    SchemaLoad(String),

    #[error("Validation failed with {} error(s)", .0.len())]
    Validation(Vec<ValidationError>),

    #[error("Missing required files: {} file(s)", .0.len())]
    MissingFiles(Vec<MissingFile>),

    #[error("Project error: {0}")]
    Project(#[from] peprs_core::error::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("JSON Schema compilation error: {0}")]
    SchemaCompile(String),
}

#[derive(Debug, Clone)]
pub struct ValidationError {
    pub path: String,
    pub message: String,
    pub sample_name: Option<String>,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.sample_name {
            Some(name) => write!(f, "sample '{}' at {}: {}", name, self.path, self.message),
            None => write!(f, "project at {}: {}", self.path, self.message),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MissingFile {
    pub sample_name: String,
    pub attribute: String,
    pub path: String,
}

impl fmt::Display for MissingFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "sample '{}': attribute '{}' references missing file '{}'",
            self.sample_name, self.attribute, self.path
        )
    }
}

pub type Result<T> = std::result::Result<T, EidoError>;
