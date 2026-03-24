pub mod error;
pub mod schema;
pub mod validate;

use peprs_core::project::Project;

use crate::error::{EidoError, Result};
use crate::schema::EidoSchema;

/// Validate an entire PEP project against an eido schema.
/// Validates both project config and all samples.
pub fn validate(project: &Project, schema_path: &str) -> Result<()> {
    let schema = schema::load_schema(schema_path)?;
    validate_with_schema(project, &schema)
}

/// Validate a PEP project against a pre-loaded eido schema.
pub fn validate_with_schema(project: &Project, schema: &EidoSchema) -> Result<()> {
    let mut all_errors = Vec::new();

    if let Err(EidoError::Validation(errs)) = validate::validate_project(project, schema) {
        all_errors.extend(errs);
    }

    if let Err(EidoError::Validation(errs)) = validate::validate_samples(project, schema) {
        all_errors.extend(errs);
    }

    if !all_errors.is_empty() {
        return Err(EidoError::Validation(all_errors));
    }

    // File validation runs after schema validation passes
    validate::validate_input_files(project, schema)?;

    Ok(())
}

/// Validate only the project-level config.
pub fn validate_project(project: &Project, schema_path: &str) -> Result<()> {
    let schema = schema::load_schema(schema_path)?;
    validate::validate_project(project, &schema)
}

/// Validate only the samples.
pub fn validate_samples(project: &Project, schema_path: &str) -> Result<()> {
    let schema = schema::load_schema(schema_path)?;
    validate::validate_samples(project, &schema)
}

/// Validate a single sample (as a JSON value) against a pre-loaded schema.
pub fn validate_single_sample(
    sample: &serde_json::Value,
    schema: &EidoSchema,
    sample_name: &str,
) -> Result<()> {
    validate::validate_single_sample(sample, schema, sample_name)
}

/// Validate that tangible file attributes point to existing files.
pub fn validate_input_files(project: &Project, schema_path: &str) -> Result<()> {
    let schema = schema::load_schema(schema_path)?;
    validate::validate_input_files(project, &schema)
}

/// Load and preprocess an eido schema (useful for inspection/reuse).
pub fn load_schema(schema_path: &str) -> Result<EidoSchema> {
    schema::load_schema(schema_path)
}
