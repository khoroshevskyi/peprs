use peprs_core::project::Project;
use peprs_eido::error::EidoError;

fn test_schema_path(name: &str) -> String {
    format!("{}/tests/data/schemas/{}", env!("CARGO_MANIFEST_DIR"), name)
}

fn load_project(name: &str) -> Project {
    let prj_path = format!(
        "{}/tests/data/peps/{}/config.yaml",
        env!("CARGO_MANIFEST_DIR"),
        name
    );
    Project::from_config(prj_path)
        .build()
        .expect("Failed to load PEP")
}

/// Python: TestSampleValidation.test_validate_works
/// validate_sample with valid schema should pass for all samples
#[test]
fn test_validate_samples_works() {
    let schema = peprs_eido::load_schema(&test_schema_path("test_schema_samples.yaml"))
        .expect("Failed to load schema");
    let project = load_project("test_pep");
    let result = peprs_eido::validate::validate_samples(&project, &schema);
    assert!(result.is_ok(), "Expected sample validation to pass: {:?}", result);
}

/// Python: TestSampleValidation.test_validate_detects_invalid
/// validate_sample with schema requiring missing attr should fail
#[test]
fn test_validate_samples_detects_invalid() {
    let schema = peprs_eido::load_schema(&test_schema_path("test_schema_sample_invalid.yaml"))
        .expect("Failed to load schema");
    let project = load_project("test_pep");
    let result = peprs_eido::validate::validate_samples(&project, &schema);
    match result {
        Err(EidoError::Validation(errors)) => {
            let newattr_errors: Vec<_> = errors
                .iter()
                .filter(|e| e.message.contains("newattr"))
                .collect();
            assert!(
                !newattr_errors.is_empty(),
                "Expected errors about missing 'newattr', got: {:?}",
                errors
            );
        }
        Ok(()) => panic!("Expected sample validation to fail for missing 'newattr'"),
        Err(e) => panic!("Expected Validation error, got: {:?}", e),
    }
}

/// Python: TestSampleValidation.test_validate_works with schema that has required genome
/// The test_pep uses imply to add genome, so sample validation should pass
#[test]
fn test_validate_samples_with_required_genome() {
    let schema = peprs_eido::load_schema(&test_schema_path("test_schema.yaml"))
        .expect("Failed to load schema");
    let project = load_project("test_pep");
    let result = peprs_eido::validate::validate_samples(&project, &schema);
    assert!(result.is_ok(), "Expected sample validation to pass (genome from imply): {:?}", result);
}
