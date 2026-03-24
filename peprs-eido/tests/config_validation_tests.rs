use peprs_core::project::Project;

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

/// Python: TestConfigValidation.test_validate_succeeds_on_invalid_sample
/// Config-only validation should pass even when the schema has invalid sample requirements,
/// because config validation does not check samples.
#[test]
fn test_validate_config_succeeds_on_invalid_sample() {
    let schema = peprs_eido::load_schema(&test_schema_path("test_schema_sample_invalid.yaml"))
        .expect("Failed to load schema");
    let project = load_project("test_pep");
    let result = peprs_eido::validate::validate_project(&project, &schema);
    assert!(
        result.is_ok(),
        "Config validation should pass regardless of sample schema: {:?}",
        result
    );
}

/// Config validation passes with the base test_schema (required: samples is stripped)
#[test]
fn test_validate_config_with_valid_schema() {
    let schema = peprs_eido::load_schema(&test_schema_path("test_schema.yaml"))
        .expect("Failed to load schema");
    let project = load_project("test_pep");
    let result = peprs_eido::validate::validate_project(&project, &schema);
    assert!(
        result.is_ok(),
        "Config validation should pass: {:?}",
        result
    );
}

/// Config validation detects missing required project-level property
#[test]
fn test_validate_config_detects_invalid_project_property() {
    let schema = peprs_eido::load_schema(&test_schema_path("test_schema_invalid.yaml"))
        .expect("Failed to load schema");
    let project = load_project("test_pep");
    let result = peprs_eido::validate::validate_project(&project, &schema);
    assert!(
        result.is_err(),
        "Config validation should fail: 'invalid' property is required but missing"
    );
}
