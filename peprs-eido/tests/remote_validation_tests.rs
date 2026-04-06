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

/// Python: TestRemoteValidation.test_validate_works_with_remote_schemas
#[test]
fn test_validate_works_with_remote_schema() {
    let project = load_project("test_pep");
    let schema_url = "http://schema.databio.org/pep/2.0.0.yaml";
    peprs_eido::validate(&project, schema_url).expect("Validation with remote schema failed");
}

/// Schemas with remote URL imports resolve the imported schema.
#[test]
fn test_schema_with_remote_import_resolves_url() {
    let schema = peprs_eido::load_schema(&test_schema_path("test_schema_imports.yaml"))
        .expect("Failed to load schema");
    // Remote URL import should be resolved
    assert_eq!(
        schema.imports.len(),
        1,
        "Remote URL import should be resolved, got {} imports",
        schema.imports.len()
    );
    // The local schema content should still be parsed
    assert!(schema.sample_schema.is_some());
}
