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
/// Remote schema URL loading is not yet supported in peprs-eido.
#[test]
#[ignore = "remote schema loading not yet supported"]
fn test_validate_works_with_remote_schema() {
    let _project = load_project("test_pep");
    // When implemented: load schema from http://schema.databio.org/pep/2.0.0.yaml
    // then validate project, config, and samples against it.
    todo!("Implement remote schema loading");
}

/// Schemas with remote URL imports are loaded but remote imports are skipped with a warning.
/// Validation still works using the local schema content.
#[test]
fn test_schema_with_remote_import_skips_url() {
    let schema = peprs_eido::load_schema(&test_schema_path("test_schema_imports.yaml"))
        .expect("Failed to load schema");
    // Remote URL import should be skipped, so imports list is empty
    assert!(
        schema.imports.is_empty(),
        "Remote URL imports should be skipped, got {} imports",
        schema.imports.len()
    );
    // The local schema content should still be parsed
    assert!(schema.sample_schema.is_some());
}
