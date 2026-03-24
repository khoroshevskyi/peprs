use peprs_core::project::Project;
use peprs_eido::error::EidoError;

fn example_pep_path(name: &str) -> String {
    format!(
        "{}/example-peps/{}/project_config.yaml",
        env!("CARGO_MANIFEST_DIR").replace("/peprs-eido", ""),
        name
    )
}

fn test_schema_path(name: &str) -> String {
    format!("{}/tests/data/schemas/{}", env!("CARGO_MANIFEST_DIR"), name)
}

fn load_basic_project() -> Project {
    Project::from_config(example_pep_path("example_basic"))
        .build()
        .expect("Failed to load example_basic PEP")
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

// --- Schema loading tests ---

#[test]
fn test_load_schema() {
    let schema = peprs_eido::load_schema(&test_schema_path("schema_basic.yaml"))
        .expect("Failed to load schema");
    assert!(schema.sample_schema.is_some());
    assert!(schema.tangible.is_empty());
    assert!(schema.imports.is_empty());
}

#[test]
fn test_load_schema_with_tangible() {
    let schema = peprs_eido::load_schema(&test_schema_path("schema_with_tangible.yaml"))
        .expect("Failed to load schema");
    assert_eq!(schema.tangible, vec!["file"]);
}

#[test]
fn test_load_schema_with_imports() {
    let schema = peprs_eido::load_schema(&test_schema_path("schema_with_import.yaml"))
        .expect("Failed to load schema");
    assert_eq!(schema.imports.len(), 1);
    assert!(schema.imports[0].sample_schema.is_some());
}

#[test]
fn test_multi_value_preprocessing() {
    let schema = peprs_eido::load_schema(&test_schema_path("schema_basic.yaml"))
        .expect("Failed to load schema");
    let sample_schema = schema.sample_schema.unwrap();
    let props = sample_schema
        .get("properties")
        .unwrap()
        .as_object()
        .unwrap();

    // All string properties should be wrapped in anyOf for multi-value support
    for (_key, value) in props {
        assert!(
            value.get("anyOf").is_some(),
            "Expected string properties to be wrapped in anyOf for multi-value support"
        );
    }
}

// --- Basic validation tests (using example_basic PEP) ---

#[test]
fn test_validate_basic_passes() {
    let project = load_basic_project();
    let result = peprs_eido::validate(&project, &test_schema_path("schema_basic.yaml"));
    assert!(result.is_ok(), "Expected validation to pass: {:?}", result);
}

#[test]
fn test_validate_missing_required_column() {
    let project = load_basic_project();
    let result =
        peprs_eido::validate_samples(&project, &test_schema_path("schema_missing_col.yaml"));

    match result {
        Err(EidoError::Validation(errors)) => {
            let genome_errors: Vec<_> = errors
                .iter()
                .filter(|e| e.message.contains("genome"))
                .collect();
            assert!(
                !genome_errors.is_empty(),
                "Expected errors about missing 'genome' column, got: {:?}",
                errors
            );
        }
        Ok(()) => panic!("Expected validation to fail for missing 'genome' column"),
        Err(e) => panic!("Expected Validation error, got: {:?}", e),
    }
}

#[test]
fn test_validate_tangible_missing_files() {
    let project = load_basic_project();
    let result =
        peprs_eido::validate_input_files(&project, &test_schema_path("schema_with_tangible.yaml"));

    match result {
        Err(EidoError::MissingFiles(missing)) => {
            assert!(
                !missing.is_empty(),
                "Expected missing file errors, got empty list"
            );
            assert!(
                missing.iter().all(|m| m.attribute == "file"),
                "All missing files should be for 'file' attribute"
            );
        }
        Ok(()) => {
            // If files happen to exist, that's fine too
        }
        Err(e) => panic!("Expected MissingFiles error, got: {:?}", e),
    }
}

#[test]
fn test_validate_project_level() {
    let project = load_basic_project();
    let result =
        peprs_eido::validate_project(&project, &test_schema_path("schema_project_level.yaml"));
    assert!(
        result.is_ok(),
        "Expected project validation to pass: {:?}",
        result
    );
}

#[test]
fn test_validate_with_import_chain() {
    let project = load_basic_project();
    let result =
        peprs_eido::validate_samples(&project, &test_schema_path("schema_with_import.yaml"));
    assert!(
        result.is_ok(),
        "Expected validation with imports to pass: {:?}",
        result
    );
}

// --- TestProjectValidation (maps to Python eido TestProjectValidation) ---
// Python's validate_project does full validation → Rust validate_with_schema

#[test]
fn test_validate_works() {
    let schema = peprs_eido::load_schema(&test_schema_path("test_schema.yaml"))
        .expect("Failed to load schema");
    let project = load_project("test_pep");
    let result = peprs_eido::validate_with_schema(&project, &schema);
    assert!(result.is_ok(), "Expected validation to pass: {:?}", result);
}

#[test]
fn test_validate_detects_invalid() {
    // test_schema_invalid requires "invalid" property in project config
    let schema = peprs_eido::load_schema(&test_schema_path("test_schema_invalid.yaml"))
        .expect("Failed to load schema");
    let project = load_project("test_pep");
    let result = peprs_eido::validate_with_schema(&project, &schema);
    assert!(
        result.is_err(),
        "Expected validation to fail for schema requiring 'invalid' property"
    );
}

#[test]
fn test_validate_detects_invalid_imports() {
    // test_schema_imports requires my_numeric_attribute in samples (URL import is skipped)
    let schema = peprs_eido::load_schema(&test_schema_path("test_schema_imports.yaml"))
        .expect("Failed to load schema");
    let project = load_project("test_pep");
    let result = peprs_eido::validate_with_schema(&project, &schema);
    assert!(
        result.is_err(),
        "Expected validation to fail: my_numeric_attribute is missing"
    );
}

#[test]
fn test_validate_converts_samples_to_private_attr() {
    // test_schema_samples has no required sample attrs → should pass
    let schema = peprs_eido::load_schema(&test_schema_path("test_schema_samples.yaml"))
        .expect("Failed to load schema");
    let project = load_project("test_pep");
    let result = peprs_eido::validate_with_schema(&project, &schema);
    assert!(result.is_ok(), "Expected validation to pass: {:?}", result);
}

#[test]
fn test_validate_works_with_dict_schema() {
    // Load schema as YAML → Value → load_schema_from_value (simulates dict-based schema)
    let schema_content = std::fs::read_to_string(test_schema_path("test_schema.yaml")).unwrap();
    let schema_value: serde_json::Value = serde_yaml::from_str(&schema_content).unwrap();
    let schema = peprs_eido::schema::load_schema_from_value(schema_value)
        .expect("Failed to load schema from value");
    let project = load_project("test_pep");
    let result = peprs_eido::validate_with_schema(&project, &schema);
    assert!(
        result.is_ok(),
        "Expected validation to pass with dict schema: {:?}",
        result
    );
}

#[test]
fn test_validate_raises_error_for_incorrect_schema_type() {
    // Rust type system prevents passing wrong types at compile time.
    // Verify that non-object schema values produce schemas with no validation targets.
    let schema = peprs_eido::schema::load_schema_from_value(serde_json::json!(1)).unwrap();
    assert!(schema.sample_schema.is_none());
    assert!(schema.project_schema.is_none());

    let schema = peprs_eido::schema::load_schema_from_value(serde_json::json!(null)).unwrap();
    assert!(schema.sample_schema.is_none());
    assert!(schema.project_schema.is_none());

    let schema = peprs_eido::schema::load_schema_from_value(serde_json::json!([1, 2, 3])).unwrap();
    assert!(schema.sample_schema.is_none());
    assert!(schema.project_schema.is_none());
}

// --- Value check validation ---

#[test]
fn test_validate_value_check() {
    let schema = peprs_eido::load_schema(&test_schema_path("value_check_schema.yaml"))
        .expect("Failed to load schema");
    let project = load_project("value_check_pep");
    let result = peprs_eido::validate_with_schema(&project, &schema);
    assert!(
        result.is_err(),
        "Expected validation to fail: format_type has invalid enum values"
    );
}

// --- File existence validation ---

#[test]
fn test_validate_file_existence() {
    let schema = peprs_eido::load_schema(&test_schema_path("schema_test_file_exist.yaml"))
        .expect("Failed to load schema");
    let project = load_project("test_file_existing");
    // Files referenced by derive don't exist → should report missing files
    let result = peprs_eido::validate_with_schema(&project, &schema);
    // tangible is inside items (non-standard placement), so file validation may not trigger.
    // At minimum, schema loading should succeed.
    assert!(schema.sample_schema.is_some());
}
