# Missing Tests — peprs-py vs peppy

Tests from the peppy Python package that are not yet ported to peprs-py, organized by reason.

## 1. Missing API Features in peprs-py

These tests require features not yet exposed in the peprs-py bindings.

### No empty constructor (`Project()` with no args)
- `TestProjectConstructor::test_empty`

### No `defer_samples_creation` support
- `TestProjectConstructor::test_instantiation` (defer=True variants)
- `TestProjectConstructor::test_no_description` (defer=True variants)
- `TestProjectConstructor::test_description` (defer=True variants)
- `TestProjectConstructor::test_amendments` (defer=True variant)
- `TestProjectConstructor::test_missing_sample_name_defer`
- `TestProjectManipulationTests::test_str_repr_correctness` (defer=True variants)
- `TestProjectManipulationTests::test_amendments_listing` (defer=True variant)
- `TestPostInitSampleCreation::test_append`
- `TestPostInitSampleCreation::test_imports`
- `TestPostInitSampleCreation::test_imply`
- `TestPostInitSampleCreation::test_duplicate`
- `TestPostInitSampleCreation::test_derive`
- `TestPostInitSampleCreation::test_issue499`
- `TestPostInitSampleCreation::test_equality`
- `TestPostInitSampleCreation::test_unequality`

### No dynamic amendment activation/deactivation
- `TestProjectManipulationTests::test_amendments_activation_interactive`
- `TestProjectManipulationTests::test_amendments_deactivation_interactive`
- `TestProjectManipulationTests::test_amendments_argument_cant_be_null`

### No `list_amendments` property
- `TestProjectManipulationTests::test_amendments_listing`

### No `sample_table` / `subsample_table` DataFrame properties
- `TestProjectManipulationTests::test_sample_updates_regenerate_df`
- `TestProjectManipulationTests::test_subsample_table_property`

### No `sample_name_colname` property
- `TestProjectConstructor::test_missing_sample_name_custom_index`
- `TestProjectConstructor::test_sample_name_custom_index`

### No `__eq__` on Project
- `TestProjectConstructor::test_equality`
- `TestProjectConstructor::test_inequality`
- `TestProjectConstructor::test_from_dict_instatiation` (relies on `p1 == p2`)
- `TestPostInitSampleCreation::test_from_dict` (relies on `p1 == p2`)
- `TestPostInitSampleCreation::test_from_pandas` (relies on `p1 == p2`)
- `TestPostInitSampleCreation::test_from_pandas_unequal`

### No pickle support
- `TestProjectConstructor::test_correct_pickle`
- `TestSample::test_pickle_in_samples`

### No Sample objects (samples are plain dicts)
- `TestSample::test_serialization` (Sample.to_yaml)
- `TestSample::test_str_repr_correctness` (Sample.__str__)
- `TestSample::test_sample_to_yaml_no_path` (Sample.to_yaml)
- `TestSample::test_sheet_dict_excludes_private_attrs` (Sample.get_sheet_dict)
- `TestSample::test_equals_samples` (Sample.__eq__)
- `TestSample::test_not_equals_samples` (Sample.__eq__)
- `TestSampleAttrMap::test_sample_getattr` (Sample attribute-style access)
- `TestSampleAttrMap::test_sample_settatr` (Sample attribute mutation)
- `TestSampleAttrMap::test_sample_len` (len(sample))

### No `from_sample_yaml()` class method
- `TestPostInitSampleCreation::test_from_yaml`

### No remote URL support in constructor
- `TestProjectConstructor::test_remote` (4 remote configs)
- `TestProjectConstructor::test_remote_simulate_no_network`
- `TestProjectConstructor::test_remote_csv_init_autodetect`
- `TestProjectConstructor::test_automerge_remote`
- `TestProjectWithoutConfigValidation::test_validate_works` (remote CSV)
- `TestProjectWithoutConfigValidation::test_validate_detects_invalid` (remote CSV)

### No `to_dict(extended=True, orient=...)` options
- `TestPostInitSampleCreation::test_description_setter` (checks `to_dict(extended=True)`)
- `TestPostInitSampleCreation::test_name_setter` (checks `to_dict(extended=True)`)

### No remote schema validation
- `TestRemoteValidation::test_validate_works_with_remote_schemas`

### No `validate_original_samples` function
- `TestSampleValidation::test_original_sample`

### No `validate_sample` by integer index
- `TestSampleValidation::test_validate_works` (index 0, 1 variants)
- `TestSampleValidation::test_validate_detects_invalid` (index 0, 1 variants)

### No schema type checking (int/None/list rejected)
- `TestProjectValidation::test_validate_raises_error_for_incorrect_schema_type`

### No relative-path schema imports
- `TestProjectValidation::test_validate_imports_with_rel_path`

### No `samples` -> `_samples` key conversion in schema
- `TestProjectValidation::test_validate_converts_samples_to_private_attr`

## 2. Missing Example PEPs

These example PEPs exist in peppy's test data but not in peprs `example-peps/`:

| Directory | Used by tests |
|---|---|
| `example_automerge` | test_automerge, test_automerge_csv |
| `example_subtable_automerge` | test_automerge_disallowed_with_subsamples |
| `example_custom_index` | test_custom_sample_table_index_config, test_sample_name_custom_index |
| `example_incorrect_index` | test_cutsom_sample_table_index_config_exception |
| `example_issue499` | test_issue499 |
| `example_missing_version` | test_missing_version |
| `example_noname` | test_missing_sample_name_derive, test_missing_sample_name, test_missing_sample_name_custom_index |
| `example_subsamples_none` | test_config_with_subsample_null |
| `example_subtables` | test_subsample_table_multiple |
| `example_multiple_subsamples` | test_custom_sample_table_index_config |
| `example_nextflow_config` | test_peppy_initializes_samples_with_correct_attributes |
| `example_nextflow_samplesheet` | test_auto_merge_duplicated_names_works_for_different_read_types |
| `example_nextflow_subsamples` | test_nextflow_subsamples |
| `example_nextflow_taxprofiler_pep` | test_to_dict_does_not_create_nans |
| `example_basic_sample_yaml` | test_from_yaml |

## 3. Eido Conversion/Filter Tests (Not Applicable)

peprs-py does not expose eido conversion/filter functionality:

- `TestConversionInfrastructure::test_plugins_are_read`
- `TestConversionInfrastructure::test_plugins_contents`
- `TestConversionInfrastructure::test_plugins_are_callable`
- `TestConversionInfrastructure::test_basic_filter`
- `TestConversionInfrastructure::test_csv_filter`
- `TestConversionInfrastructure::test_csv_filter_handles_empty_fasta_correctly`
- `TestConversionInfrastructure::test_eido_csv_filter_filters_nextflow_taxprofiler_input_correctly`
- `TestConversionInfrastructure::test_multiple_subsamples`

## 4. PEPHub Client Tests (Not Applicable)

peppy has dedicated PEPHub client tests (push/pull/samples/views CRUD). peprs-py only exposes `Project.from_pephub()` for reading. The full PEPHub client API is not exposed:

- All 27 tests in `test_pephubclient.py`
- All 9 tests in `test_manual.py`

## 5. Eido Schema Operations
- `TestSchemaReading::test_imports_file_schema`
- `TestSchemaReading::test_imports_dict_schema`
