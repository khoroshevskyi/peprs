import pytest

from peprs import Project
from peprs.eido import (
    EidoValidationError,
    PathAttrNotFoundError,
    validate_config,
    validate_input_files,
    validate_project,
    validate_sample,
)


class TestProjectValidation:
    def test_validate_works(self, eido_project_object, schema_file_path):
        """Valid project + schema passes without error."""
        validate_project(project=eido_project_object, schema=schema_file_path)

    def test_validate_detects_invalid(self, eido_project_object, schema_invalid_file_path):
        """Invalid schema raises EidoValidationError."""
        with pytest.raises(EidoValidationError):
            validate_project(project=eido_project_object, schema=schema_invalid_file_path)

    def test_validate_with_imports(self, eido_project_object, schema_imports_file_path):
        """Schema with imports that fails validation raises EidoValidationError."""
        with pytest.raises(EidoValidationError):
            validate_project(project=eido_project_object, schema=schema_imports_file_path)

    def test_validate_works_with_dict_schema(self, eido_project_object, schema_file_path):
        """Accept a dict as schema argument."""
        import yaml

        with open(schema_file_path) as f:
            schema_dict = yaml.safe_load(f)
        validate_project(project=eido_project_object, schema=schema_dict)


class TestSampleValidation:
    def test_validate_works(self, eido_project_object, schema_samples_file_path):
        """Valid sample passes validation."""
        validate_sample(
            project=eido_project_object,
            sample_name="GSM1558746",
            schema=schema_samples_file_path,
        )

    def test_validate_detects_invalid(self, eido_project_object, schema_sample_invalid_file_path):
        """Invalid sample schema raises EidoValidationError."""
        with pytest.raises(EidoValidationError):
            validate_sample(
                project=eido_project_object,
                sample_name="GSM1558746",
                schema=schema_sample_invalid_file_path,
            )

    def test_validate_invalid_sample_name(self, eido_project_object, schema_samples_file_path):
        """Non-existent sample name raises ValueError."""
        with pytest.raises(ValueError):
            validate_sample(
                project=eido_project_object,
                sample_name="bogus_sample_name",
                schema=schema_samples_file_path,
            )


class TestConfigValidation:
    def test_validate_succeeds_on_invalid_sample(
        self, eido_project_object, schema_sample_invalid_file_path
    ):
        """Config validation passes even when sample schema is invalid."""
        validate_config(project=eido_project_object, schema=schema_sample_invalid_file_path)


class TestInputFileValidation:
    def test_validate_input_files(self):
        """Missing tangible files raise PathAttrNotFoundError."""
        from .conftest import EIDO_DATA_DIR, EXAMPLE_PEPS_DIR
        import os

        # Use basic PEP (files don't exist on disk) with a top-level tangible schema
        prj = Project(os.path.join(EXAMPLE_PEPS_DIR, "example_basic", "project_config.yaml"))
        schema_path = os.path.join(EIDO_DATA_DIR, "schemas", "schema_with_tangible.yaml")
        with pytest.raises(PathAttrNotFoundError):
            validate_input_files(project=prj, schema=schema_path)

    def test_validate_values(self, pep_value_check, schema_value_check):
        """Value check schema raises EidoValidationError."""
        prj = Project(pep_value_check)
        with pytest.raises(EidoValidationError):
            validate_project(project=prj, schema=schema_value_check)
