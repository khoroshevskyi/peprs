import os

import pytest

# Repo root (two levels up from tests/)
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
EXAMPLE_PEPS_DIR = os.path.join(REPO_ROOT, "example-peps")
EIDO_DATA_DIR = os.path.join(REPO_ROOT, "peprs-eido", "tests", "data")

EXAMPLE_TYPES = [
    "basic",
    "derive",
    "imply",
    "append",
    "amendments1",
    "amendments2",
    "derive_imply",
    "duplicate",
    "imports",
    "subtable1",
    "subtable2",
    "subtable3",
    "subtable4",
    "subtable5",
    "remove",
]


def get_example_pep_path(directory_name, file_name="project_config.yaml"):
    return os.path.join(EXAMPLE_PEPS_DIR, f"example_{directory_name}", file_name)


@pytest.fixture
def example_pep_cfg_path(request):
    return get_example_pep_path(request.param)


@pytest.fixture
def example_pep_csv_path(request):
    return get_example_pep_path(request.param, "sample_table.csv")


# --- Eido fixtures ---


@pytest.fixture
def eido_schemas_path():
    return os.path.join(EIDO_DATA_DIR, "schemas")


@pytest.fixture
def eido_peps_path():
    return os.path.join(EIDO_DATA_DIR, "peps")


@pytest.fixture
def eido_project_cfg_path(eido_peps_path):
    return os.path.join(eido_peps_path, "test_pep", "config.yaml")


@pytest.fixture
def eido_project_object(eido_project_cfg_path):
    from peprs import Project

    return Project(eido_project_cfg_path)


@pytest.fixture
def schema_file_path(eido_schemas_path):
    return os.path.join(eido_schemas_path, "test_schema.yaml")


@pytest.fixture
def schema_samples_file_path(eido_schemas_path):
    return os.path.join(eido_schemas_path, "test_schema_samples.yaml")


@pytest.fixture
def schema_invalid_file_path(eido_schemas_path):
    return os.path.join(eido_schemas_path, "test_schema_invalid.yaml")


@pytest.fixture
def schema_sample_invalid_file_path(eido_schemas_path):
    return os.path.join(eido_schemas_path, "test_schema_sample_invalid.yaml")


@pytest.fixture
def schema_imports_file_path(eido_schemas_path):
    return os.path.join(eido_schemas_path, "test_schema_imports.yaml")


@pytest.fixture
def schema_file_existing(eido_schemas_path):
    return os.path.join(eido_schemas_path, "schema_test_file_exist.yaml")


@pytest.fixture
def pep_file_existing(eido_peps_path):
    return os.path.join(eido_peps_path, "test_file_existing", "config.yaml")


@pytest.fixture
def schema_value_check(eido_schemas_path):
    return os.path.join(eido_schemas_path, "value_check_schema.yaml")


@pytest.fixture
def pep_value_check(eido_peps_path):
    return os.path.join(eido_peps_path, "value_check_pep", "config.yaml")
