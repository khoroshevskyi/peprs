from typing import Any, Dict, List, Optional, Type, Union, Sequence

from peprs import Project

class EidoValidationError(Exception):
    """
    Raised when project or sample validation fails against an eido schema.

    The ``errors_by_type`` attribute groups errors by category.
    Keys are category strings (``"type_mismatch"``, ``"missing_required"``,
    ``"missing_column"``, ``"validation"``).  Values are lists of dicts with
    ``"path"``, ``"message"``, and optionally ``"sample_names"`` (a list of
    affected sample names).
    """

    errors_by_type: Dict[str, List[Dict[str, Any]]]

class PathAttrNotFoundError(Exception):
    """
    Raised when required input files specified in the schema are missing.

    The ``missing_files`` attribute contains a list of dicts, each with
    "sample_name", "attribute", and "path" keys.
    """

    missing_files: List[Dict[str, str]]

def validate_project(
    project: Project, schema: Union[str, Dict[str, Any]]
) -> None:
    """Validate a PEP project against an eido schema (both config and samples).

    :param project: the Project to validate
    :param schema: path to a schema file, or a pre-loaded schema dict
    :raises EidoValidationError: if validation fails
    :raises PathAttrNotFoundError: if file is not found in provided path

    """
    ...

def validate_sample(
    project: Project,
    sample_name: str,
    schema: Union[str, Dict[str, Any]],
) -> None:
    """Validate a single sample by name against an eido schema.

    :param project: the Project containing the sample
    :param sample_name: name of the sample to validate
    :param schema: path to a schema file, or a pre-loaded schema dict
    :raises ValueError: if the sample name is not found
    :raises EidoValidationError: if validation fails
    """
    ...

def validate_config(
    project: Project, schema: Union[str, Dict[str, Any]]
) -> None:
    """Validate only the project-level config against an eido schema.

    :param project: the Project to validate
    :param schema: path to a schema file, or a pre-loaded schema dict
    :raises EidoValidationError: if validation fails
    """
    ...

def validate_input_files(
    project: Project, schema: Union[str, Dict[str, Any]]
) -> None:
    """
    Validate that tangible file attributes point to existing files.

    :param project: the Project to validate
    :param schema: path to a schema file, or a pre-loaded schema dict
    :raises PathAttrNotFoundError: if required files are missing
    """
    ...

def schema_from_pydantic(
    sample_model: Optional[Type[Any]] = None,
    config_model: Optional[Type[Any]] = None,
    *,
    tangible: Optional[List[str]] = None,
    files: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Build an eido-compatible schema dict from Pydantic model classes.

    :param sample_model: Pydantic model class defining per-sample attributes.
    :param config_model: Pydantic model class defining project-level config attributes.
    :param tangible: sample attributes that must point to existing files.
    :param files: sample attributes that may point to files (optional existence).
    :return: dict passable to ``validate_project()``.
    """
    ...
