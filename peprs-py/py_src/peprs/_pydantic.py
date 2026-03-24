"""Convert Pydantic models to eido-compatible validation schemas."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type

if TYPE_CHECKING:
    from peprs import Project


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
    :return: dict passable to ``peprs.eido.validate_project()``.
    """
    if sample_model is None and config_model is None:
        raise ValueError("At least one of sample_model or config_model must be provided")

    schema: Dict[str, Any] = {"properties": {}}

    if sample_model is not None:
        sample_js = _get_json_schema(sample_model)
        schema["properties"]["samples"] = {
            "type": "array",
            "items": sample_js,
        }

    if config_model is not None:
        config_js = _get_json_schema(config_model)
        if "properties" in config_js:
            schema["properties"].update(config_js["properties"])
        if "required" in config_js:
            schema["required"] = config_js["required"]

    if tangible:
        schema["tangible"] = tangible
    if files:
        schema["files"] = files

    return schema


def validate_with_pydantic(
    project: Project,
    sample_model: Optional[Type[Any]] = None,
    config_model: Optional[Type[Any]] = None,
    *,
    tangible: Optional[List[str]] = None,
    files: Optional[List[str]] = None,
) -> None:
    """Validate a PEP project using Pydantic model classes.

    Convenience wrapper that converts models to a schema dict via
    ``schema_from_pydantic`` and then calls ``validate_project``.

    :param project: the Project to validate.
    :param sample_model: Pydantic model class defining per-sample attributes.
    :param config_model: Pydantic model class defining project-level config attributes.
    :param tangible: sample attributes that must point to existing files.
    :param files: sample attributes that may point to files (optional existence).
    :raises EidoValidationError: if validation fails.
    :raises PathAttrNotFoundError: if required files are missing.
    """
    from peprs.eido import validate_project

    schema = schema_from_pydantic(
        sample_model, config_model, tangible=tangible, files=files
    )
    validate_project(project, schema)


def _get_json_schema(model: Type[Any]) -> Dict[str, Any]:
    """Extract a JSON Schema dict from a Pydantic model, resolving $defs."""
    if not hasattr(model, "model_json_schema"):
        raise TypeError(
            f"{model!r} is not a Pydantic v2 model (missing model_json_schema)"
        )

    js = model.model_json_schema()

    # Pydantic v2 puts nested model definitions in $defs and uses $ref.
    # Inline them so the schema is self-contained for jsonschema validation.
    defs = js.pop("$defs", None)
    if defs:
        js = _resolve_refs(js, defs)

    # Strip metadata keys that aren't useful for validation
    js.pop("title", None)

    return js


def _resolve_refs(node: Any, defs: Dict[str, Any]) -> Any:
    """Recursively inline $ref pointers using the $defs map."""
    if isinstance(node, dict):
        if "$ref" in node:
            ref_path = node["$ref"]  # e.g. "#/$defs/SubModel"
            ref_name = ref_path.rsplit("/", 1)[-1]
            if ref_name in defs:
                resolved = defs[ref_name].copy()
                resolved.pop("title", None)
                return _resolve_refs(resolved, defs)
        return {k: _resolve_refs(v, defs) for k, v in node.items()}
    if isinstance(node, list):
        return [_resolve_refs(item, defs) for item in node]
    return node
