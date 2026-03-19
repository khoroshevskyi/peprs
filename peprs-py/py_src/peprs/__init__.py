from .peprs import *  # noqa: F403

# The Rust extension registers peprs.eido in sys.modules, which shadows the
# Python eido/ package.  Inject pure-Python additions into that module so
# `from peprs.eido import schema_from_pydantic` works.
import sys as _sys

from peprs._pydantic import schema_from_pydantic as _schema_from_pydantic

_eido_mod = _sys.modules.get("peprs.eido")
if _eido_mod is not None:
    _eido_mod.schema_from_pydantic = _schema_from_pydantic
del _schema_from_pydantic, _eido_mod, _sys
