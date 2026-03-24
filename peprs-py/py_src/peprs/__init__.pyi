from typing import Any, Dict, List, Optional

from polars import DataFrame as PolarsDataFrame
from pandas import DataFrame as PandasDataFrame

class Project:
    """A PEP (Portable Encapsulated Projects) project.

    Wraps a Rust-backed PEP project providing access to sample metadata,
    project configuration, and serialization methods.
    """

    name: Optional[str]
    """Project name, or None if not set."""

    description: Optional[str]
    """Project description, or None if not set."""

    pep_version: str
    """PEP specification version string."""

    config: Optional[Dict[str, Any]]
    """Raw project configuration as a dictionary, or None if no config exists."""

    samples: SamplesIter
    """Iterator over processed samples. Each sample is a dict of column names to values."""

    def __init__(
        self,
        path: str,
        amendments: Optional[List[str]] = None,
        sample_table_index: Optional[str] = None,
        subsample_table_index: Optional[List[str]] = None,
    ) -> None:
        """Create a new Project from a YAML config or CSV file path.

        :param path: path to a .yaml/.yml config or .csv sample table
        :param amendments: list of amendment names to activate
        :param sample_table_index: column name to use as the sample index
        :param subsample_table_index: column names for subsample table indices
        """
        ...

    @classmethod
    def from_polars(
        cls,
        df: PolarsDataFrame,
        sample_table_index: Optional[str] = None,
    ) -> "Project":
        """Create a Project from a Polars DataFrame.

        :param df: a Polars DataFrame with sample data
        :param sample_table_index: column name for the sample index (default: "sample_name")
        """
        ...

    @classmethod
    def from_pandas(
        cls,
        df: PandasDataFrame,
        sample_table_index: Optional[str] = None,
    ) -> "Project":
        """Create a Project from a Pandas DataFrame.

        :param df: a Pandas DataFrame with sample data
        :param sample_table_index: column name for the sample index (default: "sample_name")
        """
        ...

    @classmethod
    def from_dict(cls, pep_dictionary: Dict[str, Any]) -> "Project":
        """Create a Project from a Python dict.

        :param pep_dictionary: dict with keys "config", "samples", and optionally "subsamples".
            "config" should be a dict of project config.
            "samples" should be a list of sample dicts.
            "subsamples" should be a list of lists of subsample dicts.
        """
        ...

    @classmethod
    def from_pephub(cls, registry: str) -> "Project":
        """Create a Project from a PEPHub registry path.

        :param registry: PEPHub registry path (e.g. "namespace/name:tag")
        """
        ...

    def to_dict(
        self, raw: bool = False, by_sample: bool = True
    ) -> Dict[str, Any]:
        """Convert the project to a Python dict.

        :param raw: if True, include raw config/samples/subsamples; otherwise processed samples only
        :param by_sample: if True, samples are a list of row-dicts; if False, a column-dict
        :return: dict with "config", "samples", and optionally "subsamples" keys
        """
        ...

    def to_polars(self, raw: bool = False) -> PolarsDataFrame:
        """Return the samples as a Polars DataFrame.

        :param raw: if True, return raw (unprocessed) samples; otherwise processed
        :return: Polars DataFrame of samples
        """
        ...

    def to_pandas(self, raw: bool = False) -> PandasDataFrame:
        """Return the samples as a Pandas DataFrame.

        :param raw: if True, return raw (unprocessed) samples; otherwise processed
        :return: Pandas DataFrame of samples
        """
        ...

    def write_yaml(self, path: str) -> None:
        """Write processed samples to a YAML file.

        :param path: destination file path
        """
        ...

    def write_json(self, path: str) -> None:
        """Write processed samples to a JSON file.

        :param path: destination file path
        """
        ...

    def write_csv(self, path: str) -> None:
        """Write processed samples to a CSV file.

        :param path: destination file path
        """
        ...

    def write_raw(self, path: str, zipped: bool = False) -> None:
        """Write the raw project (config, samples, subsamples) to disk.

        :param path: destination path (folder or zip file)
        :param zipped: if True, write as a zip archive; otherwise as a folder
        """
        ...

    def to_yaml_string(self) -> str:
        """Return processed samples as a YAML string."""
        ...

    def to_json_string(self) -> str:
        """Return processed samples as a JSON string."""
        ...

    def to_csv_string(self) -> str:
        """Return processed samples as a CSV string."""
        ...

    def get_sample(self, name: str) -> Dict[str, Any]:
        """Look up a single sample by name.

        :param name: sample name to look up
        :return: dict of column names to values for the matching sample
        :raises ValueError: if the sample name is not found
        """
        ...

    def len(self) -> int:
        """Return the number of samples in the project."""
        ...

    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...


class SamplesIter:
    """Iterator over project samples.

    Each iteration yields a sample as a dict of column names to values.
    Supports indexing (including negative indices) and len().
    """

    def __iter__(self) -> "SamplesIter": ...

    def __next__(self) -> Dict[str, Any]:
        """Yield the next sample as a dict."""
        ...

    def __getitem__(self, index: int) -> Dict[str, Any]:
        """Get a sample by index. Supports negative indexing.

        :param index: zero-based index; negative values count from the end
        :return: dict of column names to values
        """
        ...

    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
