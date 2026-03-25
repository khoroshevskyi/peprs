import json
import os
import tempfile

import pandas as pd
import polars as pl
import pytest
import yaml

from peprs import Project


class TestToDict:
    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_to_dict(self, example_pep_cfg_path):
        """Verify to_dict returns dict with 'samples' key as list of dicts."""
        p = Project(example_pep_cfg_path)
        d = p.to_dict()
        assert "samples" in d
        assert isinstance(d["samples"], list)
        assert all(isinstance(s, dict) for s in d["samples"])
        assert len(d["samples"]) == len(p)

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_to_dict_raw(self, example_pep_cfg_path):
        """Verify to_dict(raw=True) includes config, samples, and optionally subsamples."""
        p = Project(example_pep_cfg_path)
        d = p.to_dict(raw=True)
        assert "config" in d
        assert "samples" in d
        assert isinstance(d["config"], dict)

    @pytest.mark.parametrize("example_pep_cfg_path", ["subtable1"], indirect=True)
    def test_to_dict_raw_with_subsamples(self, example_pep_cfg_path):
        """Verify to_dict(raw=True) includes subsamples when present."""
        p = Project(example_pep_cfg_path)
        d = p.to_dict(raw=True)
        assert "subsamples" in d

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_to_dict_by_column(self, example_pep_cfg_path):
        """Verify to_dict(by_sample=False) returns column-oriented dict."""
        p = Project(example_pep_cfg_path)
        d = p.to_dict(by_sample=False)
        assert "samples" in d
        samples = d["samples"]
        assert isinstance(samples, dict)
        # Each value should be a list of column values
        for col_values in samples.values():
            assert isinstance(col_values, list)


class TestToDataFrame:
    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_to_polars(self, example_pep_cfg_path):
        """Verify to_polars returns a polars DataFrame."""
        p = Project(example_pep_cfg_path)
        df = p.to_polars()
        assert isinstance(df, pl.DataFrame)
        assert len(df) == len(p)

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_to_polars_raw(self, example_pep_cfg_path):
        """Verify to_polars(raw=True) returns the unprocessed samples."""
        p = Project(example_pep_cfg_path)
        df_raw = p.to_polars(raw=True)
        df_processed = p.to_polars(raw=False)
        assert isinstance(df_raw, pl.DataFrame)
        assert isinstance(df_processed, pl.DataFrame)

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_to_pandas(self, example_pep_cfg_path):
        """Verify to_pandas returns a pandas DataFrame."""
        p = Project(example_pep_cfg_path)
        df = p.to_pandas()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == len(p)


class TestToString:
    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_to_yaml_string(self, example_pep_cfg_path):
        """Verify to_yaml_string returns valid YAML containing sample_name."""
        p = Project(example_pep_cfg_path)
        s = p.to_yaml_string()
        assert isinstance(s, str)
        parsed = yaml.safe_load(s)
        assert parsed is not None

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_to_json_string(self, example_pep_cfg_path):
        """Verify to_json_string returns valid JSON."""
        p = Project(example_pep_cfg_path)
        s = p.to_json_string()
        assert isinstance(s, str)
        parsed = json.loads(s)
        assert isinstance(parsed, list)

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_to_csv_string(self, example_pep_cfg_path):
        """Verify to_csv_string returns a CSV with header and rows."""
        p = Project(example_pep_cfg_path)
        s = p.to_csv_string()
        assert isinstance(s, str)
        lines = s.strip().split("\n")
        # Header + at least one data row
        assert len(lines) > 1
        assert "sample_name" in lines[0]


class TestWriteFiles:
    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_write_yaml(self, example_pep_cfg_path):
        """Verify write_yaml creates a file with sample data."""
        p = Project(example_pep_cfg_path)
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "samples.yaml")
            p.write_yaml(path)
            assert os.path.exists(path)
            with open(path) as f:
                content = f.read()
            assert "sample_name" in content

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_write_json(self, example_pep_cfg_path):
        """Verify write_json creates a valid JSON file."""
        p = Project(example_pep_cfg_path)
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "samples.json")
            p.write_json(path)
            assert os.path.exists(path)
            with open(path) as f:
                data = json.load(f)
            assert isinstance(data, list)

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_write_csv(self, example_pep_cfg_path):
        """Verify write_csv creates a CSV file with header."""
        p = Project(example_pep_cfg_path)
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "samples.csv")
            p.write_csv(path)
            assert os.path.exists(path)
            with open(path) as f:
                header = f.readline()
            assert "sample_name" in header
