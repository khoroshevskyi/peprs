import os

import pandas as pd
import polars as pl
import pytest

from peprs import Project, Sample

from .conftest import EXAMPLE_TYPES, get_example_pep_path


class TestProjectConstructor:
    def test_nonexistent(self):
        """Verify that an error is raised for a nonexistent config file."""
        with pytest.raises(OSError):
            Project("nonexistentfile.yaml")

    @pytest.mark.parametrize("example_pep_cfg_path", EXAMPLE_TYPES, indirect=True)
    def test_instantiation(self, example_pep_cfg_path):
        """Verify that a Project is created for every example PEP."""
        p = Project(example_pep_cfg_path)
        assert isinstance(p, Project)

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic", "imply"], indirect=True)
    def test_csv_init(self, example_pep_cfg_path):
        """Verify that a CSV file can be used to initialize a project."""
        csv_path = os.path.join(os.path.dirname(example_pep_cfg_path), "sample_table.csv")
        p = Project(csv_path)
        assert isinstance(p, Project)
        assert len(p) > 0

    @pytest.mark.parametrize("example_pep_cfg_path", ["amendments1"], indirect=True)
    def test_amendments(self, example_pep_cfg_path):
        """Verify that amendments are applied at construction."""
        p = Project(example_pep_cfg_path, amendments=["newLib"])
        assert all(s["protocol"] == "ABCD" for s in p.samples)

    @pytest.mark.parametrize("example_pep_cfg_path", ["amendments1"], indirect=True)
    def test_missing_amendment_raises(self, example_pep_cfg_path):
        """Verify that an invalid amendment name raises an error."""
        with pytest.raises(ValueError):
            Project(example_pep_cfg_path, amendments=["nonexistent"])

    @pytest.mark.parametrize("example_pep_cfg_path", EXAMPLE_TYPES, indirect=True)
    def test_description_type(self, example_pep_cfg_path):
        """Verify that description is a string or None."""
        p = Project(example_pep_cfg_path)
        assert p.description is None or isinstance(p.description, str)

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_pep_version(self, example_pep_cfg_path):
        """Verify pep_version is a string."""
        p = Project(example_pep_cfg_path)
        assert isinstance(p.pep_version, str)

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_name_getter(self, example_pep_cfg_path):
        """Verify name property returns a string or None."""
        p = Project(example_pep_cfg_path)
        name = p.name
        assert name is None or isinstance(name, str)

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_name_setter(self, example_pep_cfg_path):
        """Verify name can be set."""
        p = Project(example_pep_cfg_path)
        p.name = "new_name"
        assert p.name == "new_name"

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_description_setter(self, example_pep_cfg_path):
        """Verify description can be set."""
        p = Project(example_pep_cfg_path)
        p.description = "new_description"
        assert p.description == "new_description"

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_config_property(self, example_pep_cfg_path):
        """Verify config property returns a dict."""
        p = Project(example_pep_cfg_path)
        config = p.config
        assert isinstance(config, dict)

    @pytest.mark.parametrize("example_pep_cfg_path", EXAMPLE_TYPES, indirect=True)
    def test_len(self, example_pep_cfg_path):
        """Verify len(project) returns a positive integer."""
        p = Project(example_pep_cfg_path)
        assert len(p) > 0

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_repr(self, example_pep_cfg_path):
        """Verify repr returns a non-empty string."""
        p = Project(example_pep_cfg_path)
        r = repr(p)
        assert isinstance(r, str)
        assert len(r) > 0


class TestSampleAccess:
    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_get_sample(self, example_pep_cfg_path):
        """Verify get_sample returns a dict for a valid sample name."""
        p = Project(example_pep_cfg_path)
        first = list(p.samples)[0]
        name = first["sample_name"]
        result = p.get_sample(name)
        assert isinstance(result, Sample)
        assert result["sample_name"] == name

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_get_sample_nonexistent(self, example_pep_cfg_path):
        """Verify get_sample raises ValueError for a missing sample."""
        p = Project(example_pep_cfg_path)
        with pytest.raises(ValueError):
            p.get_sample("nonexistent_sample")

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_samples_iteration(self, example_pep_cfg_path):
        """Verify iterating over samples yields dicts."""
        p = Project(example_pep_cfg_path)
        samples = list(p.samples)
        assert len(samples) == len(p)
        for s in samples:
            assert isinstance(s, Sample)
            assert "sample_name" in s

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_samples_indexing(self, example_pep_cfg_path):
        """Verify index and negative index access on samples."""
        p = Project(example_pep_cfg_path)
        first = p.samples[0]
        last = p.samples[-1]
        assert isinstance(first, Sample)
        assert isinstance(last, Sample)
        assert "sample_name" in first

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_samples_index_out_of_range(self, example_pep_cfg_path):
        """Verify IndexError on out-of-range sample access."""
        p = Project(example_pep_cfg_path)
        with pytest.raises(IndexError):
            p.samples[9999]

    @pytest.mark.parametrize("example_pep_cfg_path", ["subtable1"], indirect=True)
    def test_subsample_merging(self, example_pep_cfg_path):
        """Verify subsample table merging produces list values in samples."""
        p = Project(example_pep_cfg_path)
        samples = list(p.samples)
        # At least one sample should have a list-type 'file' from subsample merging
        has_list = any(isinstance(s.get("file"), list) for s in samples)
        assert has_list


class TestPySample:
    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_attribute_access(self, example_pep_cfg_path):
        """Verify sample.key works the same as sample['key']."""
        p = Project(example_pep_cfg_path)
        s = p.samples[0]
        assert s.sample_name == s["sample_name"]

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_contains(self, example_pep_cfg_path):
        """Verify 'key in sample' works."""
        p = Project(example_pep_cfg_path)
        s = p.samples[0]
        assert "sample_name" in s
        assert "nonexistent_key" not in s

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_keys(self, example_pep_cfg_path):
        """Verify keys() returns column names."""
        p = Project(example_pep_cfg_path)
        s = p.samples[0]
        assert "sample_name" in s.keys()

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_get_default(self, example_pep_cfg_path):
        """Verify get() with default value."""
        p = Project(example_pep_cfg_path)
        s = p.samples[0]
        assert s.get("nonexistent", "default") == "default"
        assert s.get("nonexistent") is None

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_to_dict(self, example_pep_cfg_path):
        """Verify to_dict() returns a plain dict."""
        p = Project(example_pep_cfg_path)
        s = p.samples[0]
        d = s.to_dict()
        assert isinstance(d, dict)
        assert d["sample_name"] == s["sample_name"]

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_missing_attr_raises(self, example_pep_cfg_path):
        """Verify AttributeError on missing attribute."""
        p = Project(example_pep_cfg_path)
        s = p.samples[0]
        with pytest.raises(AttributeError):
            _ = s.nonexistent_attr

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_missing_key_raises(self, example_pep_cfg_path):
        """Verify KeyError on missing key."""
        p = Project(example_pep_cfg_path)
        s = p.samples[0]
        with pytest.raises(KeyError):
            _ = s["nonexistent_key"]

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_len(self, example_pep_cfg_path):
        """Verify len(sample) returns number of fields."""
        p = Project(example_pep_cfg_path)
        s = p.samples[0]
        assert len(s) > 0

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_repr(self, example_pep_cfg_path):
        """Verify repr includes 'Sample('."""
        p = Project(example_pep_cfg_path)
        s = p.samples[0]
        assert "Sample(" in repr(s)

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_items(self, example_pep_cfg_path):
        """Verify items() returns key-value pairs."""
        p = Project(example_pep_cfg_path)
        s = p.samples[0]
        items = s.items()
        assert len(items) == len(s)
        keys = [k for k, v in items]
        assert "sample_name" in keys

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_values(self, example_pep_cfg_path):
        """Verify values() returns the right count."""
        p = Project(example_pep_cfg_path)
        s = p.samples[0]
        assert len(s.values()) == len(s)


class TestAlternativeConstructors:
    @pytest.mark.parametrize("example_pep_csv_path", ["basic"], indirect=True)
    def test_from_pandas(self, example_pep_csv_path):
        """Verify Project.from_pandas creates a valid project."""
        df = pd.read_csv(example_pep_csv_path, dtype=str)
        p = Project.from_pandas(df)
        assert isinstance(p, Project)
        assert len(p) == len(df)

    @pytest.mark.parametrize("example_pep_csv_path", ["basic"], indirect=True)
    def test_from_polars(self, example_pep_csv_path):
        """Verify Project.from_polars creates a valid project."""
        df = pl.read_csv(example_pep_csv_path, infer_schema_length=0)
        p = Project.from_polars(df)
        assert isinstance(p, Project)
        assert len(p) == len(df)

    @pytest.mark.parametrize("example_pep_cfg_path", ["basic"], indirect=True)
    def test_from_dict(self, example_pep_cfg_path):
        """Verify Project.from_dict round-trips through to_dict."""
        p1 = Project(example_pep_cfg_path)
        d = p1.to_dict(raw=True)
        p2 = Project.from_dict(d)
        assert isinstance(p2, Project)
        assert len(p2) == len(p1)

    @pytest.mark.parametrize("example_pep_cfg_path", ["subtable1"], indirect=True)
    def test_from_dict_with_subsamples(self, example_pep_cfg_path):
        """Verify from_dict preserves subsamples."""
        p1 = Project(example_pep_cfg_path)
        d = p1.to_dict(raw=True)
        assert "subsamples" in d
        p2 = Project.from_dict(d)
        assert len(p2) == len(p1)
