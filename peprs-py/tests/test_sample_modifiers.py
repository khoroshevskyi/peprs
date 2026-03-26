import pytest

from peprs import Project


class TestSampleModifiers:
    @pytest.mark.parametrize("example_pep_cfg_path", ["append"], indirect=True)
    def test_append(self, example_pep_cfg_path):
        """Verify that the appended attribute is added to all samples."""
        p = Project(example_pep_cfg_path)
        assert all(s["read_type"] == "SINGLE" for s in p.samples)

    @pytest.mark.parametrize("example_pep_cfg_path", ["imports"], indirect=True)
    def test_imports(self, example_pep_cfg_path):
        """Verify that the imported attribute is added to all samples."""
        p = Project(example_pep_cfg_path)
        assert all(s["imported_attr"] == "imported_val" for s in p.samples)

    @pytest.mark.parametrize("example_pep_cfg_path", ["imply"], indirect=True)
    def test_imply(self, example_pep_cfg_path):
        """Verify that implied attributes are set correctly per condition."""
        p = Project(example_pep_cfg_path)
        for s in p.samples:
            if s["organism"] == "human":
                assert s["genome"] == "hg38"
            elif s["organism"] == "mouse":
                assert s["genome"] == "mm10"

    @pytest.mark.parametrize("example_pep_cfg_path", ["duplicate"], indirect=True)
    def test_duplicate(self, example_pep_cfg_path):
        """Verify that the duplicated attribute equals the original."""
        p = Project(example_pep_cfg_path)
        assert all(s["organism"] == s["animal"] for s in p.samples)

    @pytest.mark.parametrize("example_pep_cfg_path", ["derive"], indirect=True)
    def test_derive(self, example_pep_cfg_path):
        """Verify that the derived attribute exists on all samples."""
        p = Project(example_pep_cfg_path)
        assert all("file_path" in s for s in p.samples)

    @pytest.mark.parametrize("example_pep_cfg_path", ["remove"], indirect=True)
    def test_remove(self, example_pep_cfg_path):
        """Verify that the removed attribute is absent from all samples."""
        p = Project(example_pep_cfg_path)
        assert all("protocol" not in s for s in p.samples)

    @pytest.mark.parametrize("example_pep_cfg_path", ["subtable2"], indirect=True)
    def test_subtable(self, example_pep_cfg_path):
        """Verify that subsample merging produces list values for multi-valued samples."""
        p = Project(example_pep_cfg_path)
        samples = list(p.samples)
        multi_samples = [
            s for s in samples if s["sample_name"] in ("frog_1", "frog_2")
        ]
        assert all(isinstance(s["file"], list) for s in multi_samples)
