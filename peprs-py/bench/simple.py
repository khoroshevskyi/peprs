from time import time

from peprs import Project as PeprsProject
from peppy import Project as PeppyProject

def benchmark_peppy(pep_path: str):
    start = time()
    proj = PeppyProject(pep_path)
    end = time()
    print(f"Peppy loaded project with {len(proj.samples)} samples in {end - start:.2f} seconds.")
    return proj

def benchmark_peprs(pep_path: str):
    start = time()
    proj = PeprsProject(pep_path)
    end = time()
    print(f"Peprs loaded project with {len(proj.samples)} samples in {end - start:.2f} seconds.")
    return proj

PATH = "/Users/nathanleroy/Desktop/databio-encode_hg38_filtered-default/project_config.yaml"

benchmark_peppy(PATH)
benchmark_peprs(PATH)