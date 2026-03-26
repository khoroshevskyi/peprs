<h1 align="center">
<img src="docs/img/peprs_logo.svg" alt="peprs logo" height="100px">
</h1>

`peprs` - A spicy 🌶️ library for managing biological sample metadata to enable reproducible and scalable bioinformatics

Don't let sample metadata parsing bottleneck your pipelines!

## About this project

`peprs` is a rust implementation of the [PEP specification](https://pep.databio.org/) and expanded ecosystem. In short, PEP is a framework for managing biological sample metadata. PEP is a **community driven** effort to create a **fast**, **reliable**, **reusable**, and **scalable** library for handling biological sample metadata.

PEP and its ecosystem is developed and maintained by the [Databio](https://databio.org) team. As a challenge and learning experience, we have been rewriting the core components of the PEP ecosystem in Rust for performance and reliability.

We are starting with the core PEP specification for metadata management and will expand to include the full ecosystem (looper, pephub-client, pipestat). The core PEP specification is implemented in the `peprs-core` crate. The Python bindings are implemented in the `peprs-py` crate.

### 📦 Modules

- **[peprs-core](peprs-core/)** — Core library implementing the PEP specification. With core module user can create pep objects and do all kind of manipulations.
- **[peprs-eido](peprs-eido/)** — Schema-based validation of PEP projects against JSON schemas with eido-specific extensions (imports, tangible file checks).
- **[peprs-cli](peprs-cli/)** — Command-line interface with `inspect`, `validate`, and `convert` subcommands.
- **[peprs-py](peprs-py/)** — Python bindings via PyO3. Exposes the `Project` class with full Polars/Pandas DataFrame interoperability.
- **[pephub-client](pephub-client/)** — Work in progress

## ⚙️ Installation

### Python (recommended)

```bash
pip install peprs
```

### Rust

Add to your `Cargo.toml`:

```toml
[dependencies]
peprs-core = { git = "https://github.com/pepkit/peprs" }
```

### CLI (from source)

```bash
cargo install --path peprs-cli
```

Or from Python:
```
pip install peprs
```

## 🐍 Quick Python example

```python
import peprs

# Load a PEP from a YAML config file
project = peprs.Project("path/to/project_config.yaml")
# or
project = peprs.Project.from_pephub("databio/example:default")

# Inspect the project
print(project.name)
print(project.description)
print(len(project))  # number of samples

# Get samples as a Polars DataFrame
df_pl = project.to_polars()
print(df_pl)

# Get samples as a Pandas DataFrame
df_pd = project.to_pandas()
print(df_pd)

# Look up a single sample by name
sample = project.get_sample("3-1_11102016")

# Iterate over samples
for sample in project.samples:
    print(sample)

# Convert projects
project.write_csv("output.csv")
project.write_yaml("output.yaml")
project.write_json("output.json")
```

## 🚀 Afterword

We are looking forward to integrating this project with WDL, Snakemake, and Nextflow. All contributions are welcome. Please open an issue or submit a pull request.