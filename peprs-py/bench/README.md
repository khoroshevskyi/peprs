# Benchmarks: peppy vs peprs

Compares initialization time, validation time, and memory usage between peppy (Python) and peprs (Rust bindings).

## Prerequisites

```bash
pip install peppy eido matplotlib
cd peprs-py && maturin develop && cd ..
```

## Usage

Run all steps from the `peprs-py/bench/` directory.

### 1. Run benchmarks

```bash
python pyvrs.py
```

Options:
- `--paths path1.yaml path2.yaml` — custom PEP config paths
- `--schema path/to/schema.yaml` — eido schema for validation
- `--runs N` — number of repetitions (default: 3)
- `--output file.csv` — output file (default: bench_results.csv)

### 2. Summarize results

```bash
python summarize.py
```

Aggregates runs into per-(path, library) means. Options:
- `python summarize.py bench_results.csv --output bench_summary.csv`

### 3. Plot

```bash
python bench_plot.py
```

Generates three plots in `plots/`: init time, validation time, and memory usage.

Options:
- `--log-y` — log scale on the y-axis
- `--output-dir DIR` — save plots to a different directory

### All-in-one

```bash
python pyvrs.py && python summarize.py && python bench_plot.py
```
