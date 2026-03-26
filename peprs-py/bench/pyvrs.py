"""Benchmark peppy vs peprs: init time, memory, and validation."""

import argparse
import csv
import gc
import sys
import time
import tracemalloc
from pathlib import Path

from peppy import Project as PeppyProject
from peprs import Project as PeprsProject
from peprs.eido import validate_project as peprs_validate

try:
    from eido import validate_project as peppy_validate
except ImportError:
    peppy_validate = None

_BENCH_PEPS_DIR = "bench_peps"
_PEP_SIZES = [5, 20, 100, 500, 1000, 5000, 10000, 50000, 100000]#, 600000]
DEFAULT_PATHS = [
    f"{_BENCH_PEPS_DIR}/pep_{n}/config.yaml" for n in _PEP_SIZES
]

DEFAULT_SCHEMA = "../../peprs-eido/tests/data/schemas/schema_basic.yaml"
DEFAULT_N_RUNS = 3
DEFAULT_OUTPUT = "bench_results.csv"


def bench_init(lib: str, path: str):
    """Benchmark project initialization. Returns (project, time_s, memory_bytes)."""
    gc.collect()
    tracemalloc.start()
    tracemalloc.reset_peak()

    start = time.perf_counter()
    if lib == "peppy":
        project = PeppyProject(path)
    else:
        project = PeprsProject(path)
    elapsed = time.perf_counter() - start

    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    return project, elapsed, peak


def bench_validate(lib: str, project, schema_path: str):
    """Benchmark validation. Returns (time_s, passed)."""
    start = time.perf_counter()
    try:
        if lib == "peppy":
            if peppy_validate is None:
                return 0.0, None
            peppy_validate(project, schema_path)
        else:
            peprs_validate(project, schema_path)
        passed = True
    except Exception:
        passed = False
    elapsed = time.perf_counter() - start
    return elapsed, passed


def sample_count(lib: str, project) -> int:
    if lib == "peppy":
        return len(project.samples)
    return len(project.samples)


def run_benchmarks(paths: list[str], schema_path: str, n_runs: int):
    results = []
    for path in paths:
        for run in range(1, n_runs + 1):
            for lib in ["peppy", "peprs"]:
                project, init_time, mem = bench_init(lib, path)
                n = sample_count(lib, project)
                val_time, val_passed = bench_validate(lib, project, schema_path)
                row = {
                    "path": path,
                    "library": lib,
                    "run": run,
                    "n_samples": n,
                    "init_time_s": f"{init_time:.6f}",
                    "memory_bytes": mem,
                    "validation_time_s": f"{val_time:.6f}",
                    "validation_passed": val_passed,
                }
                results.append(row)
                print(
                    f"  {lib:6s} | run {run} | {n:>6d} samples | "
                    f"init {init_time:.4f}s | mem {mem:>10d}B | "
                    f"validate {val_time:.4f}s | passed={val_passed}"
                )
    return results


def write_csv(results: list[dict], output_path: str):
    if not results:
        print("No results to write.")
        return
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)
    print(f"\nResults written to {output_path}")


def print_summary(results: list[dict]):
    """Print mean times per library and speedup ratios."""
    from collections import defaultdict

    init_times = defaultdict(list)
    val_times = defaultdict(list)

    for r in results:
        lib = r["library"]
        init_times[lib].append(float(r["init_time_s"]))
        val_times[lib].append(float(r["validation_time_s"]))

    print("\n--- Summary ---")
    print(f"{'Library':<8s} {'Mean Init (s)':>14s} {'Mean Validate (s)':>18s}")
    print("-" * 44)
    for lib in ["peppy", "peprs"]:
        if lib not in init_times:
            continue
        mean_init = sum(init_times[lib]) / len(init_times[lib])
        mean_val = sum(val_times[lib]) / len(val_times[lib])
        print(f"{lib:<8s} {mean_init:>14.6f} {mean_val:>18.6f}")

    if "peppy" in init_times and "peprs" in init_times:
        peppy_init = sum(init_times["peppy"]) / len(init_times["peppy"])
        peprs_init = sum(init_times["peprs"]) / len(init_times["peprs"])
        peppy_val = sum(val_times["peppy"]) / len(val_times["peppy"])
        peprs_val = sum(val_times["peprs"]) / len(val_times["peprs"])
        print(f"\nSpeedup (peppy / peprs):")
        if peprs_init > 0:
            print(f"  Init:     {peppy_init / peprs_init:.2f}x")
        if peprs_val > 0:
            print(f"  Validate: {peppy_val / peprs_val:.2f}x")


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark peppy vs peprs")
    parser.add_argument(
        "--paths",
        nargs="+",
        default=DEFAULT_PATHS,
        help="PEP config file paths to benchmark",
    )
    parser.add_argument(
        "--schema",
        default=DEFAULT_SCHEMA,
        help="Eido schema path for validation",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=DEFAULT_N_RUNS,
        help="Number of repetitions per path per library",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="Output CSV file path",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    print(f"Benchmarking {len(args.paths)} path(s), {args.runs} run(s) each\n")
    results = run_benchmarks(args.paths, args.schema, args.runs)
    write_csv(results, args.output)
    print_summary(results)


if __name__ == "__main__":
    main()
