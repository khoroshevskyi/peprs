"""Summarize benchmark CSV: aggregate runs into median values per (path, library)."""

import argparse
import csv
import statistics
from collections import defaultdict


def read_results(input_path: str) -> list[dict]:
    with open(input_path, newline="") as f:
        return list(csv.DictReader(f))


def summarize(rows: list[dict]) -> list[dict]:
    groups = defaultdict(list)
    for row in rows:
        key = (row["path"], row["library"])
        groups[key].append(row)

    summary = []
    for (path, lib), runs in sorted(groups.items()):
        n_runs = len(runs)
        median_init = statistics.median(float(r["init_time_s"]) for r in runs)
        median_mem = statistics.median(int(r["memory_bytes"]) for r in runs)
        median_val = statistics.median(float(r["validation_time_s"]) for r in runs)
        n_samples = int(runs[0]["n_samples"])
        val_passed = runs[0]["validation_passed"]
        summary.append({
            "path": path,
            "library": lib,
            "n_runs": n_runs,
            "n_samples": n_samples,
            "median_init_time_s": f"{median_init:.6f}",
            "median_memory_bytes": int(median_mem),
            "median_validation_time_s": f"{median_val:.6f}",
            "validation_passed": val_passed,
        })
    summary.sort(key=lambda r: r["n_samples"])
    return summary


def write_csv(summary: list[dict], output_path: str):
    if not summary:
        print("No data to write.")
        return
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=summary[0].keys())
        writer.writeheader()
        writer.writerows(summary)
    print(f"Summary written to {output_path}")


def print_table(summary: list[dict]):
    print(
        f"{'Path':<50s} {'Library':<8s} {'Runs':>4s} {'Samples':>8s} "
        f"{'Init (s)':>10s} {'Mem (B)':>12s} {'Valid (s)':>10s} {'Passed':>7s}"
    )
    print("-" * 115)
    for r in summary:
        print(
            f"{r['path']:<50s} {r['library']:<8s} {r['n_runs']:>4d} {r['n_samples']:>8d} "
            f"{r['median_init_time_s']:>10s} {r['median_memory_bytes']:>12d} "
            f"{r['median_validation_time_s']:>10s} {r['validation_passed']:>7s}"
        )

    # Speedup per path
    by_path = defaultdict(dict)
    for r in summary:
        by_path[r["path"]][r["library"]] = r

    print("\n--- Speedup (peppy / peprs) ---")
    for path, libs in sorted(by_path.items()):
        if "peppy" not in libs or "peprs" not in libs:
            continue
        peppy_init = float(libs["peppy"]["median_init_time_s"])
        peprs_init = float(libs["peprs"]["median_init_time_s"])
        peppy_val = float(libs["peppy"]["median_validation_time_s"])
        peprs_val = float(libs["peprs"]["median_validation_time_s"])
        print(f"\n  {path}")
        if peprs_init > 0:
            print(f"    Init:     {peppy_init / peprs_init:.2f}x")
        if peprs_val > 0:
            print(f"    Validate: {peppy_val / peprs_val:.2f}x")


def parse_args():
    parser = argparse.ArgumentParser(description="Summarize benchmark results")
    parser.add_argument(
        "input",
        nargs="?",
        default="bench_results.csv",
        help="Input CSV from pyvrs.py (default: bench_results.csv)",
    )
    parser.add_argument(
        "--output",
        default="bench_summary.csv",
        help="Output summary CSV (default: bench_summary.csv)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    rows = read_results(args.input)
    summary = summarize(rows)
    print_table(summary)
    write_csv(summary, args.output)


if __name__ == "__main__":
    main()
