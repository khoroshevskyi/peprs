"""Plot benchmark summary: init time, validation time, and memory vs sample count."""

import argparse
import csv
import os
from collections import defaultdict

import matplotlib.pyplot as plt


def read_summary(path: str) -> list[dict]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def plot(rows: list[dict], output_dir: str, log_y: bool):
    os.makedirs(output_dir, exist_ok=True)

    data = defaultdict(lambda: {"n_samples": [], "init": [], "validate": [], "memory": []})
    for r in rows:
        lib = r["library"]
        data[lib]["n_samples"].append(int(r["n_samples"]))
        data[lib]["init"].append(float(r["median_init_time_s"]))
        data[lib]["validate"].append(float(r["median_validation_time_s"]))
        data[lib]["memory"].append(int(r["median_memory_bytes"]))

    # Plot 1: Init time
    fig, ax = plt.subplots()
    for lib in ["peppy", "peprs"]:
        if lib not in data:
            continue
        ax.plot(data[lib]["n_samples"], data[lib]["init"], marker="o", label=lib)
    ax.set_xlabel("Number of samples")
    ax.set_ylabel("Init time (s)")
    ax.set_title("Project Initialization Time")
    ax.set_xscale("log")
    if log_y:
        ax.set_yscale("log")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    path = os.path.join(output_dir, "bench_init.png")
    fig.savefig(path, dpi=150)
    print(f"Saved {path}")
    plt.close(fig)

    # Plot 2: Validation time
    fig, ax = plt.subplots()
    for lib in ["peppy", "peprs"]:
        if lib not in data:
            continue
        ax.plot(data[lib]["n_samples"], data[lib]["validate"], marker="o", label=lib)
    ax.set_xlabel("Number of samples")
    ax.set_ylabel("Validation time (s)")
    ax.set_title("Project Validation Time")
    ax.set_xscale("log")
    if log_y:
        ax.set_yscale("log")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    path = os.path.join(output_dir, "bench_validate.png")
    fig.savefig(path, dpi=150)
    print(f"Saved {path}")
    plt.close(fig)

    # Plot 3: Memory usage
    fig, ax = plt.subplots()
    for lib in ["peppy", "peprs"]:
        if lib not in data:
            continue
        mem_mb = [b / 1024 / 1024 for b in data[lib]["memory"]]
        ax.plot(data[lib]["n_samples"], mem_mb, marker="o", label=lib)
    ax.set_xlabel("Number of samples")
    ax.set_ylabel("Memory (MB)")
    ax.set_title("Memory Usage")
    ax.set_xscale("log")
    if log_y:
        ax.set_yscale("log")
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    path = os.path.join(output_dir, "bench_memory.png")
    fig.savefig(path, dpi=150)
    print(f"Saved {path}")
    plt.close(fig)


def parse_args():
    parser = argparse.ArgumentParser(description="Plot benchmark summary")
    parser.add_argument(
        "input",
        nargs="?",
        default="bench_summary.csv",
        help="Summary CSV from summarize.py (default: bench_summary.csv)",
    )
    parser.add_argument(
        "--output-dir",
        default="plots",
        help="Directory to save plots (default: plots)",
    )
    parser.add_argument(
        "--log-y",
        action="store_true",
        help="Use log scale for the y-axis (time/memory)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    rows = read_summary(args.input)
    plot(rows, args.output_dir, args.log_y)


if __name__ == "__main__":
    main()
