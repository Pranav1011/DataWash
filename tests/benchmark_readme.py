"""Time analyze() on the README's synthetic datasets and print a Markdown table.

Run with: python tests/benchmark_readme.py

Each scenario uses validate_performance.generate_test_data (seed 42) and
reports the median of three analyze() runs with default settings, so datasets
of 50K rows or more are sampled.
"""

import os
import platform
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

from validate_performance import generate_test_data  # noqa: E402

from datawash import __version__, analyze  # noqa: E402

SCENARIOS = [
    (1_000_000, 10),
    (100_000, 50),
    (10_000, 100),
    (1_000_000, 50),
    (50_000, 250),
]
RUNS = 3


def _label(n: int) -> str:
    return f"{n // 1_000_000}M" if n >= 1_000_000 else f"{n // 1_000}K"


def main() -> None:
    print(f"datawash {__version__}, Python {platform.python_version()}, ", end="")
    print(f"{platform.machine()} ({os.cpu_count()} cores), median of {RUNS} runs\n")
    print("| Dataset | analyze() time | Sampled |")
    print("|---------|----------------|---------|")
    for n_rows, n_cols in SCENARIOS:
        df = generate_test_data(n_rows, n_cols)
        times = []
        for _ in range(RUNS):
            start = time.perf_counter()
            report = analyze(df)
            times.append(time.perf_counter() - start)
        elapsed = statistics.median(times)
        sampled = "yes" if report.profile.sampled else "no"
        print(f"| {_label(n_rows)} rows x {n_cols} cols | {elapsed:.2f}s | {sampled} |")
        del df


if __name__ == "__main__":
    main()
