"""Performance benchmarking for datawash."""

import json
import os
import sys
import time
import tracemalloc

import pandas as pd

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from datawash import analyze


def measure_performance(df: pd.DataFrame, name: str, use_case: str = "general"):
    """Measure time and memory for analyze and apply_all."""
    print(f"\n{'='*60}")
    print(f"Testing: {name}")
    print(f"Shape: {df.shape[0]:,} rows x {df.shape[1]} columns")
    print(f"Memory: {df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
    print("=" * 60)

    results = {
        "name": name,
        "rows": df.shape[0],
        "cols": df.shape[1],
        "df_memory_mb": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 1),
    }

    # Measure analyze()
    print("\n[1] Testing analyze()...")
    tracemalloc.start()
    start = time.perf_counter()

    try:
        report = analyze(df, use_case=use_case)

        analyze_time = time.perf_counter() - start
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        results["analyze_time_sec"] = round(analyze_time, 2)
        results["analyze_memory_mb"] = round(peak / 1024 / 1024, 1)
        results["issues_found"] = len(report.issues)
        results["suggestions_count"] = len(report.suggestions)
        results["quality_score"] = report.quality_score

        print(f"  Time: {analyze_time:.2f}s")
        print(f"  Peak Memory: {peak / 1024 / 1024:.1f} MB")
        print(f"  Issues Found: {len(report.issues)}")
        print(f"  Suggestions: {len(report.suggestions)}")
        print(f"  Quality Score: {report.quality_score}")

    except Exception as e:
        tracemalloc.stop()
        print(f"  ERROR in analyze(): {e}")
        results["analyze_error"] = str(e)
        return results

    # Measure apply_all()
    print("\n[2] Testing apply_all()...")
    tracemalloc.start()
    start = time.perf_counter()

    try:
        clean_df = report.apply_all()

        apply_time = time.perf_counter() - start
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        results["apply_time_sec"] = round(apply_time, 2)
        results["apply_memory_mb"] = round(peak / 1024 / 1024, 1)
        results["rows_after"] = clean_df.shape[0]
        results["rows_removed"] = df.shape[0] - clean_df.shape[0]

        print(f"  Time: {apply_time:.2f}s")
        print(f"  Peak Memory: {peak / 1024 / 1024:.1f} MB")
        print(f"  Rows: {df.shape[0]:,} → {clean_df.shape[0]:,} ({results['rows_removed']:,} removed)")

    except Exception as e:
        tracemalloc.stop()
        print(f"  ERROR in apply_all(): {e}")
        results["apply_error"] = str(e)
        return results

    # Measure generate_code()
    print("\n[3] Testing generate_code()...")
    start = time.perf_counter()

    try:
        code = report.generate_code()
        codegen_time = time.perf_counter() - start
        results["codegen_time_sec"] = round(codegen_time, 2)
        results["code_lines"] = len(code.split("\n"))

        print(f"  Time: {codegen_time:.2f}s")
        print(f"  Generated: {results['code_lines']} lines")

    except Exception as e:
        print(f"  ERROR in generate_code(): {e}")
        results["codegen_error"] = str(e)

    # Total time
    results["total_time_sec"] = round(
        results.get("analyze_time_sec", 0)
        + results.get("apply_time_sec", 0)
        + results.get("codegen_time_sec", 0),
        2,
    )

    print(f"\n  TOTAL TIME: {results['total_time_sec']:.2f}s")

    return results


def test_individual_detectors(df: pd.DataFrame):
    """Test each detector individually to find bottlenecks."""
    from datawash.detectors.registry import get_all_detectors
    from datawash.profiler.engine import profile_dataset

    print(f"\n{'='*60}")
    print("Individual Detector Performance")
    print("=" * 60)

    # First, profile the data
    print("\nProfiling dataset...")
    start = time.perf_counter()
    profile = profile_dataset(df)
    profile_time = time.perf_counter() - start
    print(f"  Profiling time: {profile_time:.2f}s")

    # Test each detector
    detectors = get_all_detectors()
    detector_times = {"profiler": {"time": round(profile_time, 3)}}

    for detector_name, detector in detectors.items():
        print(f"\nTesting {detector_name}...")
        start = time.perf_counter()

        try:
            findings = detector.detect(df, profile)
            elapsed = time.perf_counter() - start
            detector_times[detector_name] = {"time": round(elapsed, 3), "findings": len(findings)}
            print(f"  Time: {elapsed:.3f}s, Findings: {len(findings)}")
        except Exception as e:
            print(f"  ERROR: {e}")
            detector_times[detector_name] = {"error": str(e)}

    return detector_times


def run_benchmarks():
    """Run all benchmarks."""
    from generate_test_data import generate_large_dataset, generate_wide_dataset

    all_results = []

    # Test different sizes
    sizes = [1_000, 10_000, 50_000, 100_000]

    for size in sizes:
        print(f"\n\n{'#'*60}")
        print(f"Generating {size:,} row dataset...")
        print("#" * 60)

        df = generate_large_dataset(size)
        result = measure_performance(df, f"{size:,} rows")
        all_results.append(result)

        # Only test individual detectors on smaller datasets
        if size <= 50_000:
            detector_results = test_individual_detectors(df)
            result["detector_breakdown"] = detector_results

        # Free memory
        del df

    # Test wide dataset
    print(f"\n\n{'#'*60}")
    print("Testing wide dataset (10K rows x 100 cols)...")
    print("#" * 60)

    df_wide = generate_wide_dataset(10_000, 100)
    result = measure_performance(df_wide, "10K x 100 cols (wide)")
    all_results.append(result)

    # Print summary
    print(f"\n\n{'='*60}")
    print("BENCHMARK SUMMARY")
    print("=" * 60)

    print(f"\n{'Dataset':<25} {'Analyze':<12} {'Apply':<12} {'Total':<12} {'Issues':<10}")
    print("-" * 70)

    for r in all_results:
        name = r["name"][:24]
        analyze_t = f"{r.get('analyze_time_sec', 'ERR')}s"
        apply_t = f"{r.get('apply_time_sec', 'ERR')}s"
        total_t = f"{r.get('total_time_sec', 'ERR')}s"
        issues = r.get("issues_found", "ERR")
        print(f"{name:<25} {analyze_t:<12} {apply_t:<12} {total_t:<12} {issues:<10}")

    return all_results


if __name__ == "__main__":
    results = run_benchmarks()

    # Save results
    os.makedirs("tests", exist_ok=True)
    with open("tests/benchmark_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    print("\nResults saved to tests/benchmark_results.json")
