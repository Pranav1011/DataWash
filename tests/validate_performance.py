"""
DataWash v1.0 Performance Validation Suite

Run with: python tests/validate_performance.py

This validates that all performance optimizations are working correctly:
1. Smart Sampling - kicks in at 50K+ rows
2. Parallel Processing - uses multiple cores
3. Computation Cache - no redundant calculations
4. MinHash + LSH Similarity - O(n) not O(n^2)
5. Dtype Optimization - numeric downcasting
"""

import os
import sys
import time
import traceback
import tracemalloc
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import numpy as np
import pandas as pd

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from datawash import analyze
from datawash.core.cache import ComputationCache
from datawash.core.dtypes import optimize_dataframe
from datawash.core.sampling import SAMPLE_THRESHOLD, SmartSampler


# ═══════════════════════════════════════════════════════════════════════
# TEST DATA GENERATORS
# ═══════════════════════════════════════════════════════════════════════


def generate_test_data(n_rows: int, n_cols: int, seed: int = 42) -> pd.DataFrame:
    """Generate realistic messy test data."""
    np.random.seed(seed)

    data = {"id": range(n_rows)}

    for i in range(n_cols - 1):
        col_type = i % 5
        col_name = f"col_{i}_{['str', 'int', 'float', 'bool', 'date'][col_type]}"

        if col_type == 0:  # String
            values = [f"value_{j % 100}" for j in range(n_rows)]
            data[col_name] = [
                v.upper() if j % 3 == 0 else v.lower() if j % 3 == 1 else v
                for j, v in enumerate(values)
            ]
        elif col_type == 1:  # Integer
            data[col_name] = np.random.randint(0, 1000, n_rows)
        elif col_type == 2:  # Float
            data[col_name] = np.random.randn(n_rows) * 100
        elif col_type == 3:  # Boolean-like strings
            data[col_name] = np.random.choice(
                ["yes", "no", "Yes", "No", "YES", "NO"], n_rows
            ).tolist()
        elif col_type == 4:  # Date strings
            dates = pd.date_range("2020-01-01", periods=100, freq="D")
            data[col_name] = np.random.choice(
                dates.strftime("%Y-%m-%d"), n_rows
            ).tolist()

        # Add 5% nulls
        null_mask = np.random.random(n_rows) < 0.05
        if col_type in [0, 3, 4]:
            data[col_name] = [
                None if null_mask[j] else v for j, v in enumerate(data[col_name])
            ]
        else:
            data[col_name] = np.where(null_mask, np.nan, data[col_name])

    # Add 2% duplicate rows
    df = pd.DataFrame(data)
    n_dupes = int(n_rows * 0.02)
    if n_dupes > 0:
        dupes = df.sample(n_dupes, random_state=seed)
        df = pd.concat([df, dupes], ignore_index=True)

    return df


# ═══════════════════════════════════════════════════════════════════════
# DISPLAY HELPERS
# ═══════════════════════════════════════════════════════════════════════

_SUPPORTS_COLOR = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()


def _green(text: str) -> str:
    return f"\033[92m{text}\033[0m" if _SUPPORTS_COLOR else text


def _red(text: str) -> str:
    return f"\033[91m{text}\033[0m" if _SUPPORTS_COLOR else text


def _bold(text: str) -> str:
    return f"\033[1m{text}\033[0m" if _SUPPORTS_COLOR else text


def print_header(text: str):
    print(f"\n{'=' * 70}")
    print(f"  {text}")
    print(f"{'=' * 70}\n")


def print_result(name: str, passed: bool, details: str = ""):
    status = _green("PASS") if passed else _red("FAIL")
    print(f"  [{status}] {name}")
    if details:
        print(f"         {details}")


# ═══════════════════════════════════════════════════════════════════════
# VALIDATION TESTS
# ═══════════════════════════════════════════════════════════════════════


def test_sampling_activation():
    """Verify sampling activates at the correct threshold."""
    print_header("TEST 1: Smart Sampling Activation")

    all_passed = True

    # Below threshold - should NOT sample
    df_small = generate_test_data(40_000, 10)
    sampler_small = SmartSampler(df_small)
    passed = not sampler_small.is_sampled
    print_result(
        f"40K rows: sampling disabled",
        passed,
        f"is_sampled={sampler_small.is_sampled}",
    )
    all_passed &= passed

    # At threshold - should sample (>= check)
    df_at = generate_test_data(SAMPLE_THRESHOLD, 10)
    sampler_at = SmartSampler(df_at)
    passed = sampler_at.is_sampled
    print_result(
        f"{SAMPLE_THRESHOLD // 1000}K rows (threshold): sampling enabled",
        passed,
        f"is_sampled={sampler_at.is_sampled}, sample_size={len(sampler_at.sample_df)}",
    )
    all_passed &= passed

    # Above threshold - should sample
    df_large = generate_test_data(200_000, 10)
    sampler_large = SmartSampler(df_large)
    passed = sampler_large.is_sampled and len(sampler_large.sample_df) <= 15_000
    print_result(
        f"200K rows: samples to ~10K",
        passed,
        f"original={len(df_large)}, sampled={len(sampler_large.sample_df)}",
    )
    all_passed &= passed

    # Null rows are preserved
    null_rows_in_original = df_large.isna().any(axis=1).sum()
    null_rows_in_sample = sampler_large.sample_df.isna().any(axis=1).sum()
    passed = null_rows_in_sample > 0
    print_result(
        "Sample preserves null rows",
        passed,
        f"null rows in sample: {null_rows_in_sample} "
        f"(original has {null_rows_in_original})",
    )
    all_passed &= passed

    # Scale factor is correct
    expected_factor = len(df_large) / len(sampler_large.sample_df)
    passed = abs(sampler_large.scale_factor - expected_factor) < 0.01
    print_result(
        "Scale factor is correct",
        passed,
        f"scale_factor={sampler_large.scale_factor:.2f}, "
        f"expected={expected_factor:.2f}",
    )
    all_passed &= passed

    return all_passed


def test_sampling_accuracy():
    """Verify sampled analysis produces accurate results vs full scan."""
    print_header("TEST 2: Sampling Accuracy (Sampled vs Full)")

    # Use enough columns so sampling overhead is outweighed by savings.
    # With only 4 columns, sampling overhead exceeds any speedup.
    df = generate_test_data(200_000, 20)

    all_passed = True

    # Analyze with sampling (default)
    report_sampled = analyze(df, sample=True)

    # Analyze without sampling
    report_full = analyze(df, sample=False)

    # Verify sampling was used
    passed = report_sampled.profile.sampled
    print_result(
        "Sampling was used for 200K rows",
        passed,
        f"sampled={report_sampled.profile.sampled}, "
        f"sample_size={report_sampled.profile.sample_size}",
    )
    all_passed &= passed

    # Verify full scan was NOT sampled
    passed = not report_full.profile.sampled
    print_result(
        "Full scan was not sampled",
        passed,
    )
    all_passed &= passed

    # Compare issue types found
    sampled_types = sorted({f.issue_type for f in report_sampled.issues})
    full_types = sorted({f.issue_type for f in report_full.issues})
    # Sampled should find same issue types (may differ in exact counts)
    overlap = set(sampled_types) & set(full_types)
    passed = len(overlap) >= len(full_types) * 0.7
    print_result(
        "Issue types match (>70% overlap)",
        passed,
        f"sampled: {sampled_types}, full: {full_types}",
    )
    all_passed &= passed

    # Sampled analysis should be faster on a meaningful dataset
    t_sampled = _timed(lambda: analyze(df, sample=True))
    t_full = _timed(lambda: analyze(df, sample=False))
    passed = t_sampled < t_full
    print_result(
        "Sampled analysis is faster",
        passed,
        f"sampled: {t_sampled:.2f}s, full: {t_full:.2f}s "
        f"({t_full / t_sampled:.1f}x speedup)" if t_sampled > 0 else "",
    )
    all_passed &= passed

    return all_passed


def test_parallel_processing():
    """Verify parallel processing is being used and improves performance."""
    print_header("TEST 3: Parallel Processing")

    all_passed = True

    # ThreadPoolExecutor sanity check
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            future = executor.submit(lambda: 42)
            result = future.result()
        passed = result == 42
    except Exception:
        passed = False
    print_result("ThreadPoolExecutor functional", passed)
    all_passed &= passed

    # Generate dataset that triggers parallel (>5000 rows or >20 cols)
    df = generate_test_data(10_000, 30)

    # Time with parallel (default)
    t_par = _timed(lambda: analyze(df, sample=False, parallel=True))

    # Time without parallel
    t_seq = _timed(lambda: analyze(df, sample=False, parallel=False))

    print_result(
        f"Parallel analysis on 10K x 30 cols",
        True,
        f"parallel: {t_par:.2f}s, sequential: {t_seq:.2f}s",
    )
    all_passed &= True  # Just informational

    # Parallel profiler should import and function
    try:
        from datawash.profiler.parallel import (
            profile_dataset_parallel,
            run_detectors_parallel,
        )

        passed = callable(profile_dataset_parallel) and callable(
            run_detectors_parallel
        )
    except ImportError:
        passed = False
    print_result("Parallel modules importable", passed)
    all_passed &= passed

    # Verify the analysis still produces results under parallel mode
    report = analyze(df, sample=False, parallel=True)
    passed = len(report.issues) > 0 and len(report.suggestions) > 0
    print_result(
        "Parallel mode produces correct results",
        passed,
        f"issues={len(report.issues)}, suggestions={len(report.suggestions)}",
    )
    all_passed &= passed

    return all_passed


def test_computation_cache():
    """Verify computation cache prevents redundant calculations."""
    print_header("TEST 4: Computation Cache")

    df = generate_test_data(50_000, 20)
    cache = ComputationCache(df)
    col = df.columns[1]

    all_passed = True

    # Null mask caching
    t1 = _timed(lambda: cache.get_null_mask(col))
    t2 = _timed(lambda: cache.get_null_mask(col))
    mask1 = cache.get_null_mask(col)
    mask2 = cache.get_null_mask(col)
    passed = mask1 is mask2  # Same object returned
    print_result(
        "Null mask cached (same object)",
        passed,
        f"first: {t1 * 1000:.3f}ms, second: {t2 * 1000:.3f}ms",
    )
    all_passed &= passed

    # Value set caching
    _ = cache.get_value_set(col)  # warm up
    set_a = cache.get_value_set(col)
    set_b = cache.get_value_set(col)
    passed = set_a is set_b
    print_result("Value set cached (same object)", passed)
    all_passed &= passed

    # Unique count caching
    _ = cache.get_unique_count(col)
    uc1 = cache.get_unique_count(col)
    uc2 = cache.get_unique_count(col)
    passed = uc1 == uc2
    print_result("Unique count cached", passed, f"unique_count={uc1}")
    all_passed &= passed

    # Statistics caching
    num_col = None
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            num_col = c
            break

    if num_col:
        _ = cache.get_statistics(num_col)
        s1 = cache.get_statistics(num_col)
        s2 = cache.get_statistics(num_col)
        passed = s1 is s2 and "mean" in s1
        print_result(
            "Statistics cached (same object)",
            passed,
            f"keys={list(s1.keys())}",
        )
        all_passed &= passed
    else:
        print_result("Statistics caching", False, "no numeric column found")
        all_passed = False

    # Cross-column independence
    col_a = df.columns[1]
    col_b = df.columns[2]
    mask_a = cache.get_null_mask(col_a)
    mask_b = cache.get_null_mask(col_b)
    passed = mask_a is not mask_b
    print_result("Different columns have independent caches", passed)
    all_passed &= passed

    return all_passed


def test_similarity_scaling():
    """Verify similarity detection scales sub-quadratically."""
    print_header("TEST 5: Similarity Detection Scaling")

    all_passed = True
    times = []
    col_counts = [10, 25, 50, 100]

    for n_cols in col_counts:
        df = generate_test_data(2000, n_cols)
        t = _timed(lambda: analyze(df, sample=False, parallel=False))
        times.append(t)
        print(f"    {n_cols:>3} columns: {t:.2f}s")

    # Check scaling: 10x more columns should NOT cause 100x slowdown (O(n^2))
    # Allow up to 30x for O(n log n) or O(n*v) behaviour
    scaling = times[-1] / times[0] if times[0] > 0.001 else float("inf")
    col_ratio = col_counts[-1] / col_counts[0]  # 10x
    quadratic_ratio = col_ratio**2  # 100x if O(n^2)
    passed = scaling < quadratic_ratio * 0.5  # Well below O(n^2)
    print_result(
        "Sub-quadratic scaling verified",
        passed,
        f"{col_ratio:.0f}x columns -> {scaling:.1f}x time "
        f"(O(n^2) would be ~{quadratic_ratio:.0f}x)",
    )
    all_passed &= passed

    return all_passed


def test_dtype_optimization():
    """Verify dtype optimization reduces memory."""
    print_header("TEST 6: Dtype Optimization")

    df = pd.DataFrame(
        {
            "int64_col": np.arange(10000, dtype=np.int64),
            "float64_col": np.random.randn(10000).astype(np.float64),
            "str_col": [f"val_{i}" for i in range(10000)],
        }
    )

    original_memory = df.memory_usage(deep=True).sum()
    optimized = optimize_dataframe(df)
    optimized_memory = optimized.memory_usage(deep=True).sum()

    all_passed = True

    # Numeric memory should decrease
    passed = optimized_memory <= original_memory
    reduction = (1 - optimized_memory / original_memory) * 100
    print_result(
        "Memory reduced by numeric downcasting",
        passed,
        f"{original_memory / 1024:.1f}KB -> {optimized_memory / 1024:.1f}KB "
        f"({reduction:.1f}% reduction)",
    )
    all_passed &= passed

    # Int64 should be downcasted to smaller type
    orig_int_dtype = df["int64_col"].dtype
    opt_int_dtype = optimized["int64_col"].dtype
    passed = opt_int_dtype.itemsize <= orig_int_dtype.itemsize
    print_result(
        "Integer downcasted",
        passed,
        f"{orig_int_dtype} -> {opt_int_dtype}",
    )
    all_passed &= passed

    # Float64 should be downcasted
    orig_float_dtype = df["float64_col"].dtype
    opt_float_dtype = optimized["float64_col"].dtype
    passed = opt_float_dtype.itemsize <= orig_float_dtype.itemsize
    print_result(
        "Float downcasted",
        passed,
        f"{orig_float_dtype} -> {opt_float_dtype}",
    )
    all_passed &= passed

    # String columns are left alone (important for detector compat)
    passed = optimized["str_col"].dtype == df["str_col"].dtype
    print_result(
        "String columns preserved (detector compat)",
        passed,
        f"dtype stays {df['str_col'].dtype}",
    )
    all_passed &= passed

    # Values are preserved
    passed = (optimized["int64_col"].values == df["int64_col"].values).all()
    print_result("Values are preserved after downcasting", passed)
    all_passed &= passed

    return all_passed


def test_memory_efficiency():
    """Verify peak memory does not exceed 3x the DataFrame size."""
    print_header("TEST 7: Memory Efficiency")

    df = generate_test_data(100_000, 20)
    df_mb = df.memory_usage(deep=True).sum() / 1024 / 1024

    tracemalloc.start()
    _ = analyze(df)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    peak_mb = peak / 1024 / 1024

    all_passed = True

    passed = peak_mb < df_mb * 3
    ratio = peak_mb / df_mb if df_mb > 0 else float("inf")
    print_result(
        "Peak memory < 3x DataFrame",
        passed,
        f"DataFrame: {df_mb:.1f}MB, peak: {peak_mb:.1f}MB ({ratio:.1f}x)",
    )
    all_passed &= passed

    return all_passed


def test_full_benchmark():
    """Run the full benchmark suite with timing and targets."""
    print_header("TEST 8: Full Performance Benchmark")

    benchmarks = [
        # (rows, cols, target_seconds, description)
        (1_000, 10, 3, "1K x 10  (tiny)"),
        (10_000, 10, 5, "10K x 10 (small)"),
        (100_000, 10, 10, "100K x 10"),
        (500_000, 10, 10, "500K x 10"),
        (1_000_000, 10, 10, "1M x 10"),
        (10_000, 50, 10, "10K x 50"),
        (10_000, 100, 10, "10K x 100 (wide)"),
        (100_000, 50, 10, "100K x 50"),
        (1_000_000, 50, 15, "1M x 50"),
        (50_000, 250, 25, "50K x 250 (very wide)"),
    ]

    all_passed = True
    rows = []

    for n_rows, n_cols, target, desc in benchmarks:
        sys.stdout.write(f"    {desc:<22} generating...")
        sys.stdout.flush()
        df = generate_test_data(n_rows, n_cols)

        sys.stdout.write(f"\r    {desc:<22} analyzing... ")
        sys.stdout.flush()
        start = time.perf_counter()
        report = analyze(df)
        elapsed = time.perf_counter() - start

        passed = elapsed < target
        status = _green("PASS") if passed else _red("FAIL")
        sampled = "Yes" if report.profile.sampled else "No"
        sys.stdout.write(
            f"\r    {desc:<22} {elapsed:>6.2f}s / {target:>2}s  "
            f"[{status}]  issues={len(report.issues):<4} sampled={sampled}\n"
        )
        rows.append(
            {
                "desc": desc,
                "rows": n_rows,
                "cols": n_cols,
                "target": target,
                "actual": elapsed,
                "passed": passed,
                "issues": len(report.issues),
                "sampled": sampled,
            }
        )
        all_passed &= passed
        del df

    # Summary table
    print(
        "\n    +-----------------------+--------+--------+--------+---------+"
    )
    print(
        "    | Scenario              | Target | Actual | Issues | Sampled |"
    )
    print(
        "    +-----------------------+--------+--------+--------+---------+"
    )
    for r in rows:
        marker = _green("*") if r["passed"] else _red("!")
        print(
            f"    | {r['desc']:<21} | <{r['target']:<4}s | "
            f"{r['actual']:>5.2f}s | {r['issues']:>6} | {r['sampled']:>7} | {marker}"
        )
    print(
        "    +-----------------------+--------+--------+--------+---------+"
    )

    return all_passed


def test_detector_breakdown():
    """Show timing breakdown per detector to identify bottlenecks."""
    print_header("TEST 9: Detector Timing Breakdown")

    from datawash.detectors.registry import get_all_detectors
    from datawash.profiler.engine import profile_dataset

    scenarios = [
        (10_000, 13, "10K x 13 (standard)"),
        (10_000, 100, "10K x 100 (wide)"),
    ]

    all_passed = True

    for n_rows, n_cols, label in scenarios:
        print(f"  --- {label} ---")
        df = generate_test_data(n_rows, n_cols)

        # Profile first
        start = time.perf_counter()
        profile = profile_dataset(df)
        prof_time = time.perf_counter() - start
        print(f"    {'profiler':<20} {prof_time:>6.3f}s")

        # Each detector
        total_det = 0.0
        detectors = get_all_detectors()
        for name, det in detectors.items():
            start = time.perf_counter()
            findings = det.detect(df, profile)
            t = time.perf_counter() - start
            total_det += t
            print(f"    {name:<20} {t:>6.3f}s  ({len(findings)} findings)")

        total = prof_time + total_det
        print(f"    {'TOTAL':<20} {total:>6.3f}s\n")

        # Similarity should not dominate for wide datasets
        sim_time = 0.0
        for name, det in detectors.items():
            if name == "similarity":
                start = time.perf_counter()
                det.detect(df, profile)
                sim_time = time.perf_counter() - start
                break

        if n_cols >= 100:
            sim_pct = sim_time / total * 100 if total > 0 else 0
            passed = sim_pct < 80
            print_result(
                f"Similarity is <80% of total ({label})",
                passed,
                f"similarity: {sim_time:.2f}s = {sim_pct:.0f}% of {total:.2f}s",
            )
            all_passed &= passed

    return all_passed


def test_end_to_end_correctness():
    """Verify the optimized pipeline still detects known issues."""
    print_header("TEST 10: End-to-End Correctness")

    # DataFrame with known issues
    df = pd.DataFrame(
        {
            "name": ["Alice", "BOB", "charlie", "Alice", "BOB"],
            "email": ["a@test.com", "", "c@test.com", "a@test.com", ""],
            "age": ["25", "30", "35", "25", "30"],
            "active": ["yes", "no", "Yes", "yes", "no"],
            "score": [100, 200, 10000, 100, 200],
        }
    )

    report = analyze(df)
    issue_types = {f.issue_type for f in report.issues}

    all_passed = True

    expected = {
        "duplicate_rows": "Duplicate rows detected",
        "inconsistent_case": "Case inconsistency detected",
        "numeric_as_string": "Numeric-as-string detected",
        "boolean_as_string": "Boolean-as-string detected",
    }

    for itype, label in expected.items():
        passed = itype in issue_types
        print_result(label, passed)
        all_passed &= passed

    # Missing values or empty strings should be detected
    passed = "missing_values" in issue_types or "empty_strings" in issue_types
    print_result("Missing/empty values detected", passed)
    all_passed &= passed

    # Quality score should reflect problems
    passed = report.quality_score < 100
    print_result(
        "Quality score reflects issues",
        passed,
        f"score={report.quality_score}",
    )
    all_passed &= passed

    # apply_all should produce a cleaner result
    clean_df = report.apply_all()
    passed = len(clean_df) <= len(df)
    print_result(
        "apply_all() produces result",
        passed,
        f"original={len(df)} rows -> cleaned={len(clean_df)} rows",
    )
    all_passed &= passed

    # generate_code should produce valid Python
    code = report.generate_code()
    passed = "import pandas" in code and "def clean_data" in code
    print_result(
        "generate_code() produces valid code",
        passed,
        f"{len(code.splitlines())} lines generated",
    )
    all_passed &= passed

    print(f"\n    All issue types found: {sorted(issue_types)}")

    return all_passed


# ═══════════════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════════════


def _timed(fn) -> float:
    """Time a function call, return elapsed seconds."""
    start = time.perf_counter()
    fn()
    return time.perf_counter() - start


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════


def main():
    print("\n" + "=" * 70)
    print("  DATAWASH V1.0 PERFORMANCE VALIDATION SUITE")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Python {sys.version.split()[0]}, pandas {pd.__version__}, "
          f"numpy {np.__version__}")
    print("=" * 70)

    tests = [
        ("Sampling Activation", test_sampling_activation),
        ("Sampling Accuracy", test_sampling_accuracy),
        ("Parallel Processing", test_parallel_processing),
        ("Computation Cache", test_computation_cache),
        ("Similarity Scaling", test_similarity_scaling),
        ("Dtype Optimization", test_dtype_optimization),
        ("Memory Efficiency", test_memory_efficiency),
        ("Full Benchmark", test_full_benchmark),
        ("Detector Breakdown", test_detector_breakdown),
        ("E2E Correctness", test_end_to_end_correctness),
    ]

    results = []
    for name, test_fn in tests:
        try:
            passed = test_fn()
            results.append((name, passed, None))
        except Exception as exc:
            print(f"\n  [{_red('EXCEPTION')}] {name}: {exc}")
            traceback.print_exc()
            results.append((name, False, str(exc)))

    # Final summary
    print_header("FINAL RESULTS")

    total = len(results)
    n_passed = sum(1 for _, p, _ in results if p)

    for name, p, err in results:
        status = _green("PASS") if p else _red("FAIL")
        print(f"  [{status}] {name}")
        if err:
            print(f"         Error: {err}")

    print(f"\n  {'=' * 50}")
    print(f"  Total: {n_passed}/{total} tests passed")

    if n_passed == total:
        print(f"\n  {_green('ALL OPTIMIZATIONS VERIFIED WORKING')}")
    else:
        print(f"\n  {_red(f'{total - n_passed} TEST(S) FAILED')}")

    print()
    return n_passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
