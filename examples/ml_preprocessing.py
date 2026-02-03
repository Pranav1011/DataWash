#!/usr/bin/env python3
"""
DataWash ML Preprocessing Example
=================================

This example demonstrates using DataWash for machine learning data preparation:
1. Use use_case="ml" for ML-optimized suggestions
2. Compare suggestion priorities between "general" and "ml" modes
3. Apply transformations important for ML pipelines
4. Show how different use cases affect prioritization

ML-specific prioritization:
- Duplicates: HIGH priority (inflates training data)
- Missing values: HIGH priority (most ML models can't handle NaN)
- Type mismatches: HIGH priority (features must be numeric)
- Outliers: MEDIUM priority (can skew model training)

Run this example:
    python examples/ml_preprocessing.py

CLI equivalent:
    datawash suggest data.csv --use-case ml
    datawash clean data.csv -o clean.csv --apply-all --use-case ml
"""

from pathlib import Path

import pandas as pd

from datawash import analyze


def main():
    # Get the path to sample data
    script_dir = Path(__file__).parent
    employees_file = script_dir / "sample_data" / "employees_messy.csv"

    # =========================================================================
    # Step 1: Load the dataset
    # =========================================================================

    print("=" * 70)
    print("ML PREPROCESSING WITH DATAWASH")
    print("=" * 70)
    print(f"\nLoading: {employees_file}")

    df = pd.read_csv(employees_file)
    print(f"Dataset: {len(df)} rows x {len(df.columns)} columns")
    print(f"\nColumns: {list(df.columns)}")

    # =========================================================================
    # Step 2: Compare general vs ML use cases
    # =========================================================================

    print("\n" + "=" * 70)
    print("COMPARING USE CASES: general vs ml")
    print("=" * 70)

    # Analyze with general use case
    report_general = analyze(df, use_case="general")
    print(f"\n[GENERAL] Quality Score: {report_general.quality_score}/100")
    print(f"[GENERAL] Suggestions: {len(report_general.suggestions)}")

    # Analyze with ML use case
    report_ml = analyze(df, use_case="ml")
    print(f"\n[ML] Quality Score: {report_ml.quality_score}/100")
    print(f"[ML] Suggestions: {len(report_ml.suggestions)}")

    # =========================================================================
    # Step 3: Show priority differences
    # =========================================================================

    print("\n" + "=" * 70)
    print("PRIORITY COMPARISON")
    print("=" * 70)

    print("\nGeneral use case priorities:")
    for s in report_general.suggestions[:8]:
        print(f"  [{s.priority.value:6}] {s.action}")

    print("\nML use case priorities (notice reordering):")
    for s in report_ml.suggestions[:8]:
        print(f"  [{s.priority.value:6}] {s.action}")

    # =========================================================================
    # Step 4: Identify ML-critical issues
    # =========================================================================

    print("\n" + "=" * 70)
    print("ML-CRITICAL ISSUES")
    print("=" * 70)

    ml_critical = [
        "duplicate_rows",
        "missing_values",
        "empty_strings",
        "numeric_as_string",
        "boolean_as_string",
        "outliers",
    ]

    print("\nIssues that affect ML model training:")
    for issue in report_ml.issues:
        if issue.issue_type in ml_critical:
            impact = {
                "duplicate_rows": "Inflates training set, causes data leakage",
                "missing_values": "Most models can't handle NaN values",
                "empty_strings": "Hidden missing values causing errors",
                "numeric_as_string": "Features must be numeric for most models",
                "boolean_as_string": "Categorical encoding issues",
                "outliers": "Can heavily skew gradient-based models",
            }.get(issue.issue_type, "General data quality issue")

            print(f"\n  {issue.issue_type}")
            print(
                f"    Columns: {', '.join(issue.columns) if issue.columns else 'N/A'}"
            )
            print(f"    ML Impact: {impact}")

    # =========================================================================
    # Step 5: Apply ML-optimized cleaning
    # =========================================================================

    print("\n" + "=" * 70)
    print("APPLYING ML-OPTIMIZED CLEANING")
    print("=" * 70)

    clean_df = report_ml.apply_all()

    print(
        f"\nQuality Score: {report_ml._last_score_before} → {report_ml._last_score_after}"
    )
    print(
        f"Rows: {len(df)} → {len(clean_df)} (removed {len(df) - len(clean_df)} duplicates/invalids)"
    )

    # =========================================================================
    # Step 6: Verify ML readiness
    # =========================================================================

    print("\n" + "=" * 70)
    print("ML READINESS CHECK")
    print("=" * 70)

    print("\nData types after cleaning:")
    for col in clean_df.columns:
        dtype = clean_df[col].dtype
        null_count = clean_df[col].isna().sum()
        print(f"  {col:20} {str(dtype):15} (nulls: {null_count})")

    # Check for remaining issues
    print("\nML Readiness:")

    # No nulls?
    total_nulls = clean_df.isna().sum().sum()
    print(
        f"  Missing values: {'PASS' if total_nulls == 0 else f'FAIL ({total_nulls} remaining)'}"
    )

    # Numeric columns are numeric?
    numeric_cols = ["salary", "performance_score", "years_experience"]
    numeric_ok = all(
        pd.api.types.is_numeric_dtype(clean_df[c])
        for c in numeric_cols
        if c in clean_df.columns
    )
    print(f"  Numeric types: {'PASS' if numeric_ok else 'FAIL'}")

    # Boolean column is boolean?
    bool_ok = (
        clean_df["is_manager"].dtype == bool
        if "is_manager" in clean_df.columns
        else True
    )
    print(f"  Boolean types: {'PASS' if bool_ok else 'FAIL'}")

    # No duplicates?
    dup_count = clean_df.duplicated().sum()
    print(
        f"  No duplicates: {'PASS' if dup_count == 0 else f'FAIL ({dup_count} remaining)'}"
    )

    # =========================================================================
    # Step 7: Show sample of cleaned data
    # =========================================================================

    print("\n" + "=" * 70)
    print("SAMPLE OF CLEANED DATA")
    print("=" * 70)

    print(clean_df.head(10).to_string())

    # =========================================================================
    # Step 8: Generate ML-ready code
    # =========================================================================

    print("\n" + "=" * 70)
    print("GENERATED PREPROCESSING CODE")
    print("=" * 70)

    code = report_ml.generate_code(style="function")
    print(code)

    # =========================================================================
    # Step 9: Export for sklearn
    # =========================================================================

    print("\n" + "=" * 70)
    print("READY FOR SKLEARN")
    print("=" * 70)

    # Prepare features for ML
    feature_cols = ["salary", "performance_score", "years_experience"]
    target_col = "is_manager"

    if all(c in clean_df.columns for c in feature_cols + [target_col]):
        X = clean_df[feature_cols]
        y = clean_df[target_col]

        print(f"\nFeature matrix X: {X.shape}")
        print(f"Target vector y: {y.shape}")
        print(f"Target distribution:\n{y.value_counts()}")

        print("\nData is ready for sklearn!")
        print("Example usage:")
        print("  from sklearn.model_selection import train_test_split")
        print("  from sklearn.ensemble import RandomForestClassifier")
        print(
            "  X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)"
        )
        print("  model = RandomForestClassifier()")
        print("  model.fit(X_train, y_train)")


if __name__ == "__main__":
    main()
