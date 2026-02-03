#!/usr/bin/env python3
"""
DataWash Quickstart Example
===========================

This example demonstrates the basic DataWash workflow:
1. Create/load messy data
2. Analyze and get a quality report
3. Review detected issues and suggestions
4. Apply fixes automatically
5. Generate reproducible Python code

Run this example:
    python examples/quickstart.py
"""

import pandas as pd
from datawash import analyze


def main():
    # =========================================================================
    # Step 1: Create sample messy data
    # =========================================================================
    print("=" * 60)
    print("STEP 1: Creating messy sample data")
    print("=" * 60)

    df = pd.DataFrame(
        {
            "name": ["John Smith", "JANE DOE", "bob wilson", "  Alice  ", "Charlie"],
            "email": ["john@email.com", "", "bob@email.com", "alice@email.com", None],
            "age": ["28", "34", "45", "29", "38"],  # Stored as strings
            "salary": [50000, 62000, 58000, 150000, 55000],  # 150000 is an outlier
            "is_active": ["yes", "Yes", "YES", "no", "No"],  # Boolean as strings
            "hire_date": [
                "2023-01-15",
                "15/02/2023",
                "March 10, 2023",
                "2023-04-20",
                "2023/05/25",
            ],
        }
    )

    print("\nOriginal Data:")
    print(df.to_string())
    print(f"\nData types:\n{df.dtypes}")

    # =========================================================================
    # Step 2: Analyze the data
    # =========================================================================
    print("\n" + "=" * 60)
    print("STEP 2: Analyzing data with DataWash")
    print("=" * 60)

    report = analyze(df)

    print(
        f"\nDataset: {report.profile.row_count} rows x {report.profile.column_count} columns"
    )
    print(f"Quality Score: {report.quality_score}/100")
    print(f"Issues Found: {len(report.issues)}")
    print(f"Suggestions: {len(report.suggestions)}")

    # =========================================================================
    # Step 3: Review detected issues
    # =========================================================================
    print("\n" + "=" * 60)
    print("STEP 3: Reviewing detected issues")
    print("=" * 60)

    for issue in report.issues:
        print(f"\n  [{issue.severity.value.upper()}] {issue.issue_type}")
        print(f"    Message: {issue.message}")
        if issue.columns:
            print(f"    Columns: {', '.join(issue.columns)}")

    # =========================================================================
    # Step 4: Review suggestions
    # =========================================================================
    print("\n" + "=" * 60)
    print("STEP 4: Reviewing suggestions")
    print("=" * 60)

    for suggestion in report.suggestions:
        print(f"\n  [{suggestion.id}] {suggestion.action}")
        print(f"      Priority: {suggestion.priority.value}")
        print(f"      Impact: {suggestion.impact}")
        print(f"      Rationale: {suggestion.rationale}")

    # =========================================================================
    # Step 5: Apply all fixes
    # =========================================================================
    print("\n" + "=" * 60)
    print("STEP 5: Applying all fixes")
    print("=" * 60)

    clean_df = report.apply_all()

    print("\nCleaned Data:")
    print(clean_df.to_string())
    print(f"\nNew data types:\n{clean_df.dtypes}")

    # =========================================================================
    # Step 6: Generate reproducible code
    # =========================================================================
    print("\n" + "=" * 60)
    print("STEP 6: Generated Python code")
    print("=" * 60)

    code = report.generate_code(style="function")
    print(f"\n{code}")

    # =========================================================================
    # Summary
    # =========================================================================
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Original Quality Score: {report._last_score_before}/100")
    print(f"  Final Quality Score:    {report._last_score_after}/100")
    print(
        f"  Improvement:            +{report._last_score_after - report._last_score_before} points"
    )


if __name__ == "__main__":
    main()
