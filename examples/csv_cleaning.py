#!/usr/bin/env python3
"""
DataWash CSV Cleaning Example
=============================

This example demonstrates a real-world workflow:
1. Load a messy CSV file
2. Analyze and review issues
3. Apply specific fixes (not all)
4. Save the cleaned CSV

Equivalent CLI commands are shown in comments throughout.

Run this example:
    python examples/csv_cleaning.py

CLI equivalent:
    datawash analyze examples/sample_data/customers_messy.csv
    datawash suggest examples/sample_data/customers_messy.csv --use-case general
    datawash clean examples/sample_data/customers_messy.csv -o customers_clean.csv --ids 1,2,3
"""

import os
from pathlib import Path

from datawash import analyze


def main():
    # Get the path to sample data
    script_dir = Path(__file__).parent
    input_file = script_dir / "sample_data" / "customers_messy.csv"
    output_file = script_dir / "sample_data" / "customers_clean.csv"

    # =========================================================================
    # Step 1: Load and analyze the CSV
    # =========================================================================
    # CLI: datawash analyze examples/sample_data/customers_messy.csv

    print("=" * 70)
    print("STEP 1: Loading and analyzing CSV")
    print("=" * 70)
    print(f"Input file: {input_file}")

    report = analyze(str(input_file))

    print(f"\nDataset Overview:")
    print(f"  Rows: {report.profile.row_count}")
    print(f"  Columns: {report.profile.column_count}")
    print(f"  Memory: {report.profile.memory_bytes / 1024:.1f} KB")
    print(f"  Duplicate rows: {report.profile.duplicate_row_count}")
    print(f"  Quality Score: {report.quality_score}/100")

    # =========================================================================
    # Step 2: Review the full summary
    # =========================================================================

    print("\n" + "=" * 70)
    print("STEP 2: Quality Summary")
    print("=" * 70)
    print(report.summary())

    # =========================================================================
    # Step 3: List all suggestions with details
    # =========================================================================
    # CLI: datawash suggest examples/sample_data/customers_messy.csv

    print("\n" + "=" * 70)
    print("STEP 3: All Suggestions")
    print("=" * 70)

    for s in report.suggestions:
        priority_icon = {"high": "!!!", "medium": " !!", "low": "  !"}[s.priority.value]
        print(f"\n  [{s.id}] {priority_icon} {s.action}")
        print(f"      Transformer: {s.transformer}")
        print(f"      Impact: {s.impact}")

    # =========================================================================
    # Step 4: Apply selected suggestions (not all)
    # =========================================================================
    # CLI: datawash clean customers_messy.csv -o customers_clean.csv --ids 1,2,3,4,5

    print("\n" + "=" * 70)
    print("STEP 4: Applying selected fixes")
    print("=" * 70)

    # Let's apply fixes for:
    # - Duplicate rows (if detected)
    # - Missing values / empty strings
    # - Case standardization
    # - Boolean conversion
    # But skip date standardization to preserve original formats

    # Find suggestion IDs we want to apply
    ids_to_apply = []
    ids_to_skip = []

    for s in report.suggestions:
        # Skip date standardization
        if "date" in s.action.lower() and "standardize" in s.action.lower():
            ids_to_skip.append((s.id, s.action))
            continue
        # Skip outlier handling (manual review preferred)
        if "outlier" in s.action.lower():
            ids_to_skip.append((s.id, s.action))
            continue
        # Apply everything else
        ids_to_apply.append(s.id)

    print(f"\nApplying {len(ids_to_apply)} suggestions: {ids_to_apply}")
    if ids_to_skip:
        print(f"Skipping {len(ids_to_skip)} suggestions:")
        for sid, action in ids_to_skip:
            print(f"  - [{sid}] {action}")

    clean_df = report.apply(ids_to_apply)

    # =========================================================================
    # Step 5: Review changes
    # =========================================================================

    print("\n" + "=" * 70)
    print("STEP 5: Before vs After comparison")
    print("=" * 70)

    original_df = report.df
    print(
        f"\n  Quality Score: {report._last_score_before} → {report._last_score_after}"
    )
    print(f"  Rows: {len(original_df)} → {len(clean_df)}")

    # Show sample of cleaned data
    print("\nSample of cleaned data (first 5 rows):")
    print(clean_df.head().to_string())

    # =========================================================================
    # Step 6: Save cleaned CSV
    # =========================================================================
    # CLI: (included in datawash clean command with -o flag)

    print("\n" + "=" * 70)
    print("STEP 6: Saving cleaned CSV")
    print("=" * 70)

    clean_df.to_csv(output_file, index=False)
    print(f"Saved to: {output_file}")
    print(f"File size: {os.path.getsize(output_file) / 1024:.1f} KB")

    # =========================================================================
    # Step 7: Generate code for reproducibility
    # =========================================================================
    # CLI: datawash codegen customers_messy.csv --ids 1,2,3,4,5 --style script

    print("\n" + "=" * 70)
    print("STEP 7: Generated cleaning script")
    print("=" * 70)

    code = report.generate_code(style="script")
    print(code)

    # Optionally save the code to a file
    code_file = script_dir / "sample_data" / "clean_customers.py"
    with open(code_file, "w") as f:
        f.write("#!/usr/bin/env python3\n")
        f.write('"""Auto-generated data cleaning script."""\n\n')
        f.write("import pandas as pd\n")
        f.write("import numpy as np\n\n")
        f.write(code)
        f.write('\n\nif __name__ == "__main__":\n')
        f.write('    df = pd.read_csv("customers_messy.csv")\n')
        f.write("    df = clean_data(df)\n")
        f.write('    df.to_csv("customers_clean.csv", index=False)\n')
        f.write('    print("Cleaning complete!")\n')

    print(f"\nSaved cleaning script to: {code_file}")


if __name__ == "__main__":
    main()
