# DataWash

Intelligent data cleaning and quality analysis for Python.

DataWash analyzes tabular data, detects quality issues using rules and statistics, suggests fixes, and generates reproducible Python code for transformations.

## Installation

```bash
pip install datawash
```

Optional extras:

```bash
pip install datawash[formats]  # Parquet + Excel support
pip install datawash[ml]       # ML-powered detection
pip install datawash[dev]      # Development tools
```

## Quick Start

```python
from datawash import analyze

report = analyze("data.csv")

# View analysis summary
print(report.summary())

# See all suggestions
for s in report.suggestions:
    print(f"[{s.id}] {s.action} -- {s.rationale}")

# Apply all fixes
clean_df = report.apply_all()

# Or apply selectively
clean_df = report.apply([1, 3, 5])

# Generate reproducible code
print(report.generate_code())
```

## CLI Usage

```bash
# Analyze a dataset
datawash analyze data.csv

# Get cleaning suggestions
datawash suggest data.csv --use-case ml

# Clean and export
datawash clean data.csv -o clean.csv --apply-all

# Generate Python code
datawash codegen data.csv --apply-all --style function
```

## What It Detects

| Detector | Issues Found |
|----------|-------------|
| **Missing** | Null values, empty strings |
| **Duplicates** | Exact duplicate rows |
| **Formats** | Inconsistent casing, date formats, whitespace |
| **Outliers** | Statistical outliers (IQR, z-score) |
| **Types** | Numeric/boolean data stored as strings |
| **Similarity** | Potentially duplicate columns |

## What It Fixes

| Transformer | Operations |
|-------------|-----------|
| **Duplicates** | Remove exact duplicates |
| **Missing** | Drop rows, fill with median/mode/value, clip outliers |
| **Types** | Convert to numeric, boolean, datetime |
| **Formats** | Strip whitespace, standardize case, normalize dates |
| **Columns** | Drop, rename, merge columns |
| **Categories** | Normalize category values |

## Configuration

```python
from datawash import analyze

report = analyze(
    "data.csv",
    use_case="ml",
    config={
        "detectors": {
            "outlier_method": "zscore",
            "outlier_threshold": 2.5,
        },
        "suggestions": {"max_suggestions": 20},
    },
)
```

## Requirements

- Python >= 3.10
- pandas, numpy, pydantic, rich, typer, scikit-learn

## License

MIT
