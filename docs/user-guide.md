# DataWash User Guide

This guide covers all DataWash features in detail.

## Table of Contents

- [Core Workflow](#core-workflow)
- [Data Loading](#data-loading)
- [Profiling](#profiling)
- [Detectors](#detectors)
- [Transformers](#transformers)
- [Suggestions](#suggestions)
- [Code Generation](#code-generation)
- [Use Cases](#use-cases)
- [Jupyter Support](#jupyter-support)

---

## Core Workflow

The typical DataWash workflow:

```
Load Data → Profile → Detect Issues → Generate Suggestions → Apply Fixes → Generate Code
```

```python
from datawash import analyze

# All-in-one: analyze handles loading, profiling, detection, and suggestions
report = analyze("data.csv")

# Review
print(report.summary())

# Fix
clean_df = report.apply_all()

# Document
code = report.generate_code()
```

---

## Data Loading

### Supported Formats

| Format | Extension | Dependency |
|--------|-----------|------------|
| CSV | `.csv` | pandas (included) |
| JSON | `.json` | pandas (included) |
| Parquet | `.parquet` | pyarrow (optional) |
| Excel | `.xlsx`, `.xls` | openpyxl (optional) |

### Loading from Files

```python
# From file path (string)
report = analyze("path/to/data.csv")

# From Path object
from pathlib import Path
report = analyze(Path("data.csv"))
```

### Loading from DataFrame

```python
import pandas as pd

df = pd.DataFrame({...})
report = analyze(df)
```

### Accessing Original Data

```python
# Get a copy of the original DataFrame
original_df = report.df
```

---

## Profiling

DataWash profiles your data automatically during analysis.

### Dataset Profile

```python
profile = report.profile

print(f"Rows: {profile.row_count}")
print(f"Columns: {profile.column_count}")
print(f"Memory: {profile.memory_bytes / 1024 / 1024:.1f} MB")
print(f"Duplicates: {profile.duplicate_row_count}")
```

### Column Profiles

```python
for col_name, col_profile in profile.columns.items():
    print(f"\n{col_name}:")
    print(f"  Type: {col_profile.dtype}")
    print(f"  Nulls: {col_profile.null_count} ({col_profile.null_ratio:.1%})")
    print(f"  Unique: {col_profile.unique_count}")
    print(f"  Sample: {col_profile.sample_values}")
```

### Statistics

For numeric columns:
```python
stats = profile.columns["price"].statistics
print(f"Mean: {stats['mean']}")
print(f"Median: {stats['median']}")
print(f"Std: {stats['std']}")
print(f"Min: {stats['min']}")
print(f"Max: {stats['max']}")
```

For categorical columns:
```python
stats = profile.columns["category"].statistics
print(f"Mode: {stats['mode']}")
print(f"Top values: {stats['top_values']}")
```

---

## Detectors

DataWash includes 6 built-in detectors.

### Missing Detector

Finds null values, empty strings, and whitespace-only values.

```python
# Issues detected:
# - missing_values: Columns with NaN/None
# - empty_strings: Columns with "" or whitespace-only
```

### Duplicate Detector

Finds exact duplicate rows.

```python
# Issues detected:
# - duplicate_rows: Dataset has X duplicate rows
```

### Format Detector

Finds formatting inconsistencies.

```python
# Issues detected:
# - inconsistent_case: Mixed uppercase/lowercase
# - inconsistent_date_format: Multiple date formats
# - whitespace_padding: Leading/trailing whitespace
```

### Outlier Detector

Finds statistical anomalies in numeric columns.

```python
# Issues detected:
# - outliers: Column has X outliers (IQR or Z-score method)
```

### Type Detector

Finds type mismatches.

```python
# Issues detected:
# - numeric_as_string: Numbers stored as strings
# - boolean_as_string: yes/no/true/false stored as strings
```

### Similarity Detector

Finds potentially duplicate columns.

```python
# Issues detected:
# - similar_columns: Columns X and Y have high name similarity
```

---

## Transformers

DataWash includes 6 transformers.

### Missing Transformer

Strategies:
- `drop_rows`: Remove rows with nulls
- `fill_median`: Fill numeric columns with median
- `fill_mode`: Fill with most common value
- `fill_value`: Fill with specific value
- `clean_empty_strings`: Convert empty strings to NaN and fill
- `clip_outliers`: Clip values to acceptable range

### Duplicates Transformer

- `drop_duplicates`: Remove duplicate rows (keep first/last)

### Types Transformer

Target types:
- `numeric`: Convert to int/float
- `boolean`: Convert yes/no/true/false to bool
- `datetime`: Parse as datetime
- `string`: Convert to string

### Formats Transformer

Operations:
- `strip_whitespace`: Remove leading/trailing spaces
- `lowercase`: Convert to lowercase
- `uppercase`: Convert to uppercase
- `titlecase`: Convert to title case
- `standardize_dates`: Convert to ISO format (YYYY-MM-DD)

### Columns Transformer

Operations:
- `drop`: Remove columns
- `rename`: Rename columns
- `merge`: Merge similar columns

### Categories Transformer

- `normalize`: Standardize category values (strip + lowercase)
- `mapping`: Apply custom value mapping

---

## Suggestions

### Understanding Suggestions

Each suggestion includes:

```python
for s in report.suggestions:
    print(f"ID: {s.id}")                    # Unique identifier
    print(f"Action: {s.action}")            # What will be done
    print(f"Priority: {s.priority.value}")  # high/medium/low
    print(f"Impact: {s.impact}")            # Expected effect
    print(f"Rationale: {s.rationale}")      # Why it's recommended
    print(f"Transformer: {s.transformer}")  # Which transformer
    print(f"Params: {s.params}")            # Transformer parameters
```

### Priority Levels

| Priority | Meaning |
|----------|---------|
| HIGH | Critical fixes, apply first |
| MEDIUM | Important improvements |
| LOW | Nice-to-have optimizations |

### Applying Suggestions

```python
# Apply all suggestions
clean_df = report.apply_all()

# Apply specific suggestions by ID
clean_df = report.apply([1, 2, 5])

# Interactive mode (prompts for each)
clean_df = report.apply_interactive()
```

### Transformation Ordering

DataWash automatically orders transformations to prevent conflicts:

1. **Structural cleaning**: Duplicates, drop rows
2. **Value normalization**: Whitespace, case changes
3. **Missing value handling**: Fill NaN
4. **Type conversion**: Boolean, numeric, dates
5. **Outlier handling**: After types are correct
6. **Column operations**: Drop, rename, merge

### Exclusion Rules

DataWash automatically excludes conflicting suggestions:

- Boolean columns won't get case change suggestions
- Date columns won't get case change suggestions
- Numeric columns won't get case change suggestions

---

## Code Generation

### Function Style

Generates a reusable `clean_data(df)` function:

```python
code = report.generate_code(style="function")
```

Output:
```python
def clean_data(df):
    """Clean the dataset."""
    # Remove duplicates
    df = df.drop_duplicates()
    # ... more transformations
    return df
```

### Script Style

Generates a standalone script:

```python
code = report.generate_code(style="script")
```

Output:
```python
# Remove duplicates
df = df.drop_duplicates()
# ... more transformations
```

### Include/Exclude Comments

```python
# With comments (default)
code = report.generate_code(include_comments=True)

# Without comments
code = report.generate_code(include_comments=False)
```

---

## Use Cases

Choose a use case for optimized suggestions:

```python
report = analyze(df, use_case="ml")
```

### General (default)

Balanced approach for data exploration.

### ML

Optimized for machine learning:
- Duplicates: HIGH priority (data leakage)
- Missing values: HIGH priority (model errors)
- Type conversions: HIGH priority (feature engineering)
- Outliers: MEDIUM priority (skewed training)

### Analytics

Optimized for analysis and reporting:
- Date formats: HIGH priority (time series)
- Consistency: HIGH priority (grouping/joins)
- Outliers: HIGH priority (skewed statistics)

### Export

Optimized for data sharing:
- Format standardization: HIGH priority
- Whitespace: HIGH priority
- Consistency: HIGH priority

---

## Jupyter Support

### Rich HTML Rendering

Reports render as HTML tables in Jupyter:

```python
# Just display the report
report  # Renders as HTML table
```

### Visualization

```python
import matplotlib.pyplot as plt

# Before/after quality scores
score_before = report._last_score_before
score_after = report._last_score_after

plt.bar(['Before', 'After'], [score_before, score_after])
plt.ylabel('Quality Score')
plt.title('Data Quality Improvement')
plt.show()
```

### Export from Notebook

```python
# Clean and save
clean_df = report.apply_all()
clean_df.to_csv("cleaned_data.csv", index=False)

# Save generated code
with open("clean_data.py", "w") as f:
    f.write(report.generate_code())
```

---

## Best Practices

### 1. Review Before Applying

Always review suggestions before applying:

```python
for s in report.suggestions:
    print(f"[{s.id}] {s.action}")
```

### 2. Apply Selectively

Don't always use `apply_all()`. Sometimes you want control:

```python
# Skip date standardization, apply everything else
ids = [s.id for s in report.suggestions if "date" not in s.action.lower()]
clean_df = report.apply(ids)
```

### 3. Generate Code for Production

Don't use DataWash in production pipelines. Generate code instead:

```python
# Development: use DataWash interactively
report = analyze(df)
clean_df = report.apply_all()

# Production: use generated code
code = report.generate_code()
# Save to clean_data.py and import in production
```

### 4. Version Your Cleaning Code

Save generated code to version control:

```python
with open("scripts/clean_customer_data.py", "w") as f:
    f.write(report.generate_code(style="function"))
```

### 5. Use Appropriate Use Cases

Choose the right use case for your goal:

```python
# For ML pipelines
report = analyze(df, use_case="ml")

# For dashboards
report = analyze(df, use_case="analytics")

# For data sharing
report = analyze(df, use_case="export")
```
