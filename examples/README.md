# DataWash Examples

This directory contains practical examples demonstrating DataWash features.

## Examples

| File | Description | Run Command |
|------|-------------|-------------|
| [quickstart.py](quickstart.py) | Basic workflow: analyze → suggest → apply → codegen | `python examples/quickstart.py` |
| [csv_cleaning.py](csv_cleaning.py) | Load CSV, clean, save with CLI equivalents | `python examples/csv_cleaning.py` |
| [ml_preprocessing.py](ml_preprocessing.py) | ML-optimized cleaning workflow | `python examples/ml_preprocessing.py` |
| [jupyter_demo.ipynb](jupyter_demo.ipynb) | Interactive notebook with visualizations | Open in Jupyter |

## Sample Data

The `sample_data/` directory contains messy datasets for testing:

| File | Description | Issues Included |
|------|-------------|-----------------|
| [customers_messy.csv](sample_data/customers_messy.csv) | 30 customer records | Mixed case, missing emails, date formats, duplicates |
| [orders_messy.csv](sample_data/orders_messy.csv) | 40 order records | Category inconsistencies, date formats, whitespace |
| [employees_messy.csv](sample_data/employees_messy.csv) | 40 employee records | Type mismatches, duplicates, boolean as strings |

## Running Examples

### Prerequisites

```bash
pip install datawash
```

### Python Examples

```bash
# From the project root
python examples/quickstart.py
python examples/csv_cleaning.py
python examples/ml_preprocessing.py
```

### Jupyter Notebook

```bash
# Install Jupyter if needed
pip install jupyter matplotlib

# Launch Jupyter
jupyter notebook examples/jupyter_demo.ipynb
```

## Example Outputs

### quickstart.py

```
============================================================
STEP 1: Creating messy sample data
============================================================

Original Data:
         name           email age  salary is_active      hire_date
0  John Smith  john@email.com  28   50000       yes     2023-01-15
1    JANE DOE                  34   62000       Yes     15/02/2023
...

============================================================
STEP 2: Analyzing data with DataWash
============================================================

Dataset: 5 rows x 6 columns
Quality Score: 52/100
Issues Found: 6
Suggestions: 5

...
```

### csv_cleaning.py

```
======================================================================
STEP 1: Loading and analyzing CSV
======================================================================
Input file: examples/sample_data/customers_messy.csv

Dataset Overview:
  Rows: 30
  Columns: 8
  Memory: 2.3 KB
  Duplicate rows: 2
  Quality Score: 65/100

...
```

### ml_preprocessing.py

```
======================================================================
ML PREPROCESSING WITH DATAWASH
======================================================================

Loading: examples/sample_data/employees_messy.csv
Dataset: 40 rows x 9 columns

======================================================================
COMPARING USE CASES: general vs ml
======================================================================

[GENERAL] Quality Score: 58/100
[GENERAL] Suggestions: 8

[ML] Quality Score: 58/100
[ML] Suggestions: 8

======================================================================
PRIORITY COMPARISON
======================================================================

General use case priorities:
  [high  ] Remove 2 duplicate rows
  [medium] Handle missing values in 'full_name'
  ...

ML use case priorities (notice reordering):
  [high  ] Remove 2 duplicate rows
  [high  ] Handle missing values in 'full_name'
  ...
```

## Customizing Examples

Feel free to modify the examples for your use case:

```python
# Use your own data
report = analyze("your_data.csv")

# Change use case
report = analyze(df, use_case="analytics")

# Adjust configuration
report = analyze(df, config={
    "detectors": {"outlier_method": "zscore"},
    "suggestions": {"max_suggestions": 10},
})
```

## Creating Your Own Examples

```python
#!/usr/bin/env python3
"""My custom DataWash example."""

import pandas as pd
from datawash import analyze

# Load your data
df = pd.read_csv("my_data.csv")

# Analyze
report = analyze(df, use_case="ml")

# Review
print(f"Quality: {report.quality_score}/100")
for s in report.suggestions:
    print(f"  [{s.id}] {s.action}")

# Clean
clean_df = report.apply_all()

# Save
clean_df.to_csv("my_data_clean.csv", index=False)

# Generate code
with open("clean_my_data.py", "w") as f:
    f.write(report.generate_code())
```
