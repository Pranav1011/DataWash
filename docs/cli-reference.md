# CLI Reference

DataWash provides a command-line interface for quick data analysis and cleaning.

## Installation

The CLI is installed automatically with DataWash:

```bash
pip install datawash
```

Verify installation:

```bash
datawash --help
```

## Commands Overview

| Command | Description |
|---------|-------------|
| `analyze` | Analyze a dataset and show quality report |
| `suggest` | Show prioritized cleaning suggestions |
| `clean` | Clean a dataset and optionally save output |
| `codegen` | Generate Python cleaning code |

---

## datawash analyze

Analyze a dataset and display a quality report.

### Usage

```bash
datawash analyze <FILE> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `FILE` | Path to the data file (CSV, JSON, Parquet, Excel) |

### Options

| Option | Description |
|--------|-------------|
| `--use-case`, `-u` | Use case: general, ml, analytics, export (default: general) |
| `--help` | Show help message |

### Examples

```bash
# Basic analysis
datawash analyze data.csv

# With ML use case
datawash analyze data.csv --use-case ml

# Short form
datawash analyze data.csv -u ml
```

### Output

```
DataWash Analysis Report
========================

Dataset: 1000 rows x 10 columns
Memory: 0.5 MB
Duplicate rows: 15

Data Quality Score: 72/100

Issues Found: 8
  [HIGH] 3 issue(s)
    - Column 'email' has 50 missing values (5.0%)
    - Column 'age' has numeric values stored as strings
    - 15 duplicate rows detected
  [MEDIUM] 3 issue(s)
    ...
  [LOW] 2 issue(s)
    ...

Suggestions: 8
Run 'datawash suggest data.csv' to see detailed suggestions.
```

---

## datawash suggest

Show prioritized cleaning suggestions.

### Usage

```bash
datawash suggest <FILE> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `FILE` | Path to the data file |

### Options

| Option | Description |
|--------|-------------|
| `--use-case`, `-u` | Use case: general, ml, analytics, export |
| `--priority`, `-p` | Filter by priority: high, medium, low |
| `--limit`, `-l` | Maximum suggestions to show (default: all) |
| `--help` | Show help message |

### Examples

```bash
# All suggestions
datawash suggest data.csv

# High priority only
datawash suggest data.csv --priority high

# ML use case, top 5
datawash suggest data.csv -u ml -l 5
```

### Output

```
DataWash Suggestions
====================

Quality Score: 72/100
Suggestions: 8

ID  Priority  Action
--  --------  ------
1   high      Remove 15 duplicate rows
2   high      Handle missing values in 'email'
3   high      Convert 'age' to numeric type
4   medium    Strip whitespace from 'name'
5   medium    Standardize case in 'status'
...

Run 'datawash clean data.csv --ids 1,2,3' to apply specific suggestions.
```

---

## datawash clean

Clean a dataset by applying suggestions.

### Usage

```bash
datawash clean <FILE> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `FILE` | Path to the data file |

### Options

| Option | Description |
|--------|-------------|
| `--output`, `-o` | Output file path |
| `--apply-all`, `-a` | Apply all suggestions |
| `--ids`, `-i` | Comma-separated suggestion IDs to apply |
| `--use-case`, `-u` | Use case: general, ml, analytics, export |
| `--help` | Show help message |

### Examples

```bash
# Apply all suggestions, save to file
datawash clean data.csv -o clean.csv --apply-all

# Apply specific suggestions
datawash clean data.csv -o clean.csv --ids 1,2,5

# Apply all with ML use case
datawash clean data.csv -o clean.csv -a -u ml

# Preview without saving (no -o flag)
datawash clean data.csv --apply-all
```

### Output

```
DataWash Cleaning
=================

Input: data.csv (1000 rows)
Use case: general

Applying 8 suggestions...
  [1] Remove duplicate rows: 15 rows affected
  [2] Handle missing values in 'email': 50 rows affected
  [3] Convert 'age' to numeric: 1000 rows affected
  ...

Quality Score: 72 → 95 (+23)
Rows: 1000 → 985 (removed 15)

Saved to: clean.csv
```

---

## datawash codegen

Generate Python code for cleaning transformations.

### Usage

```bash
datawash codegen <FILE> [OPTIONS]
```

### Arguments

| Argument | Description |
|----------|-------------|
| `FILE` | Path to the data file |

### Options

| Option | Description |
|--------|-------------|
| `--output`, `-o` | Output file for generated code |
| `--style`, `-s` | Code style: function, script (default: function) |
| `--apply-all`, `-a` | Generate code for all suggestions |
| `--ids`, `-i` | Comma-separated suggestion IDs |
| `--use-case`, `-u` | Use case: general, ml, analytics, export |
| `--help` | Show help message |

### Examples

```bash
# Generate function for all suggestions
datawash codegen data.csv --apply-all

# Generate script for specific suggestions
datawash codegen data.csv --ids 1,2,3 --style script

# Save to file
datawash codegen data.csv -a -o clean_data.py
```

### Output (function style)

```python
def clean_data(df):
    """Clean the dataset."""
    import pandas as pd
    import numpy as np

    # Remove duplicate rows
    df = df.drop_duplicates()

    # Fill missing values in 'email'
    df['email'] = df['email'].fillna(df['email'].mode().iloc[0])

    # Convert 'age' to numeric
    df['age'] = pd.to_numeric(df['age'], errors='coerce')

    return df
```

### Output (script style)

```python
import pandas as pd
import numpy as np

# Remove duplicate rows
df = df.drop_duplicates()

# Fill missing values in 'email'
df['email'] = df['email'].fillna(df['email'].mode().iloc[0])

# Convert 'age' to numeric
df['age'] = pd.to_numeric(df['age'], errors='coerce')
```

---

## Global Options

These options work with all commands:

| Option | Description |
|--------|-------------|
| `--help` | Show help for any command |
| `--version` | Show DataWash version |

```bash
datawash --version
datawash analyze --help
```

---

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Error (file not found, invalid options, etc.) |

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `DATAWASH_NO_COLOR` | Disable colored output |

```bash
DATAWASH_NO_COLOR=1 datawash analyze data.csv
```

---

## Tips

### Piping Output

```bash
# Save analysis to file
datawash analyze data.csv > report.txt

# Pipe suggestions to grep
datawash suggest data.csv | grep "missing"
```

### Scripting

```bash
#!/bin/bash
# Clean multiple files

for file in data/*.csv; do
    output="clean/$(basename $file)"
    datawash clean "$file" -o "$output" --apply-all
done
```

### Combining with Other Tools

```bash
# Download, clean, and upload
curl -O https://example.com/data.csv
datawash clean data.csv -o clean.csv --apply-all
aws s3 cp clean.csv s3://mybucket/
```
