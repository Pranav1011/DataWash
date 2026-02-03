# Getting Started with DataWash

This guide will help you get DataWash up and running quickly.

## Installation

### Basic Installation

```bash
pip install datawash
```

### With Optional Dependencies

```bash
# For Parquet and Excel file support
pip install datawash[formats]

# For ML-powered features (coming soon)
pip install datawash[ml]

# All optional dependencies
pip install datawash[all]

# Development tools
pip install datawash[dev]
```

### From Source

```bash
git clone https://github.com/Pranav1011/DataWash.git
cd DataWash
pip install -e .
```

## Verify Installation

```python
import datawash
print(datawash.__version__)  # Should print "0.1.0"
```

Or from command line:

```bash
datawash --help
```

## Your First Analysis

### Using Python

```python
import pandas as pd
from datawash import analyze

# Create some messy data
df = pd.DataFrame({
    "name": ["John", "JANE", "bob", "  Alice  "],
    "age": ["25", "30", "35", "28"],  # Stored as strings!
    "active": ["yes", "Yes", "YES", "no"],
})

# Analyze it
report = analyze(df)

# See what DataWash found
print(f"Quality Score: {report.quality_score}/100")
print(f"Issues: {len(report.issues)}")
print(f"Suggestions: {len(report.suggestions)}")

# Apply all fixes
clean_df = report.apply_all()
print(clean_df)
```

### Using the CLI

```bash
# Save the data to a CSV
echo "name,age,active
John,25,yes
JANE,30,Yes
bob,35,YES
Alice,28,no" > sample.csv

# Analyze it
datawash analyze sample.csv

# See suggestions
datawash suggest sample.csv

# Clean it
datawash clean sample.csv -o clean.csv --apply-all
```

## Understanding the Output

### Quality Score

DataWash assigns a quality score from 0-100:

| Score | Meaning |
|-------|---------|
| 90-100 | Excellent - minimal issues |
| 70-89 | Good - some issues to address |
| 50-69 | Fair - several issues need attention |
| 0-49 | Poor - significant cleaning needed |

### Issues

Issues are problems detected in your data:

```python
for issue in report.issues:
    print(f"[{issue.severity.value}] {issue.issue_type}")
    print(f"  {issue.message}")
    print(f"  Columns: {issue.columns}")
```

Severities:
- **HIGH**: Critical issues that will cause errors
- **MEDIUM**: Important issues that affect analysis
- **LOW**: Minor issues worth addressing

### Suggestions

Suggestions are actionable fixes:

```python
for s in report.suggestions:
    print(f"[{s.id}] {s.action}")
    print(f"  Priority: {s.priority.value}")
    print(f"  Impact: {s.impact}")
    print(f"  Rationale: {s.rationale}")
```

## Applying Fixes

### Apply All

```python
clean_df = report.apply_all()
```

### Apply Selectively

```python
# Apply only suggestions 1, 3, and 5
clean_df = report.apply([1, 3, 5])
```

### Interactive Mode

```python
# Review each suggestion before applying
clean_df = report.apply_interactive()
```

## Generating Code

Get reproducible Python code:

```python
# As a function
code = report.generate_code(style="function")
print(code)

# As a script
code = report.generate_code(style="script")
```

## Loading Different Formats

```python
# CSV
report = analyze("data.csv")

# JSON
report = analyze("data.json")

# Parquet (requires pyarrow)
report = analyze("data.parquet")

# Excel (requires openpyxl)
report = analyze("data.xlsx")

# DataFrame directly
report = analyze(df)
```

## Next Steps

- Read the [User Guide](user-guide.md) for complete feature documentation
- Check out [Examples](../examples/) for practical use cases
- See [API Reference](api-reference.md) for detailed function documentation
- Explore [CLI Reference](cli-reference.md) for command-line options
