# Manual Testing Checklist

Before publishing to PyPI, run through these tests to verify everything works.

## Test Dataset

Use the test file: `examples/sample_data/test_manual.csv`

This dataset contains these intentional issues:
- **Duplicate row**: Rows 1 and 6 are identical
- **Missing values**: Empty email (row 3), empty salary (row 8), empty phone (row 8), whitespace-only email (row 11)
- **Case inconsistency**: department has "Engineering", "engineering", "ENGINEERING"
- **Boolean as string**: is_active has "yes/Yes/YES/no/No/true/false/TRUE/FALSE"
- **Mixed date formats**: "2023-01-15", "15/02/2023", "March 10 2023", "2023/05/25"
- **Numeric as string**: age has "invalid" in row 7
- **Whitespace**: name "  Alice Johnson  " has leading/trailing spaces
- **Outlier**: salary 150000 is an outlier compared to others

---

## Part 1: CLI Testing

### Setup
```bash
# Create and activate a clean virtual environment
python -m venv /tmp/datawash_publish_test
source /tmp/datawash_publish_test/bin/activate

# Install from local build
cd /path/to/Dataprep
pip install dist/datawash-0.1.0-py3-none-any.whl

# Verify installation
datawash --help
```

### Test 1: Analyze Command
```bash
datawash analyze examples/sample_data/test_manual.csv
```

**Expected output should show:**
- [ ] Dataset Overview: 12 rows x 9 columns
- [ ] Column profiles table with types, nulls, unique counts
- [ ] Issues detected (duplicates, missing values, type issues, etc.)
- [ ] Suggestions list with IDs, priorities, actions

### Test 2: Suggest Command
```bash
# Basic suggest
datawash suggest examples/sample_data/test_manual.csv

# With ML use case (higher priority for missing values)
datawash suggest examples/sample_data/test_manual.csv --use-case ml

# Filter by priority
datawash suggest examples/sample_data/test_manual.csv --priority high

# Limit suggestions
datawash suggest examples/sample_data/test_manual.csv --limit 5
```

**Expected:**
- [ ] Suggestions displayed with priorities
- [ ] ML use case shows different priority ordering
- [ ] Priority filter works
- [ ] Limit flag respected

### Test 3: Clean Command
```bash
# Apply all suggestions
datawash clean examples/sample_data/test_manual.csv -o /tmp/clean_all.csv --apply-all

# Apply specific suggestions (adjust IDs based on your output)
datawash clean examples/sample_data/test_manual.csv -o /tmp/clean_some.csv --apply 1,2,3

# With codegen output
datawash clean examples/sample_data/test_manual.csv -o /tmp/clean.csv --apply-all --codegen /tmp/clean_code.py
```

**Expected:**
- [ ] Clean file created at output path
- [ ] Quality score before/after displayed
- [ ] Row count change shown (duplicate removed)
- [ ] Codegen file created when requested

**Verify cleaned data:**
```bash
# Check duplicate was removed
wc -l /tmp/clean_all.csv  # Should be 12 (11 data rows + header, not 13)

# Check the cleaned data
head -5 /tmp/clean_all.csv
```

### Test 4: Codegen Command
```bash
# Generate function-style code
datawash codegen examples/sample_data/test_manual.csv --apply-all

# Generate script-style code
datawash codegen examples/sample_data/test_manual.csv --apply-all --style script

# Save to file
datawash codegen examples/sample_data/test_manual.csv --apply-all -o /tmp/cleaning_script.py
```

**Expected:**
- [ ] Function style wraps code in `def clean_data(df):`
- [ ] Script style is standalone executable
- [ ] File output works

### Test 5: Error Handling
```bash
# Non-existent file
datawash analyze /nonexistent/file.csv
# Expected: Error message about file not found

# Unsupported format
datawash analyze examples/README.md
# Expected: Error about unsupported format

# Missing required flags
datawash clean examples/sample_data/test_manual.csv -o /tmp/out.csv
# Expected: Error asking for --apply or --apply-all
```

---

## Part 2: Python API Testing

```python
# Start Python REPL
python
```

### Test 6: Basic Workflow
```python
import pandas as pd
from datawash import analyze

# Load and analyze
report = analyze("examples/sample_data/test_manual.csv")

# Check basics
print(f"Rows: {report.profile.row_count}")        # Should be 12
print(f"Columns: {report.profile.column_count}")  # Should be 9
print(f"Quality Score: {report.quality_score}")   # Should be < 100
print(f"Issues: {len(report.issues)}")            # Should be > 0
print(f"Suggestions: {len(report.suggestions)}")  # Should be > 0

# View summary
print(report.summary())
```

**Expected:**
- [ ] 12 rows, 9 columns
- [ ] Quality score less than 100
- [ ] Multiple issues detected
- [ ] Multiple suggestions generated

### Test 7: Apply Transformations
```python
# Apply all
clean_df = report.apply_all()

print(f"Original rows: {len(report.df)}")  # 12
print(f"Cleaned rows: {len(clean_df)}")    # 11 (duplicate removed)

# Check scores
print(f"Before: {report._last_score_before}")
print(f"After: {report._last_score_after}")
# After should be higher than before
```

### Test 8: Selective Apply
```python
report2 = analyze("examples/sample_data/test_manual.csv")

# See suggestion IDs
for s in report2.suggestions:
    print(f"[{s.id}] {s.priority.value}: {s.action}")

# Apply only first 2
clean_df = report2.apply([1, 2])
```

### Test 9: Code Generation
```python
report3 = analyze("examples/sample_data/test_manual.csv")
report3.apply_all()

# Function style
code = report3.generate_code(style="function")
print(code)

# Script style
code_script = report3.generate_code(style="script")
print(code_script)
```

**Expected:**
- [ ] Code is valid Python
- [ ] Uses repr() for column names (check for quotes)
- [ ] Includes pandas/numpy imports

### Test 10: DataFrame Input
```python
# Create messy DataFrame directly
df = pd.DataFrame({
    "name": ["Alice", "BOB", "alice"],
    "value": [1, None, 3]
})

report = analyze(df)
print(f"Issues: {len(report.issues)}")
clean = report.apply_all()
print(clean)
```

### Test 11: Use Cases
```python
df = pd.DataFrame({
    "feature": [1, None, 3, 4, 5],
    "label": [0, 1, 0, 1, 0]
})

# Compare priorities
report_general = analyze(df, use_case="general")
report_ml = analyze(df, use_case="ml")

print("General priorities:")
for s in report_general.suggestions[:3]:
    print(f"  {s.priority.value}: {s.action}")

print("\nML priorities:")
for s in report_ml.suggestions[:3]:
    print(f"  {s.priority.value}: {s.action}")
```

---

## Part 3: Run Example Scripts

```bash
# Quickstart example
python examples/quickstart.py

# CSV cleaning example
python examples/csv_cleaning.py

# ML preprocessing example
python examples/ml_preprocessing.py
```

**Expected:**
- [ ] All three scripts run without errors
- [ ] Output shows analysis → suggestions → cleaning → code generation

---

## Part 4: Edge Cases

### Test 12: Empty DataFrame
```python
import pandas as pd
from datawash import analyze

empty = pd.DataFrame()
report = analyze(empty)
print(f"Score: {report.quality_score}")  # Should be 100
print(f"Issues: {len(report.issues)}")   # Should be 0
```

### Test 13: Single Row
```python
single = pd.DataFrame({"a": [1], "b": ["x"]})
report = analyze(single)
print(f"Score: {report.quality_score}")
clean = report.apply_all()
print(clean)
```

### Test 14: All Nulls Column
```python
nulls = pd.DataFrame({
    "good": [1, 2, 3],
    "bad": [None, None, None]
})
report = analyze(nulls)
for issue in report.issues:
    print(f"{issue.issue_type}: {issue.message}")
```

### Test 15: Special Characters in Column Names
```python
special = pd.DataFrame({
    "normal": [1, None, 3],
    "with space": [4, 5, 6],
    "with'quote": [7, 8, None],
})
report = analyze(special)
report.apply_all()
code = report.generate_code()
print(code)
# Verify column names are properly quoted
```

---

## Part 5: Performance Check

```bash
# Generate a larger test file (10,000 rows)
python -c "
import pandas as pd
import numpy as np

n = 10000
df = pd.DataFrame({
    'id': range(n),
    'name': np.random.choice(['Alice', 'Bob', 'Charlie', None], n),
    'value': np.random.randn(n),
    'category': np.random.choice(['A', 'B', 'C', 'a', 'b'], n),
})
# Add some duplicates
df = pd.concat([df, df.head(100)])
df.to_csv('/tmp/large_test.csv', index=False)
print(f'Created {len(df)} rows')
"

# Time the analysis
time datawash analyze /tmp/large_test.csv
```

**Expected:**
- [ ] Analysis completes in reasonable time (< 10 seconds for 10K rows)
- [ ] No memory errors

---

## Final Verification Checklist

Before publishing, confirm:

- [ ] All CLI commands work (`analyze`, `suggest`, `clean`, `codegen`)
- [ ] All CLI flags work (`--use-case`, `--priority`, `--limit`, `--apply`, `--apply-all`, `-o`, `--codegen`, `--style`)
- [ ] Python API works (`analyze()`, `report.apply()`, `report.apply_all()`, `report.generate_code()`)
- [ ] Error messages are helpful (not exposing data values)
- [ ] Example scripts run successfully
- [ ] Edge cases handled gracefully
- [ ] Performance is acceptable

---

## Cleanup

```bash
# Remove test environment
deactivate
rm -rf /tmp/datawash_publish_test
rm /tmp/clean*.csv /tmp/clean*.py /tmp/large_test.csv 2>/dev/null
```
