# DataWash

<p align="center">
  <strong>Intelligent data cleaning and quality analysis for Python</strong>
</p>

<p align="center">
  <a href="#installation">Installation</a> •
  <a href="#quick-start">Quick Start</a> •
  <a href="#features">Features</a> •
  <a href="#documentation">Documentation</a> •
  <a href="#examples">Examples</a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue" alt="Python">
  <img src="https://img.shields.io/badge/coverage-92%25-brightgreen" alt="Coverage">
  <img src="https://img.shields.io/badge/tests-114%20passing-brightgreen" alt="Tests">
  <img src="https://img.shields.io/badge/license-MIT-green" alt="License">
</p>

---

DataWash analyzes your tabular data, detects quality issues, suggests prioritized fixes, and generates reproducible Python code — all in a few lines of code.

```python
from datawash import analyze

report = analyze("messy_data.csv")
print(f"Quality Score: {report.quality_score}/100")
clean_df = report.apply_all()
print(report.generate_code())
```

## Why DataWash?

| Problem | DataWash Solution |
|---------|-------------------|
| Missing values silently break ML models | Automatic detection + smart filling strategies |
| Inconsistent date formats cause parsing errors | Detects and standardizes to ISO format |
| Duplicate rows inflate statistics | Identifies and removes exact duplicates |
| Boolean values stored as "yes"/"no" strings | Converts to proper boolean type |
| Manual data cleaning is tedious and error-prone | Generates reproducible Python code |

## Installation

```bash
pip install datawash
```

**Optional extras:**

```bash
pip install datawash[formats]  # Parquet + Excel support
pip install datawash[ml]       # ML-powered detection (coming soon)
pip install datawash[all]      # All optional dependencies
pip install datawash[dev]      # Development tools
```

## Quick Start

### Python API

```python
from datawash import analyze

# 1. Analyze your data (sampling enabled by default for large datasets)
report = analyze("data.csv")  # or pass a DataFrame

# 2. Check quality score
print(f"Quality Score: {report.quality_score}/100")
print(f"Issues Found: {len(report.issues)}")

# 3. Review suggestions
for s in report.suggestions:
    print(f"[{s.id}] {s.action}")

# 4. Apply all fixes
clean_df = report.apply_all()

# 5. Or apply selectively
clean_df = report.apply([1, 3, 5])  # by suggestion ID

# 6. Generate reproducible code
print(report.generate_code())

# Disable sampling for exact results on large datasets
report = analyze("data.csv", sample=False)

# Disable parallel processing
report = analyze("data.csv", parallel=False)
```

### Command Line

```bash
# Analyze and see quality report
datawash analyze data.csv

# Get prioritized suggestions
datawash suggest data.csv --use-case ml

# Clean and export
datawash clean data.csv -o clean.csv --apply-all

# Generate Python code
datawash codegen data.csv --apply-all
```

## Features

### Data Quality Detection

| Detector | What It Finds |
|----------|---------------|
| **Missing** | Null values, empty strings, whitespace-only values |
| **Duplicates** | Exact duplicate rows |
| **Formats** | Mixed case, inconsistent dates, whitespace padding |
| **Outliers** | Statistical anomalies (IQR or Z-score) |
| **Types** | Numbers/booleans stored as strings |
| **Similarity** | Potentially duplicate columns |

### Smart Transformations

| Transformer | Operations |
|-------------|------------|
| **Missing** | Drop rows, fill with median/mode/value, clean empty strings |
| **Duplicates** | Remove exact duplicates |
| **Types** | Convert to numeric, boolean, datetime |
| **Formats** | Standardize case, dates, strip whitespace |
| **Columns** | Drop, rename, merge columns |
| **Categories** | Normalize categorical values |

### Intelligent Suggestion System

- **Conflict Resolution**: Automatically prevents conflicting transformations
- **Execution Ordering**: Applies fixes in optimal order (6 phases)
- **Use-Case Aware**: Priorities adjust for ML, analytics, or export workflows
- **Contextual Rationale**: Every suggestion explains why it's recommended

### Code Generation

```python
# Generate a reusable cleaning function
code = report.generate_code(style="function")

# Or a standalone script
code = report.generate_code(style="script")
```

## Performance

DataWash v0.2.0 is optimized for large datasets:

| Dataset | Time | Throughput |
|---------|------|------------|
| 1M rows x 10 cols | 0.72s | 1.4M rows/sec |
| 100K rows x 50 cols | 2.13s | 47K rows/sec |
| 10K rows x 100 cols | 4.35s | 2.3K rows/sec |
| 1M rows x 50 cols | 3.24s | 309K rows/sec |
| 50K rows x 250 cols | 9.99s | 5K rows/sec |

**Optimizations include:**
- Smart sampling for datasets >=50K rows (10-20x speedup)
- Parallel column profiling and detection
- 31% memory reduction via dtype optimization
- O(n) similarity detection with MinHash + LSH

## Examples

We provide ready-to-run examples in the `examples/` directory:

| Example | Description |
|---------|-------------|
| [`quickstart.py`](https://github.com/Pranav1011/DataWash/blob/main/examples/quickstart.py) | Basic workflow: analyze → suggest → apply → codegen |
| [`csv_cleaning.py`](https://github.com/Pranav1011/DataWash/blob/main/examples/csv_cleaning.py) | Load CSV, clean, save with CLI equivalents |
| [`ml_preprocessing.py`](https://github.com/Pranav1011/DataWash/blob/main/examples/ml_preprocessing.py) | ML-optimized cleaning workflow |
| [`jupyter_demo.ipynb`](https://github.com/Pranav1011/DataWash/blob/main/examples/jupyter_demo.ipynb) | Interactive notebook with visualizations |

**Sample datasets** in `examples/sample_data/`:
- `customers_messy.csv` - Names, emails, phones with various issues
- `orders_messy.csv` - Dates, amounts, categories with inconsistencies
- `employees_messy.csv` - Mixed types, duplicates, outliers

```bash
# Run an example
python examples/quickstart.py
```

## Documentation

| Document | Description |
|----------|-------------|
| [Getting Started](https://github.com/Pranav1011/DataWash/blob/main/docs/getting-started.md) | Installation and first steps |
| [User Guide](https://github.com/Pranav1011/DataWash/blob/main/docs/user-guide.md) | Complete feature walkthrough |
| [API Reference](https://github.com/Pranav1011/DataWash/blob/main/docs/api-reference.md) | Detailed API documentation |
| [CLI Reference](https://github.com/Pranav1011/DataWash/blob/main/docs/cli-reference.md) | Command-line interface guide |
| [Configuration](https://github.com/Pranav1011/DataWash/blob/main/docs/configuration.md) | Customization options |
| [Contributing](https://github.com/Pranav1011/DataWash/blob/main/docs/contributing.md) | How to contribute |

## Use Cases

Choose a use case to get optimized suggestions:

```python
report = analyze(df, use_case="ml")  # or "general", "analytics", "export"
```

| Use Case | Prioritizes |
|----------|-------------|
| `general` | Balanced approach for exploration |
| `ml` | Duplicates, missing values, type conversions |
| `analytics` | Consistency, date formats, outliers |
| `export` | Format standardization, clean values |

## Configuration

```python
report = analyze(
    "data.csv",
    use_case="ml",
    config={
        "detectors": {
            "outlier_method": "zscore",  # or "iqr"
            "outlier_threshold": 2.5,
            "min_similarity": 0.8,
        },
        "suggestions": {
            "max_suggestions": 20,
        },
    },
)
```

## Project Status

| Metric | Value |
|--------|-------|
| Source Code | ~2,900 lines |
| Test Code | ~1,270 lines |
| Tests | 114 passing |
| Coverage | ~92% |
| Python | 3.10, 3.11, 3.12 |
| Platforms | Linux, macOS, Windows |

### What's Working

- ✅ Multi-format loading (CSV, JSON, Parquet, Excel)
- ✅ Comprehensive profiling and statistics
- ✅ 6 detectors for common data quality issues
- ✅ 6 transformers with multiple operations each
- ✅ Smart suggestion system with conflict resolution
- ✅ Reproducible Python code generation
- ✅ Rich CLI with colored output
- ✅ Jupyter notebook support

### What's Next

- ML-powered semantic similarity detection
- Fuzzy duplicate detection for near-duplicate rows
- Advanced imputation (KNN, MICE)
- Cloud storage connectors (S3, BigQuery)
- PII detection for sensitive data
- Schema validation for expected column types and constraints

## Requirements

- **Python** >= 3.10
- **Core**: pandas, numpy, pydantic, rich, typer, scikit-learn
- **Optional**: pyarrow (Parquet), openpyxl (Excel)

## Development

```bash
# Clone and install
git clone https://github.com/Pranav1011/DataWash.git
cd DataWash
pip install -e ".[dev,all]"

# Run tests
pytest

# Format code
black src tests
ruff check src tests
```

## Contributing

Contributions welcome! See [CONTRIBUTING.md](https://github.com/Pranav1011/DataWash/blob/main/docs/contributing.md) for guidelines.

**Areas where help is needed:**
- ML module implementation (sentence-transformers)
- Additional detectors (PII, schema validation)
- Performance optimization
- Documentation and examples
- Cloud connectors

## License

MIT License - see [LICENSE](https://github.com/Pranav1011/DataWash/blob/main/LICENSE) for details.

## Acknowledgments

Built with [pandas](https://pandas.pydata.org/), [pydantic](https://pydantic-docs.helpmanual.io/), [rich](https://rich.readthedocs.io/), [typer](https://typer.tiangolo.com/), and [scikit-learn](https://scikit-learn.org/).
