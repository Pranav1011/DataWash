# DataWash

**Intelligent data cleaning and quality analysis for Python.**

DataWash analyzes tabular data, detects quality issues using rules and statistics, suggests prioritized fixes, and generates reproducible Python code for transformations. 

**Current Status:** 🚧 **Alpha** - Core functionality complete, ML features planned for next phase.

## Installation

```bash
pip install datawash
```

Optional extras:

```bash
pip install datawash[formats]  # Parquet + Excel support
pip install datawash[ml]       # ML-powered detection (planned)
pip install datawash[all]      # All optional dependencies
pip install datawash[dev]      # Development tools
```

## Quick Start

```python
from datawash import analyze

# Analyze your data
report = analyze("data.csv")

# View comprehensive analysis
print(report.summary())               # Quality score + overview
print(f"Quality Score: {report.quality_score}/100")

# Examine issues found
for issue in report.issues:
    print(f"[{issue.severity}] {issue.category}: {issue.description}")

# See prioritized suggestions
for s in report.suggestions:
    print(f"[{s.id}] {s.action} -- {s.rationale}")

# Apply all fixes automatically
clean_df = report.apply_all()

# Or apply selectively by ID
clean_df = report.apply([1, 3, 5])

# Or review and apply interactively
clean_df = report.apply_interactive()

# Generate reproducible Python code
print(report.generate_code())
```

## CLI Usage

```bash
# Analyze a dataset and view quality report
datawash analyze data.csv

# Get cleaning suggestions prioritized by use case
datawash suggest data.csv --use-case ml

# Clean and export with automatic fixes
datawash clean data.csv -o clean.csv --apply-all

# Apply specific suggestions by ID
datawash clean data.csv -o clean.csv --ids 1,2,5

# Generate reproducible Python code
datawash codegen data.csv --apply-all --style function
```

## Features

### ✅ Currently Implemented

#### Data Loading & Profiling
- **Multi-format support**: CSV, JSON, Parquet, Excel
- **Comprehensive profiling**: Column types, statistics, patterns, missing values
- **Quality scoring**: 0-100 data quality score with detailed breakdown

#### Quality Detection (6 Detectors)
- **Missing values**: Nulls, empty strings, whitespace-only values
- **Duplicates**: Exact duplicate row detection
- **Format issues**: Mixed case, inconsistent date formats, leading/trailing whitespace
- **Outliers**: IQR and Z-score based detection for numeric columns
- **Type mismatches**: Numeric/boolean data stored as strings
- **Similar columns**: String similarity detection (Levenshtein distance)

#### Data Transformations (6 Transformers)
- **Duplicates**: Remove exact duplicate rows
- **Missing data**: Drop rows, fill with median/mode/custom value, clean empty strings (combined convert+fill)
- **Type conversion**: Convert to numeric, boolean, datetime
- **Format standardization**: Strip whitespace, standardize case (upper/lower/title), normalize dates
- **Column operations**: Drop, rename, merge columns
- **Category normalization**: Standardize categorical values

#### Intelligent Suggestion System
- **Use-case aware**: Prioritization for ML, analytics, export, or general purposes
- **Contextual rationale**: Each suggestion includes why it's recommended
- **Selective application**: Apply all, specific IDs, or interactive review
- **Conflict resolution**: Automatic exclusion of conflicting transformations (e.g., no case changes on boolean/date columns)
- **Execution ordering**: Transformations applied in optimal order to prevent conflicts

#### Code Generation
- **Reproducible workflows**: Generate Python code from applied transformations
- **Multiple styles**: Function wrapper or standalone script
- **Tested output**: Generated code includes proper imports and error handling

#### CLI & UX
- **Rich terminal output**: Colored tables, progress bars, formatted reports
- **Interactive mode**: Step-by-step review and application with prompts
- **Jupyter support**: HTML representation in notebooks via `_repr_html_()`
- **Cross-platform**: Works on Linux, macOS, Windows

### 🚧 In Progress / Planned

#### ML-Powered Features (Phase 4)
- **Semantic similarity**: Column similarity using sentence transformers
- **Fuzzy duplicate detection**: MinHash-based approximate matching
- **ML type classification**: Advanced type inference using learned patterns
- **Embedding-based clustering**: Group similar records

#### Extended Detection
- **Schema validation**: Validate against expected schemas
- **Referential integrity**: Check foreign key relationships
- **Business rules**: Custom validation rules
- **PII detection**: Identify personally identifiable information

#### Advanced Transformations
- **Smart imputation**: KNN, MICE, or model-based missing value filling
- **Encoding**: One-hot, target, ordinal encoding for ML
- **Outlier handling**: More sophisticated outlier treatment strategies
- **Feature engineering**: Automated feature generation suggestions

#### Performance & Scale
- **Lazy evaluation**: Dask/Polars backend for large datasets
- **Incremental processing**: Stream processing for files > memory
- **Parallel execution**: Multi-core transformation application
- **Caching**: Memoization of expensive operations

#### Integration & Deployment
- **Pipeline export**: Export to Airflow, Prefect, or Dagster
- **API server**: REST API for web integration
- **Cloud connectors**: S3, BigQuery, Snowflake, etc.
- **Monitoring**: Data quality monitoring over time

## Configuration

DataWash is highly configurable. Customize detection thresholds, suggestion limits, and behavior:

```python
from datawash import analyze

report = analyze(
    "data.csv",
    use_case="ml",  # Prioritize suggestions for ML workflows
    config={
        "detectors": {
            "outlier_method": "zscore",      # or "iqr"
            "outlier_threshold": 2.5,        # Z-score threshold
            "min_similarity": 0.8,           # Column similarity threshold
        },
        "suggestions": {
            "max_suggestions": 20,           # Limit suggestion count
        },
        "profiling": {
            "sample_rows": 10000,            # Sample for large datasets
        },
    },
)
```

### Use Cases

Choose a use case to get prioritized suggestions:

- **`general`**: Balanced approach for data exploration
- **`ml`**: Prioritize numeric types, handle missing values, remove duplicates
- **`analytics`**: Focus on consistency and format standardization
- **`export`**: Prepare clean data for sharing/export

## Project Status

### Statistics
- **~2,600 lines** of source code
- **~1,000 lines** of test code  
- **114 tests** with **92% coverage**
- **10 modules** fully implemented
- **1 module** (ml) planned for Phase 4

### Test Coverage
- ✅ Unit tests for all detectors
- ✅ Unit tests for all transformers
- ✅ Unit tests for profiler, adapters, CLI
- ✅ Integration tests for end-to-end workflows
- ✅ Edge case tests (empty data, single row, all nulls, etc.)
- ✅ Code generation validation

### What Works Today
1. Load data from CSV, JSON, Parquet, Excel
2. Profile dataset with comprehensive statistics
3. Detect 6 types of data quality issues
4. Generate prioritized, actionable suggestions
5. Apply transformations automatically or selectively
6. Generate reproducible Python code
7. Use via Python API or CLI
8. Rich terminal output with progress indicators

### What's Next
1. **ML-powered detection** (sentence-transformers, datasketch)
2. **Advanced imputation** strategies
3. **Performance optimizations** for large datasets
4. **Cloud integrations** (S3, BigQuery, etc.)
5. **Pipeline export** to orchestration tools

## Requirements

- **Python** >= 3.10
- **Core dependencies**: pandas, numpy, pydantic, rich, typer, scikit-learn, python-Levenshtein
- **Optional**: pyarrow (Parquet), openpyxl (Excel), sentence-transformers (ML), datasketch (ML)

## Development

```bash
# Clone the repository
git clone https://github.com/Pranav1011/DataWash.git
cd DataWash

# Install with dev dependencies
pip install -e ".[dev,all]"

# Run tests
pytest

# Run tests with coverage
pytest --cov=datawash --cov-report=html

# Format code
black src tests
ruff check src tests

# Type checking
mypy src
```

## Contributing

Contributions are welcome! Areas where help is needed:

1. **ML module implementation** - sentence-transformers integration for semantic similarity
2. **Additional detectors** - PII detection, schema validation, business rules
3. **Performance optimization** - Dask/Polars backends for large datasets
4. **Documentation** - Examples, tutorials, API docs
5. **Cloud connectors** - S3, BigQuery, Snowflake adapters

Please open an issue to discuss before starting work on major features.

## Roadmap

### Version 0.2.0 (Q2 2026)
- [ ] ML module with sentence-transformers integration
- [ ] Fuzzy duplicate detection with MinHash
- [ ] Advanced imputation strategies (KNN, MICE)
- [ ] Performance benchmarks and optimization

### Version 0.3.0 (Q3 2026)
- [ ] Dask backend for large datasets
- [ ] Cloud storage connectors
- [ ] Pipeline export (Airflow, Prefect)
- [ ] Web API server

### Version 1.0.0 (Q4 2026)
- [ ] Production-ready with comprehensive testing
- [ ] Complete documentation and examples
- [ ] Performance guarantees
- [ ] Stable API

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

Built with:
- [pandas](https://pandas.pydata.org/) - Data manipulation
- [pydantic](https://pydantic-docs.helpmanual.io/) - Data validation
- [rich](https://rich.readthedocs.io/) - Terminal formatting
- [typer](https://typer.tiangolo.com/) - CLI framework
- [scikit-learn](https://scikit-learn.org/) - Statistical methods
