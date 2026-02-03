# Contributing to DataWash

Thank you for your interest in contributing to DataWash! This guide will help you get started.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Testing](#testing)
- [Code Style](#code-style)
- [Pull Request Process](#pull-request-process)
- [Areas for Contribution](#areas-for-contribution)

---

## Code of Conduct

Please be respectful and constructive in all interactions. We welcome contributors of all experience levels.

---

## Getting Started

### Prerequisites

- Python 3.10 or higher
- Git
- pip

### Fork and Clone

1. Fork the repository on GitHub
2. Clone your fork:

```bash
git clone https://github.com/YOUR_USERNAME/DataWash.git
cd DataWash
```

3. Add upstream remote:

```bash
git remote add upstream https://github.com/Pranav1011/DataWash.git
```

---

## Development Setup

### Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### Install Dependencies

```bash
# Install with all development dependencies
pip install -e ".[dev,all]"
```

### Verify Setup

```bash
# Run tests
pytest

# Check formatting
black --check src tests

# Check linting
ruff check src tests
```

---

## Making Changes

### Create a Branch

```bash
# Update main
git checkout main
git pull upstream main

# Create feature branch
git checkout -b feature/your-feature-name
```

### Branch Naming

- `feature/` - New features
- `fix/` - Bug fixes
- `docs/` - Documentation
- `refactor/` - Code refactoring
- `test/` - Test additions/improvements

### Commit Messages

Use clear, descriptive commit messages:

```
Add PII detection for email patterns

- Add EmailDetector class
- Add tests for email pattern matching
- Update documentation
```

---

## Testing

### Running Tests

```bash
# All tests
pytest

# With coverage
pytest --cov=datawash --cov-report=html

# Specific test file
pytest tests/test_detectors.py

# Specific test
pytest tests/test_detectors.py::TestMissingDetector::test_detects_nulls

# Verbose output
pytest -v
```

### Writing Tests

Tests live in the `tests/` directory. Follow existing patterns:

```python
# tests/test_myfeature.py

import pandas as pd
import pytest
from datawash import analyze


def test_my_feature():
    """Test description."""
    df = pd.DataFrame({"col": [1, 2, 3]})
    report = analyze(df)
    assert report.quality_score > 0


class TestMyDetector:
    def test_detects_issue(self):
        """Test that issue is detected."""
        df = pd.DataFrame({"col": [None, 2, 3]})
        report = analyze(df)
        assert any(i.issue_type == "missing_values" for i in report.issues)
```

### Test Fixtures

Use fixtures from `tests/conftest.py`:

```python
def test_with_fixture(messy_df):
    """Test using the messy_df fixture."""
    report = analyze(messy_df)
    assert len(report.issues) > 0
```

---

## Code Style

### Formatting

We use Black for formatting:

```bash
# Format all files
black src tests

# Check without modifying
black --check src tests
```

Configuration in `pyproject.toml`:
- Line length: 88 characters
- Target: Python 3.10+

### Linting

We use Ruff for linting:

```bash
# Check for issues
ruff check src tests

# Auto-fix issues
ruff check --fix src tests
```

### Type Hints

Use type hints for all public functions:

```python
def analyze(
    data: pd.DataFrame | str | Path,
    config: Optional[Config | dict[str, Any]] = None,
    use_case: str = "general",
) -> Report:
    """Analyze a dataset and return a Report."""
    ...
```

### Docstrings

Use Google-style docstrings:

```python
def my_function(param1: str, param2: int) -> bool:
    """Short description.

    Longer description if needed.

    Args:
        param1: Description of param1.
        param2: Description of param2.

    Returns:
        Description of return value.

    Raises:
        ValueError: If param2 is negative.
    """
    ...
```

---

## Pull Request Process

### Before Submitting

1. **Tests pass**: `pytest`
2. **Code formatted**: `black src tests`
3. **Linting passes**: `ruff check src tests`
4. **Documentation updated** (if needed)
5. **CHANGELOG updated** (for user-facing changes)

### Submitting

1. Push your branch:
   ```bash
   git push origin feature/your-feature-name
   ```

2. Open a Pull Request on GitHub

3. Fill in the PR template:
   - Description of changes
   - Related issue (if any)
   - Test coverage
   - Documentation updates

### Review Process

1. Maintainers will review your PR
2. Address any feedback
3. Once approved, your PR will be merged

---

## Areas for Contribution

### High Priority

#### ML Module
Implement ML-powered features:
- Semantic column similarity (sentence-transformers)
- Fuzzy duplicate detection (MinHash/LSH)
- ML-based type inference

```python
# Example: Add to src/datawash/detectors/ml_detector.py
class SemanticSimilarityDetector(BaseDetector):
    """Detect semantically similar columns using embeddings."""
    ...
```

#### Additional Detectors
- **PII Detection**: Emails, phone numbers, SSN, credit cards
- **Schema Validation**: Validate against expected types
- **Referential Integrity**: Foreign key checks

#### Performance
- Dask backend for large datasets
- Polars integration
- Parallel transformation execution

### Medium Priority

#### Cloud Connectors
- S3 adapter
- BigQuery adapter
- Snowflake adapter

#### Advanced Transformations
- KNN imputation
- MICE imputation
- Feature encoding (one-hot, target, ordinal)

#### Pipeline Export
- Airflow DAG generation
- Prefect flow generation

### Documentation
- More examples
- Tutorial notebooks
- Video walkthroughs

---

## Project Structure

```
DataWash/
├── src/datawash/
│   ├── __init__.py         # Package exports
│   ├── adapters/           # Data loading
│   ├── core/               # Models, config, report
│   ├── profiler/           # Dataset profiling
│   ├── detectors/          # Issue detection
│   ├── suggestors/         # Suggestion generation
│   ├── transformers/       # Data transformations
│   ├── codegen/            # Code generation
│   └── cli/                # Command-line interface
├── tests/                  # Test files
├── examples/               # Usage examples
├── docs/                   # Documentation
└── pyproject.toml          # Project config
```

---

## Adding a New Detector

1. Create detector file:
   ```python
   # src/datawash/detectors/my_detector.py
   from datawash.detectors.base import BaseDetector
   from datawash.detectors.registry import register_detector

   class MyDetector(BaseDetector):
       @property
       def name(self) -> str:
           return "my_detector"

       def detect(self, df, profile) -> list[Finding]:
           findings = []
           # Detection logic here
           return findings

   register_detector(MyDetector())
   ```

2. Add to `__init__.py`:
   ```python
   from datawash.detectors.my_detector import MyDetector
   ```

3. Add tests:
   ```python
   # tests/test_detectors.py
   class TestMyDetector:
       def test_detects_issue(self):
           ...
   ```

4. Update documentation

---

## Adding a New Transformer

1. Create transformer file:
   ```python
   # src/datawash/transformers/my_transformer.py
   from datawash.transformers.base import BaseTransformer
   from datawash.transformers.registry import register_transformer

   class MyTransformer(BaseTransformer):
       @property
       def name(self) -> str:
           return "my_transformer"

       def transform(self, df, **params):
           # Transformation logic
           return result_df, TransformationResult(...)

       def generate_code(self, **params) -> str:
           return "# Generated code"

   register_transformer(MyTransformer())
   ```

2. Add mapping in `suggestors/engine.py`:
   ```python
   _SUGGESTION_MAP["my_issue"] = {
       "action": "Fix my issue",
       "transformer": "my_transformer",
       ...
   }
   ```

3. Add tests and documentation

---

## Questions?

- Open an issue for questions
- Join discussions on GitHub
- Check existing issues before creating new ones

Thank you for contributing!
