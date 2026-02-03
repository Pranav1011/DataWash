# DataWash - Intelligent Data Cleaning

## Name: `datawash`

## Changes from Original Spec

1. **Renamed** from `dataprep` to `datawash` (avoids PyPI conflict)
2. **Optional ML extras** - `datawash[ml]` for sentence-transformers, datasketch. Core package uses only pandas/numpy/scikit-learn/pydantic/rich/typer
3. **String similarity first** - Similar column detection uses difflib/Levenshtein by default, embeddings only with `[ml]` installed
4. **Fuzzy duplicates optional** - MinHash-based detection requires `[ml]` extras
5. **Implementation before tests** - Write implementation, then tests per module
6. **Code generation designed per-transformer** - Each transformer owns its code generation with tested edge cases

## Project Structure

```
datawash/
├── pyproject.toml
├── SPEC.md
├── .gitignore
├── src/
│   └── datawash/
│       ├── __init__.py
│       ├── core/
│       │   ├── __init__.py
│       │   ├── report.py
│       │   ├── config.py
│       │   ├── models.py
│       │   └── exceptions.py
│       ├── adapters/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── csv_adapter.py
│       │   ├── parquet_adapter.py
│       │   ├── excel_adapter.py
│       │   └── json_adapter.py
│       ├── profiler/
│       │   ├── __init__.py
│       │   ├── engine.py
│       │   ├── statistics.py
│       │   └── patterns.py
│       ├── detectors/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── registry.py
│       │   ├── missing_detector.py
│       │   ├── duplicate_detector.py
│       │   ├── format_detector.py
│       │   ├── outlier_detector.py
│       │   ├── type_detector.py
│       │   └── similarity_detector.py
│       ├── ml/
│       │   ├── __init__.py
│       │   ├── embeddings.py
│       │   ├── type_classifier.py
│       │   └── fuzzy_match.py
│       ├── suggestors/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── engine.py
│       │   └── prioritizer.py
│       ├── transformers/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── registry.py
│       │   ├── duplicates.py
│       │   ├── missing.py
│       │   ├── types.py
│       │   ├── formats.py
│       │   ├── columns.py
│       │   └── categories.py
│       ├── codegen/
│       │   ├── __init__.py
│       │   └── generator.py
│       └── cli/
│           ├── __init__.py
│           ├── main.py
│           └── formatters.py
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_adapters.py
│   ├── test_profiler.py
│   ├── test_detectors.py
│   ├── test_suggestors.py
│   ├── test_transformers.py
│   ├── test_codegen.py
│   ├── test_cli.py
│   └── test_integration.py
```

## Dependencies

### Core (always installed)
```
pandas>=1.5.0
numpy>=1.21.0
pydantic>=2.0.0
rich>=13.0.0
typer>=0.9.0
scikit-learn>=1.0.0
python-Levenshtein>=0.21.0
```

### Optional ML extras (`datawash[ml]`)
```
sentence-transformers>=2.2.0
datasketch>=1.5.0
```

### Optional format extras (`datawash[formats]`)
```
pyarrow>=10.0.0
openpyxl>=3.0.0
```

### Dev
```
pytest>=7.0.0
pytest-cov>=4.0.0
black>=23.0.0
ruff>=0.1.0
mypy>=1.0.0
```

## Build Order

### Phase 1: Foundation + End-to-End Loop
- pyproject.toml, project skeleton
- core/models.py, core/config.py, core/exceptions.py
- adapters/base.py, adapters/csv_adapter.py
- profiler/ (engine, statistics, patterns)
- detectors/base.py, missing_detector, duplicate_detector
- suggestors/ (minimal engine mapping findings to suggestions)
- transformers/base.py, duplicates.py, missing.py
- core/report.py wired end-to-end
- Tests for Phase 1

### Phase 2: More Detectors + Transformers + Codegen
- format_detector, outlier_detector, type_detector (pattern-based)
- similarity_detector (difflib-based, no ML)
- transformers: types.py, formats.py, columns.py, categories.py
- codegen/generator.py
- suggestors/prioritizer.py
- Tests for Phase 2

### Phase 3: CLI
- cli/main.py (typer), cli/formatters.py (rich)
- Commands: analyze, suggest, clean, codegen
- Tests for Phase 3

### Phase 4: Additional Adapters + ML
- parquet, excel, json adapters
- ml/embeddings.py, ml/type_classifier.py, ml/fuzzy_match.py
- Enhanced similarity_detector and type_detector with ML paths
- Tests for Phase 4

## API (unchanged from original spec)

```python
from datawash import analyze

report = analyze("data.csv")
print(report.suggestions)
clean_df = report.apply_all()
code = report.generate_code()
```

```bash
datawash analyze data.csv
datawash suggest data.csv --use-case ml
datawash clean data.csv -o clean.csv --apply-all
```
