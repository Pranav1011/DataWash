# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

## [0.1.0] - 2025-02-01

### Added
- Core analysis pipeline: analyze, profile, detect, suggest, transform, codegen
- Data adapters: CSV, JSON, Parquet (requires pyarrow), Excel (requires openpyxl)
- Profiler with numeric/categorical statistics and pattern detection
- 6 detectors: missing, duplicates, format, outliers, types, similarity
- 6 transformers: duplicates, missing, types, formats, columns, categories
- Code generation in function and script styles
- CLI with analyze, suggest, clean, and codegen commands
- Rich terminal output with colored tables
- Jupyter notebook support via _repr_html_()
- Pydantic-based configuration system
- Data quality score (0-100)
- Use-case based suggestion filtering (general, ml, analytics, export)
- 91 tests
