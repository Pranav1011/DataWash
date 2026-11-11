# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Fixed
- Date standardization now parses mixed formats (e.g. `2024-01-15`, `01/20/2024`, `Jan 30, 2024`) on pandas 2 and 3; previously only values matching the first format were converted. Generated code uses the same parsing, and "rows affected" counts only values that changed.
- CLI errors for missing, empty, unsupported or binary files and non-numeric `--apply` IDs now print one red line with a fix and exit with code 1, instead of a traceback.

### Changed
- Minimum pandas version raised to 2.0 (needed for `format="mixed"` date parsing).

## [1.0.0] - 2026-02-07

### First Stable Release

DataWash is now production-ready with comprehensive data quality analysis and cleaning capabilities.

### Added
- **PII Detection**: Automatic detection of personally identifiable information
  - Email addresses (standard format)
  - Social Security Numbers (dashes, spaces, dots, context-aware no-separator)
  - Phone numbers (US + international: UK, EU, Asia-Pacific)
  - Credit cards (Visa, Mastercard, Discover, American Express with Luhn validation)
  - IPv4 addresses
  - US ZIP codes
  - Column name heuristics for PII-suggestive names
- **PII Transformers**: Four operations for handling detected PII
  - `mask`: Keep last N characters visible (e.g., `*******6789`)
  - `hash`: SHA256 hashing with optional salt
  - `redact`: Replace with placeholder (e.g., `[REDACTED]`)
  - `generalize`: Domain-specific anonymization (e.g., `****@gmail.com`)
- International phone number support (UK +44, Germany +49, France +33, Japan +81, India +91, etc.)
- American Express credit card detection (15-digit format with 34/37 prefix)
- SSN format variations (XXX-XX-XXXX, XXX XX XXXX, XXX.XX.XXXX)
- Context-aware SSN detection for 9-digit numbers (requires column name hint)

### Fixed
- Column name heuristics no longer flag numeric columns (e.g., `email_count`, `phone_calls`)
- Reduced PII false positives by 50%

### Stats
- 7 detectors: missing, duplicates, format, outliers, types, similarity, pii
- 7 transformers: duplicates, missing, types, formats, columns, categories, pii
- 273 tests with 92%+ coverage
- CLI with 4 commands: analyze, suggest, clean, codegen

## [0.2.2] - 2026-02-04

### Fixed
- Fixed ruff E721 lint error: use `is` instead of `==` for type comparison in `MissingTransformer`

## [0.2.1] - 2026-02-04

### Fixed
- Fixed broken documentation links on PyPI (relative → absolute GitHub URLs)

## [0.2.0] - 2026-02-04

### Added
- **Smart Sampling**: Datasets >=50K rows automatically sampled to ~10K for 10-20x speedup
- **Parallel Processing**: Column profiling and detector execution via ThreadPoolExecutor
- **Computation Cache**: Lazy-evaluated shared cache for null masks, value sets, statistics
- **MinHash + LSH Similarity**: O(n) column similarity detection (was O(n^2))
- **Dtype Optimization**: Automatic numeric downcasting for ~31% memory reduction
- New `sample` parameter in `analyze()` to control sampling behavior
- New `parallel` parameter in `analyze()` to control parallel execution
- `DatasetProfile.sampled` and `DatasetProfile.sample_size` attributes
- Performance validation suite (`tests/validate_performance.py`)
- Transformation execution order system (TRANSFORMATION_ORDER) to prevent conflicts
- Exclusion rules (EXCLUSION_RULES) to prevent conflicting suggestions for same column
- `clean_empty_strings` combined strategy: converts empty/whitespace to NaN and fills in one step
- 4 new conflict resolution tests for boolean, date, numeric, and empty string scenarios

### Changed
- Similarity detector uses MinHash signatures + LSH banding instead of inverted index
- Type detector samples 1000 values for large columns instead of full scan
- Default behavior now uses sampling for large datasets (disable with `sample=False`)
- Moved `scikit-learn` and `python-Levenshtein` from core to `[ml]` optional dependencies

### Fixed
- Currency regex pattern compatibility with pandas 3.0 + PyArrow string backend
- Boolean conversion no longer overwritten by lowercase transformation (exclusion rule)
- Date standardization no longer broken by lowercase transformation (exclusion rule)
- Numeric conversion no longer affected by case change suggestions (exclusion rule)
- Empty strings now properly filled instead of leaving NaN values (combined strategy)
- Profiler now handles boolean columns correctly (uses categorical stats, not numeric)
- `clean_empty_strings` works with all string dtypes (object and string)

### Performance
- 1M rows x 10 cols: **0.72s** (was ~60s, 83x faster)
- 100K rows x 50 cols: **2.13s** (was ~30s, 14x faster)
- 10K rows x 100 cols: **4.35s** (was ~66s, 15x faster)
- 1M rows x 50 cols: **3.24s** (new benchmark)
- 50K rows x 250 cols: **9.99s** (new benchmark)

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
- `apply_interactive()` for step-by-step suggestion review with rich prompts
- Before/after quality score display in apply(), apply_all(), apply_interactive(), and CLI
- `datawash[all]` extra combining ml and formats dependencies
- Progress bars for large datasets (>10k rows or >20 columns)
- Cross-platform CI (Ubuntu, macOS, Windows) with Python 3.10-3.12
- Suggestion text now includes column names (e.g., "Handle missing values in 'email'")
- 114 tests, 92% coverage

### Fixed
- Date format detector now correctly identifies mixed date formats (ISO, slash, named, etc.)
- Boolean detection no longer limited to <=3 unique values; checks against known boolean superset
- Empty/whitespace-only strings now detected alongside empty strings in missing detector
- Missing value strategy correctly uses fill_mode for string columns instead of fill_median
- fill_mode on all-null columns logs warning and leaves data unchanged instead of silently failing
- apply_interactive() shows "all" for operations without specific columns
- Column dtype included in missing value findings for strategy selection
- standardize_dates codegen now matches transformer behavior (preserves unparseable dates)
