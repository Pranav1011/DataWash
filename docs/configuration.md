# Configuration Guide

DataWash is highly configurable to suit different use cases and preferences.

## Configuration Methods

### 1. As Dictionary

```python
from datawash import analyze

report = analyze(df, config={
    "detectors": {
        "outlier_method": "zscore",
        "outlier_threshold": 2.5,
    },
    "suggestions": {
        "max_suggestions": 20,
    },
})
```

### 2. As Config Object

```python
from datawash import analyze
from datawash.core.config import Config

config = Config(
    use_case="ml",
    detectors={"outlier_method": "zscore"},
    suggestions={"max_suggestions": 20},
)

report = analyze(df, config=config)
```

---

## Configuration Options

### Use Case

```python
report = analyze(df, use_case="ml")
```

| Value | Description |
|-------|-------------|
| `general` | Balanced approach (default) |
| `ml` | Optimized for machine learning |
| `analytics` | Optimized for analysis/reporting |
| `export` | Optimized for data sharing |

### Detector Configuration

```python
config = {
    "detectors": {
        # Which detectors to run
        "enabled": ["missing", "duplicates", "formats", "outliers", "types", "similarity", "pii"],

        # Outlier detection method
        "outlier_method": "iqr",  # or "zscore"

        # Outlier threshold
        "outlier_threshold": 1.5,  # IQR multiplier or Z-score

        # Column similarity threshold
        "similarity_name_threshold": 0.8,  # 0.0 to 1.0
    }
}
```

#### Enabled Detectors

Control which detectors run:

```python
# Run only specific detectors
config = {
    "detectors": {
        "enabled": ["missing", "duplicates"],
    }
}
```

Available detectors:
- `missing` - Null values, empty strings
- `duplicates` - Exact duplicate rows
- `formats` - Case, dates, whitespace
- `outliers` - Statistical anomalies
- `types` - Type mismatches
- `similarity` - Similar column names
- `pii` - Emails, phones, SSNs, credit cards, IPs, ZIP codes, PII-like column names

#### Outlier Detection

**IQR Method** (default):
```python
config = {
    "detectors": {
        "outlier_method": "iqr",
        "outlier_threshold": 1.5,  # Standard is 1.5
    }
}
```

Values outside `Q1 - 1.5*IQR` to `Q3 + 1.5*IQR` are flagged.

**Z-Score Method**:
```python
config = {
    "detectors": {
        "outlier_method": "zscore",
        "outlier_threshold": 3.0,  # Standard deviations
    }
}
```

Values with Z-score > 3.0 (or < -3.0) are flagged.

#### Similarity Detection

```python
config = {
    "detectors": {
        "similarity_name_threshold": 0.8,  # 80% similar names flagged
    }
}
```

Uses Levenshtein distance to compare column names.

### Suggestion Configuration

```python
config = {
    "suggestions": {
        "max_suggestions": 50,  # Maximum suggestions to return
        "mask_names": False,  # Also mask columns like 'customer_name'
    }
}
```

PII columns (emails, phones, SSNs and so on) get a masking suggestion, and it
runs after whitespace, case and type fixes so the masked text is the cleaned
value. PII columns are never filled from other rows, since that would copy one
person's email or phone onto another. Columns flagged only because their name
looks like a person's name (`name`, `first_name`, `last_name`, `full_name`) are
not masked unless `mask_names` is `True` (CLI: `--mask-names`).

### Code Generation Configuration

```python
config = {
    "codegen": {
        "include_comments": True,  # Add comments to generated code
    }
}
```

---

## Use Case Details

### General

Default balanced settings:

```python
# Priority boosts: none (all equal)
```

### ML (Machine Learning)

Prioritizes issues that affect model training:

```python
# Priority boosts:
# - duplicate_rows: 1.5x (data leakage)
# - missing_values: 1.3x (model errors)
# - numeric_as_string: 1.3x (feature issues)
# - boolean_as_string: 1.2x (encoding issues)
# - outliers: 1.2x (skewed training)
# - similar_columns: 1.4x (feature redundancy)
```

### Analytics

Prioritizes issues that affect analysis accuracy:

```python
# Priority boosts:
# - missing_values: 1.5x (incomplete analysis)
# - outliers: 1.3x (skewed statistics)
# - inconsistent_date_format: 1.4x (time series issues)
# - inconsistent_case: 1.2x (grouping issues)
```

### Export

Prioritizes issues that affect data sharing:

```python
# Priority boosts:
# - inconsistent_date_format: 1.5x (parsing issues)
# - whitespace_padding: 1.4x (display issues)
# - inconsistent_case: 1.3x (consistency)
# - numeric_as_string: 1.3x (type preservation)
```

---

## Complete Configuration Example

```python
from datawash import analyze

report = analyze(
    "data.csv",
    use_case="ml",
    config={
        "detectors": {
            # Only run these detectors
            "enabled": ["missing", "duplicates", "types", "outliers"],

            # Use Z-score for outliers with strict threshold
            "outlier_method": "zscore",
            "outlier_threshold": 2.5,

            # Skip similar column detection
            # (not in enabled list)
        },
        "suggestions": {
            # Limit to top 10 suggestions
            "max_suggestions": 10,
        },
        "codegen": {
            # Clean code without comments
            "include_comments": False,
        },
    },
)
```

---

## Configuration Tips

### 1. Start with Defaults

For most cases, defaults work well:

```python
report = analyze(df)  # Uses sensible defaults
```

### 2. Choose the Right Use Case

Let the use case handle prioritization:

```python
# For ML pipelines
report = analyze(df, use_case="ml")

# For dashboards/reports
report = analyze(df, use_case="analytics")
```

### 3. Tune Outlier Detection

Different data needs different thresholds:

```python
# Financial data (tight threshold)
config = {"detectors": {"outlier_threshold": 2.0}}

# Sensor data (loose threshold)
config = {"detectors": {"outlier_threshold": 3.5}}
```

### 4. Disable Unnecessary Detectors

Speed up analysis by skipping irrelevant detectors:

```python
# Skip similarity check for datasets with unique column names
config = {"detectors": {"enabled": ["missing", "duplicates", "formats", "outliers", "types"]}}
```

### 5. Limit Suggestions for Large Datasets

Avoid information overload:

```python
config = {"suggestions": {"max_suggestions": 15}}
```
