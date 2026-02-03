# DataWash Documentation

Welcome to the DataWash documentation!

## Quick Links

| Document | Description |
|----------|-------------|
| [Getting Started](getting-started.md) | Installation and first steps |
| [User Guide](user-guide.md) | Complete feature walkthrough |
| [API Reference](api-reference.md) | Detailed API documentation |
| [CLI Reference](cli-reference.md) | Command-line interface guide |
| [Configuration](configuration.md) | Customization options |
| [Contributing](contributing.md) | How to contribute |

## What is DataWash?

DataWash is a Python library for intelligent data cleaning and quality analysis. It:

1. **Analyzes** your data to detect quality issues
2. **Suggests** prioritized fixes
3. **Applies** transformations automatically or selectively
4. **Generates** reproducible Python code

## Quick Example

```python
from datawash import analyze

# Analyze your data
report = analyze("messy_data.csv")

# Check quality
print(f"Quality Score: {report.quality_score}/100")

# Apply fixes
clean_df = report.apply_all()

# Generate code
print(report.generate_code())
```

## Features

- **6 Detectors**: Missing values, duplicates, formats, outliers, types, similarity
- **6 Transformers**: Missing handling, deduplication, type conversion, formatting, columns, categories
- **Smart Suggestions**: Conflict resolution, execution ordering, use-case awareness
- **Code Generation**: Reproducible Python code in function or script style
- **Rich CLI**: Colored output, progress bars, interactive mode
- **Jupyter Support**: HTML rendering in notebooks

## Installation

```bash
pip install datawash
```

## Next Steps

1. Follow the [Getting Started](getting-started.md) guide
2. Explore [Examples](../examples/)
3. Read the [User Guide](user-guide.md) for in-depth coverage
