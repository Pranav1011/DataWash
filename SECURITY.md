# Security Policy

## Data Privacy

**DataWash runs entirely locally.** Your data is never transmitted to external servers, and no telemetry or analytics are collected.

### Network Activity

The core datawash package makes **zero network calls**. All processing happens on your local machine.

When using the optional `[ml]` extra (`pip install datawash[ml]`), sentence-transformer models may be downloaded from HuggingFace on first use. **Only model weights are downloaded - your data is never uploaded.**

## Reporting Vulnerabilities

If you discover a security vulnerability, please email the maintainers privately rather than opening a public issue. We will respond within 48 hours.

## Security Considerations for Sensitive Data

### Console Output Contains Sample Values

When running `datawash analyze`, sample values from your data are displayed in the console output. This is intended for data exploration but means:

- **Do not run `datawash analyze` on sensitive data in shared terminals or logs**
- Sample values are limited to 3 values per column
- Consider redirecting output when processing PII or confidential data

### Generated Code

The `datawash codegen` command generates Python code that includes:
- Column names from your dataset (as string literals)
- Transformation logic (never actual data values)

**Always review generated code before:**
- Sharing with others (column names may reveal data schema)
- Running in production environments
- Committing to version control

Column names with special characters are safely escaped using Python's `repr()` function, preventing code injection.

### File Operations

- DataWash only accesses files you explicitly specify via command line or API
- **Output files are overwritten without warning** - use caution with the `-o` flag
- No temporary files are created
- No files are read or written without explicit user action

### Memory

- Data is held in memory during processing (standard pandas behavior)
- No data is cached to disk
- No global state persists between sessions
- For highly sensitive data, consider running in an isolated environment

### Error Messages

Error messages include:
- File paths (user-provided)
- Column names (metadata)
- Row/column counts (aggregates)

Error messages **never** include actual data values.

### Logging

DataWash uses Python's standard logging module. By default, only WARNING and above are shown. Logging includes:
- File paths being processed
- Row/column counts
- Detector names and issue counts
- Transformation results (counts only)

Logging **never** includes actual data values.

## Dependencies

All dependencies are well-maintained, reputable open-source packages:

| Package | Purpose | License |
|---------|---------|---------|
| pandas | Data manipulation | BSD |
| numpy | Numerical operations | BSD |
| pydantic | Configuration validation | MIT |
| rich | Terminal formatting | MIT |
| typer | CLI framework | MIT |
| scikit-learn | Statistical methods | BSD |

Run `pip-audit` periodically to check for vulnerabilities in dependencies:

```bash
pip install pip-audit
pip-audit
```

## Best Practices for Sensitive Data

1. **Review output before sharing** - Console output includes sample values
2. **Use isolated environments** - Consider Docker or virtual environments
3. **Review generated code** - Check before committing or sharing
4. **Redirect output for PII** - Pipe to /dev/null or a secure file
5. **Update regularly** - Keep datawash and dependencies updated

## Secure Usage Example

```bash
# For sensitive data, suppress console output
datawash analyze sensitive.csv > /dev/null 2>&1

# Or use the Python API with logging disabled
import logging
logging.disable(logging.CRITICAL)

from datawash import analyze
report = analyze("sensitive.csv")
# Process report programmatically without console output
```
