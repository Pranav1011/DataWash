# API Reference

Complete API documentation for DataWash.

## Main Functions

### analyze()

```python
def analyze(
    data: pd.DataFrame | str | Path,
    config: Optional[Config | dict] = None,
    use_case: str = "general",
    sample: bool = True,
    parallel: bool = True,
) -> Report
```

Analyze a dataset and return a Report object.

**Parameters:**
- `data`: DataFrame or path to a data file (CSV, JSON, Parquet, Excel)
- `config`: Optional configuration dict or Config object
- `use_case`: One of "general", "ml", "analytics", "export"
- `sample`: Enable smart sampling for datasets >=50K rows (default: True)
- `parallel`: Enable parallel column profiling and detection (default: True)

**Returns:**
- `Report` object with issues, suggestions, and cleaning methods

**Performance Notes:**
- With `sample=True` (default), datasets >=50K rows are sampled to ~10K rows
- Sampling provides ~95% accuracy with 10-20x speedup
- Use `sample=False` for exact results on large datasets (slower)
- Parallel processing uses ThreadPoolExecutor for column-level profiling and detector execution

**Example:**
```python
from datawash import analyze

# From file
report = analyze("data.csv")

# From DataFrame
report = analyze(df)

# With use case
report = analyze(df, use_case="ml")

# With config
report = analyze(df, config={"suggestions": {"max_suggestions": 10}})

# Disable sampling for exact results on large datasets
report = analyze(df, sample=False)

# Disable parallel processing
report = analyze(df, parallel=False)
```

---

## Report Class

### Properties

#### report.df

```python
@property
def df(self) -> pd.DataFrame
```

Returns a copy of the original DataFrame.

#### report.profile

```python
@property
def profile(self) -> DatasetProfile
```

Returns the dataset profile with statistics.

#### report.issues

```python
@property
def issues(self) -> list[Finding]
```

Returns all detected data quality issues.

#### report.suggestions

```python
@property
def suggestions(self) -> list[Suggestion]
```

Returns prioritized list of suggestions.

#### report.quality_score

```python
@property
def quality_score(self) -> int
```

Returns data quality score from 0 to 100.

### Methods

#### report.apply()

```python
def apply(self, suggestion_ids: list[int]) -> pd.DataFrame
```

Apply selected suggestions by ID.

**Parameters:**
- `suggestion_ids`: List of suggestion IDs to apply

**Returns:**
- Cleaned DataFrame

**Example:**
```python
clean_df = report.apply([1, 3, 5])
```

#### report.apply_all()

```python
def apply_all(self) -> pd.DataFrame
```

Apply all suggestions.

**Returns:**
- Cleaned DataFrame

**Example:**
```python
clean_df = report.apply_all()
```

#### report.apply_interactive()

```python
def apply_interactive(
    input_fn: Optional[Callable] = None,
    console: Optional[Console] = None,
) -> pd.DataFrame
```

Interactively apply suggestions with user prompts.

**Parameters:**
- `input_fn`: Custom input function (for testing)
- `console`: Rich Console for output

**Returns:**
- Cleaned DataFrame

**Example:**
```python
clean_df = report.apply_interactive()
# Prompts: [a]pply / [s]kip / apply [A]ll / [q]uit
```

#### report.generate_code()

```python
def generate_code(self, style: str = "function") -> str
```

Generate Python code for applied transformations.

**Parameters:**
- `style`: "function" or "script"

**Returns:**
- Python code as string

**Example:**
```python
code = report.generate_code(style="function")
print(code)
```

#### report.summary()

```python
def summary(self) -> str
```

Get human-readable analysis summary.

**Returns:**
- Summary string

**Example:**
```python
print(report.summary())
```

---

## Data Models

### Finding

Represents a detected data quality issue.

```python
@dataclass
class Finding:
    issue_type: str          # Type of issue (e.g., "missing_values")
    category: str            # Category (e.g., "completeness")
    message: str             # Human-readable description
    severity: Severity       # HIGH, MEDIUM, or LOW
    confidence: float        # 0.0 to 1.0
    columns: list[str]       # Affected columns
    details: dict            # Additional details
```

### Suggestion

Represents an actionable fix.

```python
@dataclass
class Suggestion:
    id: int                  # Unique identifier
    finding: Finding         # Related issue
    action: str              # Description of fix
    transformer: str         # Transformer name
    params: dict             # Transformer parameters
    priority: Severity       # HIGH, MEDIUM, or LOW
    impact: str              # Expected impact
    rationale: str           # Why it's recommended
```

### DatasetProfile

Dataset-level statistics.

```python
@dataclass
class DatasetProfile:
    row_count: int
    column_count: int
    memory_bytes: int
    columns: dict[str, ColumnProfile]
    duplicate_row_count: int
    sampled: bool = False          # True if smart sampling was used
    sample_size: Optional[int]     # Number of rows in sample (if sampled)
```

### ColumnProfile

Column-level statistics.

```python
@dataclass
class ColumnProfile:
    name: str
    dtype: str
    null_count: int
    null_ratio: float
    unique_count: int
    unique_ratio: float
    sample_values: list
    statistics: dict
    patterns: list[str]
```

### TransformationResult

Result of a transformation.

```python
@dataclass
class TransformationResult:
    transformer: str
    params: dict
    rows_affected: int
    columns_affected: list[str]
    code: str
```

### Severity

```python
class Severity(Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
```

---

## Configuration

### Config Class

```python
class Config:
    use_case: str = "general"
    detectors: DetectorConfig
    suggestions: SuggestionConfig
    codegen: CodegenConfig
```

### DetectorConfig

```python
class DetectorConfig:
    enabled: list[str] = ["missing", "duplicates", "format", "outliers", "types", "similarity"]
    outlier_method: str = "iqr"  # or "zscore"
    outlier_threshold: float = 1.5
    min_similarity: float = 0.8
```

### SuggestionConfig

```python
class SuggestionConfig:
    max_suggestions: int = 50
```

### CodegenConfig

```python
class CodegenConfig:
    include_comments: bool = True
```

### Using Config

```python
from datawash import analyze

# As dict
report = analyze(df, config={
    "detectors": {
        "outlier_method": "zscore",
        "outlier_threshold": 2.5,
    },
    "suggestions": {
        "max_suggestions": 20,
    },
})

# As Config object
from datawash.core.config import Config

config = Config(
    use_case="ml",
    detectors={"outlier_method": "zscore"},
)
report = analyze(df, config=config)
```

---

## Low-Level Functions

### Profiler

```python
from datawash.profiler import profile_dataset

profile = profile_dataset(df)
```

### Detectors

```python
from datawash.detectors import run_all_detectors

findings = run_all_detectors(df, profile)

# Or run specific detectors
from datawash.detectors import get_detector

detector = get_detector("missing")
findings = detector.detect(df, profile)
```

### Transformers

```python
from datawash.transformers import run_transformer

result_df, result = run_transformer(
    "missing",
    df,
    strategy="fill_median",
    columns=["price"],
)
```

### Suggestions

```python
from datawash.suggestors import generate_suggestions

suggestions = generate_suggestions(findings, max_suggestions=20, use_case="ml")
```

### Code Generation

```python
from datawash.codegen import generate_code

code = generate_code(
    transformation_results,
    style="function",
    include_comments=True,
)
```
