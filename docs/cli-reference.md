# CLI Reference

DataWash installs a `datawash` command with four subcommands. Every option
below matches `datawash <command> --help`. The output samples come from
`examples/sample_data/customers_messy.csv` (30 rows, 8 columns).

```bash
pip install datawash
datawash --help
```

| Command | Description |
|---------|-------------|
| `analyze` | Quality score, column profiles, issues and suggestions |
| `suggest` | Prioritized cleaning suggestions with their IDs |
| `clean` | Apply suggestions and write the cleaned file |
| `codegen` | Print or save the pandas code for the applied suggestions |

Suggestion IDs depend on `--use-case` and `--mask-names`, so pass the same
values to `suggest` and to `clean` / `codegen`.

---

## datawash analyze

```bash
datawash analyze FILE [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `FILE` | Data file: CSV, JSON, Parquet or Excel (Parquet and Excel need `datawash[formats]`) |
| `--use-case`, `-u` | `general` (default), `ml`, `analytics` or `export` |
| `--mask-names` | Also suggest masking name columns such as `customer_name` |
| `--help` | Show help |

Output (first panel):

```
╭──────────────────────── Dataset Overview ────────────────────────╮
│ Data quality score: 48/100                                       │
│ 30 rows x 8 columns | Memory: 10.4 KB | Duplicate rows: 0        │
╰──────────────────────────────────────────────────────────────────╯
```

It then prints the column profiles, the issues table and the suggestions table.

---

## datawash suggest

```bash
datawash suggest FILE [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--use-case`, `-u` | `general` (default), `ml`, `analytics` or `export` |
| `--priority`, `-p` | `high`, `medium`, `low` or `all` (default) |
| `--limit`, `-l` | Maximum suggestions to show (default: 20) |
| `--mask-names` | Also suggest masking name columns |
| `--help` | Show help |

```bash
datawash suggest data.csv --priority high
datawash suggest data.csv -u ml -l 5
```

Output (trimmed):

```
┏━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ # ┃ Priority ┃ Action                                  ┃ Because                                 ┃
┡━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ 1 │   HIGH   │ Mask or redact PII in 'email'           │ Column 'email' contains 25 potential    │
│   │          │                                         │ email addresses (100% of non-null       │
│   │          │                                         │ values)                                 │
│ 3 │  MEDIUM  │ Standardize date format in              │ Column 'signup_date' has mixed date     │
│   │          │ 'signup_date'                           │ formats (12 YYYY-MM-DD, 3 NN/NN/YYYY, 3 │
│   │          │                                         │ DD-Mon-YYYY, 7 Month DD, YYYY)          │
│ 4 │   LOW    │ Strip whitespace from values in         │ Column 'first_name' has 1 value with    │
│   │          │ 'first_name'                            │ leading/trailing whitespace             │
```

---

## datawash clean

```bash
datawash clean FILE -o OUTPUT (--apply IDS | --apply-all) [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--output`, `-o` | Output file (required); the extension picks the format |
| `--apply`, `-a` | Comma-separated suggestion IDs, e.g. `--apply 1,3,5` |
| `--apply-all` | Apply every suggestion |
| `--use-case`, `-u` | `general` (default), `ml`, `analytics` or `export` |
| `--codegen` | Also save the generated Python code to this file |
| `--mask-names` | Also mask name columns |
| `--help` | Show help |

```bash
datawash clean data.csv -o clean.csv --apply-all
datawash clean data.csv -o clean.csv --apply 1,2,5 --codegen clean.py
datawash clean data.csv -o clean.csv --apply-all -u ml
```

Output:

```
Saved cleaned data to clean.csv
╭──────────────────────── Cleaning Summary ────────────────────────╮
│ Applied 9 transformation(s)                                      │
│ Rows: 30 → 30 (0 removed)                                        │
╰──────────────────────────────────────────────────────────────────╯
Quality score: 48 → 83 (+35)
```

An ID that is not in the suggestion list stops the command before anything is
written:

```
$ datawash clean data.csv -o clean.csv --apply 99
Unknown suggestion ID(s): 99. Valid IDs are 1-9; see 'datawash suggest' with the same --use-case.
```

---

## datawash codegen

```bash
datawash codegen FILE [OPTIONS]
```

Applies every suggestion unless `--apply` is given, then prints the pandas code
(or writes it with `-o`). Running the code on the input file gives the same
result as `datawash clean` with the same options.

| Option | Description |
|--------|-------------|
| `--apply`, `-a` | Comma-separated suggestion IDs |
| `--apply-all` | Generate code for all suggestions (the default) |
| `--style`, `-s` | `function` (default) defines `clean_data(df)`; `script` reads and writes CSV |
| `--output`, `-o` | Write the code to this file instead of printing it |
| `--use-case`, `-u` | `general` (default), `ml`, `analytics` or `export` |
| `--mask-names` | Also mask name columns |
| `--help` | Show help |

```bash
datawash codegen data.csv > clean_data.py
datawash codegen data.csv --apply 4,8 --style script
```

Output of the second command:

```python
import pandas as pd
import numpy as np

# Load data
df = pd.read_csv("input.csv")  # Update path as needed

# formats: 1 rows affected
df['first_name'] = df['first_name'].astype(str).str.strip()

# formats: 25 rows affected
df['first_name'] = df['first_name'].astype(str).str.lower()

# Save cleaned data
df.to_csv("output.csv", index=False)
```

The code is printed as plain text, so piping it to a file gives a runnable
script.

---

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Bad input: missing, empty, unsupported or binary file; non-numeric or unknown `--apply` IDs; neither `--apply` nor `--apply-all` given to `clean`; unsupported output format |
| 2 | Usage error from the argument parser (unknown option, missing argument) |

Colors follow Rich's defaults: set `NO_COLOR=1` to turn them off.

---

## Scripting

```bash
for file in data/*.csv; do
    datawash clean "$file" -o "clean/$(basename "$file")" --apply-all
done
```
