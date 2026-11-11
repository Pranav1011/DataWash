# CLAIMS — DataWash

Each README claim, the evidence for it and how to reproduce it.

| Claim (as in README) | Status | Evidence artifact | Reproduce | Verified |
|---|---|---|---|---|
| Published open-source Python library for data-quality profiling + cleaning | VERIFIED (1.0.0 on PyPI; 1.1.0, which adds `[ml]`, not yet published) | pypi.org/project/datawash; `pyproject.toml` | `pip install datawash` | 2026-11-11 |
| Profiling + cleaning (7 detectors + opt-in fuzzy duplicates, 7 transformers, quality score, code generation) | VERIFIED | `src/datawash/{detectors,transformers,profiler,codegen}/` | `datawash clean examples/sample_data/customers_messy.csv -o clean.csv --apply-all --codegen clean.py` | 2026-11-11 |
| Quality score 48 → 83 on `customers_messy.csv`; generated code reproduces `clean.csv` | VERIFIED | `tests/test_cli.py` (codegen replay tests) | command above, then run `clean.py` | 2026-11-11 |
| 292 tests pass, 26 skipped (ML dependencies not installed) | VERIFIED | `tests/` | `pytest` | 2026-11-11 |
| 82% line coverage | VERIFIED | — | `pytest --cov=datawash` on a `[dev]` install | 2026-11-11 |
| Performance table (e.g. 1M x 10 in 1.09s on an M3 Pro) | VERIFIED | `tests/benchmark_readme.py` | `python tests/benchmark_readme.py` | 2026-11-11 |
