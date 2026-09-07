# CLAIMS — DataWash
Created 2026-09-05 (WO-1). Flagship. Cleanest claims in the portfolio — all VERIFIED.

| Claim (as on resume/README) | Status | Evidence artifact | Reproduce | Verified |
|---|---|---|---|---|
| Authored & **published** open-source Python library for data-quality profiling + cleaning | VERIFIED | live on PyPI (pypi.org/project/datawash, author verified); `dist/` wheels; `pyproject.toml` | `pip install datawash` | 2026-09-05 |
| Real profiling + cleaning (PII 6 cats, detectors, transformers, quality score, code-gen) | VERIFIED | `src/datawash/{detectors,transformers,profiler,codegen}/` (~5,221 LOC) | run CLI | 2026-09-05 |
| Test rigor (~92% coverage) | VERIFIED | 20 test files, ~232 test fns, `.coverage` | `pytest` | 2026-09-05 |

**Only fix:** README labels `[ml]` extra "coming soon" but the ML code (`src/datawash/ml/`) already ships — drop the label.
