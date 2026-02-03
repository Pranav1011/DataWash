"""Shared test fixtures."""

from __future__ import annotations
import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Basic DataFrame with mixed types."""
    return pd.DataFrame(
        {
            "id": range(1, 11),
            "name": [
                "Alice",
                "Bob",
                "Charlie",
                "David",
                "Eve",
                "Frank",
                "Grace",
                "Hank",
                "Ivy",
                "Jack",
            ],
            "age": [25, 30, 35, 40, 28, 33, 29, 45, 31, 27],
            "salary": [
                50000.0,
                60000.0,
                70000.0,
                80000.0,
                55000.0,
                65000.0,
                52000.0,
                90000.0,
                58000.0,
                51000.0,
            ],
            "email": [
                "a@b.com",
                "b@c.com",
                "c@d.com",
                "d@e.com",
                "e@f.com",
                "f@g.com",
                "g@h.com",
                "h@i.com",
                "i@j.com",
                "j@k.com",
            ],
        }
    )


@pytest.fixture
def messy_df() -> pd.DataFrame:
    """DataFrame with quality issues."""
    return pd.DataFrame(
        {
            "name": [
                "Alice",
                "Bob",
                "alice",
                " Charlie ",
                "Bob",
                "Bob",
                "David",
                "EVE",
                "frank",
                "Grace",
            ],
            "age": ["25", "30", "25", "35", "30", "30", "40", "22", "28", "33"],
            "email": [
                "a@b.com",
                None,
                "a@b.com",
                "c@d.com",
                None,
                None,
                "d@e.com",
                None,
                "f@g.com",
                "g@h.com",
            ],
            "score": [100, 200, 100, 999, 200, 200, 150, 180, 170, 160],
            "active": [
                "yes",
                "no",
                "yes",
                "Yes",
                "no",
                "no",
                "yes",
                "YES",
                "No",
                "yes",
            ],
        }
    )


@pytest.fixture
def empty_df() -> pd.DataFrame:
    """Empty DataFrame."""
    return pd.DataFrame()


@pytest.fixture
def single_row_df() -> pd.DataFrame:
    """Single-row DataFrame."""
    return pd.DataFrame({"a": [1], "b": ["x"]})
