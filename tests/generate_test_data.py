"""Generate test datasets for performance benchmarking."""

import random
import string
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def generate_large_dataset(n_rows: int, seed: int = 42) -> pd.DataFrame:
    """Generate a realistic messy dataset with n_rows rows."""
    np.random.seed(seed)
    random.seed(seed)

    # Helper functions
    def random_string(length=10):
        return "".join(random.choices(string.ascii_letters, k=length))

    def random_email(name):
        domains = ["gmail.com", "yahoo.com", "company.com", "outlook.com"]
        return f"{name.lower().replace(' ', '.')}@{random.choice(domains)}"

    def random_phone():
        formats = [
            f"{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
            f"({random.randint(100,999)}) {random.randint(100,999)}-{random.randint(1000,9999)}",
            f"+1{random.randint(1000000000,9999999999)}",
            f"{random.randint(1000000000,9999999999)}",
        ]
        return random.choice(formats)

    def random_date():
        formats = [
            lambda d: d.strftime("%Y-%m-%d"),
            lambda d: d.strftime("%m/%d/%Y"),
            lambda d: d.strftime("%d-%b-%Y"),
            lambda d: d.strftime("%B %d, %Y"),
        ]
        base = datetime(2020, 1, 1)
        d = base + timedelta(days=random.randint(0, 1500))
        return random.choice(formats)(d)

    # First names and last names for realistic data
    first_names = [
        "James", "Mary", "John", "Patricia", "Robert", "Jennifer",
        "Michael", "Linda", "William", "Elizabeth", "David", "Barbara",
        "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah",
    ]
    last_names = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
        "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
    ]
    departments = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Operations"]
    statuses = ["active", "inactive", "pending", "suspended"]

    # Generate base data
    data = {
        "id": range(1, n_rows + 1),
        "first_name": [random.choice(first_names) for _ in range(n_rows)],
        "last_name": [random.choice(last_names) for _ in range(n_rows)],
        "email": [],
        "phone": [random_phone() for _ in range(n_rows)],
        "age": [random.randint(18, 80) for _ in range(n_rows)],
        "salary": [round(random.uniform(30000, 200000), 2) for _ in range(n_rows)],
        "department": [random.choice(departments) for _ in range(n_rows)],
        "hire_date": [random_date() for _ in range(n_rows)],
        "is_active": [
            random.choice(["yes", "no", "Yes", "No", "YES", "NO", "true", "false", "True", "False"])
            for _ in range(n_rows)
        ],
        "status": [random.choice(statuses) for _ in range(n_rows)],
        "score": [round(random.uniform(0, 100), 2) for _ in range(n_rows)],
        "notes": [
            random_string(random.randint(10, 100)) if random.random() > 0.3 else ""
            for _ in range(n_rows)
        ],
    }

    # Generate emails based on names
    for i in range(n_rows):
        name = f"{data['first_name'][i]} {data['last_name'][i]}"
        data["email"].append(random_email(name))

    df = pd.DataFrame(data)

    # Introduce messiness

    # 1. Random case variations in text columns
    for col in ["first_name", "last_name", "department", "status"]:
        mask = np.random.random(n_rows) < 0.3
        df.loc[mask, col] = df.loc[mask, col].apply(
            lambda x: random.choice([x.upper(), x.lower(), x.title()])
            if isinstance(x, str)
            else x
        )

    # 2. Add whitespace padding randomly
    for col in ["first_name", "last_name", "email"]:
        mask = np.random.random(n_rows) < 0.1
        df.loc[mask, col] = df.loc[mask, col].apply(
            lambda x: f"  {x}  " if isinstance(x, str) else x
        )

    # 3. Introduce nulls (5-10% per column)
    for col in ["email", "phone", "salary", "notes", "score"]:
        null_rate = random.uniform(0.05, 0.10)
        mask = np.random.random(n_rows) < null_rate
        df.loc[mask, col] = np.nan

    # 4. Introduce empty strings
    for col in ["email", "notes"]:
        mask = np.random.random(n_rows) < 0.03
        df.loc[mask, col] = ""

    # 5. Store some numeric columns as strings (type issues)
    if random.random() > 0.5:
        df["age"] = df["age"].astype(str)

    # 6. Add outliers to numeric columns
    outlier_indices = np.random.choice(n_rows, size=int(n_rows * 0.02), replace=False)
    df.loc[outlier_indices, "salary"] = df.loc[outlier_indices, "salary"] * random.uniform(5, 10)

    outlier_indices = np.random.choice(n_rows, size=int(n_rows * 0.01), replace=False)
    df.loc[outlier_indices, "score"] = random.uniform(500, 1000)

    # 7. Add exact duplicates (2-3%)
    n_dupes = int(n_rows * 0.025)
    dupe_indices = np.random.choice(n_rows, size=n_dupes, replace=False)
    dupes = df.iloc[dupe_indices].copy()
    df = pd.concat([df, dupes], ignore_index=True)

    return df


def generate_wide_dataset(n_rows: int, n_cols: int, seed: int = 42) -> pd.DataFrame:
    """Generate dataset with many columns."""
    np.random.seed(seed)
    random.seed(seed)

    data = {"id": range(n_rows)}

    for i in range(n_cols):
        col_type = random.choice(["numeric", "string", "date", "bool"])
        col_name = f"col_{i}_{col_type}"

        if col_type == "numeric":
            data[col_name] = np.random.randn(n_rows) * 100
            # Add some nulls
            null_mask = np.random.random(n_rows) < 0.05
            data[col_name] = np.where(null_mask, np.nan, data[col_name])
        elif col_type == "string":
            values = [f"value_{j}" for j in range(20)]
            data[col_name] = [random.choice(values) for _ in range(n_rows)]
            # Add case variations
            data[col_name] = [
                random.choice([v.upper(), v.lower(), v.title()]) for v in data[col_name]
            ]
        elif col_type == "date":
            base = datetime(2020, 1, 1)
            data[col_name] = [
                (base + timedelta(days=random.randint(0, 1000))).strftime("%Y-%m-%d")
                for _ in range(n_rows)
            ]
        elif col_type == "bool":
            data[col_name] = [
                random.choice(["yes", "no", "Yes", "No", "true", "false"])
                for _ in range(n_rows)
            ]

    return pd.DataFrame(data)


if __name__ == "__main__":
    import os

    os.makedirs("tests/test_data", exist_ok=True)

    sizes = [1000, 10000, 100000]
    for size in sizes:
        print(f"Generating {size:,} rows...")
        df = generate_large_dataset(size)
        df.to_csv(f"tests/test_data/test_{size}.csv", index=False)
        print(f"  Saved: {df.shape[0]:,} rows x {df.shape[1]} cols")

    # Wide dataset
    print("Generating wide dataset (10K rows x 100 cols)...")
    df_wide = generate_wide_dataset(10000, 100)
    df_wide.to_csv("tests/test_data/test_wide.csv", index=False)
    print(f"  Saved: {df_wide.shape[0]:,} rows x {df_wide.shape[1]} cols")
