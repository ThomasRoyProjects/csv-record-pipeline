import os
import pandas as pd


def load_csv_safe(path):
    """
    Load CSV as strings, safe for mixed data.
    """
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def read_csv_headers(path):
    """
    Read only the header row from a CSV file.
    """
    return list(pd.read_csv(path, nrows=0).columns)


def write_csv(df, path):
    """
    Write CSV and create parent directories if needed.
    """
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    df.to_csv(path, index=False)
