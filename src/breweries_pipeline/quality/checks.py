import pandas as pd

REQUIRED_COLS = ["id", "name", "brewery_type", "country", "state"]

def check_required_columns(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

def check_not_null(df: pd.DataFrame, cols=None) -> None:
    cols = cols or REQUIRED_COLS
    chk = {c: int(df[c].isna().sum()) for c in cols}
    chk = {k: v for k, v in chk.items() if v > 0}
    if chk:
        raise ValueError(f"Nulls found in required columns: {chk}")

def check_unique_ids(df: pd.DataFrame) -> None:
    dup = int(df["id"].duplicated().sum())
    if dup > 0:
        raise ValueError(f"Duplicate ids found: {dup}")