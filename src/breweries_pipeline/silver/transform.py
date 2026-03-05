from pathlib import Path
import logging
import pandas as pd

from breweries_pipeline.quality.checks import (
    check_required_columns,
    check_not_null,
    check_unique_ids,
)

logger = logging.getLogger(__name__)

#columns expected in the dataset
SILVER_COLS = [
    "id",
    "name",
    "brewery_type",
    "city",
    "state",
    "country",
    "latitude",
    "longitude",
]

def run_silver(data_dir: Path) -> Path:
    #define bronze and silver directories
    bronze_dir = data_dir / "bronze"
    silver_dir = data_dir / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    #locate latest bronze file
    files = sorted(bronze_dir.glob("*.ndjson"))
    if not files:
        raise FileNotFoundError("No bronze files found. Run bronze step first.")

    latest_file = files[-1]
    logger.info("Starting silver transform from bronze file: %s", latest_file)

    #read raw dataset
    df = pd.read_json(latest_file, lines=True)

    #enforce expected schema
    df = df.reindex(columns=SILVER_COLS).reset_index(drop=True)

    #normalize categorical fields
    df["country"] = df["country"].fillna("UNKNOWN").astype("string")
    df["state"] = df["state"].fillna("UNKNOWN").astype("string")
    df["brewery_type"] = df["brewery_type"].fillna("UNKNOWN").astype("string")

    #convert coordinates to numeric
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    #run data quality validations
    check_required_columns(df)
    check_not_null(df)
    check_unique_ids(df)

    logger.info("Silver checks passed. Writing parquet partitioned by country/state to: %s", silver_dir)

    #store dataset as partitioned parquet
    df.to_parquet(
        silver_dir,
        engine="pyarrow",
        partition_cols=["country", "state"],
        index=False,
    )

    logger.info("Silver completed: rows=%s output_dir=%s", len(df), silver_dir)
    return silver_dir