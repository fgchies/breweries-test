from pathlib import Path
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def run_gold(data_dir: Path) -> Path:
    #define input and output directories
    silver_dir = data_dir / "silver"
    gold_dir = data_dir / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Starting gold aggregation from silver dataset: %s", silver_dir)

    #read dataset from silver layer
    df = pd.read_parquet(silver_dir)

    #aggregate number of breweries by location and type
    agg = (
        df.groupby(["country", "state", "brewery_type"])
        .size()
        .reset_index(name="brewery_count")
    )

    #define output file
    out_path = gold_dir / "breweries_aggregated.parquet"

    #store dataset
    agg.to_parquet(out_path, index=False)

    logger.info("Gold aggregation completed: rows=%s output=%s", len(agg), out_path)

    return out_path