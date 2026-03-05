from pathlib import Path
import pandas as pd


def run_gold(data_dir: Path) -> Path:
    #define input and output directories
    silver_dir = data_dir / "silver"
    gold_dir = data_dir / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)

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

    return out_path