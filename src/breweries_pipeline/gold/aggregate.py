from pathlib import Path
import pandas as pd


def run_gold(data_dir: Path) -> Path:
    silver_dir = data_dir / "silver"
    gold_dir = data_dir / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(silver_dir)

    agg = (
        df.groupby(["country", "state", "brewery_type"])
        .size()
        .reset_index(name="brewery_count")
    )

    out_path = gold_dir / "breweries_aggregated.parquet"

    agg.to_parquet(out_path, index=False)

    return out_path