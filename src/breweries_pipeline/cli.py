import argparse
from pathlib import Path

from breweries_pipeline.config import Settings, ensure_dirs
from breweries_pipeline.logging_conf import setup_logging
from breweries_pipeline.bronze.ingest import write_bronze
from breweries_pipeline.silver.transform import run_silver
from breweries_pipeline.gold.aggregate import run_gold


def main():
    #define CLI arguments
    parser = argparse.ArgumentParser(prog="breweries-pipeline")
    parser.add_argument("step", choices=["bronze", "silver", "gold", "all"])
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--per-page", type=int, default=None)
    parser.add_argument("--log-level", default="INFO")

    args = parser.parse_args()

    #initialize logging configuration
    setup_logging(args.log_level)

    #load project settings and ensure directories exist
    settings = Settings()
    ensure_dirs(settings)

    #use CLI parameter or default configuration
    per_page = args.per_page or settings.per_page

    #run bronze ingestion
    if args.step in ("bronze", "all"):
        out_bronze = write_bronze(
            data_dir=Path(settings.data_dir),
            per_page=per_page,
            max_pages=args.max_pages,
        )

    #run silver transformation
    if args.step in ("silver", "all"):
        out_silver = run_silver(Path(settings.data_dir))

    #run gold aggregation
    if args.step in ("gold", "all"):
        out_gold = run_gold(Path(settings.data_dir))


if __name__ == "__main__":
    #entrypoint for CLI execution
    main()