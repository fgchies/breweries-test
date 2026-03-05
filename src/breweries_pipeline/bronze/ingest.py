import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from breweries_pipeline.clients.openbrewery import OpenBreweryClient

logger = logging.getLogger(__name__)


def write_bronze(
    data_dir: Path,
    per_page: int = 200,
    max_pages: Optional[int] = None,
    **filters: Any,
) -> Path:
    """Fetch breweries from API and store raw NDJSON."""
    
    bronze_dir = data_dir / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_file = bronze_dir / f"breweries_{run_id}.ndjson"

    client = OpenBreweryClient()

    total_rows = 0
    pages = 0

    with output_file.open("w", encoding="utf-8") as f:
        for breweries_page in client.iter_breweries(
            per_page=per_page,
            max_pages=max_pages,
            **filters,
        ):
            pages += 1

            for brewery in breweries_page:
                f.write(json.dumps(brewery, ensure_ascii=False) + "\n")
                total_rows += 1

    logger.info(
        "Bronze ingestion completed: pages=%s rows=%s output=%s",
        pages,
        total_rows,
        output_file,
    )

    return output_file