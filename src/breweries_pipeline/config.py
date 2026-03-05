from dataclasses import dataclass
from pathlib import Path
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


@dataclass(frozen=True)
class Settings:
    data_dir: Path = Path(os.getenv("DATA_DIR", "./data"))
    per_page: int = int(os.getenv("PER_PAGE", "200"))
    request_timeout: int = int(os.getenv("REQUEST_TIMEOUT", "10"))
    retries: int = int(os.getenv("RETRIES", "5"))
    backoff_base: float = float(os.getenv("BACKOFF_BASE", "0.5"))


def ensure_dirs(settings: Settings) -> None:
    (settings.data_dir / "bronze").mkdir(parents=True, exist_ok=True)
    (settings.data_dir / "silver").mkdir(parents=True, exist_ok=True)
    (settings.data_dir / "gold").mkdir(parents=True, exist_ok=True)