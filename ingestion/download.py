"""
Download raw CSV files from ISTAC API with fallback to local backup.
"""

import shutil
from pathlib import Path

import requests

TIMEOUT_SECONDS = 30
RAW_DIR = Path("data/raw")
BACKUP_DIR = Path("data/raw/backup")

SOURCES = [
    {
        "url": "https://datos.canarias.es/api/estadisticas/statistical-resources/v1.0/datasets/ISTAC/C00028A_000159/~latest.csv?lang=en",
        "dest": RAW_DIR / "tourist_accommodations.csv",
        "backup": BACKUP_DIR / "tourist_accommodations.csv",
    },
    {
        "url": "https://datos.canarias.es/api/estadisticas/statistical-resources/v1.0/datasets/ISTAC/C00028A_000060/~latest.csv?lang=en",
        "dest": RAW_DIR / "tourist_age_sex.csv",
        "backup": BACKUP_DIR / "tourist_age_sex.csv",
    },
    {
        "url": "https://datos.canarias.es/api/estadisticas/statistical-resources/v1.0/datasets/ISTAC/C00028A_000064/~latest.csv?lang=en",
        "dest": RAW_DIR / "tourist_revenue.csv",
        "backup": BACKUP_DIR / "tourist_revenue.csv",
    },
]


def download_file(url: str, dest: Path, backup: Path) -> None:
    """Download a CSV file from url to dest. Falls back to backup on failure."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        print(f"Downloading {dest.name} ...")
        response = requests.get(url, timeout=TIMEOUT_SECONDS, stream=True)
        response.raise_for_status()
        with dest.open("wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"  -> Saved to {dest}")
    except Exception as exc:
        print(f"  -> Failed ({exc}). Using backup: {backup}")
        if not backup.exists():
            raise FileNotFoundError(f"Backup not found: {backup}") from exc
        shutil.copy2(backup, dest)
        print(f"  -> Copied from backup to {dest}")


def run() -> None:
    """Download all source CSV files."""
    for source in SOURCES:
        download_file(source["url"], source["dest"], source["backup"])
    print("Ingestion complete.")


if __name__ == "__main__":
    run()
