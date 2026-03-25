"""
Canary Islands Tourism - Data Engineering Pipeline
Entrypoint for local execution of pipeline steps.

Usage:
    uv run main.py ingest
"""

import sys


def main() -> None:
    command = sys.argv[1] if len(sys.argv) > 1 else "help"

    if command == "ingest":
        from ingestion.download import run
        run()
    else:
        print("Available commands:")
        print("  ingest    - Download raw CSV files from ISTAC API")


if __name__ == "__main__":
    main()

