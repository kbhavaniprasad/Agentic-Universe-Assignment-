from __future__ import annotations

import argparse
import os
from pathlib import Path

from .pipeline import InteractionPipeline

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEMAS = REPOSITORY_ROOT / "assignment-a-data" / "schemas"
DEFAULT_DATABASE = REPOSITORY_ROOT / "data" / "interaction-ledger.sqlite3"


def main() -> None:
    parser = argparse.ArgumentParser(description="Assignment A interaction ledger pipeline")
    commands = parser.add_subparsers(dest="command", required=True)

    ingest = commands.add_parser("ingest", help="ingest one generated data drop")
    ingest.add_argument("drop", type=Path)
    ingest.add_argument("--db", type=Path, default=DEFAULT_DATABASE)
    ingest.add_argument("--schemas", type=Path, default=DEFAULT_SCHEMAS)

    report = commands.add_parser("report", help="write the five report files")
    report.add_argument("--out", type=Path, required=True)
    report.add_argument("--db", type=Path, default=DEFAULT_DATABASE)
    report.add_argument("--schemas", type=Path, default=DEFAULT_SCHEMAS)

    args = parser.parse_args()
    key = os.environ.get("PII_ENCRYPTION_KEY")
    if not key:
        parser.error(
            "set PII_ENCRYPTION_KEY to a Fernet key; generate one with "
            "`python -c \"from cryptography.fernet import Fernet; "
            "print(Fernet.generate_key().decode())\"`"
        )

    pipeline = InteractionPipeline(args.db, args.schemas, key)
    try:
        if args.command == "ingest":
            pipeline.ingest_drop(args.drop)
        else:
            quality = pipeline.report(args.out)
            print(
                f"Wrote reports to {args.out}; "
                f"{quality['lead_rows_total']} lead rows, "
                f"{quality['events_total']} event lines processed."
            )
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
