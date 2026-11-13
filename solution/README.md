# Assignment A implementation

The CLI is available as `python -m solution`. See the repository
[`README.md`](../README.md) for setup, encryption-key configuration, and end-to-end commands.

- `normalization.py` implements header, phone, date, decimal, and enum normalization.
- `pipeline.py` implements lead/event ingestion, SQLite persistence, link resolution, and reports.
- `__main__.py` provides the `ingest` and `report` commands.
