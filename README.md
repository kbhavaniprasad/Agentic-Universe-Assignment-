# Agentic Universe: Interaction Ledger & Reporting Pipeline

This repository contains the solution for **Assignment A (Data)** only. The assignment prompt,
workflow schemas, disposition catalogue, and synthetic-data generator are in
[`assignment-a-data/`](assignment-a-data/).

## Setup

Requires Python 3.11 or newer.

```powershell
python -m pip install -r requirements.txt
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
$env:PII_ENCRYPTION_KEY = "<paste the generated key>"
```

Keep the encryption key secret and use the same key for every ingestion and report run against
the database. The default SQLite database is `data/interaction-ledger.sqlite3`; generated input
and report files are not committed.

## Generate data and run

```powershell
python assignment-a-data\tools\generate_data.py --seed 42 --scale small --out data
python -m solution ingest data\drop_1
python -m solution ingest data\drop_2
python -m solution report --out reports
python -m pytest -q
```

The CLI also accepts `--db <path>` and `--schemas <path>` on both commands. Ingesting a drop
again is a no-op; source files changed after ingestion are rejected rather than silently skipped.

## Implementation overview

- Lead rows are schema-validated and normalized. Invalid rows are recorded without PII; valid
  duplicates merge non-null values while retaining the first batch ID.
- Schema-marked PII is Fernet-encrypted before storage. Phone matching uses a keyed HMAC index,
  not a plaintext phone number.
- Events are stored in an append-only SQLite ledger. Event payloads are encrypted, duplicate IDs
  are dropped, and orphan links are resolved when the matching lead arrives in a later drop.
- Reports are derived from linked events and the checked-in disposition catalogue.

See [`SUBMISSION.md`](SUBMISSION.md) for design choices and limitations.
