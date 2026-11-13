# Submission

## Candidate
- **Name:** Bhavaniprasad
- **Assignment chosen:** A (Interaction Ledger & Reporting Pipeline)

## How to run

Install dependencies, generate a Fernet key, and set it in `PII_ENCRYPTION_KEY` as documented in
[`README.md`](README.md). Then run:

```powershell
python assignment-a-data\tools\generate_data.py --seed 42 --scale small --out data
python -m solution ingest data\drop_1
python -m solution ingest data\drop_2
python -m solution report --out reports
python -m pytest -q
```

## Design

SQLite stores normalized leads, rejections, ingestion-file fingerprints, quality counters, event
records, and mutable event-to-lead links. The `interaction_events` table is guarded by triggers
that reject updates and deletes; link resolution is maintained separately so an event can remain
unchanged when its lead arrives in a later drop.

The database stores schema-marked PII and event payloads as Fernet ciphertext. A keyed HMAC of
the canonical phone supports identity matching without storing the phone in plaintext. Keep the
encryption key stable and outside the repository; losing it makes encrypted values unreadable.

Reports are derived from linked events. Disposition overrides take precedence over disposition
sets, and the catalogue determines terminal and success outcomes.

## Decisions & assumptions

| Decision | Why |
|---|---|
| Track ingested files by resolved path and content hash | Re-ingest is a no-op, while modified input is rejected explicitly. |
| Keep orphan events and resolve links again after lead ingestion | Events from an earlier drop must contribute once their lead arrives. |
| Store only normalized non-PII lead fields as JSON | The workflow schemas can evolve without changing the SQLite table layout. |
| Keep payloads encrypted and omit the event's raw phone | The ledger can retain payload data without persisting event PII in plaintext. |
| For duplicate headers mapping to one schema field, use the rightmost non-empty value | Makes otherwise ambiguous duplicate columns deterministic. |

## Testing and limitations

The tests cover normalization, merge/re-ingest behavior, encrypted PII storage, orphan resolution,
event deduplication, disposition overrides, bucket rules, and report output. The implementation
uses SQLite and computes report aggregates in Python; large-scale performance and concurrent
multi-process ingestion have not been benchmarked.
