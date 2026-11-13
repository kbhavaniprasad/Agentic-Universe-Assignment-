# AI Usage Disclosure

## Tools used

GitHub Copilot (agent assistance).

## What I used them for

- Read and implement the Assignment A data-ingestion and reporting requirements.
- Draft the SQLite schema, normalization and report-generation code, and focused tests.
- Replace an earlier implementation for the other assignment with the requested Assignment A-only
  structure after I clarified which assignment I wanted to submit.

## What I wrote or decided myself

I chose Assignment A, requested an A-only structure, and reviewed the design choices documented
in `SUBMISSION.md`.

## Where the AI was wrong or unhelpful

The initial work in this repository targeted a different assignment. I corrected the scope and
asked for Assignment A instead.

## How I verified AI-generated code

I ran the Assignment A test suite (5 passed) and generated seed-42 small data, ingested both drops,
and produced all five report files. The tests check delayed orphan resolution, idempotent re-ingest,
the report contracts, and encrypted PII storage.
