# Submission

## Candidate
- **Name:** Copilot-assisted solution
- **Assignment chosen:** B (Workflow)
- **Time spent (honest estimate):** ~6-8 hours

## How to run
```bash
python -m pip install -r requirements.txt
cd assignment-b-workflow
python -m mock_telephony --failure-scale 0
# In a second terminal:
cd ..
python -m solution
# or run the tests:
python -m pytest -q
```

## Design
The dispatcher uses a SQLite-backed state store for campaign metadata, lead state, outbound call records, DND phones, and webhook deduplication. Every decision is persisted before a follow-up action: request_id is generated, the call record is inserted, and the provider call is attempted only after that durable record exists. This makes restarts safe with no double-dialling or lost lead state.

Lead lifecycle:

```text
new / not_started
   -> candidate selected by priority (callback > retry > fresh)
   -> request_id persisted
   -> POST /v1/calls
   -> accepted (call_sid stored) / rejected / ambiguous 503/504
   -> webhook or polling resolves final status
   -> terminal disposition => completed
   -> retry/no-answer => next_retry_at scheduled
   -> exhausted after max_attempts => exhausted
   -> DND phone => dnd_blocked for all matching leads
```

I used a database state machine instead of Temporal because the assignment explicitly allows durable SQLite-based orchestration and it keeps the implementation easy to reason about while still surviving crashes and restarts.

## Decisions & assumptions
| Decision | Why | Alternative considered |
|---|---|---|
| Use SQLite instead of Temporal | Durable enough for the assignment and simpler to run locally | Temporal would be more operationally heavy and unnecessary here |
| Persist request_id + outbound call row before the provider call | Makes retries idempotent and prevents double-dialling during crashes | Storing work only in memory would be unsafe |
| Treat DND as global by phone | Required by the spec; ensures no later call to opted-out numbers | Per-campaign DND would violate R8 |
| Schedule callbacks before retries and retries before fresh leads | Matches the assignment priority rules and avoids violating the phone scheduling contract | Echoing a generic queue would violate business rules |

## Failure modes considered
- Provider returns 503 or 504: dispatcher retries later or re-checks call status using request_id/call_sid.
- Webhooks arrive twice: dedupe table prevents double-handling.
- Webhook is lost: polling of pending calls and reconcile loop eventually resolves state.
- Crash between choosing a lead and placing the call: persisted call record and request_id allow safe reprocessing.
- DND leads: DND is applied globally and other leads with the same phone are marked `dnd_blocked`.

Not fully handled in this lightweight version: a multi-process horizontal scale-out concurrency control beyond a single local dispatcher process, and a full production-grade operational observability layer.

## Testing
I validated the assignment-specific scheduling logic with pytest, including:
- window acceptance/outside-window checks
- next-window opening behaviour
- callback > retry > fresh prioritisation

I also ensured the project imports cleanly from the repo root via `pytest.ini`.

## Results
The core scheduling and window logic is implemented and verified in the project tests. The repository is structured so the mocked telephony provider can be run locally, and the dispatcher can be launched against it with a clean setup.

## If I had another day
- Add a stronger end-to-end run against the mock telephony provider with failure injection enabled.
- Harden the state reconciliation loop for edge cases around lost webhooks and ambiguous responses.
- Improve metrics and `/status` observations for per-campaign progress and callback lateness.
