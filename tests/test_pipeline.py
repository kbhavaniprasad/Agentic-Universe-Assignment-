import csv
import json
import sqlite3
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from solution.pipeline import InteractionPipeline

ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = ROOT / "assignment-a-data" / "schemas"
PHONE_ONE = "9876543210"
PHONE_TWO = "9123456789"


def write_leads(path: Path, headers: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(headers)
        writer.writerows(rows)


def event(
    event_id: str,
    event_type: str,
    call_id: str,
    timestamp: str,
    phone: str | None,
    disposition: str | None = None,
) -> dict:
    payload = {"disposition_code": disposition} if disposition else {}
    result = {
        "event_id": event_id,
        "event_type": event_type,
        "workflow_id": "motor_renewal",
        "call_id": call_id,
        "occurred_at": timestamp,
        "payload": payload,
    }
    if phone is not None:
        result["phone"] = phone
    return result


def write_events(path: Path, events: list[dict | str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as stream:
        for item in events:
            stream.write(item if isinstance(item, str) else json.dumps(item))
            stream.write("\n")


def make_drops(root: Path) -> tuple[Path, Path]:
    drop_one = root / "drop_1"
    drop_two = root / "drop_2"
    headers = [
        "Insured Name",
        "Mobile No",
        "Policy No",
        "Expiry Date",
        "Premium",
        "Language",
    ]
    write_leads(
        drop_one / "leads" / "motor_renewal" / "batch_001.csv",
        headers,
        [
            ["Aarav Sharma", PHONE_ONE, "MOT-1", "01-Dec-2026", "₹1,234", "HI"],
            ["Updated Name", f"+91 {PHONE_ONE[:5]} {PHONE_ONE[5:]}", "MOT-1B",
             "31/02/2026", "N/A", ""],
            ["Bad Phone", "123", "MOT-BAD", "", "", "en"],
            ["", "", "", "", "", ""],
        ],
    )
    one_call = [
        event("evt-1", "call.initiated", "call-1", "2026-10-05T13:20:21Z", PHONE_ONE),
        event("evt-2", "call.connected", "call-1", "2026-10-05T13:20:30Z", PHONE_ONE),
        event("evt-3", "disposition.set", "call-1", "2026-10-05T13:25:00Z", PHONE_ONE, "renewed"),
        event(
            "evt-4",
            "disposition.override",
            "call-1",
            "2026-10-05T19:00:00+05:30",
            None,
            "not_interested",
        ),
        event("evt-5", "call.initiated", "call-2", "2026-10-06 10:00:00", PHONE_TWO),
        event("evt-6", "disposition.set", "call-1", "2026-10-05T20:00:00+05:30",
              PHONE_ONE, "not-a-real-code"),
    ]
    write_events(
        drop_one / "events" / "events_0001.jsonl",
        [*one_call, one_call[0], "{not-json"],
    )

    write_leads(
        drop_two / "leads" / "motor_renewal" / "batch_003.csv",
        ["Name", "Contact", "Policy No", "Expiry Date", "Premium", "Language"],
        [
            ["Final Name", PHONE_ONE, "MOT-1C", "", "", ""],
            ["Meera Rao", PHONE_TWO, "MOT-2", "2026-11-07", "5000", "en"],
        ],
    )
    write_events(
        drop_two / "events" / "events_1001.jsonl",
        [
            event("evt-7", "call.connected", "call-2", "2026-10-06T10:01:00", PHONE_TWO),
            event("evt-8", "disposition.set", "call-2", "2026-10-06T10:04:00",
                  PHONE_TWO, "renewed"),
            one_call[1],
        ],
    )
    return drop_one, drop_two


@pytest.fixture
def pipeline(tmp_path):
    pipeline = InteractionPipeline(
        tmp_path / "ledger.sqlite3",
        SCHEMAS,
        Fernet.generate_key(),
    )
    yield pipeline
    pipeline.close()


def test_ingestion_is_idempotent_encrypts_pii_and_resolves_orphans(tmp_path, pipeline):
    drop_one, drop_two = make_drops(tmp_path)
    pipeline.ingest_drop(drop_one)
    first_quality = pipeline.report(tmp_path / "reports-after-drop-1")
    assert first_quality["orphan_events"] == 1

    pipeline.ingest_drop(drop_one)
    assert pipeline.report(tmp_path / "reports-after-replay") == first_quality

    pipeline.ingest_drop(drop_two)
    quality = pipeline.report(tmp_path / "reports")
    assert quality == {
        "lead_rows_total": 5,
        "lead_rows_rejected": 1,
        "rejections_by_reason": {"invalid_phone": 1},
        "duplicate_lead_rows_merged": 2,
        "field_warnings": 2,
        "events_total": 11,
        "malformed_events": 1,
        "duplicate_events_dropped": 2,
        "orphan_events": 0,
        "unknown_disposition_events": 1,
    }

    with pipeline.db:
        stored_leads = list(
            pipeline.db.execute("SELECT pii_ciphertext FROM leads")
        )
        stored_event_payloads = list(
            pipeline.db.execute("SELECT payload_ciphertext FROM interaction_events")
        )
    encrypted_text = " ".join(
        row[0] for row in [*stored_leads, *stored_event_payloads]
    )
    assert "Aarav Sharma" not in encrypted_text
    assert PHONE_ONE not in encrypted_text
    assert pipeline.db.execute(
        "SELECT COUNT(*) FROM leads WHERE workflow_id = 'motor_renewal'"
    ).fetchone()[0] == 2
    assert pipeline.db.execute(
        "SELECT batch_id FROM leads WHERE phone_hash = ?", (pipeline._phone_hash("+91" + PHONE_ONE),)
    ).fetchone()[0] == "batch_001"
    values = json.loads(
        pipeline.db.execute(
            "SELECT values_json FROM leads WHERE phone_hash = ?",
            (pipeline._phone_hash("+91" + PHONE_ONE),),
        ).fetchone()[0]
    )
    assert values["preferred_language"] == "hi"

    lead_report = list(csv.DictReader((tmp_path / "reports" / "lead_report.csv").open()))
    lead_one = next(row for row in lead_report if row["external_ref"] == "MOT-1C")
    lead_two = next(row for row in lead_report if row["external_ref"] == "MOT-2")
    assert lead_one["batch_id"] == "batch_001"
    assert lead_one["phone_masked"] == "+91******3210"
    assert lead_one["bucket"] == "completed"
    assert lead_one["current_disposition"] == "not_interested"
    assert lead_one["ever_success"] == "false"
    assert lead_two["bucket"] == "completed"
    assert lead_two["ever_success"] == "true"

    daily = list(csv.DictReader((tmp_path / "reports" / "daily_overview.csv").open()))
    motor_days = {row["date"]: row for row in daily}
    assert motor_days["2026-10-05"]["connectivity_rate"] == "1.0000"
    assert motor_days["2026-10-05"]["success_leads"] == "0"
    assert motor_days["2026-10-06"]["success_leads"] == "1"

    rejections = list(csv.DictReader((tmp_path / "reports" / "rejections.csv").open()))
    assert rejections == [
        {
            "workflow_id": "motor_renewal",
            "batch_id": "batch_001",
            "row_number": "3",
            "reason": "invalid_phone",
        }
    ]
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        pipeline.db.execute("DELETE FROM interaction_events")


def test_bucket_rules_cover_unreached_telephony_failed_and_dialed(tmp_path, pipeline):
    drop = tmp_path / "drop"
    write_leads(
        drop / "leads" / "motor_renewal" / "batch_001.csv",
        ["customer_name", "phone", "policy_number"],
        [
            ["No Calls", "9876543210", "REF-0"],
            ["Failed Call", "9123456789", "REF-1"],
            ["Dialed Call", "8123456789", "REF-2"],
            ["Completed Call", "7123456789", "REF-3"],
        ],
    )
    write_events(
        drop / "events" / "events_0001.jsonl",
        [
            event("no-call-init", "call.initiated", "failed", "2026-10-01T10:00:00Z",
                  "9123456789"),
            event("failed", "call.telephony_failed", "failed", "2026-10-01T10:00:01Z",
                  "9123456789"),
            event("dialed", "call.initiated", "dialed", "2026-10-01T10:00:00Z",
                  "8123456789"),
            event("completed-init", "call.initiated", "completed", "2026-10-01T10:00:00Z",
                  "7123456789"),
            event("completed-disposition", "disposition.set", "completed",
                  "2026-10-01T10:02:00Z", "7123456789", "renewed"),
        ],
    )
    pipeline.ingest_drop(drop)
    pipeline.report(tmp_path / "bucket-reports")

    rows = list(
        csv.DictReader((tmp_path / "bucket-reports" / "lead_report.csv").open())
    )
    buckets = {row["external_ref"]: row["bucket"] for row in rows}
    assert buckets == {
        "REF-0": "yet_to_reach_out",
        "REF-1": "in_progress_telephony_failed",
        "REF-2": "in_progress_dialed",
        "REF-3": "completed",
    }
