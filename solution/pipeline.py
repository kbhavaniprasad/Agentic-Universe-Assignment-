from __future__ import annotations

import base64
import csv
import hashlib
import hmac
import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet

from .normalization import normalize_header, normalize_phone, parse_value

IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc


class InteractionPipeline:
    def __init__(
        self,
        database: str | Path,
        schemas_dir: str | Path,
        encryption_key: str | bytes,
    ) -> None:
        key = encryption_key.encode("ascii") if isinstance(encryption_key, str) else encryption_key
        self.fernet = Fernet(key)
        key_material = base64.urlsafe_b64decode(key)
        self.phone_index_key = hmac.new(
            key_material, b"agentic-universe-phone-index-v1", hashlib.sha256
        ).digest()
        self.database_path = Path(database)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.database_path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys = ON")
        self.db.execute("PRAGMA journal_mode = WAL")
        self._create_tables()

        self.schemas: dict[str, dict[str, Any]] = {}
        self.dispositions: dict[str, dict[str, tuple[bool, bool]]] = defaultdict(dict)
        for path in sorted(Path(schemas_dir).glob("*.json")):
            schema = json.loads(path.read_text(encoding="utf-8"))
            self.schemas[schema["workflow_id"]] = schema
        dispositions_path = Path(schemas_dir) / "dispositions.csv"
        with dispositions_path.open(encoding="utf-8-sig", newline="") as stream:
            for row in csv.DictReader(stream):
                self.dispositions[row["workflow_id"]][row["code"]] = (
                    row["is_terminal"].strip().lower() == "true",
                    row["is_success"].strip().lower() == "true",
                )

    def close(self) -> None:
        self.db.close()

    def _create_tables(self) -> None:
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY,
                workflow_id TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                phone_hash TEXT NOT NULL,
                pii_ciphertext TEXT NOT NULL,
                values_json TEXT NOT NULL,
                UNIQUE(workflow_id, phone_hash)
            );
            CREATE TABLE IF NOT EXISTS rejections (
                workflow_id TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                row_number INTEGER NOT NULL,
                reason TEXT NOT NULL,
                PRIMARY KEY(workflow_id, batch_id, row_number)
            );
            CREATE TABLE IF NOT EXISTS source_files (
                file_key TEXT PRIMARY KEY,
                sha256 TEXT NOT NULL,
                kind TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS quality_counters (
                name TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS interaction_events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                workflow_id TEXT,
                call_id TEXT,
                occurred_at_utc TEXT,
                phone_hash TEXT,
                disposition_code TEXT,
                payload_ciphertext TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_events_phone
                ON interaction_events(workflow_id, phone_hash);
            CREATE INDEX IF NOT EXISTS ix_events_call
                ON interaction_events(workflow_id, call_id);
            CREATE TABLE IF NOT EXISTS event_links (
                event_id TEXT PRIMARY KEY REFERENCES interaction_events(event_id),
                lead_id INTEGER REFERENCES leads(id)
            );
            CREATE INDEX IF NOT EXISTS ix_event_links_lead ON event_links(lead_id);
            CREATE TRIGGER IF NOT EXISTS interaction_events_no_update
            BEFORE UPDATE ON interaction_events
            BEGIN
                SELECT RAISE(ABORT, 'interaction_events is append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS interaction_events_no_delete
            BEFORE DELETE ON interaction_events
            BEGIN
                SELECT RAISE(ABORT, 'interaction_events is append-only');
            END;
            """
        )

    def _phone_hash(self, canonical_phone: str) -> str:
        return hmac.new(
            self.phone_index_key, canonical_phone.encode("ascii"), hashlib.sha256
        ).hexdigest()

    def _encrypt_json(self, value: dict[str, Any]) -> str:
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        return self.fernet.encrypt(encoded).decode("ascii")

    def _decrypt_json(self, value: str) -> dict[str, Any]:
        return json.loads(self.fernet.decrypt(value.encode("ascii")))

    @staticmethod
    def _file_hash(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _already_ingested(self, path: Path, kind: str) -> bool:
        file_key = str(path.resolve())
        digest = self._file_hash(path)
        row = self.db.execute(
            "SELECT sha256, kind FROM source_files WHERE file_key = ?", (file_key,)
        ).fetchone()
        if row is None:
            return False
        if row["sha256"] != digest or row["kind"] != kind:
            raise ValueError(f"Ingested source file changed since last run: {path}")
        return True

    def _record_file(self, path: Path, kind: str) -> None:
        self.db.execute(
            "INSERT INTO source_files(file_key, sha256, kind) VALUES (?, ?, ?)",
            (str(path.resolve()), self._file_hash(path), kind),
        )

    def _add_counter(self, name: str, amount: int = 1) -> None:
        self.db.execute(
            """
            INSERT INTO quality_counters(name, value) VALUES (?, ?)
            ON CONFLICT(name) DO UPDATE SET value = value + excluded.value
            """,
            (name, amount),
        )

    def ingest_drop(self, drop_path: str | Path) -> None:
        root = Path(drop_path).resolve()
        if not root.is_dir():
            raise FileNotFoundError(f"Drop directory does not exist: {root}")

        leads_root = root / "leads"
        if leads_root.exists():
            for workflow_dir in sorted(path for path in leads_root.iterdir() if path.is_dir()):
                if workflow_dir.name not in self.schemas:
                    raise ValueError(f"No lead schema for workflow: {workflow_dir.name}")
                for path in sorted(workflow_dir.glob("*.csv"), key=lambda item: item.name):
                    self._ingest_lead_file(path, workflow_dir.name)

        events_root = root / "events"
        if events_root.exists():
            for path in sorted(events_root.glob("*.jsonl"), key=lambda item: item.name):
                self._ingest_event_file(path)

    def _ingest_lead_file(self, path: Path, workflow_id: str) -> None:
        if self._already_ingested(path, "leads"):
            return
        schema = self.schemas[workflow_id]
        fields = schema["fields"]
        header_map: dict[str, list[int]] = defaultdict(list)

        with self.db:
            with path.open(encoding="utf-8-sig", newline="") as stream:
                reader = csv.reader(stream)
                try:
                    headers = next(reader)
                except StopIteration:
                    headers = []
                accepted_names: dict[str, str] = {}
                for field in fields:
                    for name in [field["name"], *field.get("aliases", [])]:
                        accepted_names[normalize_header(name)] = field["name"]
                for index, header in enumerate(headers):
                    field_name = accepted_names.get(normalize_header(header))
                    if field_name is not None:
                        header_map[field_name].append(index)

                batch_id = path.stem
                row_number = 0
                for row in reader:
                    if not any(cell.strip() for cell in row if cell is not None):
                        continue
                    row_number += 1
                    self._add_counter("lead_rows_total")

                    raw_values: dict[str, str | None] = {}
                    for field_name, indexes in header_map.items():
                        candidates = [row[index] if index < len(row) else None for index in indexes]
                        raw_values[field_name] = next(
                            (value for value in reversed(candidates) if value and value.strip()),
                            candidates[-1] if candidates else None,
                        )

                    normalized: dict[str, Any] = {}
                    warning_count = 0
                    rejection: str | None = None
                    for field in fields:
                        name = field["name"]
                        raw = raw_values.get(name)
                        if raw is None or not raw.strip():
                            normalized[name] = None
                            if field.get("required"):
                                rejection = f"missing_required:{name}"
                                break
                            continue
                        value, valid = parse_value(field, raw)
                        normalized[name] = value
                        if field["type"] == "phone" and not valid:
                            rejection = "invalid_phone"
                            break
                        if not valid:
                            warning_count += 1

                    if rejection is not None:
                        self.db.execute(
                            """
                            INSERT INTO rejections(workflow_id, batch_id, row_number, reason)
                            VALUES (?, ?, ?, ?)
                            """,
                            (workflow_id, batch_id, row_number, rejection),
                        )
                        self._add_counter("lead_rows_rejected")
                        self._add_counter(f"rejection:{rejection}")
                        continue

                    canonical_phone = normalized["phone"]
                    phone_hash = self._phone_hash(canonical_phone)
                    pii_fields = {
                        field["name"] for field in fields if field.get("pii", False)
                    }
                    pii_values = {
                        name: normalized.get(name)
                        for name in pii_fields
                        if normalized.get(name) is not None
                    }
                    ordinary_values = {
                        name: normalized.get(name)
                        for name in normalized
                        if name not in pii_fields
                    }
                    existing = self.db.execute(
                        """
                        SELECT id, pii_ciphertext, values_json
                        FROM leads WHERE workflow_id = ? AND phone_hash = ?
                        """,
                        (workflow_id, phone_hash),
                    ).fetchone()

                    if existing is None:
                        merged_pii = pii_values
                        merged_values = ordinary_values
                        self._apply_defaults(fields, merged_values, merged_pii, pii_fields)
                        self.db.execute(
                            """
                            INSERT INTO leads(
                                workflow_id, batch_id, phone_hash, pii_ciphertext, values_json
                            ) VALUES (?, ?, ?, ?, ?)
                            """,
                            (
                                workflow_id,
                                batch_id,
                                phone_hash,
                                self._encrypt_json(merged_pii),
                                json.dumps(merged_values, ensure_ascii=False, separators=(",", ":")),
                            ),
                        )
                    else:
                        merged_pii = self._decrypt_json(existing["pii_ciphertext"])
                        merged_values = json.loads(existing["values_json"])
                        for name, value in pii_values.items():
                            if value is not None:
                                merged_pii[name] = value
                        for name, value in ordinary_values.items():
                            if value is not None:
                                merged_values[name] = value
                        self._apply_defaults(fields, merged_values, merged_pii, pii_fields)
                        self.db.execute(
                            """
                            UPDATE leads SET pii_ciphertext = ?, values_json = ?
                            WHERE id = ?
                            """,
                            (
                                self._encrypt_json(merged_pii),
                                json.dumps(merged_values, ensure_ascii=False, separators=(",", ":")),
                                existing["id"],
                            ),
                        )
                        self._add_counter("duplicate_lead_rows_merged")
                    self._add_counter("field_warnings", warning_count)

            self._resolve_pending_links()
            self._record_file(path, "leads")

    @staticmethod
    def _apply_defaults(
        fields: list[dict[str, Any]],
        values: dict[str, Any],
        pii_values: dict[str, Any],
        pii_fields: set[str],
    ) -> None:
        for field in fields:
            name = field["name"]
            if "default" not in field:
                continue
            destination = pii_values if name in pii_fields else values
            if destination.get(name) is None:
                destination[name] = field["default"]

    @staticmethod
    def _timestamp_utc(value: Any) -> str | None:
        if not isinstance(value, str) or not value.strip():
            return None
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=IST)
        return parsed.astimezone(UTC).isoformat(timespec="microseconds")

    def _ingest_event_file(self, path: Path) -> None:
        if self._already_ingested(path, "events"):
            return
        with self.db:
            with path.open(encoding="utf-8-sig") as stream:
                for line in stream:
                    if not line.strip():
                        continue
                    self._add_counter("events_total")
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError:
                        self._add_counter("malformed_events")
                        continue
                    if (
                        not isinstance(event, dict)
                        or event.get("event_id") is None
                        or not str(event.get("event_id")).strip()
                    ):
                        self._add_counter("malformed_events")
                        continue
                    event_id = str(event["event_id"])
                    if self.db.execute(
                        "SELECT 1 FROM interaction_events WHERE event_id = ?", (event_id,)
                    ).fetchone():
                        self._add_counter("duplicate_events_dropped")
                        continue

                    event_type = str(event.get("event_type") or "")
                    workflow_id = event.get("workflow_id")
                    workflow_id = str(workflow_id) if workflow_id is not None else None
                    call_id = event.get("call_id")
                    call_id = str(call_id) if call_id is not None else None
                    raw_phone = event.get("phone")
                    canonical_phone = (
                        normalize_phone(str(raw_phone)) if raw_phone is not None else None
                    )
                    phone_hash = (
                        self._phone_hash(canonical_phone) if canonical_phone is not None else None
                    )
                    payload = event.get("payload")
                    payload = payload if isinstance(payload, dict) else {}
                    disposition_code = payload.get("disposition_code")
                    disposition_code = (
                        str(disposition_code) if disposition_code is not None else None
                    )
                    self.db.execute(
                        """
                        INSERT INTO interaction_events(
                            event_id, event_type, workflow_id, call_id, occurred_at_utc,
                            phone_hash, disposition_code, payload_ciphertext
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            event_id,
                            event_type,
                            workflow_id,
                            call_id,
                            self._timestamp_utc(event.get("occurred_at")),
                            phone_hash,
                            disposition_code,
                            self._encrypt_json(payload),
                        ),
                    )
                    self.db.execute(
                        "INSERT INTO event_links(event_id, lead_id) VALUES (?, NULL)",
                        (event_id,),
                    )
            self._resolve_pending_links()
            self._record_file(path, "events")

    def _resolve_pending_links(self) -> None:
        self.db.execute(
            """
            UPDATE event_links
            SET lead_id = (
                SELECT leads.id
                FROM interaction_events AS events
                JOIN leads ON leads.workflow_id = events.workflow_id
                           AND leads.phone_hash = events.phone_hash
                WHERE events.event_id = event_links.event_id
            )
            WHERE lead_id IS NULL
              AND EXISTS (
                  SELECT 1
                  FROM interaction_events AS events
                  JOIN leads ON leads.workflow_id = events.workflow_id
                             AND leads.phone_hash = events.phone_hash
                  WHERE events.event_id = event_links.event_id
                    AND events.event_type != 'disposition.override'
              )
            """
        )
        self.db.execute(
            """
            UPDATE event_links
            SET lead_id = (
                SELECT source_link.lead_id
                FROM interaction_events AS override_event
                JOIN interaction_events AS source_event
                  ON source_event.workflow_id = override_event.workflow_id
                 AND source_event.call_id = override_event.call_id
                 AND source_event.event_type != 'disposition.override'
                JOIN event_links AS source_link ON source_link.event_id = source_event.event_id
                WHERE override_event.event_id = event_links.event_id
                  AND source_link.lead_id IS NOT NULL
                ORDER BY source_event.event_id
                LIMIT 1
            )
            WHERE lead_id IS NULL
              AND EXISTS (
                  SELECT 1 FROM interaction_events
                  WHERE event_id = event_links.event_id
                    AND event_type = 'disposition.override'
              )
            """
        )

    def _quality_report(self) -> dict[str, Any]:
        counters = {
            row["name"]: row["value"]
            for row in self.db.execute("SELECT name, value FROM quality_counters")
        }
        reasons = {
            row["reason"]: row["count"]
            for row in self.db.execute(
                "SELECT reason, COUNT(*) AS count FROM rejections GROUP BY reason ORDER BY reason"
            )
        }
        orphan_count = self.db.execute(
            "SELECT COUNT(*) FROM event_links WHERE lead_id IS NULL"
        ).fetchone()[0]
        known_codes = self.dispositions
        unknown_count = 0
        for row in self.db.execute(
            """
            SELECT events.workflow_id, events.disposition_code
            FROM interaction_events AS events
            JOIN event_links ON event_links.event_id = events.event_id
            WHERE event_links.lead_id IS NOT NULL
              AND events.event_type LIKE 'disposition.%'
            """
        ):
            if row["disposition_code"] not in known_codes.get(row["workflow_id"], {}):
                unknown_count += 1

        return {
            "lead_rows_total": counters.get("lead_rows_total", 0),
            "lead_rows_rejected": counters.get("lead_rows_rejected", 0),
            "rejections_by_reason": reasons,
            "duplicate_lead_rows_merged": counters.get("duplicate_lead_rows_merged", 0),
            "field_warnings": counters.get("field_warnings", 0),
            "events_total": counters.get("events_total", 0),
            "malformed_events": counters.get("malformed_events", 0),
            "duplicate_events_dropped": counters.get("duplicate_events_dropped", 0),
            "orphan_events": orphan_count,
            "unknown_disposition_events": unknown_count,
        }

    def report(self, output_dir: str | Path) -> dict[str, Any]:
        leads = list(self.db.execute("SELECT * FROM leads ORDER BY workflow_id, id"))
        lead_rows: list[dict[str, Any]] = []
        leads_by_id: dict[int, dict[str, Any]] = {}
        for row in leads:
            lead = {
                "id": row["id"],
                "workflow_id": row["workflow_id"],
                "batch_id": row["batch_id"],
                "phone_hash": row["phone_hash"],
                "pii": self._decrypt_json(row["pii_ciphertext"]),
                "values": json.loads(row["values_json"]),
                "calls": [],
            }
            leads_by_id[row["id"]] = lead

        linked_events: dict[tuple[int, str], list[sqlite3.Row]] = defaultdict(list)
        for event in self.db.execute(
            """
            SELECT events.*, event_links.lead_id
            FROM interaction_events AS events
            JOIN event_links ON event_links.event_id = events.event_id
            WHERE event_links.lead_id IS NOT NULL
            """
        ):
            if event["event_type"].startswith("disposition."):
                if event["disposition_code"] not in self.dispositions.get(
                    event["workflow_id"], {}
                ):
                    continue
            call_id = event["call_id"] or f"unkeyed:{event['event_id']}"
            linked_events[(event["lead_id"], call_id)].append(event)

        daily: dict[tuple[str, str], dict[str, Any]] = {}
        for (lead_id, _call_id), events in linked_events.items():
            initiated = [event for event in events if event["event_type"] == "call.initiated"]
            if not initiated:
                continue
            initiated.sort(
                key=lambda event: (
                    event["occurred_at_utc"] is None,
                    event["occurred_at_utc"] or "",
                    event["event_id"],
                )
            )
            start = initiated[0]["occurred_at_utc"]
            call = {
                "started_utc": start,
                "started_ist": self._to_ist(start),
                "connected": any(event["event_type"] == "call.connected" for event in events),
                "telephony_failed": any(
                    event["event_type"] == "call.telephony_failed" for event in events
                ),
                "disposition": self._effective_disposition(events),
            }
            leads_by_id[lead_id]["calls"].append(call)

        batch_counts: dict[tuple[str, str], dict[str, Any]] = {}
        for lead in leads_by_id.values():
            calls = lead["calls"]
            calls.sort(
                key=lambda call: (
                    call["started_utc"] is None,
                    call["started_utc"] or "",
                )
            )
            dispositions = [
                call["disposition"] for call in calls if call["disposition"] is not None
            ]
            if not calls:
                bucket = "yet_to_reach_out"
            elif any(
                self.dispositions[lead["workflow_id"]][code][0]
                for code in dispositions
            ):
                bucket = "completed"
            elif all(call["telephony_failed"] for call in calls):
                bucket = "in_progress_telephony_failed"
            else:
                bucket = "in_progress_dialed"
            key = (lead["workflow_id"], lead["batch_id"])
            summary = batch_counts.setdefault(
                key,
                {
                    "workflow_id": lead["workflow_id"],
                    "batch_id": lead["batch_id"],
                    "total_leads": 0,
                    "yet_to_reach_out": 0,
                    "in_progress_telephony_failed": 0,
                    "in_progress_dialed": 0,
                    "completed": 0,
                },
            )
            summary["total_leads"] += 1
            summary[bucket] += 1

            current_disposition = dispositions[-1] if dispositions else None
            phone = lead["pii"].get("phone", "")
            lead_rows.append(
                {
                    "workflow_id": lead["workflow_id"],
                    "external_ref": lead["values"].get(self._reference_field(lead["workflow_id"]))
                    or "",
                    "batch_id": lead["batch_id"],
                    "phone_masked": f"+91******{phone[-4:]}" if phone else "",
                    "bucket": bucket,
                    "total_calls": len(calls),
                    "connected_calls": sum(call["connected"] for call in calls),
                    "last_call_at": self._to_ist(calls[-1]["started_utc"])
                    if calls and calls[-1]["started_utc"]
                    else "",
                    "current_disposition": current_disposition or "",
                    "ever_success": any(
                        self.dispositions[lead["workflow_id"]][code][1]
                        for code in dispositions
                    ),
                }
            )

            successful_dates = set()
            for call in calls:
                started_ist = call["started_ist"]
                if started_ist is None:
                    continue
                date_key = started_ist.date().isoformat()
                daily_key = (lead["workflow_id"], date_key)
                metrics = daily.setdefault(
                    daily_key,
                    {
                        "workflow_id": lead["workflow_id"],
                        "date": date_key,
                        "total_calls": 0,
                        "connected_calls": 0,
                        "_success_leads": set(),
                    },
                )
                metrics["total_calls"] += 1
                metrics["connected_calls"] += int(call["connected"])
                if (
                    call["disposition"] is not None
                    and self.dispositions[lead["workflow_id"]][call["disposition"]][1]
                ):
                    successful_dates.add(date_key)
            for date_key in successful_dates:
                daily[(lead["workflow_id"], date_key)]["_success_leads"].add(lead_id)

        lead_rows.sort(key=lambda row: (row["workflow_id"], row["external_ref"]))
        batch_rows = [batch_counts[key] for key in sorted(batch_counts)]
        daily_rows = []
        for key in sorted(daily):
            item = daily[key]
            daily_rows.append(
                {
                    "workflow_id": item["workflow_id"],
                    "date": item["date"],
                    "total_calls": item["total_calls"],
                    "connected_calls": item["connected_calls"],
                    "connectivity_rate": f"{item['connected_calls'] / item['total_calls']:.4f}",
                    "success_leads": len(item["_success_leads"]),
                }
            )

        rejections = [
            dict(row)
            for row in self.db.execute(
                """
                SELECT workflow_id, batch_id, row_number, reason FROM rejections
                ORDER BY workflow_id, batch_id, row_number
                """
            )
        ]
        quality = self._quality_report()
        output = Path(output_dir)
        output.mkdir(parents=True, exist_ok=True)
        self._write_csv(
            output / "batch_summary.csv",
            [
                "workflow_id",
                "batch_id",
                "total_leads",
                "yet_to_reach_out",
                "in_progress_telephony_failed",
                "in_progress_dialed",
                "completed",
            ],
            batch_rows,
        )
        self._write_csv(
            output / "daily_overview.csv",
            [
                "workflow_id",
                "date",
                "total_calls",
                "connected_calls",
                "connectivity_rate",
                "success_leads",
            ],
            daily_rows,
        )
        self._write_csv(
            output / "lead_report.csv",
            [
                "workflow_id",
                "external_ref",
                "batch_id",
                "phone_masked",
                "bucket",
                "total_calls",
                "connected_calls",
                "last_call_at",
                "current_disposition",
                "ever_success",
            ],
            lead_rows,
            boolean_fields={"ever_success"},
        )
        self._write_csv(
            output / "rejections.csv",
            ["workflow_id", "batch_id", "row_number", "reason"],
            rejections,
        )
        (output / "data_quality.json").write_text(
            json.dumps(quality, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        return quality

    def _reference_field(self, workflow_id: str) -> str:
        for field in self.schemas[workflow_id]["fields"]:
            if field.get("reference"):
                return field["name"]
        raise ValueError(f"No reference field configured for workflow: {workflow_id}")

    def _effective_disposition(self, events: list[sqlite3.Row]) -> str | None:
        overrides = [
            event
            for event in events
            if event["event_type"] == "disposition.override"
            and event["disposition_code"] in self.dispositions.get(event["workflow_id"], {})
        ]
        candidates = overrides or [
            event
            for event in events
            if event["event_type"] == "disposition.set"
            and event["disposition_code"] in self.dispositions.get(event["workflow_id"], {})
        ]
        if not candidates:
            return None
        candidates.sort(
            key=lambda event: (
                event["occurred_at_utc"] is None,
                event["occurred_at_utc"] or "",
                event["event_id"],
            )
        )
        return candidates[-1]["disposition_code"]

    @staticmethod
    def _to_ist(value: str | None) -> datetime | None:
        if value is None:
            return None
        return datetime.fromisoformat(value).astimezone(IST)

    @staticmethod
    def _write_csv(
        path: Path,
        headers: list[str],
        rows: list[dict[str, Any]],
        boolean_fields: set[str] | None = None,
    ) -> None:
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=headers, lineterminator="\n")
            writer.writeheader()
            for row in rows:
                output_row = dict(row)
                for field in boolean_fields or set():
                    output_row[field] = "true" if row[field] else "false"
                writer.writerow(output_row)
