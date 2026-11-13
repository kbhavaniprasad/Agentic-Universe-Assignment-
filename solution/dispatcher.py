from __future__ import annotations

import csv
import hashlib
import hmac
import json
import os
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

IST = timezone(timedelta(hours=5, minutes=30))
DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
TERMINAL_DISPOSITIONS = {"interested", "not_interested", "wrong_number", "dnd"}
RETRY_MINUTES = {"no_answer": 60, "busy": 60, "failed": 15}


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(IST).isoformat()


def hm(text: str) -> tuple[int, int]:
    h, m = text.split(":")
    return int(h), int(m)


def in_window(t: datetime, start: str, end: str, days: list[str] | None, grace_s: float = 0.0) -> bool:
    for probe in (t, t - timedelta(seconds=grace_s), t + timedelta(seconds=grace_s)):
        lt = probe.astimezone(IST)
        if days is not None and DAYS[lt.weekday()] not in days:
            continue
        sh, sm = hm(start)
        eh, em = hm(end)
        s = lt.replace(hour=sh, minute=sm, second=0, microsecond=0)
        e = lt.replace(hour=eh, minute=em, second=0, microsecond=0)
        if s <= lt < e:
            return True
    return False


def next_open(t: datetime, start: str, end: str, days: list[str]) -> datetime:
    lt = t.astimezone(IST)
    for d in range(0, 9):
        day = (lt + timedelta(days=d)).replace(second=0, microsecond=0)
        if DAYS[day.weekday()] not in days:
            continue
        sh, sm = hm(start)
        eh, em = hm(end)
        s = day.replace(hour=sh, minute=sm)
        e = day.replace(hour=eh, minute=em)
        if d == 0:
            if lt < s:
                return s
            if lt < e:
                return lt
            continue
        return s
    return lt


@dataclass
class LeadRow:
    campaign_id: str
    lead_id: str
    phone: str
    attempts: int = 0
    last_status: str | None = None
    last_disposition: str | None = None
    callback_at: str | None = None
    next_retry_at: str | None = None
    terminal_disposition: str | None = None
    final_state: str | None = None
    dnd_blocked: bool = False
    active_call_sid: str | None = None

    @classmethod
    def from_db(cls, row: tuple[Any, ...]) -> "LeadRow":
        return cls(
            campaign_id=row[0],
            lead_id=row[1],
            phone=row[2],
            attempts=int(row[3]),
            last_status=row[4],
            last_disposition=row[5],
            callback_at=row[6],
            next_retry_at=row[7],
            terminal_disposition=row[8],
            final_state=row[9],
            dnd_blocked=bool(row[10]),
            active_call_sid=row[11],
        )


class DispatcherDB:
    def __init__(self, db_path: str | os.PathLike[str]):
        self.db_path = str(db_path)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS campaign (
                    campaign_id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    max_attempts INTEGER NOT NULL,
                    window_start TEXT NOT NULL,
                    window_end TEXT NOT NULL,
                    days TEXT NOT NULL,
                    webhook_secret TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS lead_state (
                    campaign_id TEXT NOT NULL,
                    lead_id TEXT NOT NULL,
                    phone TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_status TEXT,
                    last_disposition TEXT,
                    callback_at TEXT,
                    next_retry_at TEXT,
                    terminal_disposition TEXT,
                    final_state TEXT,
                    dnd_blocked INTEGER NOT NULL DEFAULT 0,
                    active_call_sid TEXT,
                    PRIMARY KEY (campaign_id, lead_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS outbound_call (
                    request_id TEXT PRIMARY KEY,
                    call_sid TEXT,
                    campaign_id TEXT NOT NULL,
                    lead_id TEXT NOT NULL,
                    phone TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    attempt_no INTEGER NOT NULL,
                    callback_url TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    accepted_at TEXT,
                    state TEXT NOT NULL DEFAULT 'pending',
                    status TEXT,
                    disposition TEXT,
                    callback_at TEXT,
                    ended_at TEXT,
                    last_seen TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dnd_phone (
                    phone TEXT PRIMARY KEY,
                    first_seen_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS webhook_seen (
                    key TEXT PRIMARY KEY,
                    received_at TEXT NOT NULL
                )
                """
            )

    def upsert_campaign(self, campaign: dict[str, Any], webhook_secret: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO campaign (campaign_id, tenant_id, max_attempts, window_start, window_end, days, webhook_secret) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    campaign["campaign_id"],
                    campaign["tenant_id"],
                    int(campaign["max_attempts"]),
                    campaign["window"]["start"],
                    campaign["window"]["end"],
                    ",".join(campaign["window"]["days"]),
                    webhook_secret,
                ),
            )

    def upsert_lead(self, campaign_id: str, lead_id: str, phone: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO lead_state (campaign_id, lead_id, phone, attempts, last_status, last_disposition, callback_at, next_retry_at, terminal_disposition, final_state, dnd_blocked, active_call_sid) VALUES (?, ?, ?, 0, NULL, NULL, NULL, NULL, NULL, NULL, 0, NULL)",
                (campaign_id, lead_id, phone),
            )

    def mark_lead_state(self, lead: LeadRow) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE lead_state SET
                    attempts = ?,
                    last_status = ?,
                    last_disposition = ?,
                    callback_at = ?,
                    next_retry_at = ?,
                    terminal_disposition = ?,
                    final_state = ?,
                    dnd_blocked = ?,
                    active_call_sid = ?
                WHERE campaign_id = ? AND lead_id = ?
                """,
                (
                    lead.attempts,
                    lead.last_status,
                    lead.last_disposition,
                    lead.callback_at,
                    lead.next_retry_at,
                    lead.terminal_disposition,
                    lead.final_state,
                    int(lead.dnd_blocked),
                    lead.active_call_sid,
                    lead.campaign_id,
                    lead.lead_id,
                ),
            )

    def get_all_leads(self) -> list[LeadRow]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT campaign_id, lead_id, phone, attempts, last_status, last_disposition, callback_at, next_retry_at, terminal_disposition, final_state, dnd_blocked, active_call_sid FROM lead_state ORDER BY campaign_id, lead_id"
            ).fetchall()
        return [LeadRow.from_db(row) for row in rows]

    def get_all_dnd_phones(self) -> set[str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT phone FROM dnd_phone").fetchall()
        return {row[0] for row in rows}

    def add_dnd_phone(self, phone: str, now: datetime) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO dnd_phone (phone, first_seen_at) VALUES (?, ?)",
                (phone, iso_utc(now)),
            )

    def get_campaign(self, campaign_id: str) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT tenant_id, max_attempts, window_start, window_end, days, webhook_secret FROM campaign WHERE campaign_id = ?",
                (campaign_id,),
            ).fetchone()
        if not row:
            raise KeyError(campaign_id)
        return {
            "campaign_id": campaign_id,
            "tenant_id": row[0],
            "max_attempts": int(row[1]),
            "window": {"start": row[2], "end": row[3], "days": row[4].split(",") if row[4] else []},
            "webhook_secret": row[5],
        }

    def get_campaigns(self) -> dict[str, dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT campaign_id, tenant_id, max_attempts, window_start, window_end, days, webhook_secret FROM campaign"
            ).fetchall()
        out: dict[str, dict[str, Any]] = {}
        for row in rows:
            cid = row[0]
            out[cid] = {
                "campaign_id": cid,
                "tenant_id": row[1],
                "max_attempts": int(row[2]),
                "window": {"start": row[3], "end": row[4], "days": row[5].split(",") if row[5] else []},
                "webhook_secret": row[6],
            }
        return out

    def upsert_outbound_call(self, *, request_id: str, campaign_id: str, lead_id: str, phone: str, tenant_id: str, attempt_no: int, callback_url: str, created_at: datetime, state: str = "pending") -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO outbound_call (
                    request_id, call_sid, campaign_id, lead_id, phone, tenant_id, attempt_no, callback_url,
                    created_at, accepted_at, state, status, disposition, callback_at, ended_at, last_seen
                ) VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL, NULL, NULL, NULL, ?)
                ON CONFLICT(request_id) DO UPDATE SET
                    campaign_id = excluded.campaign_id,
                    lead_id = excluded.lead_id,
                    phone = excluded.phone,
                    tenant_id = excluded.tenant_id,
                    attempt_no = excluded.attempt_no,
                    callback_url = excluded.callback_url,
                    created_at = excluded.created_at,
                    state = excluded.state,
                    last_seen = excluded.last_seen
                """,
                (
                    request_id,
                    campaign_id,
                    lead_id,
                    phone,
                    tenant_id,
                    attempt_no,
                    callback_url,
                    iso_utc(created_at),
                    state,
                    iso_utc(created_at),
                ),
            )

    def mark_call_accepted(self, request_id: str, call_sid: str, accepted_at: datetime) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE outbound_call SET call_sid = ?, accepted_at = ?, state = 'accepted', last_seen = ? WHERE request_id = ?",
                (call_sid, iso_utc(accepted_at), iso_utc(accepted_at), request_id),
            )

    def mark_call_ended(self, request_id: str, *, status: str | None, disposition: str | None, callback_at: str | None, ended_at: str | None, state: str = "ended") -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE outbound_call SET status = ?, disposition = ?, callback_at = ?, ended_at = ?, state = ? WHERE request_id = ?",
                (status, disposition, callback_at, ended_at, state, request_id),
            )

    def get_pending_calls(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT request_id, call_sid, campaign_id, lead_id, phone, tenant_id, attempt_no, callback_url, created_at, accepted_at, state, status, disposition, callback_at, ended_at FROM outbound_call WHERE state IN ('pending','accepted','active','unknown') ORDER BY created_at"
            ).fetchall()
        return [
            {
                "request_id": row[0],
                "call_sid": row[1],
                "campaign_id": row[2],
                "lead_id": row[3],
                "phone": row[4],
                "tenant_id": row[5],
                "attempt_no": row[6],
                "callback_url": row[7],
                "created_at": row[8],
                "accepted_at": row[9],
                "state": row[10],
                "status": row[11],
                "disposition": row[12],
                "callback_at": row[13],
                "ended_at": row[14],
            }
            for row in rows
        ]

    def set_call_state(self, request_id: str, state: str) -> None:
        with self._connect() as conn:
            conn.execute("UPDATE outbound_call SET state = ?, last_seen = ? WHERE request_id = ?", (state, time.strftime("%Y-%m-%dT%H:%M:%S%z"), request_id))

    def set_call_sid(self, request_id: str, call_sid: str) -> None:
        with self._connect() as conn:
            conn.execute("UPDATE outbound_call SET call_sid = ? WHERE request_id = ?", (call_sid, request_id))

    def mark_webhook_seen(self, key: str) -> None:
        with self._connect() as conn:
            conn.execute("INSERT OR IGNORE INTO webhook_seen (key, received_at) VALUES (?, ?)", (key, time.strftime("%Y-%m-%dT%H:%M:%S%z")))

    def is_webhook_seen(self, key: str) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM webhook_seen WHERE key = ?", (key,)).fetchone()
        return row is not None


@dataclass
class SchedulerCandidate:
    priority: int
    candidate_time: datetime
    lead: LeadRow


class Dispatcher:
    def __init__(self, fixtures_dir: str | os.PathLike[str], state_db_path: str | os.PathLike[str] | None = None):
        self.fixtures_dir = Path(fixtures_dir)
        self.db = DispatcherDB(state_db_path or self.fixtures_dir.parent / "solution" / "dispatcher_state.db")
        self.config = json.loads((self.fixtures_dir / "campaigns.json").read_text())
        self._initialise_campaigns_and_leads()

    def _initialise_campaigns_and_leads(self) -> None:
        for tenant in self.config["tenants"]:
            pass
        for campaign in self.config["campaigns"]:
            self.db.upsert_campaign(campaign, self.config["webhook_secret"])
            path = self.fixtures_dir / campaign["leads_file"]
            with path.open(newline="") as fh:
                for row in csv.DictReader(fh):
                    self.db.upsert_lead(campaign["campaign_id"], row["lead_id"], row["phone"])

    def get_campaign(self, campaign_id: str) -> dict[str, Any]:
        return self.db.get_campaign(campaign_id)

    def get_leads(self) -> list[LeadRow]:
        return self.db.get_all_leads()

    def get_dnd_phones(self) -> set[str]:
        return self.db.get_all_dnd_phones()

    def provider_clock(self) -> datetime:
        try:
            resp = httpx.get("http://127.0.0.1:8099/v1/clock", timeout=5.0)
            if resp.status_code == 200:
                data = resp.json()
                return datetime.fromisoformat(data["sim_now"]).astimezone(IST)
        except httpx.HTTPError:
            pass
        return datetime.now(IST)

    def _datetime_from_iso(self, value: str | None) -> datetime | None:
        if not value:
            return None
        parsed = datetime.fromisoformat(value)
        return parsed.astimezone(IST) if parsed.tzinfo else parsed.replace(tzinfo=IST)

    def _campaign_window(self, campaign: dict[str, Any]) -> tuple[str, str, list[str]]:
        return campaign["window"]["start"], campaign["window"]["end"], campaign["window"]["days"]

    def _candidate(self, lead: LeadRow, campaign: dict[str, Any], now: datetime, dnd_phones: set[str]) -> SchedulerCandidate | None:
        if lead.final_state in {"completed", "exhausted", "dnd_blocked"}:
            return None
        if lead.active_call_sid is not None:
            return None
        if lead.phone in dnd_phones:
            lead.dnd_blocked = True
            lead.final_state = "dnd_blocked"
            self.db.mark_lead_state(lead)
            return None
        if lead.attempts >= int(campaign["max_attempts"]):
            lead.final_state = "exhausted"
            self.db.mark_lead_state(lead)
            return None

        lead_status = lead.last_status
        if lead.last_disposition == "callback_requested":
            callback_at = self._datetime_from_iso(lead.callback_at)
            if callback_at is not None and now >= callback_at:
                if in_window(now, campaign["window"]["start"], campaign["window"]["end"], campaign["window"]["days"], self.config.get("window_grace_seconds", 120)) and in_window(now, self.config["regulatory_window"]["start"], self.config["regulatory_window"]["end"], None, self.config.get("window_grace_seconds", 120)):
                    return SchedulerCandidate(0, now, lead)
                return scheduler_sort_key_time(lead, now, campaign, "callback")
        if lead_status in RETRY_MINUTES and lead.next_retry_at:
            retry_at = self._datetime_from_iso(lead.next_retry_at)
            if retry_at is not None and now >= retry_at:
                if in_window(now, campaign["window"]["start"], campaign["window"]["end"], campaign["window"]["days"], self.config.get("window_grace_seconds", 120)) and in_window(now, self.config["regulatory_window"]["start"], self.config["regulatory_window"]["end"], None, self.config.get("window_grace_seconds", 120)):
                    return SchedulerCandidate(1, now, lead)
                return scheduler_sort_key_time(lead, now, campaign, "retry")
        if lead.attempts == 0 and in_window(now, campaign["window"]["start"], campaign["window"]["end"], campaign["window"]["days"], self.config.get("window_grace_seconds", 120)) and in_window(now, self.config["regulatory_window"]["start"], self.config["regulatory_window"]["end"], None, self.config.get("window_grace_seconds", 120)):
            return scheduler_sort_key_time(lead, now, campaign, "fresh")
        if lead.attempts == 0:
            return scheduler_sort_key_time(lead, now, campaign, "fresh")
        return None

    def choose_next_lead(self, now: datetime) -> tuple[str, str, LeadRow] | None:
        candidates: list[SchedulerCandidate] = []
        dnd_phones = self.get_dnd_phones()
        for lead in self.get_leads():
            campaign = self.get_campaign(lead.campaign_id)
            candidate = self._candidate(lead, campaign, now, dnd_phones)
            if candidate is not None:
                candidates.append(candidate)
        if not candidates:
            return None
        best = min(candidates, key=lambda c: (c.priority, c.candidate_time))
        return best.lead.campaign_id, best.lead.lead_id, best.lead

    def has_active_call_for_phone(self, phone: str) -> bool:
        with self.db._connect() as conn:
            row = conn.execute("SELECT 1 FROM outbound_call WHERE phone = ? AND state IN ('accepted','active') LIMIT 1", (phone,)).fetchone()
        return row is not None

    def has_active_call_for_lead(self, campaign_id: str, lead_id: str) -> bool:
        with self.db._connect() as conn:
            row = conn.execute("SELECT 1 FROM outbound_call WHERE campaign_id = ? AND lead_id = ? AND state IN ('accepted','active') LIMIT 1", (campaign_id, lead_id)).fetchone()
        return row is not None

    def _request_id_for(self, campaign_id: str, lead_id: str, current_attempts: int) -> str:
        return f"{campaign_id}:{lead_id}:{current_attempts + 1}:{uuid.uuid4().hex[:16]}"

    def dial_lead(self, campaign_id: str, lead_id: str, phone: str, callback_url: str, now: datetime) -> bool:
        campaign = self.get_campaign(campaign_id)
        if self.has_active_call_for_phone(phone):
            return False
        if self.has_active_call_for_lead(campaign_id, lead_id):
            return False
        if phone in self.get_dnd_phones():
            return False

        lead = None
        for row in self.get_leads():
            if row.campaign_id == campaign_id and row.lead_id == lead_id:
                lead = row
                break
        if lead is None:
            return False

        attempt_no = lead.attempts + 1
        request_id = self._request_id_for(campaign_id, lead_id, lead.attempts)
        self.db.upsert_outbound_call(
            request_id=request_id,
            campaign_id=campaign_id,
            lead_id=lead_id,
            phone=phone,
            tenant_id=campaign["tenant_id"],
            attempt_no=attempt_no,
            callback_url=callback_url,
            created_at=now,
            state="queued",
        )

        payload = {
            "request_id": request_id,
            "campaign_id": campaign_id,
            "lead_id": lead_id,
            "phone": phone,
            "callback_url": callback_url,
        }
        try:
            response = httpx.post("http://127.0.0.1:8099/v1/calls", json=payload, timeout=10.0)
        except httpx.HTTPError:
            self.db.set_call_state(request_id, "pending")
            return False

        if response.status_code in {200, 201}:
            body = response.json()
            call_sid = body.get("call_sid")
            if not call_sid and response.status_code == 200 and "call_sid" in body:
                call_sid = body["call_sid"]
            if call_sid:
                self.db.mark_call_accepted(request_id, call_sid, now)
                self.db.set_call_sid(request_id, call_sid)
                # The provider counts accepted calls and we persist attempt count only once accepted.
                with self.db._connect() as conn:
                    conn.execute(
                        "UPDATE lead_state SET attempts = attempts + 1, active_call_sid = ?, last_status = 'dialed', final_state = NULL WHERE campaign_id = ? AND lead_id = ?",
                        (call_sid, campaign_id, lead_id),
                    )
                return True
            self.db.set_call_state(request_id, "unknown")
            return False

        if response.status_code in {503, 504}:
            self.db.set_call_state(request_id, "unknown")
            return False

        if response.status_code in {400, 409, 422, 429}:
            self.db.set_call_state(request_id, "rejected")
            return False

        self.db.set_call_state(request_id, "pending")
        return False

    def _request_call_detail(self, request_id: str) -> dict[str, Any] | None:
        try:
            response = httpx.get(f"http://127.0.0.1:8099/v1/calls?request_id={request_id}", timeout=10.0)
            if response.status_code == 200:
                return response.json()
            return None
        except httpx.HTTPError:
            return None

    def _call_detail(self, call_sid: str) -> dict[str, Any] | None:
        try:
            response = httpx.get(f"http://127.0.0.1:8099/v1/calls/{call_sid}", timeout=10.0)
            if response.status_code == 200:
                return response.json()
            return None
        except httpx.HTTPError:
            return None

    def reconcile_pending_calls(self) -> None:
        sim_now = self.provider_clock()
        for call in self.db.get_pending_calls():
            request_id = call["request_id"]
            if not call["call_sid"]:
                detail = self._request_call_detail(request_id)
                if not detail:
                    continue
                if detail.get("call_sid"):
                    self.db.mark_call_accepted(request_id, detail["call_sid"], sim_now)
                    self.db.set_call_sid(request_id, detail["call_sid"])
                    self.db.set_call_state(request_id, "accepted")
                else:
                    continue
            detail = self._call_detail(call["call_sid"])
            if detail is None:
                continue
            state = detail.get("state")
            if state == "ended":
                result = detail.get("result") or {}
                self._apply_call_result(result)
                self.db.set_call_state(request_id, "ended")
                continue
            self.db.set_call_state(request_id, "active")

    def _apply_call_result(self, result: dict[str, Any]) -> None:
        if not result:
            return
        request_id = result.get("request_id")
        call_sid = result.get("call_sid")
        if request_id:
            self.db.mark_call_ended(request_id, status=result.get("status"), disposition=result.get("disposition"), callback_at=result.get("callback_at"), ended_at=result.get("ended_at"), state="ended")
        campaign_id = result.get("campaign_id")
        lead_id = result.get("lead_id")
        phone = result.get("phone")
        if not campaign_id or not lead_id:
            return

        with self.db._connect() as conn:
            row = conn.execute(
                "SELECT attempts, last_status, last_disposition, callback_at, next_retry_at, terminal_disposition, final_state, dnd_blocked, active_call_sid FROM lead_state WHERE campaign_id = ? AND lead_id = ?",
                (campaign_id, lead_id),
            ).fetchone()
        if row is None:
            return

        attempts = int(row[0])
        last_status = result.get("status")
        disposition = result.get("disposition")
        callback_at = result.get("callback_at")
        next_retry_at = None
        if last_status in {"no_answer", "busy", "failed"}:
            ended_at = parse_iso(result.get("ended_at"))
            minutes = RETRY_MINUTES.get(last_status, 0)
            if ended_at is not None:
                next_retry_at = (ended_at + timedelta(minutes=minutes)).astimezone(IST).isoformat()

        lead = LeadRow(
            campaign_id=campaign_id,
            lead_id=lead_id,
            phone=phone,
            attempts=attempts,
            last_status=last_status,
            last_disposition=disposition,
            callback_at=callback_at,
            next_retry_at=next_retry_at,
            terminal_disposition=disposition if disposition in TERMINAL_DISPOSITIONS else None,
            final_state=None,
            dnd_blocked=False,
            active_call_sid=None,
        )

        if disposition in TERMINAL_DISPOSITIONS:
            lead.final_state = "completed"
            lead.terminal_disposition = disposition
        elif disposition == "dnd":
            self.db.add_dnd_phone(phone, parse_iso(result.get("ended_at")) or datetime.now(IST))
            lead.terminal_disposition = "dnd"
            lead.final_state = "completed"
        elif last_status in {"no_answer", "busy", "failed"}:
            lead.final_state = None

        lead.dnd_blocked = phone in self.db.get_all_dnd_phones()
        if phone in self.db.get_all_dnd_phones() and lead.final_state not in {"completed"}:
            lead.final_state = "dnd_blocked"
        self.db.mark_lead_state(lead)

        for other in self.get_leads():
            if other.phone == phone and other.campaign_id != campaign_id:
                if other.final_state not in {"completed", "exhausted"}:
                    other.final_state = "dnd_blocked"
                    other.dnd_blocked = True
                    self.db.mark_lead_state(other)

    def _handle_webhook(self, body: bytes, headers: dict[str, str]) -> JSONResponse:
        signature = headers.get("X-AU-Signature", "")
        payload = body.decode("utf-8")
        event = json.loads(payload)
        campaign_id = event.get("campaign_id")
        if not campaign_id:
            return JSONResponse({"error": "missing_campaign_id"}, status_code=400)
        secret = self.db.get_campaign(campaign_id)["webhook_secret"]
        expected = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, signature):
            return JSONResponse({"error": "bad_signature"}, status_code=401)
        dedupe_key = f"{event.get('call_sid')}:{event.get('request_id')}:{event.get('campaign_id')}:{event.get('lead_id')}"
        if self.db.is_webhook_seen(dedupe_key):
            return JSONResponse({"status": "duplicate"}, status_code=200)
        self.db.mark_webhook_seen(dedupe_key)
        self._apply_call_result(event)
        return JSONResponse({"status": "ok"}, status_code=200)

    def _ensure_terminal_states(self) -> None:
        dnd_phones = self.get_dnd_phones()
        for lead in self.get_leads():
            if lead.final_state in {"completed", "exhausted", "dnd_blocked"}:
                continue
            if lead.phone in dnd_phones and lead.final_state not in {"completed"}:
                lead.dnd_blocked = True
                lead.final_state = "dnd_blocked"
                self.db.mark_lead_state(lead)
                continue
            campaign = self.get_campaign(lead.campaign_id)
            if lead.attempts >= campaign["max_attempts"]:
                lead.final_state = "exhausted"
                self.db.mark_lead_state(lead)

    def write_final_state_csv(self, output_path: str | os.PathLike[str]) -> None:
        self._ensure_terminal_states()
        rows: list[tuple[str, str, str, int, str, str]] = []
        for lead in self.get_leads():
            if lead.final_state is None:
                continue
            rows.append((lead.campaign_id, lead.lead_id, lead.final_state, lead.attempts, lead.last_status or "", lead.last_disposition or ""))
        rows.sort(key=lambda r: (r[0], r[1]))
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with output_file.open("w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["campaign_id", "lead_id", "final_state", "attempts", "last_status", "last_disposition"])
            writer.writerows(rows)

    def run(self, *, max_cycles: int | None = None, poll_interval: float = 1.0, callback_url: str | None = None) -> None:
        callback_url = callback_url or "http://127.0.0.1:8200/webhooks/telephony"
        safe_polled = 0
        while True:
            self.reconcile_pending_calls()
            self._ensure_terminal_states()
            now = self.provider_clock()
            if max_cycles is not None and safe_polled >= max_cycles:
                return
            next_call = self.choose_next_lead(now)
            if next_call is not None:
                campaign_id, lead_id, lead = next_call
                self.dial_lead(campaign_id, lead_id, lead.phone, callback_url, now)
            safe_polled += 1
            time.sleep(poll_interval)


def scheduler_sort_key_time(lead: LeadRow, now: datetime, campaign: dict[str, Any], kind: str) -> SchedulerCandidate:
    start, end, days = campaign["window"]["start"], campaign["window"]["end"], campaign["window"]["days"]

    if kind == "callback" and lead.callback_at:
        at = parse_iso(lead.callback_at)
        if at is not None:
            candidate_time = at
            if not in_window(at, start, end, days, 120):
                candidate_time = next_open(at, start, end, days)
            return SchedulerCandidate(0, candidate_time, lead)

    if kind == "retry" and lead.next_retry_at:
        at = parse_iso(lead.next_retry_at)
        if at is not None:
            candidate_time = at
            if not in_window(at, start, end, days, 120):
                candidate_time = next_open(at, start, end, days)
            return SchedulerCandidate(1, candidate_time, lead)

    if kind == "fresh":
        candidate_time = now
        if not in_window(now, start, end, days, 120):
            candidate_time = next_open(now, start, end, days)
        return SchedulerCandidate(2, candidate_time, lead)

    return SchedulerCandidate(2, now, lead)


def create_app(fixtures_dir: str | os.PathLike[str]):
    dispatcher = Dispatcher(fixtures_dir)

    app = FastAPI(title="Agentic Universe dispatcher")

    @app.post("/webhooks/telephony")
    async def telephony_webhook(request: Request):
        body = await request.body()
        headers = dict(request.headers)
        return dispatcher._handle_webhook(body, headers)

    @app.get("/status")
    async def status():
        return {"leads": len(dispatcher.get_leads()), "dnd_phones": sorted(dispatcher.get_dnd_phones())}

    return app


def main() -> None:
    import argparse
    import uvicorn

    parser = argparse.ArgumentParser(description="Run the durable campaign dispatcher")
    parser.add_argument("--fixtures", type=str, default=str(Path(__file__).resolve().parents[1] / "assignment-b-workflow" / "fixtures"))
    parser.add_argument("--db", type=str, default=str(Path(__file__).resolve().parent / "dispatcher_state.db"))
    parser.add_argument("--host", type=str, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8200)
    parser.add_argument("--simulate-only", action="store_true")
    args = parser.parse_args()

    dispatcher = Dispatcher(args.fixtures, args.db)

    if args.simulate_only:
        dispatcher.run(callback_url="http://127.0.0.1:8200/webhooks/telephony")
        return

    app = create_app(args.fixtures)
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
