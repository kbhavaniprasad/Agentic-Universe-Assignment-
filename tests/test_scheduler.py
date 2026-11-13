from datetime import datetime, timedelta, timezone

from solution.dispatcher import LeadRow, in_window, next_open, scheduler_sort_key_time

IST = timezone(timedelta(hours=5, minutes=30))


def test_in_window_accepts_open_hours():
    now = datetime(2026, 11, 12, 12, 0, tzinfo=IST)
    assert in_window(now, "09:00", "21:00", None, 120)


def test_in_window_rejects_outside_window():
    now = datetime(2026, 11, 12, 8, 30, tzinfo=IST)
    assert not in_window(now, "09:00", "21:00", None, 120)


def test_next_open_skips_to_next_allowed_day():
    now = datetime(2026, 11, 12, 19, 30, tzinfo=IST)
    nxt = next_open(now, "10:00", "19:00", ["mon", "tue", "wed", "thu", "fri", "sat"])
    assert nxt.date() == datetime(2026, 11, 13, 0, 0, tzinfo=IST).date()
    assert nxt.hour == 10


def test_callbacks_take_priority_over_retries_and_fresh_leads():
    now = datetime(2026, 11, 12, 12, 0, tzinfo=IST)
    campaign = {
        "window": {"start": "10:00", "end": "20:30", "days": ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]}
    }

    callback = LeadRow("c_acme_feedback", "fb_0001", "+919999999999", attempts=1, callback_at="2026-11-12T12:15:00+05:30")
    retry = LeadRow("c_acme_feedback", "fb_0002", "+919999999998", attempts=1, last_status="failed", next_retry_at="2026-11-12T12:00:00+05:30")
    fresh = LeadRow("c_acme_feedback", "fb_0003", "+919999999997", attempts=0)

    candidates = [
        scheduler_sort_key_time(callback, now, campaign, "callback"),
        scheduler_sort_key_time(retry, now, campaign, "retry"),
        scheduler_sort_key_time(fresh, now, campaign, "fresh"),
    ]

    assert [c.priority for c in sorted(candidates, key=lambda c: (c.priority, c.candidate_time))] == [0, 1, 2]
