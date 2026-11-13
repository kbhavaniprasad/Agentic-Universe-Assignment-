from __future__ import annotations

import re
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP


_HEADER_SEPARATORS = re.compile(r"[_\s]+")
_NON_DIGITS = re.compile(r"\D")
_CURRENCY_PREFIX = re.compile(r"^(?:₹|rs\.?|inr)\s*", re.IGNORECASE)
_DECIMAL_VALUE = re.compile(r"^[+-]?(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d+)?$")
_MONTHS = {
    name.lower(): number
    for number, name in enumerate(
        ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"),
        start=1,
    )
}


def normalize_header(value: str) -> str:
    return _HEADER_SEPARATORS.sub(" ", value.strip().lower())


def normalize_phone(value: str) -> str | None:
    digits = _NON_DIGITS.sub("", value)
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if len(digits) != 10 or digits[0] not in "6789":
        return None
    return f"+91{digits}"


def normalize_date(value: str) -> str | None:
    value = value.strip()
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            return date.fromisoformat(value).isoformat()
        if re.fullmatch(r"\d{2}/\d{2}/\d{4}", value):
            day, month, year = map(int, value.split("/"))
            return date(year, month, day).isoformat()
        if re.fullmatch(r"\d{2}-\d{2}-\d{4}", value):
            day, month, year = map(int, value.split("-"))
            return date(year, month, day).isoformat()
        match = re.fullmatch(r"(\d{2})-([A-Za-z]{3})-(\d{4})", value)
        if match:
            day, month_name, year = match.groups()
            return date(int(year), _MONTHS[month_name.lower()], int(day)).isoformat()
    except (ValueError, KeyError):
        return None
    return None


def normalize_decimal(value: str) -> str | None:
    value = _CURRENCY_PREFIX.sub("", value.strip())
    if not _DECIMAL_VALUE.fullmatch(value):
        return None
    value = value.replace(",", "")
    try:
        amount = Decimal(value)
    except InvalidOperation:
        return None
    if not amount.is_finite() or amount < 0:
        return None
    try:
        return str(amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    except InvalidOperation:
        return None


def normalize_enum(value: str, values: list[str]) -> str | None:
    canonical = {candidate.lower(): candidate.lower() for candidate in values}
    return canonical.get(value.strip().lower())


def parse_value(field: dict, raw_value: str | None) -> tuple[object | None, bool]:
    if raw_value is None or not raw_value.strip():
        return None, True
    value = raw_value.strip()
    field_type = field["type"]
    if field_type == "string":
        return value, True
    if field_type == "phone":
        normalized = normalize_phone(value)
        return normalized, normalized is not None
    if field_type == "date":
        normalized = normalize_date(value)
        return normalized, normalized is not None
    if field_type == "decimal":
        normalized = normalize_decimal(value)
        return normalized, normalized is not None
    if field_type == "enum":
        normalized = normalize_enum(value, field["values"])
        return normalized, normalized is not None
    raise ValueError(f"Unsupported field type: {field_type}")
