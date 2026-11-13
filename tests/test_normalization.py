from solution.normalization import (
    normalize_date,
    normalize_decimal,
    normalize_header,
    normalize_phone,
)


def test_phone_formats_and_validation():
    assert normalize_phone("+91 98765-43210") == "+919876543210"
    assert normalize_phone("09123456789") == "+919123456789"
    assert normalize_phone("9123456789") == "+919123456789"
    assert normalize_phone("5123456789") is None
    assert normalize_phone("N/A") is None


def test_date_formats_are_day_first_and_calendar_valid():
    assert normalize_date("2026-12-01") == "2026-12-01"
    assert normalize_date("01/12/2026") == "2026-12-01"
    assert normalize_date("01-12-2026") == "2026-12-01"
    assert normalize_date("01-dEC-2026") == "2026-12-01"
    assert normalize_date("31/02/2026") is None


def test_decimal_currency_and_header_normalization():
    assert normalize_decimal("₹1,234") == "1234.00"
    assert normalize_decimal("Rs. 12.345") == "12.35"
    assert normalize_decimal("INR -1") is None
    assert normalize_decimal("1,23") is None
    assert normalize_decimal("twelve") is None
    assert normalize_header("  Mobile__No ") == "mobile no"
