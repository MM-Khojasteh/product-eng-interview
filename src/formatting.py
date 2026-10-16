"""Persian text formatting — the UI's only digit/currency/locale surface.

Extracted from main.py §"Persian text helpers" (zero functional changes).
Registered as Jinja filters in view_templates.py; EN_DIGITS feeds parse_date().
"""

from __future__ import annotations

FA_DIGITS = str.maketrans("0123456789,.", "۰۱۲۳۴۵۶۷۸۹٬٫")
EN_DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")


def toman(value: int) -> str:
    """1_240_000 → «۱٬۲۴۰٬۰۰۰ تومان»"""
    return f"{value:,}".translate(FA_DIGITS) + " تومان"


def fa_num(value: object) -> str:
    """Latin digits → Persian digits (also ',' → '٬' and '.' → '٫')."""
    return str(value).translate(FA_DIGITS)


def date_fa(datenum: str) -> str:
    """'14050713' → '۱۴۰۵/۰۷/۱۳'"""
    return f"{datenum[:4]}/{datenum[4:6]}/{datenum[6:]}".translate(FA_DIGITS)


def duration_fa(total_minutes: int) -> str:
    """95 → «۱ ساعت و ۳۵ دقیقه» — no minutes part when zero. The one definition
    for the live read model (flights.group_quotes) and the mock catalog."""
    hours, minutes = divmod(total_minutes, 60)
    text = f"{hours} ساعت و {minutes} دقیقه" if minutes else f"{hours} ساعت"
    return text.translate(FA_DIGITS)
