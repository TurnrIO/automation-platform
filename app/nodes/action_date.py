"""Date / time operations action node.

Operations:
  now        — return current UTC time (no extra config needed)
  format     — format a date string using strftime pattern
  parse      — parse a date string into components
  add        — add an amount of time to a date
  subtract   — subtract an amount of time from a date
  diff       — difference between two dates (returns seconds + human label)

All date inputs accept ISO 8601 strings (e.g. "2024-01-15T09:00:00") or
Unix timestamps (numeric strings / ints).

Output always includes:
  { iso, unix, formatted, year, month, day, hour, minute, second, weekday }
"""
from datetime import datetime, timezone, timedelta
from app.nodes._utils import _render

import logging

logger = logging.getLogger(__name__)
NODE_TYPE = "action.date"
LABEL = "Date / Time"

_UNITS = {
    "second": "seconds", "seconds": "seconds",
    "minute": "minutes", "minutes": "minutes",
    "hour":   "hours",   "hours":   "hours",
    "day":    "days",    "days":    "days",
    "week":   "weeks",   "weeks":   "weeks",
}


def _parse_dt(value: str) -> datetime:
    """Parse ISO 8601 or Unix timestamp string → aware UTC datetime."""
    value = value.strip()
    if not value:
        return datetime.now(timezone.utc)
    # Unix timestamp (int or float)
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except (ValueError, OSError):
        pass
    # ISO with timezone
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
    ):
        try:
            dt = datetime.strptime(value, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: {value!r}")


def _dt_to_dict(dt: datetime, fmt: str = "%Y-%m-%d %H:%M:%S") -> dict:
    return {
        "iso":       dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "unix":      int(dt.timestamp()),
        "formatted": dt.strftime(fmt),
        "year":      dt.year,
        "month":     dt.month,
        "day":       dt.day,
        "hour":      dt.hour,
        "minute":    dt.minute,
        "second":    dt.second,
        "weekday":   dt.strftime("%A"),
    }


def run(config, inp, context, logger, creds=None, **kwargs):
    operation = config.get("operation", "now")
    date_val  = _render(config.get("date", ""),  context, creds)
    date2_val = _render(config.get("date2", ""), context, creds)
    fmt       = _render(config.get("format", "%Y-%m-%d %H:%M:%S"), context, creds)
    amount    = _render(config.get("amount", "1"), context, creds)
    unit      = _render(config.get("unit", "days"), context, creds).lower().strip()

    if operation == "now":
        dt = datetime.now(timezone.utc)
        result = _dt_to_dict(dt, fmt)
        logger.info("Date now: %s", result["iso"])
        return result

    if operation in ("format", "parse", "add", "subtract"):
        dt = _parse_dt(date_val)

        if operation in ("format", "parse"):
            result = _dt_to_dict(dt, fmt)
            logger.info("Date %s: %s", operation, result["iso"])
            return result

        # add / subtract
        td_unit = _UNITS.get(unit, "days")
        try:
            n = float(amount)
        except (ValueError, TypeError):
            n = 1.0
        td = timedelta(**{td_unit: n})
        dt2 = dt + td if operation == "add" else dt - td
        result = _dt_to_dict(dt2, fmt)
        logger.info("Date %s %s %s: %s", operation, n, td_unit, result["iso"])
        return result

    if operation == "diff":
        dt_a = _parse_dt(date_val)
        dt_b = _parse_dt(date2_val) if date2_val else datetime.now(timezone.utc)
        delta = dt_b - dt_a
        total_seconds = int(delta.total_seconds())
        abs_sec = abs(total_seconds)
        if abs_sec < 60:
            human = f"{abs_sec} second{'s' if abs_sec != 1 else ''}"
        elif abs_sec < 3600:
            m = abs_sec // 60
            human = f"{m} minute{'s' if m != 1 else ''}"
        elif abs_sec < 86400:
            h = abs_sec // 3600
            human = f"{h} hour{'s' if h != 1 else ''}"
        else:
            d = abs_sec // 86400
            human = f"{d} day{'s' if d != 1 else ''}"
        result = {
            "seconds": total_seconds,
            "minutes": round(total_seconds / 60, 2),
            "hours":   round(total_seconds / 3600, 2),
            "days":    round(total_seconds / 86400, 2),
            "human":   human,
            "past":    total_seconds < 0,
        }
        logger.info("Date diff: %s", human)
        return result

    raise ValueError(f"action.date: unknown operation {operation!r}")
