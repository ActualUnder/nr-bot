from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import re
from zoneinfo import ZoneInfo

UTC = dt.timezone.utc
UK = ZoneInfo("Europe/London")


class InvalidFeedMessage(ValueError):
    pass


def timestamp(value, *, optional=False) -> float | None:
    if value in (None, "", " ") and optional:
        return None
    try:
        result = float(value) / 1000.0
        if not math.isfinite(result) or result <= 0:
            raise ValueError()
        # Permit the TSR 'until further notice' date in year 4000.
        dt.datetime.fromtimestamp(result, UTC)
        return result
    except (ValueError, TypeError, OverflowError, OSError) as exc:
        raise InvalidFeedMessage("missing or invalid millisecond timestamp") from exc


def trust_timestamp(value, mode: str, *, optional=False) -> float | None:
    result = timestamp(value, optional=optional)
    if result is None or mode == "utc":
        return result
    # TRUST's documented affected fields encode local wall time as UTC.
    # Activation origin_dep_timestamp and queue timestamps are genuine UTC.
    wall = dt.datetime.fromtimestamp(result, UTC).replace(tzinfo=None)
    return wall.replace(tzinfo=UK, fold=0).timestamp()


def iso(value) -> str | None:
    return dt.datetime.fromtimestamp(value, UTC).isoformat() if value is not None else None


def fingerprint(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def objects(body) -> list[dict]:
    try:
        parsed = json.loads(body) if isinstance(body, (str, bytes)) else body
    except (ValueError, TypeError) as exc:
        raise InvalidFeedMessage("invalid JSON") from exc
    result = parsed if isinstance(parsed, list) else [parsed]
    if not all(isinstance(x, dict) for x in result):
        raise InvalidFeedMessage("expected a JSON object or list of objects")
    return result


def berth(value) -> str:
    # Preserve all four characters; '0019' is not '19'. IDs are area qualified.
    value = str(value or "").strip().upper()
    if not re.fullmatch(r"[A-Z0-9]{1,4}", value):
        raise InvalidFeedMessage("invalid berth ID")
    return value


def clean(value, limit=160) -> str:
    return str(value or "").strip()[:limit]


def location_key(value) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value).lower()).strip()


def is_true(value) -> bool:
    return str(value).lower() == "true"
