from datetime import datetime, timezone


def date_to_iso_seconds(dt: datetime):
    """Convert a datetime to an ISO 8601 string with seconds precision, e.g. 2020-12-25T10:45:26Z"""
    return (
        dt.replace(tzinfo=timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )
