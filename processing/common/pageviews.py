"""Pure Python parser shared by batch and replay implementations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import re

SCHEMA_VERSION = "3"
ALLOWED_DOMAINS = {"en", "en.m", "vi", "vi.m"}
MAX_VIEWS = 2**63 - 1


class RecordError(ValueError):
    """A stable reason code suitable for quality reports."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def source_hours(filename: str):
    """The filename names the UTC end of an hourly interval."""
    match = re.fullmatch(r"pageviews-(\d{8})-(\d{2})0000(?:\.gz)?", filename)
    if not match:
        raise RecordError("unrecognized_source_filename")
    try:
        end = datetime.strptime(''.join(match.groups()), '%Y%m%d%H').replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise RecordError("unrecognized_source_filename") from exc
    return end - timedelta(hours=1), end


def event_id(source_file: str, source_line: int, domain: str, title: str) -> str:
    if source_line < 1:
        raise ValueError("source_line must be one-based")
    return hashlib.sha256('\x1f'.join((source_file, str(source_line), domain, title)).encode('utf-8')).hexdigest()


@dataclass(frozen=True)
class PageviewRecord:
    domain_code: str
    project: str
    access_variant: str
    page_title: str
    views: int
    legacy_response_size: str | None = None


def parse_line(line: str) -> PageviewRecord | None:
    """Parse one Wikimedia record; ignore other projects and reject malformed values."""
    fields = line.rstrip("\r\n").split(" ")
    # Excluded projects do not enter the selected-project quality denominator.
    if fields[0] not in ALLOWED_DOMAINS:
        return None
    if len(fields) not in (3, 4):
        raise RecordError("invalid_field_count")
    domain, title, raw_views = fields[:3]
    if domain not in ALLOWED_DOMAINS:
        return None
    if not title:
        raise RecordError("empty_title")
    if not re.fullmatch(r"-?[0-9]+", raw_views):
        raise RecordError("invalid_views")
    # Avoid Python's integer-string digit limit and align with Spark LongType.
    digits = raw_views.lstrip('-').lstrip('0') or '0'
    if len(digits) > 19 or (len(digits) == 19 and digits > str(MAX_VIEWS)):
        raise RecordError("invalid_views")
    views = int(('-' if raw_views.startswith('-') else '') + digits)
    if views < 0:
        raise RecordError("negative_views")
    project, _, variant = domain.partition(".")
    return PageviewRecord(
        domain_code=domain,
        project=project,
        access_variant="mobile" if variant == "m" else "desktop",
        page_title=title,
        views=views,
        legacy_response_size=fields[3] if len(fields) == 4 else None,
    )
