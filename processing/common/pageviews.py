"""Pure Python parser shared by batch and replay implementations."""

from __future__ import annotations

from dataclasses import dataclass

SCHEMA_VERSION = "1"
ALLOWED_DOMAINS = {"en", "en.m", "vi", "vi.m"}


@dataclass(frozen=True)
class PageviewRecord:
    domain_code: str
    project: str
    access_variant: str
    page_title: str
    views: int


def parse_line(line: str) -> PageviewRecord | None:
    """Parse one Wikimedia record; ignore other projects and reject malformed values."""
    fields = line.rstrip("\r\n").split(" ")
    if len(fields) not in (3, 4):
        raise ValueError(f"expected 3 or 4 fields, got {len(fields)}")
    domain, title, raw_views = fields[:3]
    if domain not in ALLOWED_DOMAINS:
        return None
    if not title:
        raise ValueError("page_title is empty")
    try:
        views = int(raw_views)
    except ValueError as exc:
        raise ValueError(f"invalid count_views: {raw_views!r}") from exc
    if views < 0:
        raise ValueError("count_views cannot be negative")
    project, _, variant = domain.partition(".")
    return PageviewRecord(
        domain_code=domain,
        project=project,
        access_variant="mobile" if variant == "m" else "desktop",
        page_title=title,
        views=views,
    )
