"""Download hourly Wikimedia pageview dumps and write a reusable JSONL manifest."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import time
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE_URL = "https://dumps.wikimedia.org/other/pageviews"
USER_AGENT = "WikipediaAttentionObservatory/1.0 (academic project)"


def dump_url(hour_end: datetime) -> str:
    """Return the Wikimedia URL for the hour ending at *hour_end* (UTC)."""
    stamp = hour_end.strftime("%Y%m%d-%H0000")
    return f"{BASE_URL}/{hour_end:%Y/%Y-%m}/pageviews-{stamp}.gz"


def _s3_client(endpoint_url: str | None):
    try:
        import boto3
    except ImportError as exc:  # pragma: no cover - depends on optional dependency
        raise RuntimeError("Install boto3 to upload to MinIO/S3") from exc
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_gzip(path: Path) -> None:
    """Read through EOF so gzip verifies every member's CRC and trailer."""
    with gzip.open(path, "rb") as source:
        while source.read(1024 * 1024):
            pass


def download_hour(
    hour_end: datetime,
    output_dir: Path,
    *,
    bucket: str | None = None,
    endpoint_url: str | None = None,
    retries: int = 3,
) -> dict[str, Any]:
    """Download one compressed dump, preserve it locally, and optionally upload it."""
    url = dump_url(hour_end)
    filename = url.rsplit("/", 1)[-1]
    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / filename
    partial = destination.with_suffix(destination.suffix + ".part")

    if retries < 1:
        raise ValueError("retries must be >= 1")

    existing_valid = False
    if destination.exists():
        try:
            _validate_gzip(destination)
            existing_valid = True
        except (OSError, EOFError, zlib.error):
            # Keep the old file until a verified replacement is available.
            pass

    if existing_valid:
        status = "already_present"
    else:
        status = "downloaded"
        for attempt in range(retries):
            try:
                request = Request(url, headers={"User-Agent": USER_AGENT})
                with urlopen(request, timeout=90) as response, partial.open("wb") as target:
                    while chunk := response.read(1024 * 1024):
                        target.write(chunk)
                # Catch interrupted or invalid responses before considering the file complete.
                _validate_gzip(partial)
                partial.replace(destination)
                break
            except HTTPError as exc:
                partial.unlink(missing_ok=True)
                if exc.code == 404:
                    return _manifest_entry(hour_end, url, None, "missing", error="HTTP 404")
                if attempt + 1 == retries:
                    return _manifest_entry(hour_end, url, None, "failed", error=str(exc))
            except (URLError, OSError, EOFError, zlib.error) as exc:
                partial.unlink(missing_ok=True)
                if attempt + 1 == retries:
                    return _manifest_entry(hour_end, url, None, "failed", error=str(exc))
            time.sleep(min(2**attempt, 8))

    checksum = _sha256(destination)
    if bucket:
        client = _s3_client(endpoint_url)
        key = f"raw/{hour_end:%Y/%m/%d}/{filename}"
        client.upload_file(str(destination), bucket, key)
        raw_uri = f"s3a://{bucket}/{key}"
    else:
        raw_uri = str(destination.resolve())

    entry = _manifest_entry(hour_end, url, raw_uri, status)
    entry.update({"checksum_sha256": checksum, "size_bytes": destination.stat().st_size})
    return entry


def _manifest_entry(
    hour_end: datetime, url: str, raw_uri: str | None, status: str, *, error: str | None = None
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "hour_end": hour_end.astimezone(timezone.utc).isoformat(),
        "source_url": url,
        "raw_uri": raw_uri,
        "status": status,
    }
    if error:
        entry["error"] = error
    return entry


def iter_hours(start: datetime, end: datetime):
    """Yield UTC hour-end timestamps in [start, end), requiring aligned hours."""
    current = start.astimezone(timezone.utc)
    stop = end.astimezone(timezone.utc)
    if current.minute or current.second or current.microsecond or stop.minute or stop.second or stop.microsecond:
        raise ValueError("start and end must be aligned to UTC hours")
    while current < stop:
        yield current
        current += timedelta(hours=1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="UTC start, e.g. 2026-01-01T00:00:00Z")
    parser.add_argument("--end", required=True, help="Exclusive UTC end, e.g. 2026-01-02T00:00:00Z")
    parser.add_argument("--output", default="data/raw", help="Local raw dump directory")
    parser.add_argument("--manifest", default="data/manifests/pageviews.jsonl")
    parser.add_argument("--bucket", help="Optional MinIO/S3 bucket; credentials use AWS_* env vars")
    parser.add_argument("--endpoint-url", help="MinIO endpoint, e.g. http://localhost:9000")
    args = parser.parse_args()
    parse_time = lambda value: datetime.fromisoformat(value.replace("Z", "+00:00"))
    start, end = parse_time(args.start), parse_time(args.end)
    manifest_path = Path(args.manifest)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("a", encoding="utf-8") as manifest:
        for hour_end in iter_hours(start, end):
            entry = download_hour(
                hour_end,
                Path(args.output),
                bucket=args.bucket,
                endpoint_url=args.endpoint_url,
            )
            manifest.write(json.dumps(entry, ensure_ascii=False) + "\n")
            print(json.dumps(entry, ensure_ascii=False))
    if args.bucket:
        _s3_client(args.endpoint_url).upload_file(
            str(manifest_path), args.bucket, "manifests/pageviews.jsonl"
        )


if __name__ == "__main__":
    main()
