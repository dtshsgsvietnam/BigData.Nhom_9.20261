"""Local PB-005 architecture proof; standard library, no distributed services.

Synthetic Wikimedia-format fixture -> isolated branch outputs -> SQLite loaders.
This is an executable architecture example, not the production Spark/Kafka jobs.
"""

import argparse
from contextlib import closing
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def check_config(config):
    require(set(config["branches"]) == {"history", "replay"}, "Expected two branches")
    uris = [config["raw_uri"]]
    tables = []
    for name, branch in config["branches"].items():
        for key in ("output_uri", "checkpoint_uri"):
            uri = branch[key]
            require("{snapshot_id}" in uri and "{run_id}" in uri, f"Missing identity: {key}")
            require(f"/{name}/" in uri and uri.endswith("/"), f"Missing branch namespace: {uri}")
            uris.append(uri)
        require(re.fullmatch(r"[a-z_]+", branch["table"]) is not None, "Invalid table name")
        tables.append(branch["table"])
    require(len(set(tables)) == 2, "Serving tables must be separate")
    require(all(not a.startswith(b) and not b.startswith(a)
                for i, a in enumerate(uris) for b in uris[i + 1:]), "Storage namespaces overlap")


def source_events(manifest, fixture_dir, config, run_id):
    events = []
    for item in manifest["files"]:
        path = fixture_dir / item["name"]
        require(digest(path) == item["sha256"], f"Raw checksum mismatch: {path.name}")
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            project, title, views, size = line.split()
            require(project in ("en", "vi"), "Unexpected fixture project")
            # Draft source identity: independent of branch and run.
            identity = f"{manifest['snapshot_id']}\n{item['name']}\n{number}"
            events.append({
                "schema_version": config["contract_version"],
                "event_id": hashlib.sha256(identity.encode()).hexdigest(),
                "snapshot_id": manifest["snapshot_id"], "run_id": run_id,
                "event_time": item["event_time"], "project": project,
                "page_title": title, "views": int(views), "content_size": int(size),
            })
    return events


def process_branch(workspace, branch, events, stop_after=None):
    """One JSON file per event makes a retry after output-before-checkpoint safe."""
    checkpoint = workspace / "checkpoints" / branch / "progress.json"
    expected = {
        "snapshot_id": events[0]["snapshot_id"], "run_id": events[0]["run_id"],
        "schema_version": events[0]["schema_version"], "branch": branch,
    }
    state = read_json(checkpoint) if checkpoint.exists() else {**expected, "next_index": 0}
    require(all(state.get(k) == v for k, v in expected.items()), "Checkpoint identity mismatch")
    start = state["next_index"]
    require(isinstance(start, int) and 0 <= start <= len(events), "Invalid checkpoint position")
    end = min(len(events), start + stop_after) if stop_after is not None else len(events)
    output = workspace / "outputs" / branch
    output.mkdir(parents=True, exist_ok=True)
    for index in range(start, end):
        event = {**events[index], "branch": branch}
        write_json(output / f"{event['event_id']}.json", event)
        write_json(checkpoint, {**expected, "next_index": index + 1})
    return [read_json(path) for path in sorted(output.glob("*.json"))]


def load(connection, table, branch, rows):
    """Idempotent reference loader: one row per run/event in each branch table."""
    require(re.fullmatch(r"[a-z_]+", table) is not None, "Invalid table")
    connection.execute(f'''CREATE TABLE IF NOT EXISTS {table} (
        run_id TEXT NOT NULL, event_id TEXT NOT NULL, snapshot_id TEXT NOT NULL,
        event_time TEXT NOT NULL, project TEXT NOT NULL, page_title TEXT NOT NULL,
        views INTEGER NOT NULL, content_size INTEGER NOT NULL,
        schema_version TEXT NOT NULL, branch TEXT NOT NULL,
        PRIMARY KEY (run_id, event_id))''')
    for row in rows:
        require(row["branch"] == branch, "Loader received another branch's output")
        connection.execute(f'''INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, event_id) DO UPDATE SET
            snapshot_id=excluded.snapshot_id, event_time=excluded.event_time,
            project=excluded.project, page_title=excluded.page_title,
            views=excluded.views, content_size=excluded.content_size,
            schema_version=excluded.schema_version, branch=excluded.branch''',
            tuple(row[key] for key in (
                "run_id", "event_id", "snapshot_id", "event_time", "project",
                "page_title", "views", "content_size", "schema_version", "branch")))
    connection.commit()


def verify(workspace, config_path, fixture_dir):
    config = read_json(config_path)
    check_config(config)
    manifest = read_json(fixture_dir / "manifest.json")
    require(manifest["snapshot_id"] == config["snapshot_id"], "Snapshot mismatch")
    # Fresh destination avoids mixing evidence from different configurations.
    workspace.mkdir(parents=True, exist_ok=False)
    run_id = workspace.name
    events = source_events(manifest, fixture_dir, config, run_id)
    require(len(events) > 2, "Recovery proof needs at least three events")
    history = process_branch(workspace, "history", events)
    history_checkpoint = digest(workspace / "checkpoints/history/progress.json")
    partial = process_branch(workspace, "replay", events, stop_after=2)
    require(len(partial) == 2, "Replay interruption was not simulated")
    replay = process_branch(workspace, "replay", events)
    require(history_checkpoint == digest(workspace / "checkpoints/history/progress.json"),
            "Replay changed history checkpoint")
    require(len(history) == len(replay) == len(events), "Missing or duplicate output")
    compare = lambda rows: sorted((r["event_id"], r["event_time"], r["project"],
                                   r["page_title"], r["views"], r["content_size"]) for r in rows)
    require(compare(history) == compare(replay), "Branch results differ")
    # A new run preserves event IDs while receiving a different run identity.
    other = source_events(manifest, fixture_dir, config, run_id + "-second")
    require([r["event_id"] for r in events] == [r["event_id"] for r in other],
            "Event identity changed between runs")
    counts = {}
    with closing(sqlite3.connect(workspace / "serving.sqlite")) as connection:
        for branch, rows in (("history", history), ("replay", replay)):
            table = config["branches"][branch]["table"]
            load(connection, table, branch, rows)
            load(connection, table, branch, rows)
            count, total = connection.execute(f"SELECT COUNT(*), SUM(views) FROM {table}").fetchone()
            require(count == len(events), "Loader duplicated or lost events")
            require(total == sum(r["views"] for r in events), "Serving total differs")
            counts[table] = {"rows": count, "views": total}
    source_events(manifest, fixture_dir, config, run_id)  # Raw files are still intact.
    result = {
        "status": "PASS", "scope": "local architecture proof; no Spark/Kafka/ClickHouse deployment",
        "snapshot_id": manifest["snapshot_id"], "run_id": run_id,
        "contract_status": config["contract_status"],
        "config_sha256": digest(config_path), "manifest_sha256": digest(fixture_dir / "manifest.json"),
        "python": sys.version.split()[0], "machine": sys.platform,
        "checks": ["raw checksums", "isolated namespaces", "same events and values",
                   "interrupted replay resume", "history checkpoint unchanged",
                   "stable event IDs across runs", "idempotent loaders", "raw unchanged"],
        "tables": counts,
    }
    write_json(workspace / "result.json", result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True, help="New local evidence directory")
    parser.add_argument("--config", type=Path, default=ROOT / "config/architecture.json")
    parser.add_argument("--fixture", type=Path, default=ROOT / "tests/fixtures/pb005")
    args = parser.parse_args()
    try:
        print(json.dumps(verify(args.workspace, args.config, args.fixture), ensure_ascii=False, indent=2))
    except (ValueError, OSError, KeyError, sqlite3.Error) as error:
        parser.exit(1, f"FAIL: {error}\n")
