#!/usr/bin/env python3
"""Archive PostgreSQL request_logs by Singapore calendar day, then delete safely."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import tarfile
import time
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Any, BinaryIO
from zoneinfo import ZoneInfo

import psycopg
from psycopg import sql


ARCHIVE_VERSION = 2
SUPPORTED_ARCHIVE_VERSIONS = {1, ARCHIVE_VERSION}
DEFAULT_ARCHIVE_DIR = "/data/request-log-archives"
DEFAULT_TIMEZONE = "Asia/Singapore"
DEFAULT_RETENTION_DAYS = 15
TABLE_NAME = "request_logs"
PROGRESS_BYTES = 1024 * 1024 * 1024
XZ_COMMAND = (
    "xz",
    "-T2",
    "--lzma2=preset=9e,dict=1536MiB,lc=4,lp=0,pb=0",
    "-c",
)


def log(message: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] {message}", flush=True)


def database_url() -> str:
    value = (os.getenv("DATABASE_URL") or "").strip()
    if not value:
        raise RuntimeError("DATABASE_URL is required")
    if value.startswith("jdbc:postgresql://"):
        value = "postgresql://" + value[len("jdbc:postgresql://") :]
    return value


def connect(*, autocommit: bool = False) -> psycopg.Connection[Any]:
    return psycopg.connect(
        database_url(),
        connect_timeout=15,
        autocommit=autocommit,
        application_name="ai-gateway-request-log-archive",
    )


def local_day_bounds(day: date, zone: ZoneInfo) -> tuple[str, str]:
    start = datetime.combine(day, datetime_time.min, tzinfo=zone).astimezone(timezone.utc)
    end = datetime.combine(day + timedelta(days=1), datetime_time.min, tzinfo=zone).astimezone(timezone.utc)
    return start.isoformat(timespec="microseconds"), end.isoformat(timespec="microseconds")


def ensure_created_at_index() -> None:
    log("ensuring request_logs(created_at) index")
    with connect(autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_request_logs_created_at "
                "ON request_logs(created_at)"
            )


def table_columns() -> list[dict[str, Any]]:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type, udt_name, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                ORDER BY ordinal_position
                """,
                (TABLE_NAME,),
            )
            return [
                {
                    "name": row[0],
                    "data_type": row[1],
                    "udt_name": row[2],
                    "nullable": row[3] == "YES",
                    "default": row[4],
                }
                for row in cur.fetchall()
            ]


def day_stats(start: str, end: str) -> dict[str, int | None]:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*), min(id), max(id),
                       COALESCE(sum(octet_length(request_body) + octet_length(response_body)), 0)
                FROM request_logs
                WHERE created_at >= %s AND created_at < %s
                """,
                (start, end),
            )
            row = cur.fetchone()
    return {
        "row_count": int(row[0]),
        "min_id": int(row[1]) if row[1] is not None else None,
        "max_id": int(row[2]) if row[2] is not None else None,
        "payload_bytes": int(row[3]),
    }


class HashingWriter:
    def __init__(self, target: BinaryIO) -> None:
        self.target = target
        self.sha256 = hashlib.sha256()
        self.bytes_written = 0
        self.next_progress = PROGRESS_BYTES

    def write(self, data: bytes | memoryview) -> int:
        self.target.write(data)
        self.sha256.update(data)
        self.bytes_written += len(data)
        if self.bytes_written >= self.next_progress:
            log(f"exported {self.bytes_written / 1024 / 1024 / 1024:.1f} GiB")
            self.next_progress += PROGRESS_BYTES
        return len(data)


class ProgressReader:
    def __init__(self, source: BinaryIO, action: str) -> None:
        self.source = source
        self.action = action
        self.bytes_read = 0
        self.next_progress = PROGRESS_BYTES

    def read(self, size: int = -1) -> bytes:
        data = self.source.read(size)
        self.bytes_read += len(data)
        if self.bytes_read >= self.next_progress:
            log(f"{self.action} {self.bytes_read / 1024 / 1024 / 1024:.1f} GiB")
            self.next_progress += PROGRESS_BYTES
        return data


def export_binary_copy(
    copy_path: Path,
    columns: list[dict[str, Any]],
    start: str,
    end: str,
) -> tuple[int, str]:
    column_sql = sql.SQL(", ").join(sql.Identifier(str(item["name"])) for item in columns)
    copy_sql = sql.SQL(
        "COPY (SELECT {columns} FROM {table} "
        "WHERE created_at >= {start} AND created_at < {end} ORDER BY id) "
        "TO STDOUT WITH (FORMAT BINARY)"
    ).format(
        columns=column_sql,
        table=sql.Identifier(TABLE_NAME),
        start=sql.Literal(start),
        end=sql.Literal(end),
    )
    with connect() as conn:
        with conn.cursor() as cur, copy_path.open("wb") as target:
            writer = HashingWriter(target)
            with cur.copy(copy_sql) as copy:
                for chunk in copy:
                    writer.write(chunk)
            target.flush()
            os.fsync(target.fileno())
    return writer.bytes_written, writer.sha256.hexdigest()


def restore_readme(day: date, columns: list[dict[str, Any]]) -> str:
    names = ", ".join(str(item["name"]) for item in columns)
    copy_name = f"request_logs_{day.isoformat()}.copy"
    return f"""AI Gateway request_logs archive for {day.isoformat()} ({DEFAULT_TIMEZONE})

Format: PostgreSQL binary COPY.

Restore into a compatible request_logs table after checking for ID conflicts:
  ARCHIVE_DATABASE_URL="${{DATABASE_URL#jdbc:}}"
  tar -xJf request_logs_{day.isoformat()}.tar.xz
  psql "$ARCHIVE_DATABASE_URL" -c "\\copy request_logs ({names}) FROM '{copy_name}' WITH (FORMAT binary)"
  psql "$ARCHIVE_DATABASE_URL" -c "SELECT setval(pg_get_serial_sequence('request_logs','id'), COALESCE(MAX(id), 1), true) FROM request_logs"

Verify request_logs_{day.isoformat()}.manifest.json before restoring.
"""


def build_archive(
    day: date,
    stage_dir: Path,
    final_path: Path,
    manifest: dict[str, Any],
) -> None:
    copy_name = str(manifest["copy_file"])
    copy_path = stage_dir / copy_name
    manifest_name = f"request_logs_{day.isoformat()}.manifest.json"
    readme_name = f"request_logs_{day.isoformat()}.README.txt"
    manifest_path = stage_dir / manifest_name
    readme_path = stage_dir / readme_name
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    readme_path.write_text(restore_readme(day, manifest["columns"]), encoding="utf-8")

    partial_path = final_path.with_suffix(final_path.suffix + ".partial")
    partial_path.unlink(missing_ok=True)
    log(f"compressing {copy_path.name} into {final_path.name}: {' '.join(XZ_COMMAND)}")
    with partial_path.open("wb") as compressed:
        process = subprocess.Popen(
            XZ_COMMAND,
            stdin=subprocess.PIPE,
            stdout=compressed,
            stderr=subprocess.PIPE,
        )
        if process.stdin is None or process.stderr is None:
            process.kill()
            process.wait()
            raise RuntimeError("failed to open xz pipeline")
        try:
            with tarfile.open(fileobj=process.stdin, mode="w|") as archive:
                archive.add(manifest_path, arcname=manifest_name, recursive=False)
                archive.add(readme_path, arcname=readme_name, recursive=False)
                info = archive.gettarinfo(str(copy_path), arcname=copy_name)
                with copy_path.open("rb") as source:
                    archive.addfile(info, ProgressReader(source, "compressed"))
            process.stdin.close()
            stderr = process.stderr.read().decode("utf-8", errors="replace").strip()
            return_code = process.wait()
        except Exception as exc:
            if not process.stdin.closed:
                process.stdin.close()
            if process.poll() is None:
                process.kill()
            stderr = process.stderr.read().decode("utf-8", errors="replace").strip()
            return_code = process.wait()
            partial_path.unlink(missing_ok=True)
            if return_code != 0:
                raise RuntimeError(f"xz failed with exit code {return_code}: {stderr}") from exc
            raise
        if return_code != 0:
            partial_path.unlink(missing_ok=True)
            raise RuntimeError(f"xz failed with exit code {return_code}: {stderr}")
        compressed.flush()
        os.fsync(compressed.fileno())
    os.chmod(partial_path, 0o640)
    os.replace(partial_path, final_path)
    directory_fd = os.open(final_path.parent, os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def verify_archive(path: Path, *, full: bool) -> dict[str, Any]:
    manifest: dict[str, Any] | None = None
    copy_sha256 = hashlib.sha256()
    copy_bytes = 0
    next_progress = PROGRESS_BYTES
    if path.name.endswith(".tar.xz"):
        archive_mode = "r|xz"
    elif path.name.endswith(".tar.gz"):
        archive_mode = "r|gz"
    else:
        raise RuntimeError(f"unsupported archive extension: {path}")
    with tarfile.open(path, archive_mode) as archive:
        for member in archive:
            source = archive.extractfile(member)
            if source is None:
                continue
            if member.name.endswith(".manifest.json"):
                manifest = json.loads(source.read().decode("utf-8"))
                if not full:
                    break
            elif full and member.name.endswith(".copy"):
                while True:
                    chunk = source.read(1024 * 1024)
                    if not chunk:
                        break
                    copy_sha256.update(chunk)
                    copy_bytes += len(chunk)
                    if copy_bytes >= next_progress:
                        log(f"verified {copy_bytes / 1024 / 1024 / 1024:.1f} GiB")
                        next_progress += PROGRESS_BYTES
    if manifest is None:
        raise RuntimeError(f"manifest missing from {path}")
    if int(manifest.get("archive_version", 0)) not in SUPPORTED_ARCHIVE_VERSIONS:
        raise RuntimeError(f"unsupported archive version in {path}")
    if full:
        if copy_bytes != int(manifest["copy_bytes"]):
            raise RuntimeError(f"copy size mismatch in {path}: {copy_bytes} != {manifest['copy_bytes']}")
        if copy_sha256.hexdigest() != manifest["copy_sha256"]:
            raise RuntimeError(f"copy checksum mismatch in {path}")
    return manifest


def delete_archived_rows(start: str, end: str, expected_count: int) -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM request_logs WHERE created_at >= %s AND created_at < %s",
                (start, end),
            )
            deleted = int(cur.rowcount)
            if deleted != expected_count:
                conn.rollback()
                raise RuntimeError(f"delete count mismatch: {deleted} != {expected_count}")
        conn.commit()
    log(f"deleted {expected_count} archived rows")


def check_disk_space(archive_dir: Path, estimated_payload_bytes: int) -> None:
    free = shutil.disk_usage(archive_dir).free
    required = estimated_payload_bytes * 2 + 5 * 1024 * 1024 * 1024
    if free < required:
        raise RuntimeError(
            f"insufficient archive disk space: free={free}, required={required}"
        )


def archive_day(
    day: date,
    zone: ZoneInfo,
    archive_dir: Path,
    columns: list[dict[str, Any]],
) -> None:
    start, end = local_day_bounds(day, zone)
    stats = day_stats(start, end)
    row_count = int(stats["row_count"] or 0)
    final_path = archive_dir / f"request_logs_{day.isoformat()}.tar.xz"
    legacy_path = archive_dir / f"request_logs_{day.isoformat()}.tar.gz"
    log(
        f"day={day.isoformat()} rows={row_count} "
        f"payload={int(stats['payload_bytes'] or 0) / 1024 / 1024 / 1024:.2f} GiB"
    )

    existing_path = final_path if final_path.exists() else legacy_path if legacy_path.exists() else None
    if existing_path is not None:
        manifest = verify_archive(existing_path, full=row_count > 0)
        if str(manifest.get("local_day")) != day.isoformat():
            raise RuntimeError(f"archive day mismatch in {existing_path}")
        archived_count = int(manifest["row_count"])
        if row_count == 0:
            log(f"archive already complete: {existing_path}")
            return
        if row_count != archived_count:
            raise RuntimeError(
                f"existing archive row count differs from database: {archived_count} != {row_count}"
            )
        delete_archived_rows(start, end, archived_count)
        return

    if row_count == 0:
        log("no rows; skipping empty archive")
        return

    check_disk_space(archive_dir, int(stats["payload_bytes"] or 0))
    stage_dir = archive_dir / ".staging" / day.isoformat()
    if stage_dir.exists():
        shutil.rmtree(stage_dir)
    stage_dir.mkdir(parents=True, mode=0o700)
    copy_name = f"request_logs_{day.isoformat()}.copy"
    copy_path = stage_dir / copy_name
    started_at = time.monotonic()
    try:
        copy_bytes, copy_sha256 = export_binary_copy(copy_path, columns, start, end)
        manifest = {
            "archive_version": ARCHIVE_VERSION,
            "format": "postgresql-binary-copy",
            "table": TABLE_NAME,
            "local_day": day.isoformat(),
            "timezone": str(zone),
            "utc_start": start,
            "utc_end": end,
            "row_count": row_count,
            "min_id": stats["min_id"],
            "max_id": stats["max_id"],
            "payload_bytes": stats["payload_bytes"],
            "copy_file": copy_name,
            "copy_bytes": copy_bytes,
            "copy_sha256": copy_sha256,
            "compression": {
                "format": "xz",
                "command": list(XZ_COMMAND),
            },
            "columns": columns,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        build_archive(day, stage_dir, final_path, manifest)
        try:
            verify_archive(final_path, full=True)
        except Exception:
            final_path.unlink(missing_ok=True)
            raise
        current_stats = day_stats(start, end)
        if int(current_stats["row_count"] or 0) != row_count:
            raise RuntimeError("database rows changed while archive was being created")
        delete_archived_rows(start, end, row_count)
        log(
            f"completed {final_path.name} size={final_path.stat().st_size / 1024 / 1024 / 1024:.2f} GiB "
            f"elapsed={time.monotonic() - started_at:.1f}s"
        )
    finally:
        if stage_dir.exists():
            shutil.rmtree(stage_dir)


def min_available_day(cutoff: date, zone: ZoneInfo) -> date | None:
    _, cutoff_utc = local_day_bounds(cutoff - timedelta(days=1), zone)
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT min(created_at) FROM request_logs WHERE created_at < %s",
                (cutoff_utc,),
            )
            value = cur.fetchone()[0]
    if not value:
        return None
    return datetime.fromisoformat(str(value)).astimezone(zone).date()


def run_vacuum() -> None:
    log("running VACUUM (ANALYZE) request_logs")
    with connect(autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("VACUUM (ANALYZE) request_logs")
    log("VACUUM (ANALYZE) completed")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--day", help="archive one local calendar day (YYYY-MM-DD)")
    target.add_argument("--day-offset", type=int, help="archive N local calendar days ago")
    target.add_argument(
        "--before-retention",
        action="store_true",
        help="archive every day older than the retention window",
    )
    parser.add_argument("--archive-dir", default=DEFAULT_ARCHIVE_DIR)
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    parser.add_argument("--retention-days", type=int, default=DEFAULT_RETENTION_DAYS)
    parser.add_argument("--vacuum-after", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if shutil.which(XZ_COMMAND[0]) is None:
        raise RuntimeError("xz executable is required")
    zone = ZoneInfo(args.timezone)
    today = datetime.now(zone).date()
    archive_dir = Path(args.archive_dir).resolve()
    archive_dir.mkdir(parents=True, mode=0o750, exist_ok=True)
    lock_path = archive_dir / ".archive.lock"
    with lock_path.open("a+") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        ensure_created_at_index()
        columns = table_columns()
        if not columns:
            raise RuntimeError("request_logs table has no columns")

        if args.day:
            days = [date.fromisoformat(args.day)]
        elif args.day_offset is not None:
            if args.day_offset < 1:
                raise RuntimeError("--day-offset must be at least 1")
            days = [today - timedelta(days=args.day_offset)]
        else:
            if args.retention_days < 1:
                raise RuntimeError("--retention-days must be at least 1")
            retained_start = today - timedelta(days=args.retention_days)
            first_day = min_available_day(retained_start, zone)
            days = []
            while first_day is not None and first_day < retained_start:
                days.append(first_day)
                first_day += timedelta(days=1)

        log(f"archive targets: {len(days)} day(s)")
        for day in days:
            archive_day(day, zone, archive_dir, columns)
        if args.vacuum_after:
            run_vacuum()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        log(f"ERROR: {exc}")
        raise
