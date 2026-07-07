import asyncio
import base64
import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import AsyncIterator
from urllib.parse import urlsplit

import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, StreamingResponse


DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/ai_gateway.sqlite3")
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "600"))
MAX_CAPTURE_BYTES = int(os.getenv("MAX_CAPTURE_BYTES", "0"))

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "host",
    "date",
    "server",
}

app = FastAPI(title="AI Gateway", docs_url=None, redoc_url=None)


class LogSocketManager:
    def __init__(self) -> None:
        self.connections: set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.connections.add(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        self.connections.discard(websocket)

    async def broadcast(self, payload: dict) -> None:
        dead_connections = []
        for websocket in list(self.connections):
            try:
                await websocket.send_json(payload)
            except Exception:
                dead_connections.append(websocket)
        for websocket in dead_connections:
            self.disconnect(websocket)


log_socket_manager = LogSocketManager()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_db() -> None:
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS request_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                method TEXT NOT NULL,
                target_url TEXT NOT NULL,
                client_host TEXT,
                request_headers TEXT NOT NULL,
                request_body BLOB NOT NULL,
                request_body_truncated INTEGER NOT NULL DEFAULT 0,
                response_status INTEGER,
                response_headers TEXT,
                response_body BLOB NOT NULL DEFAULT X'',
                response_body_truncated INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                duration_ms INTEGER,
                upstream_duration_ms INTEGER,
                first_byte_ms INTEGER,
                output_tokens INTEGER
            )
            """
        )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(request_logs)").fetchall()}
        if "upstream_duration_ms" not in columns:
            conn.execute("ALTER TABLE request_logs ADD COLUMN upstream_duration_ms INTEGER")
        if "first_byte_ms" not in columns:
            conn.execute("ALTER TABLE request_logs ADD COLUMN first_byte_ms INTEGER")
        if "output_tokens" not in columns:
            conn.execute("ALTER TABLE request_logs ADD COLUMN output_tokens INTEGER")


@app.on_event("startup")
async def startup() -> None:
    ensure_db()


def db_execute(sql: str, params: tuple = ()) -> sqlite3.Cursor:
    ensure_db()
    conn = sqlite3.connect(DATABASE_PATH)
    try:
        conn.execute("PRAGMA busy_timeout=5000")
        cur = conn.execute(sql, params)
        conn.commit()
        return cur
    finally:
        conn.close()


def create_log(
    method: str,
    target_url: str,
    client_host: str | None,
    request_headers: dict[str, str],
    request_body: bytes,
    request_body_truncated: bool,
) -> int:
    cur = db_execute(
        """
        INSERT INTO request_logs (
            created_at, method, target_url, client_host, request_headers,
            request_body, request_body_truncated
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            utc_now(),
            method,
            target_url,
            client_host,
            json.dumps(request_headers, ensure_ascii=False, indent=2),
            request_body,
            int(request_body_truncated),
        ),
    )
    return int(cur.lastrowid)


def finish_log(
    log_id: int,
    status_code: int | None,
    response_headers: dict[str, str] | None,
    response_body: bytes | bytearray,
    response_body_truncated: bool,
    started_at: float,
    upstream_started_at: float | None = None,
    finished_at: float | None = None,
    first_byte_at: float | None = None,
    error: str | None = None,
) -> None:
    finished_at = finished_at or time.perf_counter()
    upstream_duration_ms = None
    if upstream_started_at is not None:
        upstream_duration_ms = int((finished_at - upstream_started_at) * 1000)
    first_byte_ms = None
    if first_byte_at is not None:
        first_byte_ms = int((first_byte_at - started_at) * 1000)
    response_bytes = bytes(response_body)
    db_execute(
        """
        UPDATE request_logs
        SET response_status = ?,
            response_headers = ?,
            response_body = ?,
            response_body_truncated = ?,
            error = ?,
            duration_ms = ?,
            upstream_duration_ms = ?,
            first_byte_ms = ?,
            output_tokens = ?
        WHERE id = ?
        """,
        (
            status_code,
            json.dumps(response_headers or {}, ensure_ascii=False, indent=2),
            response_bytes,
            int(response_body_truncated),
            error,
            int((finished_at - started_at) * 1000),
            upstream_duration_ms,
            first_byte_ms,
            output_tokens_from_body(response_bytes),
            log_id,
        ),
    )


def capture_bytes(data: bytes) -> tuple[bytes, bool]:
    if MAX_CAPTURE_BYTES <= 0 or len(data) <= MAX_CAPTURE_BYTES:
        return data, False
    return data[:MAX_CAPTURE_BYTES], True


def parse_json_bytes(body: bytes) -> object | None:
    if not body:
        return None
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def parse_sse_events(text: str) -> list[dict[str, str]]:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    events: list[dict[str, str]] = []
    for block in text.split("\n\n"):
        event_name = ""
        data_lines = []
        for raw_line in block.splitlines():
            line = raw_line.rstrip()
            if line.startswith("event:"):
                event_name = line[6:].strip()
            elif line.startswith("data:"):
                data_lines.append(line[5:].lstrip())
        if data_lines:
            events.append({"event": event_name or "message", "data": "\n".join(data_lines)})
    return events


def parse_completed_response_from_sse(body: bytes) -> object | None:
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return None
    for item in reversed(parse_sse_events(text)):
        if item["event"] == "response.completed":
            try:
                return json.loads(item["data"])
            except json.JSONDecodeError:
                return None
    return None


def find_reasoning_tokens(payload: object) -> int | None:
    if isinstance(payload, dict):
        for path in (
            ("usage", "output_tokens_details", "reasoning_tokens"),
            ("usage", "completion_tokens_details", "reasoning_tokens"),
            ("response", "usage", "output_tokens_details", "reasoning_tokens"),
            ("response", "usage", "completion_tokens_details", "reasoning_tokens"),
        ):
            current: object = payload
            for key in path:
                if not isinstance(current, dict) or key not in current:
                    break
                current = current[key]
            else:
                if isinstance(current, int):
                    return current
                if isinstance(current, str) and current.isdigit():
                    return int(current)

        for value in payload.values():
            found = find_reasoning_tokens(value)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = find_reasoning_tokens(item)
            if found is not None:
                return found
    return None


def find_output_tokens(payload: object) -> int | None:
    if isinstance(payload, dict):
        for path in (
            ("usage", "output_tokens"),
            ("usage", "completion_tokens"),
            ("usage", "output_token_count"),
            ("response", "usage", "output_tokens"),
            ("response", "usage", "completion_tokens"),
            ("message", "usage", "output_tokens"),
        ):
            current: object = payload
            for key in path:
                if not isinstance(current, dict) or key not in current:
                    break
                current = current[key]
            else:
                if isinstance(current, int):
                    return current
                if isinstance(current, str) and current.isdigit():
                    return int(current)

        for value in payload.values():
            found = find_output_tokens(value)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = find_output_tokens(item)
            if found is not None:
                return found
    return None


def reasoning_tokens_from_body(body: bytes) -> int | None:
    direct = find_reasoning_tokens(parse_json_bytes(body))
    if direct is not None:
        return direct
    completed = find_reasoning_tokens(parse_completed_response_from_sse(body))
    if completed is not None:
        return completed
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return None
    for item in reversed(parse_sse_events(text)):
        if item["data"] == "[DONE]":
            continue
        try:
            parsed = json.loads(item["data"])
        except json.JSONDecodeError:
            continue
        found = find_reasoning_tokens(parsed)
        if found is not None:
            return found
    return None


def output_tokens_from_body(body: bytes) -> int | None:
    direct = find_output_tokens(parse_json_bytes(body))
    if direct is not None:
        return direct
    completed = find_output_tokens(parse_completed_response_from_sse(body))
    if completed is not None:
        return completed
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return None
    for item in reversed(parse_sse_events(text)):
        if item["data"] == "[DONE]":
            continue
        try:
            parsed = json.loads(item["data"])
        except json.JSONDecodeError:
            continue
        found = find_output_tokens(parsed)
        if found is not None:
            return found
    return None


def api_type_from_payload(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None

    payload_type = str(payload.get("type") or "").lower()
    object_type = str(payload.get("object") or "").lower()
    if payload_type.startswith("response") or payload.get("response"):
        return "responses"
    if object_type.startswith("chat.completion") or isinstance(payload.get("choices"), list):
        return "chat_completions"
    if payload_type.startswith("message") or object_type.startswith("message") or payload.get("message"):
        return "messages"
    return None


def api_type_from_body(body: bytes) -> str | None:
    direct = api_type_from_payload(parse_json_bytes(body))
    if direct:
        return direct
    completed = api_type_from_payload(parse_completed_response_from_sse(body))
    if completed:
        return completed

    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return None
    events = parse_sse_events(text)
    if any(item["event"] == "response.completed" for item in events):
        return "responses"

    parsed_events = []
    for item in events:
        if item["data"] == "[DONE]":
            continue
        try:
            parsed_events.append(json.loads(item["data"]))
        except json.JSONDecodeError:
            pass
    for payload in parsed_events:
        detected = api_type_from_payload(payload)
        if detected:
            return detected
    if any(item["event"].startswith("message_") or item["event"].startswith("content_block_") for item in events):
        return "messages"
    return None


def api_type_from_log(target_url: str, request_body: bytes, response_body: bytes) -> str:
    normalized_url = target_url.lower()
    if "/chat/completions" in normalized_url:
        return "chat_completions"
    if "/responses" in normalized_url:
        return "responses"
    if "/messages" in normalized_url:
        return "messages"
    return api_type_from_body(response_body) or api_type_from_body(request_body) or "other"


def append_capture(existing: bytearray, chunk: bytes) -> bool:
    if MAX_CAPTURE_BYTES <= 0:
        existing.extend(chunk)
        return False
    remaining = MAX_CAPTURE_BYTES - len(existing)
    if remaining > 0:
        existing.extend(chunk[:remaining])
    return len(chunk) > remaining


def filtered_request_headers(request: Request) -> dict[str, str]:
    return {
        key: value
        for key, value in request.headers.items()
        if key.lower() not in HOP_BY_HOP_HEADERS
    }


def filtered_response_headers(headers: httpx.Headers) -> dict[str, str]:
    return {
        key: value
        for key, value in headers.items()
        if key.lower() not in HOP_BY_HOP_HEADERS
    }


def validate_target_url(target_url: str) -> str | None:
    parsed = urlsplit(target_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return "Target URL must start with http:// or https://"
    return None


def tps_from_values(output_tokens: int | None, duration_ms: int | None, first_byte_ms: int | None) -> float | None:
    if not output_tokens or duration_ms is None:
        return None
    generation_ms = duration_ms
    if first_byte_ms is not None and duration_ms > first_byte_ms:
        generation_ms = duration_ms - first_byte_ms
    if generation_ms <= 0:
        return None
    return round(output_tokens / (generation_ms / 1000), 2)


def row_to_summary(row: sqlite3.Row) -> dict:
    output_tokens = row["output_tokens"] if row["output_tokens"] is not None else output_tokens_from_body(row["response_body"] or b"")
    return {
        "id": row["id"],
        "created_at": row["created_at"],
        "method": row["method"],
        "target_url": row["target_url"],
        "client_host": row["client_host"],
        "response_status": row["response_status"],
        "duration_ms": row["duration_ms"],
        "upstream_duration_ms": row["upstream_duration_ms"],
        "first_byte_ms": row["first_byte_ms"],
        "output_tokens": output_tokens,
        "tps": tps_from_values(output_tokens, row["duration_ms"], row["first_byte_ms"]),
        "gateway_overhead_ms": (
            row["duration_ms"] - row["upstream_duration_ms"]
            if row["duration_ms"] is not None and row["upstream_duration_ms"] is not None
            else None
        ),
        "error": row["error"],
        "request_body_bytes": len(row["request_body"] or b""),
        "response_body_bytes": len(row["response_body"] or b""),
        "reasoning_tokens": reasoning_tokens_from_body(row["response_body"] or b""),
        "api_type": api_type_from_log(
            row["target_url"] or "",
            row["request_body"] or b"",
            row["response_body"] or b"",
        ),
        "request_body_truncated": bool(row["request_body_truncated"]),
        "response_body_truncated": bool(row["response_body_truncated"]),
    }


def list_log_summaries(limit: int = 100) -> list[dict]:
    limit = max(1, min(limit, 500))
    ensure_db()
    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, created_at, method, target_url, client_host, response_status,
                   duration_ms, upstream_duration_ms, first_byte_ms, output_tokens, error, request_body, response_body,
                   request_body_truncated, response_body_truncated
            FROM request_logs
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [row_to_summary(row) for row in rows]


async def broadcast_logs(changed_id: int | None = None) -> None:
    rows = await asyncio.to_thread(list_log_summaries, 50)
    await log_socket_manager.broadcast(
        {
            "type": "logs",
            "changed_id": changed_id,
            "rows": rows,
        }
    )


async def announce_created(log_id_task: asyncio.Task[int]) -> None:
    try:
        log_id = await log_id_task
        await broadcast_logs(log_id)
    except Exception as exc:
        print(f"Failed to announce log creation: {exc}", flush=True)


async def finish_log_async(log_id_task: asyncio.Task[int], *args) -> None:
    try:
        log_id = await log_id_task
        await asyncio.to_thread(finish_log, log_id, *args)
        await broadcast_logs(log_id)
    except Exception as exc:
        print(f"Failed to finish log: {exc}", flush=True)


def body_payload(body: bytes) -> dict:
    try:
        text = body.decode("utf-8")
        return {"encoding": "utf-8", "text": text}
    except UnicodeDecodeError:
        return {
            "encoding": "base64",
            "text": base64.b64encode(body).decode("ascii"),
        }


@app.get("/", response_class=HTMLResponse)
async def dashboard() -> str:
    return """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="theme-color" content="#f5f7fb" />
  <title>AI Gateway</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #080b10;
      --panel: #10151f;
      --panel-soft: #151b27;
      --panel-raised: #192231;
      --line: #253044;
      --line-strong: #3d4b63;
      --text: #f4f7fb;
      --muted: #97a4b8;
      --muted-strong: #c7d1df;
      --accent: #27d17f;
      --accent-2: #35b7ff;
      --accent-soft: rgba(39, 209, 127, .12);
      --warn: #f5b84b;
      --good: #33d17a;
      --bad: #ff5c73;
      --code-bg: #05070b;
      --code-text: #dce7f5;
      --shadow: 0 20px 50px rgba(0, 0, 0, .28);
      --radius: 10px;
    }
    * { box-sizing: border-box; }
    html {
      height: 100%;
      background: var(--bg);
      overflow: hidden;
    }
    body {
      margin: 0;
      height: 100%;
      background:
        radial-gradient(circle at 18% 0%, rgba(53, 183, 255, .12), transparent 32%),
        radial-gradient(circle at 78% 0%, rgba(39, 209, 127, .10), transparent 30%),
        var(--bg);
      color: var(--text);
      font: 14px/1.5 Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      overflow: hidden;
      -webkit-tap-highlight-color: rgba(39, 209, 127, .18);
    }
    a, button, input { touch-action: manipulation; }
    .skip-link {
      position: absolute;
      left: 12px;
      top: -44px;
      z-index: 5;
      background: var(--text);
      color: #fff;
      padding: 8px 10px;
      border-radius: 6px;
    }
    .skip-link:focus-visible { top: 10px; outline: 3px solid #7dd3fc; outline-offset: 2px; }
    .shell {
      height: 100dvh;
      display: grid;
      grid-template-rows: auto 1fr;
      overflow: hidden;
    }
    header.app-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 20px;
      padding: 14px max(20px, env(safe-area-inset-left)) 14px max(20px, env(safe-area-inset-right));
      border-bottom: 1px solid var(--line);
      background: rgba(16, 21, 31, .9);
      backdrop-filter: blur(14px);
      z-index: 3;
    }
    h1 {
      font-size: 18px;
      line-height: 1.15;
      margin: 0;
      font-weight: 760;
      text-wrap: balance;
    }
    .brand {
      display: flex;
      align-items: center;
      gap: 10px;
      min-width: 0;
    }
    .mark {
      width: 36px;
      height: 36px;
      border-radius: 9px;
      display: grid;
      place-items: center;
      background: linear-gradient(145deg, rgba(8, 13, 22, .98), rgba(5, 31, 22, .96));
      border: 1px solid rgba(39, 209, 127, .38);
      box-shadow: 0 0 24px rgba(39, 209, 127, .18), inset 0 1px 0 rgba(255, 255, 255, .08);
      flex: 0 0 auto;
      overflow: hidden;
    }
    .mark svg {
      width: 32px;
      height: 32px;
      display: block;
    }
    .subtitle {
      color: var(--muted);
      font-size: 12px;
      margin-top: 2px;
    }
    button {
      border: 1px solid var(--line);
      background: #111827;
      color: var(--text);
      min-height: 40px;
      padding: 0 14px;
      border-radius: 8px;
      cursor: pointer;
      font-weight: 600;
      font: inherit;
      transition: border-color .18s ease, background-color .18s ease, color .18s ease, box-shadow .18s ease;
    }
    button:hover {
      border-color: rgba(39, 209, 127, .48);
      color: var(--accent);
      background: var(--accent-soft);
      box-shadow: 0 0 0 3px rgba(39, 209, 127, .06);
    }
    button:focus-visible,
    input:focus-visible {
      outline: 3px solid rgba(14, 165, 233, .35);
      outline-offset: 2px;
      border-color: var(--accent);
    }
    .toolbar {
      display: flex;
      gap: 8px;
      align-items: center;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .status-dot {
      width: 9px;
      height: 9px;
      border-radius: 999px;
      background: var(--good);
      box-shadow: 0 0 0 3px rgba(21, 128, 61, .13);
    }
    .live-status {
      display: flex;
      align-items: center;
      gap: 8px;
      color: var(--muted);
      font-size: 12px;
      min-height: 40px;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 0 12px;
      background: rgba(5, 7, 11, .34);
    }
    main {
      display: grid;
      grid-template-columns: minmax(380px, 34%) minmax(0, 1fr);
      height: 100%;
      min-height: 0;
      overflow: hidden;
    }
    .sidebar {
      border-right: 1px solid var(--line);
      background: rgba(10, 14, 21, .78);
      min-width: 0;
      min-height: 0;
      display: grid;
      grid-template-rows: auto 1fr;
      overflow: hidden;
    }
    .filters {
      display: grid;
      gap: 12px;
      padding: 16px;
      border-bottom: 1px solid var(--line);
      background: rgba(16, 21, 31, .82);
    }
    label {
      display: grid;
      gap: 5px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
    }
    input[type="search"] {
      width: 100%;
      height: 44px;
      border: 1px solid var(--line);
      border-radius: 9px;
      color: var(--text);
      background: var(--code-bg);
      padding: 0 12px;
      font: inherit;
    }
    .type-filter {
      display: grid;
      gap: 6px;
    }
    .type-filter-title {
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
    }
    .type-filter-options {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 6px;
      padding: 4px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: var(--code-bg);
    }
    .type-filter-options button {
      min-height: 34px;
      height: 34px;
      padding: 0 8px;
      border: 0;
      border-radius: 8px;
      background: transparent;
      color: var(--muted);
      font-size: 12px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .type-filter-options button.active {
      color: var(--accent);
      background: var(--accent-soft);
      box-shadow: inset 0 0 0 1px rgba(39, 209, 127, .22);
    }
    .summary-strip {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 8px;
    }
    .metric {
      min-width: 0;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: linear-gradient(180deg, rgba(25, 34, 49, .98), rgba(16, 21, 31, .98));
      padding: 10px;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, .03);
    }
    .metric span {
      display: block;
      color: var(--muted);
      font-size: 11px;
      overflow-wrap: anywhere;
    }
    .metric strong {
      display: block;
      margin-top: 2px;
      font-size: 18px;
      font-variant-numeric: tabular-nums;
    }
    .list {
      overflow: auto;
      min-height: 0;
      padding: 8px;
    }
    .item {
      width: 100%;
      text-align: left;
      border: 1px solid transparent;
      border-radius: var(--radius);
      height: auto;
      min-height: 88px;
      padding: 12px;
      display: grid;
      gap: 7px;
      background: transparent;
      content-visibility: auto;
      contain-intrinsic-size: 86px;
    }
    .item + .item { margin-top: 6px; }
    .item:hover {
      background: rgba(25, 34, 49, .72);
      border-color: var(--line);
    }
    .item.active {
      background: linear-gradient(180deg, rgba(39, 209, 127, .13), rgba(53, 183, 255, .08));
      border-color: rgba(39, 209, 127, .42);
      box-shadow: inset 3px 0 0 var(--accent), 0 12px 28px rgba(0, 0, 0, .20);
    }
    .meta {
      display: flex;
      gap: 8px;
      align-items: center;
      color: var(--muted);
      font-size: 12px;
      min-width: 0;
      font-variant-numeric: tabular-nums;
    }
    .meta .grow { flex: 1; min-width: 0; }
    .method {
      color: var(--accent);
      font-weight: 800;
      min-width: 46px;
      letter-spacing: .02em;
    }
    .badge {
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 2px 7px;
      background: rgba(5, 7, 11, .62);
      font-weight: 700;
      font-size: 11px;
      font-variant-numeric: tabular-nums;
    }
    .status.ok { color: var(--good); border-color: rgba(21, 128, 61, .3); }
    .status.err { color: var(--bad); border-color: rgba(185, 28, 28, .3); }
    .status.warn { color: var(--warn); border-color: rgba(180, 83, 9, .3); }
    .badge.anomaly {
      color: var(--bad);
      border-color: rgba(255, 92, 115, .44);
      background: rgba(255, 92, 115, .10);
    }
    .badge.api-type {
      color: var(--accent-2);
      border-color: rgba(53, 183, 255, .34);
      background: rgba(53, 183, 255, .08);
    }
    .url {
      color: var(--text);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 12px;
      min-width: 0;
    }
    .detail {
      padding: 18px;
      overflow: auto;
      height: 100%;
      min-height: 0;
      min-width: 0;
    }
    .empty {
      min-height: 220px;
      display: grid;
      place-items: center;
      color: var(--muted);
      text-align: center;
      padding: 24px;
    }
    .detail-head {
      display: grid;
      gap: 13px;
      margin-bottom: 16px;
      padding: 14px;
      border: 1px solid rgba(61, 75, 99, .82);
      border-radius: 14px;
      background:
        linear-gradient(180deg, rgba(25, 34, 49, .92), rgba(12, 17, 26, .92)),
        var(--panel);
      box-shadow: 0 18px 44px rgba(0, 0, 0, .26), inset 0 1px 0 rgba(255, 255, 255, .04);
    }
    .detail-topline {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      align-items: start;
      gap: 12px;
    }
    .endpoint-block {
      min-width: 0;
      display: grid;
      gap: 8px;
    }
    .endpoint-badges {
      display: flex;
      align-items: center;
      gap: 6px;
      flex-wrap: wrap;
    }
    .endpoint-url {
      min-width: 0;
      color: var(--text);
      font: 13px/1.45 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      overflow-wrap: anywhere;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      min-height: 24px;
      max-width: 100%;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 0 9px;
      background: rgba(5, 7, 11, .46);
      color: var(--muted-strong);
      font-size: 11px;
      font-weight: 800;
      line-height: 1;
      font-variant-numeric: tabular-nums;
      white-space: nowrap;
    }
    .pill.method-pill {
      color: var(--accent);
      border-color: rgba(39, 209, 127, .32);
      background: rgba(39, 209, 127, .08);
    }
    .pill.type-pill {
      color: var(--accent-2);
      border-color: rgba(53, 183, 255, .34);
      background: rgba(53, 183, 255, .08);
    }
    .pill.status-pill.ok {
      color: var(--good);
      border-color: rgba(51, 209, 122, .34);
      background: rgba(51, 209, 122, .08);
    }
    .pill.status-pill.err {
      color: var(--bad);
      border-color: rgba(255, 92, 115, .38);
      background: rgba(255, 92, 115, .10);
    }
    .pill.status-pill.warn {
      color: var(--warn);
      border-color: rgba(245, 184, 75, .36);
      background: rgba(245, 184, 75, .09);
    }
    .metric-grid {
      display: grid;
      grid-template-columns: repeat(6, minmax(108px, 1fr));
      gap: 8px;
    }
    .metric-card {
      min-width: 0;
      border: 1px solid rgba(61, 75, 99, .74);
      border-radius: 10px;
      background: rgba(5, 7, 11, .38);
      padding: 10px;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, .035);
    }
    .metric-card.primary {
      border-color: rgba(53, 183, 255, .30);
      background: linear-gradient(180deg, rgba(53, 183, 255, .105), rgba(5, 7, 11, .36));
    }
    .metric-card.good {
      border-color: rgba(51, 209, 122, .28);
      background: linear-gradient(180deg, rgba(51, 209, 122, .09), rgba(5, 7, 11, .35));
    }
    .metric-card.warn {
      border-color: rgba(245, 184, 75, .40);
      background: linear-gradient(180deg, rgba(245, 184, 75, .12), rgba(5, 7, 11, .35));
    }
    .metric-card.danger {
      border-color: rgba(255, 92, 115, .42);
      background: linear-gradient(180deg, rgba(255, 92, 115, .13), rgba(5, 7, 11, .35));
    }
    .metric-label {
      color: var(--muted);
      font-size: 11px;
      font-weight: 700;
      margin-bottom: 4px;
      overflow-wrap: anywhere;
    }
    .metric-value {
      color: var(--text);
      font-size: 16px;
      font-weight: 800;
      line-height: 1.2;
      font-variant-numeric: tabular-nums;
      overflow-wrap: anywhere;
    }
    .metric-card.primary .metric-value { color: #dff6ff; }
    .metric-card.good .metric-value { color: #d9fbe7; }
    .metric-card.warn .metric-value { color: #fff0c7; }
    .metric-card.danger .metric-value { color: #ffd9df; }
    .detail-meta {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 8px;
      padding-top: 1px;
    }
    .meta-chip {
      min-width: 0;
      border: 1px solid rgba(61, 75, 99, .62);
      border-radius: 10px;
      background: rgba(5, 7, 11, .28);
      padding: 8px 10px;
    }
    .meta-label {
      display: block;
      color: var(--muted);
      font-size: 11px;
      font-weight: 700;
      margin-bottom: 2px;
    }
    .meta-value {
      color: var(--muted-strong);
      font-size: 12px;
      font-variant-numeric: tabular-nums;
      overflow-wrap: anywhere;
    }
    h2 {
      font-size: 18px;
      margin: 0;
      line-height: 1.35;
      text-wrap: balance;
      overflow-wrap: anywhere;
    }
    h3 {
      font-size: 12px;
      margin: 0 0 8px;
      color: var(--muted);
      letter-spacing: .02em;
      text-transform: uppercase;
    }
    section {
      margin-bottom: 14px;
      scroll-margin-top: 84px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(16, 21, 31, .78);
      padding: 14px;
    }
    details.collapsible-section {
      margin-bottom: 14px;
      scroll-margin-top: 84px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(16, 21, 31, .78);
      padding: 0;
      overflow: hidden;
    }
    details.collapsible-section summary {
      display: block;
      cursor: pointer;
      padding: 14px;
      list-style: none;
    }
    details.collapsible-section summary::-webkit-details-marker { display: none; }
    details.collapsible-section summary .copy-row { margin-bottom: 0; }
    details.collapsible-section summary h3::before {
      content: "▶";
      display: inline-block;
      width: 14px;
      color: #93c5fd;
      font-size: 10px;
      margin-right: 6px;
    }
    details.collapsible-section[open] summary h3::before { content: "▼"; }
    .collapsible-content {
      padding: 0 14px 14px;
    }
    pre {
      margin: 0;
      padding: 12px;
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: var(--code-bg);
      color: var(--code-text);
      white-space: pre-wrap;
      word-break: break-word;
      font: 12px/1.5 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      max-height: 44vh;
    }
    .json-viewer {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: var(--code-bg);
      color: var(--code-text);
      padding: 10px 12px;
      overflow: auto;
      max-height: 52vh;
      font: 12px/1.55 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }
    .json-node,
    .json-leaf { margin: 2px 0; }
    .json-node summary {
      cursor: pointer;
      min-height: 24px;
      display: grid;
      grid-template-columns: 14px minmax(0, auto) auto 1fr;
      align-items: center;
      column-gap: 6px;
      border-radius: 4px;
      overflow-wrap: anywhere;
      list-style: none;
    }
    .json-node summary::-webkit-details-marker { display: none; }
    .json-node summary::before {
      content: "▶";
      color: #93c5fd;
      font-size: 10px;
      line-height: 1;
      transform-origin: center;
    }
    .json-node[open] > summary::before { content: "▼"; }
    .json-node summary:hover { background: rgba(219, 234, 254, .08); }
    .json-node summary:focus-visible {
      outline: 2px solid rgba(125, 211, 252, .7);
      outline-offset: 2px;
    }
    .json-children {
      margin-left: 7px;
      padding-left: 10px;
      border-left: 1px solid rgba(219, 234, 254, .22);
    }
    .json-key { color: #93c5fd; }
    .json-type { color: #a7f3d0; }
    .json-string { color: #fde68a; overflow-wrap: anywhere; }
    .json-number { color: #f9a8d4; }
    .json-boolean { color: #c4b5fd; }
    .json-null { color: #94a3b8; }
    .json-preview { color: #94a3b8; }
    .json-leaf {
      display: grid;
      grid-template-columns: 14px minmax(0, auto) minmax(0, 1fr);
      align-items: start;
      column-gap: 6px;
      min-height: 22px;
      overflow-wrap: anywhere;
    }
    .json-leaf::before {
      content: "";
      width: 14px;
    }
    .kv {
      border: 1px solid var(--line);
      border-radius: 10px;
      overflow: hidden;
      background: var(--code-bg);
    }
    .kv-row {
      display: grid;
      grid-template-columns: minmax(140px, 28%) minmax(0, 1fr);
      border-top: 1px solid var(--line);
    }
    .kv-row:first-child { border-top: 0; }
    .kv-key,
    .kv-value {
      padding: 9px 10px;
      min-width: 0;
      overflow-wrap: anywhere;
      font: 12px/1.45 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }
    .kv-key {
      background: var(--panel-soft);
      color: var(--muted);
      font-weight: 700;
      border-right: 1px solid var(--line);
    }
    .kv-value { color: var(--code-text); }
    .label { color: var(--muted); font-size: 12px; margin-bottom: 3px; }
    .tabs {
      display: flex;
      gap: 6px;
      border-bottom: 1px solid var(--line);
      margin: 2px 0 14px;
      overflow-x: auto;
      background: rgba(16, 21, 31, .72);
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 4px;
    }
    .tab {
      border: 0;
      border-radius: 8px;
      background: transparent;
      color: var(--muted);
      height: 38px;
      flex: 0 0 auto;
    }
    .tab:hover { background: rgba(255, 255, 255, .04); color: var(--text); }
    .tab.active {
      color: var(--accent);
      background: rgba(39, 209, 127, .12);
      box-shadow: inset 0 0 0 1px rgba(39, 209, 127, .22);
    }
    .copy-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
      margin-bottom: 8px;
    }
    .copy-row h3 { margin: 0; }
    .row-actions {
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 8px;
      flex-wrap: wrap;
    }
    .view-switch {
      display: inline-flex;
      align-items: center;
      gap: 2px;
      padding: 2px;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: var(--code-bg);
    }
    .view-switch button {
      height: 28px;
      padding: 0 9px;
      border: 0;
      background: transparent;
      color: var(--muted);
    }
    .view-switch button.active {
      background: var(--accent-soft);
      color: var(--accent);
      box-shadow: inset 0 0 0 1px rgba(15, 118, 110, .18);
    }
    .secondary { color: var(--muted); }
    [hidden] { display: none !important; }
    @media (prefers-reduced-motion: reduce) {
      *, *::before, *::after {
        animation-duration: .01ms !important;
        animation-iteration-count: 1 !important;
        scroll-behavior: auto !important;
      }
    }
    @media (max-width: 820px) {
      main { grid-template-columns: 1fr; }
      header.app-header { align-items: flex-start; flex-direction: column; }
      .toolbar { width: 100%; justify-content: space-between; }
      .sidebar { border-right: 0; border-bottom: 1px solid var(--line); }
      .list { max-height: 42vh; }
      .detail { height: 100%; }
      .detail-topline,
      .detail-meta { grid-template-columns: 1fr; }
      .metric-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .kv-row { grid-template-columns: 1fr; }
      .kv-key { border-right: 0; border-bottom: 1px solid var(--line); }
    }
    @media (min-width: 821px) and (max-width: 1180px) {
      .metric-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    }
  </style>
</head>
<body>
  <a class="skip-link" href="#detail">跳到请求详情</a>
  <div class="shell">
    <header class="app-header">
      <div class="brand">
        <div class="mark" aria-hidden="true">
          <svg viewBox="0 0 64 64" role="img">
            <defs>
              <linearGradient id="logoFlow" x1="12" y1="10" x2="52" y2="54" gradientUnits="userSpaceOnUse">
                <stop stop-color="#35B7FF"/>
                <stop offset=".48" stop-color="#27D17F"/>
                <stop offset="1" stop-color="#A7F3D0"/>
              </linearGradient>
              <radialGradient id="logoGlow" cx="32" cy="32" r="30" gradientUnits="userSpaceOnUse">
                <stop stop-color="#27D17F" stop-opacity=".28"/>
                <stop offset="1" stop-color="#27D17F" stop-opacity="0"/>
              </radialGradient>
            </defs>
            <rect width="64" height="64" rx="16" fill="#071017"/>
            <circle cx="32" cy="32" r="29" fill="url(#logoGlow)"/>
            <path d="M15 32c5.8-8.7 11.4-13 17-13s11.2 4.3 17 13c-5.8 8.7-11.4 13-17 13s-11.2-4.3-17-13Z" fill="none" stroke="url(#logoFlow)" stroke-width="4" stroke-linejoin="round"/>
            <path d="M22 42V25.5c0-2.5 3.2-3.6 4.8-1.7l15 18.2V22" fill="none" stroke="#F4F7FB" stroke-width="4" stroke-linecap="round" stroke-linejoin="round"/>
            <circle cx="15" cy="32" r="4" fill="#35B7FF"/>
            <circle cx="49" cy="32" r="4" fill="#27D17F"/>
            <circle cx="32" cy="19" r="3.5" fill="#A7F3D0"/>
          </svg>
        </div>
        <div>
          <h1 translate="no">AI Gateway</h1>
          <div class="subtitle">Realtime proxy inspector</div>
        </div>
      </div>
      <div class="toolbar">
        <div class="live-status" aria-live="polite"><span class="status-dot" aria-hidden="true"></span><span id="liveText">连接中…</span></div>
        <button id="refresh" type="button">刷新详情</button>
      </div>
    </header>
    <main>
      <aside class="sidebar" aria-label="请求记录">
        <div class="filters">
          <label for="search">
            搜索请求
            <input id="search" name="gateway-search" type="search" autocomplete="off" placeholder="例如 /v1/chat/completions…" />
          </label>
          <div class="type-filter" aria-label="接口类型过滤">
            <div class="type-filter-title">接口类型</div>
            <div class="type-filter-options" role="group" aria-label="接口类型">
              <button type="button" data-api-type-filter="chat_completions">ChatComplations</button>
              <button type="button" data-api-type-filter="responses">Response</button>
              <button type="button" data-api-type-filter="messages">Messages</button>
            </div>
          </div>
          <div class="summary-strip" aria-label="记录统计">
            <div class="metric"><span>Total</span><strong id="totalCount">0</strong></div>
            <div class="metric"><span>Success</span><strong id="successCount">0</strong></div>
            <div class="metric"><span>Errors</span><strong id="errorCount">0</strong></div>
          </div>
        </div>
        <nav class="list" id="list" aria-label="最近请求"></nav>
      </aside>
      <section class="detail" id="detail" tabindex="-1" aria-live="polite">
        <div class="empty">暂无记录</div>
      </section>
    </main>
  </div>
  <script>
    const listEl = document.getElementById('list');
    const detailEl = document.getElementById('detail');
    const searchEl = document.getElementById('search');
    const typeFilterButtons = Array.from(document.querySelectorAll('[data-api-type-filter]'));
    const liveTextEl = document.getElementById('liveText');
    const totalCountEl = document.getElementById('totalCount');
    const successCountEl = document.getElementById('successCount');
    const errorCountEl = document.getElementById('errorCount');
    const dateFormatter = new Intl.DateTimeFormat(navigator.languages, {
      month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit'
    });
    const numberFormatter = new Intl.NumberFormat(navigator.languages);
    const byteFormatter = new Intl.NumberFormat(navigator.languages, { maximumFractionDigits: 1 });
    let activeId = null;
    let activeTab = 'request';
    let rowsCache = [];
    let activeResponseBodyView = 'json';
    let activeDetailPending = false;
    let activeApiTypeFilter = '';
    let logSocket = null;
    let reconnectTimer = null;

    function esc(value) {
      return String(value ?? '').replace(/[&<>"']/g, c => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
      }[c]));
    }

    function statusClass(status) {
      if (!status) return 'warn';
      return status >= 200 && status < 400 ? 'ok' : 'err';
    }

    function statusClassForRow(row) {
      return row.error ? 'err' : statusClass(row.response_status);
    }

    function statusLabel(row) {
      return row.response_status ?? (row.error ? 'error' : 'pending');
    }

    function formatDate(value) {
      return value ? dateFormatter.format(new Date(value)) : '-';
    }

    function formatBytes(value) {
      const bytes = Number(value || 0);
      if (bytes < 1024) return `${numberFormatter.format(bytes)} B`;
      if (bytes < 1024 * 1024) return `${byteFormatter.format(bytes / 1024)} KB`;
      return `${byteFormatter.format(bytes / 1024 / 1024)} MB`;
    }

    function formatMs(value) {
      return value === null || value === undefined ? '-' : `${numberFormatter.format(value)} ms`;
    }

    function formatNumberValue(value) {
      return value === null || value === undefined ? '-' : numberFormatter.format(value);
    }

    function formatTps(value) {
      return value === null || value === undefined ? '-' : `${numberFormatter.format(value)} tok/s`;
    }

    function apiTypeLabel(value) {
      return {
        chat_completions: 'ChatComplations',
        responses: 'Response',
        messages: 'Messages',
        other: 'Other',
      }[value] || 'Other';
    }

    function gatewayOverhead(row) {
      if (row.gateway_overhead_ms !== undefined && row.gateway_overhead_ms !== null) return row.gateway_overhead_ms;
      if (row.duration_ms === null || row.duration_ms === undefined) return null;
      if (row.upstream_duration_ms === null || row.upstream_duration_ms === undefined) return null;
      return row.duration_ms - row.upstream_duration_ms;
    }

    function formatHeaderText(headers) {
      return Object.entries(headers || {})
        .map(([key, value]) => `${key}: ${value}`)
        .join('\\n');
    }

    function renderHeaders(headers) {
      const entries = Object.entries(headers || {});
      if (!entries.length) return '<div class="empty">没有 Header</div>';
      return `<div class="kv">${entries.map(([key, value]) => `
        <div class="kv-row">
          <div class="kv-key" translate="no">${esc(key)}</div>
          <div class="kv-value" translate="no">${esc(value)}</div>
        </div>
      `).join('')}</div>`;
    }

    function prettyBody(body) {
      const text = body || '';
      if (!text.trim()) return '(empty)';
      try {
        return JSON.stringify(JSON.parse(text), null, 2);
      } catch {
        return text;
      }
    }

    function jsonSummary(value) {
      if (Array.isArray(value)) return `Array(${numberFormatter.format(value.length)})`;
      if (value && typeof value === 'object') return `Object(${numberFormatter.format(Object.keys(value).length)})`;
      return typeof value;
    }

    function primitiveClass(value) {
      if (value === null) return 'json-null';
      if (typeof value === 'string') return 'json-string';
      if (typeof value === 'number') return 'json-number';
      if (typeof value === 'boolean') return 'json-boolean';
      return 'json-preview';
    }

    function primitivePreview(value) {
      if (value === null) return 'null';
      if (typeof value === 'string') return JSON.stringify(value);
      return String(value);
    }

    function renderJsonValue(value, key = '', depth = 0) {
      const keyHtml = key === '' ? '' : `<span class="json-key">${esc(key)}:</span>`;
      if (value && typeof value === 'object') {
        const isArray = Array.isArray(value);
        const entries = isArray ? value.map((item, index) => [String(index), item]) : Object.entries(value);
        return `
          <details class="json-node" open>
            <summary>${keyHtml}<span class="json-type">${isArray ? 'Array' : 'Object'}</span><span class="json-preview">${esc(jsonSummary(value))}</span></summary>
            <div class="json-children">
              ${entries.length ? entries.map(([childKey, childValue]) => renderJsonValue(childValue, childKey, depth + 1)).join('') : '<div class="json-leaf json-preview">(empty)</div>'}
            </div>
          </details>
        `;
      }
      return `<div class="json-leaf">${keyHtml}<span class="${primitiveClass(value)}">${esc(primitivePreview(value))}</span></div>`;
    }

    function renderBodyContent(text) {
      const body = text || '';
      if (!body.trim()) return '<pre translate="no">(empty)</pre>';
      try {
        return `<div class="json-viewer" translate="no">${renderJsonValue(JSON.parse(body))}</div>`;
      } catch {
        return `<pre translate="no">${esc(body)}</pre>`;
      }
    }

    function isSseResponse(row) {
      const contentType = Object.entries(row.response_headers || {})
        .find(([key]) => key.toLowerCase() === 'content-type')?.[1] || '';
      return contentType.toLowerCase().includes('text/event-stream') || /^event:|\\ndata:/m.test(row.response_body.text || '');
    }

    function parseSseEvents(text) {
      const blocks = String(text || '').split(/\\r?\\n\\r?\\n/);
      const events = [];
      for (const block of blocks) {
        let eventName = '';
        const dataLines = [];
        for (const rawLine of block.split(/\\r?\\n/)) {
          const line = rawLine.trimEnd();
          if (line.startsWith('event:')) eventName = line.slice(6).trim();
          if (line.startsWith('data:')) dataLines.push(line.slice(5).trimStart());
        }
        if (dataLines.length) {
          const data = dataLines.join('\\n');
          events.push({ event: eventName || 'message', data });
        }
      }
      return events;
    }

    function tryParseJson(text) {
      try {
        return JSON.parse(text);
      } catch {
        return null;
      }
    }

    function completedResponseJsonFromSse(text) {
      const events = parseSseEvents(text);

      for (let index = events.length - 1; index >= 0; index -= 1) {
        const item = events[index];
        if (item.event === 'response.completed') {
          const parsed = tryParseJson(item.data);
          return parsed ? JSON.stringify(parsed, null, 2) : item.data;
        }
      }

      const chatChunks = events
        .filter(item => item.data !== '[DONE]')
        .map(item => tryParseJson(item.data))
        .filter(Boolean)
        .filter(item => Array.isArray(item.choices));
      if (chatChunks.length) {
        const content = chatChunks
          .map(item => item.choices?.[0]?.delta?.content || item.choices?.[0]?.message?.content || '')
          .join('');
        const toolCalls = chatChunks
          .flatMap(item => item.choices?.[0]?.delta?.tool_calls || [])
          .filter(Boolean);
        const lastChunk = chatChunks[chatChunks.length - 1];
        return JSON.stringify(
          {
            type: 'chat.completions',
            id: lastChunk.id || chatChunks[0].id || null,
            model: lastChunk.model || chatChunks[0].model || null,
            content,
            tool_calls: toolCalls,
            finish_reason: lastChunk.choices?.[0]?.finish_reason || null,
            chunks: chatChunks,
          },
          null,
          2
        );
      }

      const messageEvents = events
        .map(item => ({ event: item.event, data: tryParseJson(item.data) }))
        .filter(item => item.data);
      if (messageEvents.some(item => item.event.startsWith('message_') || item.event.startsWith('content_block_'))) {
        const content = messageEvents
          .map(item => item.data.delta?.text || item.data.content_block?.text || '')
          .join('');
        const messageStart = messageEvents.find(item => item.event === 'message_start')?.data?.message || null;
        let messageDelta = null;
        for (let index = messageEvents.length - 1; index >= 0; index -= 1) {
          if (messageEvents[index].event === 'message_delta') {
            messageDelta = messageEvents[index].data;
            break;
          }
        }
        return JSON.stringify(
          {
            type: 'messages',
            message: messageStart,
            content,
            stop_reason: messageDelta?.delta?.stop_reason || null,
            usage: messageDelta?.usage || messageStart?.usage || null,
            events: messageEvents,
          },
          null,
          2
        );
      }

      return '';
    }

    function updateUrlState() {
      const params = new URLSearchParams(window.location.search);
      if (activeId) params.set('id', String(activeId));
      params.set('tab', activeTab);
      if (activeApiTypeFilter) params.set('type', activeApiTypeFilter);
      else params.delete('type');
      const q = searchEl.value.trim();
      if (q) params.set('q', q);
      else params.delete('q');
      const next = `${window.location.pathname}?${params.toString()}`;
      window.history.replaceState(null, '', next);
    }

    function filteredRows() {
      const q = searchEl.value.trim().toLowerCase();
      return rowsCache.filter(row => {
        if (activeApiTypeFilter && row.api_type !== activeApiTypeFilter) return false;
        if (!q) return true;
        return `${row.method} ${row.target_url} ${statusLabel(row)} ${row.error ?? ''} ${apiTypeLabel(row.api_type)}`.toLowerCase().includes(q);
      });
    }

    function renderTypeFilters() {
      typeFilterButtons.forEach(button => {
        const isActive = button.dataset.apiTypeFilter === activeApiTypeFilter;
        button.classList.toggle('active', isActive);
        button.setAttribute('aria-pressed', String(isActive));
      });
    }

    function renderList() {
      const rows = filteredRows();
      const success = rows.filter(row => row.response_status >= 200 && row.response_status < 400).length;
      const errors = rows.filter(row => row.error || row.response_status >= 400).length;
      totalCountEl.textContent = numberFormatter.format(rows.length);
      successCountEl.textContent = numberFormatter.format(success);
      errorCountEl.textContent = numberFormatter.format(errors);
      renderTypeFilters();

      if (!rows.length) {
        listEl.innerHTML = '<div class="empty">没有匹配的请求记录</div>';
        if (!rowsCache.length) detailEl.innerHTML = '<div class="empty">暂无记录</div>';
        return;
      }

      listEl.innerHTML = rows.map(row => `
        <button class="item ${row.id === activeId ? 'active' : ''}" type="button" data-id="${row.id}" aria-current="${row.id === activeId ? 'true' : 'false'}">
          <div class="meta">
            <span class="method" translate="no">${esc(row.method)}</span>
            <span class="badge status ${statusClassForRow(row)}">${esc(statusLabel(row))}</span>
            <span class="badge api-type">${esc(apiTypeLabel(row.api_type))}</span>
            ${row.reasoning_tokens === 516 ? '<span class="badge anomaly" title="reasoning_tokens 异常">516</span>' : ''}
            <span class="grow"></span>
            <span>${esc(formatMs(row.duration_ms))}</span>
          </div>
          <div class="url" translate="no">${esc(row.target_url)}</div>
          <div class="meta">
            <span>#${numberFormatter.format(row.id)}</span>
            <span>${esc(formatDate(row.created_at))}</span>
            <span>Req ${esc(formatBytes(row.request_body_bytes))}</span>
            <span>Res ${esc(formatBytes(row.response_body_bytes))}</span>
          </div>
        </button>
      `).join('');
      listEl.querySelectorAll('.item').forEach(item => {
        item.addEventListener('click', () => loadDetail(Number(item.dataset.id)));
      });
    }

    function applyRows(rows, { refreshPendingDetail = false } = {}) {
      rowsCache = rows;
      renderList();
      if (refreshPendingDetail && activeId && activeDetailPending && rowsCache.some(row => row.id === activeId)) {
        loadDetail(activeId, false);
      }
      liveTextEl.textContent = `最近更新 ${dateFormatter.format(new Date())}`;
    }

    async function loadList({ refreshDetail = true } = {}) {
      liveTextEl.textContent = '正在加载…';
      const res = await fetch('/api/logs?limit=50');
      const rows = await res.json();
      applyRows(rows);
      const params = new URLSearchParams(window.location.search);
      const selected = Number(params.get('id')) || activeId || rowsCache[0]?.id;
      const nextTab = params.get('tab');
      if (nextTab) activeTab = nextTab;
      if (selected && rowsCache.some(row => row.id === selected)) {
        await loadDetail(selected, false);
      } else if (!rowsCache.length) {
        detailEl.innerHTML = '<div class="empty">暂无记录</div>';
      }
      liveTextEl.textContent = `最近更新 ${dateFormatter.format(new Date())}`;
    }

    function connectLogSocket() {
      if (logSocket && (logSocket.readyState === WebSocket.OPEN || logSocket.readyState === WebSocket.CONNECTING)) return;
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      logSocket = new WebSocket(`${protocol}//${window.location.host}/ws/logs`);
      logSocket.addEventListener('open', () => {
        liveTextEl.textContent = 'WebSocket 已连接';
      });
      logSocket.addEventListener('message', event => {
        try {
          const message = JSON.parse(event.data);
          if (message.type === 'logs' && Array.isArray(message.rows)) {
            applyRows(message.rows, { refreshPendingDetail: true });
          }
        } catch {
          liveTextEl.textContent = 'WebSocket 消息解析失败';
        }
      });
      logSocket.addEventListener('close', () => {
        liveTextEl.textContent = 'WebSocket 已断开，准备重连…';
        clearTimeout(reconnectTimer);
        reconnectTimer = setTimeout(connectLogSocket, 2000);
      });
      logSocket.addEventListener('error', () => {
        logSocket.close();
      });
    }

    function setTab(tab) {
      activeTab = tab;
      document.querySelectorAll('[data-panel]').forEach(panel => {
        panel.hidden = panel.dataset.panel !== tab;
      });
      document.querySelectorAll('.tab').forEach(button => {
        const isActive = button.dataset.tab === tab;
        button.classList.toggle('active', isActive);
        button.setAttribute('aria-selected', String(isActive));
      });
      updateUrlState();
    }

    function setResponseBodyView(view) {
      activeResponseBodyView = view;
      detailEl.querySelectorAll('[data-response-body-view]').forEach(panel => {
        panel.hidden = panel.dataset.responseBodyView !== view;
      });
      detailEl.querySelectorAll('[data-response-view-button]').forEach(button => {
        const isActive = button.dataset.responseViewButton === view;
        button.classList.toggle('active', isActive);
        button.setAttribute('aria-pressed', String(isActive));
      });
    }

    async function copyText(text) {
      await navigator.clipboard.writeText(text || '');
      liveTextEl.textContent = '已复制到剪贴板';
    }

    async function loadDetail(id, shouldFocus = true) {
      activeId = id;
      const res = await fetch(`/api/logs/${id}`);
      const row = await res.json();
      activeDetailPending = row.response_status === null && !row.error;
      const responseIsSse = isSseResponse(row);
      const completedJson = responseIsSse ? completedResponseJsonFromSse(row.response_body.text) : '';
      const responseJsonText = responseIsSse ? (completedJson || '(没有找到可解析的 SSE JSON)') : prettyBody(row.response_body.text);
      const responseSseText = row.response_body.text || '(empty)';
      const requestBodyText = prettyBody(row.request_body.text);
      const requestHeaderText = formatHeaderText(row.request_headers);
      const responseHeaderText = formatHeaderText(row.response_headers);
      const overheadMs = gatewayOverhead(row);
      const reasoningTokens = row.reasoning_tokens;
      const apiType = row.api_type || 'other';
      const isReasoningAnomaly = reasoningTokens === 516;
      activeResponseBodyView = responseIsSse && ['json', 'sse'].includes(activeResponseBodyView) ? activeResponseBodyView : (responseIsSse ? 'json' : 'body');
      renderList();
      detailEl.innerHTML = `
        <div class="detail-head">
          <div class="detail-topline">
            <div class="endpoint-block">
              <div class="endpoint-badges">
                <span class="pill method-pill" translate="no">${esc(row.method)}</span>
                <span class="pill type-pill">${esc(apiTypeLabel(apiType))}</span>
                <span class="pill status-pill ${statusClassForRow(row)}">${esc(statusLabel(row))}</span>
                ${isReasoningAnomaly ? '<span class="pill status-pill err">reasoning 516</span>' : ''}
              </div>
              <div class="endpoint-url" translate="no">${esc(row.target_url)}</div>
            </div>
            <button type="button" data-copy="url">复制 URL</button>
          </div>
          <div class="metric-grid" aria-label="请求关键指标">
            <div class="metric-card primary"><div class="metric-label">本项目耗时</div><div class="metric-value">${esc(formatMs(row.duration_ms))}</div></div>
            <div class="metric-card"><div class="metric-label">上游接口耗时</div><div class="metric-value">${esc(formatMs(row.upstream_duration_ms))}</div></div>
            <div class="metric-card ${overheadMs !== null && overheadMs > 80 ? 'warn' : 'good'}"><div class="metric-label">差值</div><div class="metric-value">${esc(formatMs(overheadMs))}</div></div>
            <div class="metric-card primary"><div class="metric-label">首字用时</div><div class="metric-value">${esc(formatMs(row.first_byte_ms))}</div></div>
            <div class="metric-card good"><div class="metric-label">TPS</div><div class="metric-value">${esc(formatTps(row.tps))}</div></div>
            <div class="metric-card ${isReasoningAnomaly ? 'danger' : ''}"><div class="metric-label">Reasoning Tokens</div><div class="metric-value">${esc(formatNumberValue(reasoningTokens))}</div></div>
          </div>
          <div class="detail-meta">
            <div class="meta-chip"><span class="meta-label">Output Tokens</span><span class="meta-value">${esc(formatNumberValue(row.output_tokens))}</span></div>
            <div class="meta-chip"><span class="meta-label">Request Body</span><span class="meta-value">${esc(formatBytes(row.request_body.text.length))}${row.request_body_truncated ? ' · truncated' : ''}</span></div>
            <div class="meta-chip"><span class="meta-label">Response Body</span><span class="meta-value">${esc(formatBytes(row.response_body.text.length))}${row.response_body_truncated ? ' · truncated' : ''}</span></div>
            <div class="meta-chip"><span class="meta-label">Created</span><span class="meta-value">${esc(formatDate(row.created_at))}</span></div>
          </div>
        </div>
        <div class="tabs" role="tablist" aria-label="请求详情">
          <button class="tab" type="button" role="tab" data-tab="request">Request</button>
          <button class="tab" type="button" role="tab" data-tab="response">Response</button>
        </div>
        <div data-panel="request">
          <details class="collapsible-section">
            <summary><div class="copy-row"><h3>Request Header</h3><button type="button" data-copy="requestHeaders">复制 Header</button></div></summary>
            <div class="collapsible-content">${renderHeaders(row.request_headers)}</div>
          </details>
          <section>
            <div class="copy-row"><h3>Request Body${row.request_body_truncated ? ' (truncated)' : ''}</h3><button type="button" data-copy="requestBody">复制 Body</button></div>
            ${renderBodyContent(row.request_body.text)}
          </section>
        </div>
        <div data-panel="response" hidden>
          <details class="collapsible-section">
            <summary><div class="copy-row"><h3>Response Header</h3><button type="button" data-copy="responseHeaders">复制 Header</button></div></summary>
            <div class="collapsible-content">${renderHeaders(row.response_headers)}</div>
          </details>
          <section>
            <div class="copy-row">
              <h3>Response Body${row.response_body_truncated ? ' (truncated)' : ''}</h3>
              <div class="row-actions">
                ${responseIsSse ? `
                  <div class="view-switch" aria-label="Response Body 视图">
                    <button type="button" data-response-view-button="json">JSON</button>
                    <button type="button" data-response-view-button="sse">SSE</button>
                  </div>
                ` : ''}
                <button type="button" data-copy="responseBody">复制 Body</button>
              </div>
            </div>
            ${responseIsSse ? `
              <div data-response-body-view="json">${renderBodyContent(responseJsonText)}</div>
              <pre data-response-body-view="sse" translate="no" hidden>${esc(responseSseText)}</pre>
            ` : `
              <div data-response-body-view="body">${renderBodyContent(row.response_body.text)}</div>
            `}
          </section>
        </div>
      `;
      detailEl.querySelectorAll('.tab').forEach(button => {
        button.addEventListener('click', () => setTab(button.dataset.tab));
      });
      const copyMap = {
        url: row.target_url,
        requestHeaders: requestHeaderText,
        requestBody: requestBodyText,
        responseHeaders: responseHeaderText,
        responseBody: responseIsSse ? (activeResponseBodyView === 'json' ? responseJsonText : responseSseText) : responseJsonText,
      };
      detailEl.querySelectorAll('[data-copy]').forEach(button => {
        button.addEventListener('click', event => {
          event.preventDefault();
          event.stopPropagation();
          if (button.dataset.copy === 'responseBody' && responseIsSse) {
            copyText(activeResponseBodyView === 'json' ? responseJsonText : responseSseText);
            return;
          }
          copyText(copyMap[button.dataset.copy]);
        });
      });
      detailEl.querySelectorAll('[data-response-view-button]').forEach(button => {
        button.addEventListener('click', () => setResponseBodyView(button.dataset.responseViewButton));
      });
      setResponseBodyView(activeResponseBodyView);
      setTab(['request', 'response'].includes(activeTab) ? activeTab : 'request');
      if (shouldFocus) detailEl.focus({ preventScroll: true });
    }

    const initialParams = new URLSearchParams(window.location.search);
    searchEl.value = initialParams.get('q') || '';
    activeApiTypeFilter = initialParams.get('type') || '';
    if (!['', 'chat_completions', 'responses', 'messages'].includes(activeApiTypeFilter)) {
      activeApiTypeFilter = '';
    }
    searchEl.addEventListener('input', () => {
      renderList();
      updateUrlState();
    });
    typeFilterButtons.forEach(button => {
      button.addEventListener('click', () => {
        const nextType = button.dataset.apiTypeFilter || '';
        activeApiTypeFilter = activeApiTypeFilter === nextType ? '' : nextType;
        renderList();
        updateUrlState();
      });
    });
    document.getElementById('refresh').addEventListener('click', () => loadList({ refreshDetail: true }));
    loadList({ refreshDetail: true });
    connectLogSocket();
  </script>
</body>
</html>
"""


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/logs")
async def api_logs(limit: int = 100) -> list[dict]:
    return list_log_summaries(limit)


@app.websocket("/ws/logs")
async def websocket_logs(websocket: WebSocket) -> None:
    await log_socket_manager.connect(websocket)
    try:
        await websocket.send_json({"type": "logs", "changed_id": None, "rows": list_log_summaries(50)})
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        log_socket_manager.disconnect(websocket)


@app.get("/api/logs/{log_id}")
async def api_log_detail(log_id: int) -> JSONResponse:
    ensure_db()
    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM request_logs WHERE id = ?", (log_id,)).fetchone()
    if row is None:
        return JSONResponse({"error": "not found"}, status_code=404)

    output_tokens = row["output_tokens"] if row["output_tokens"] is not None else output_tokens_from_body(row["response_body"] or b"")
    return JSONResponse(
        {
            "id": row["id"],
            "created_at": row["created_at"],
            "method": row["method"],
            "target_url": row["target_url"],
            "client_host": row["client_host"],
            "request_headers": json.loads(row["request_headers"] or "{}"),
            "request_body": body_payload(row["request_body"] or b""),
            "request_body_truncated": bool(row["request_body_truncated"]),
            "response_status": row["response_status"],
            "response_headers": json.loads(row["response_headers"] or "{}"),
            "response_body": body_payload(row["response_body"] or b""),
            "response_body_truncated": bool(row["response_body_truncated"]),
            "reasoning_tokens": reasoning_tokens_from_body(row["response_body"] or b""),
            "api_type": api_type_from_log(
                row["target_url"] or "",
                row["request_body"] or b"",
                row["response_body"] or b"",
            ),
            "error": row["error"],
            "duration_ms": row["duration_ms"],
            "upstream_duration_ms": row["upstream_duration_ms"],
            "first_byte_ms": row["first_byte_ms"],
            "output_tokens": output_tokens,
            "tps": tps_from_values(output_tokens, row["duration_ms"], row["first_byte_ms"]),
            "gateway_overhead_ms": (
                row["duration_ms"] - row["upstream_duration_ms"]
                if row["duration_ms"] is not None and row["upstream_duration_ms"] is not None
                else None
            ),
        }
    )


@app.api_route("/{target_url:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
async def proxy(target_url: str, request: Request):
    if request.url.query:
        target_url = f"{target_url}?{request.url.query}"

    validation_error = validate_target_url(target_url)
    if validation_error:
        return PlainTextResponse(validation_error, status_code=400)

    started_at = time.perf_counter()
    raw_request_body = await request.body()
    captured_request_body, request_truncated = capture_bytes(raw_request_body)
    log_id_task = asyncio.create_task(
        asyncio.to_thread(
            create_log,
            request.method,
            target_url,
            request.client.host if request.client else None,
            dict(request.headers),
            captured_request_body,
            request_truncated,
        )
    )
    asyncio.create_task(announce_created(log_id_task))
    response_capture = bytearray()
    response_truncated = False

    timeout = httpx.Timeout(REQUEST_TIMEOUT_SECONDS, connect=30.0)
    client = httpx.AsyncClient(timeout=timeout, follow_redirects=False, trust_env=False)
    upstream_request = client.build_request(
        request.method,
        target_url,
        headers=filtered_request_headers(request),
        content=raw_request_body,
    )

    upstream_started_at = time.perf_counter()
    try:
        upstream_response = await client.send(upstream_request, stream=True)
    except Exception as exc:
        await client.aclose()
        finished_at = time.perf_counter()
        body = f"Upstream request failed: {exc}".encode("utf-8")
        asyncio.create_task(
            finish_log_async(
                log_id_task,
                502,
                {"content-type": "text/plain; charset=utf-8"},
                body,
                False,
                started_at,
                upstream_started_at,
                finished_at,
                None,
                str(exc),
            )
        )
        return PlainTextResponse(body.decode("utf-8"), status_code=502)

    response_headers = filtered_response_headers(upstream_response.headers)

    async def stream_response() -> AsyncIterator[bytes]:
        nonlocal response_truncated
        first_byte_at = None
        error = None
        try:
            async for chunk in upstream_response.aiter_bytes():
                if first_byte_at is None:
                    first_byte_at = time.perf_counter()
                response_truncated = append_capture(response_capture, chunk) or response_truncated
                yield chunk
        except Exception as exc:
            error = str(exc)
            raise
        finally:
            finished_at = time.perf_counter()
            await upstream_response.aclose()
            await client.aclose()
            asyncio.create_task(
                finish_log_async(
                    log_id_task,
                    upstream_response.status_code,
                    dict(upstream_response.headers),
                    response_capture,
                    response_truncated,
                    started_at,
                    upstream_started_at,
                    finished_at,
                    first_byte_at,
                    error,
                )
            )

    return StreamingResponse(
        stream_response(),
        status_code=upstream_response.status_code,
        headers=response_headers,
        media_type=upstream_response.headers.get("content-type"),
    )
