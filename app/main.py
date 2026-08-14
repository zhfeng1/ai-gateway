import asyncio
import base64
import json
import os
import sqlite3
import threading
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from html import escape
from typing import Any, AsyncIterator
from urllib.parse import quote, urlsplit

import httpx
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, StreamingResponse

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - SQLite deployments do not need psycopg at import time.
    psycopg = None
    dict_row = None


DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/ai_gateway.sqlite3")
DATABASE_TYPE = (os.getenv("DATABASE_TYPE") or os.getenv("DB_TYPE") or "sqlite").strip().lower()
DATABASE_URL = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_DSN") or os.getenv("POSTGRES_URL")
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "600"))
MAX_CAPTURE_BYTES = int(os.getenv("MAX_CAPTURE_BYTES", "0"))
APP_COMMIT = (os.getenv("APP_COMMIT") or "unknown").strip() or "unknown"
PERFORMANCE_LOG_ENABLED = os.getenv("PERFORMANCE_LOG_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
PERFORMANCE_LOG_THRESHOLD_MS = int(float(os.getenv("PERFORMANCE_LOG_THRESHOLD_MS", "100")))
HTTP_MAX_CONNECTIONS = int(os.getenv("HTTP_MAX_CONNECTIONS", "500"))
HTTP_MAX_KEEPALIVE_CONNECTIONS = int(os.getenv("HTTP_MAX_KEEPALIVE_CONNECTIONS", "200"))
HTTP_KEEPALIVE_EXPIRY_SECONDS = float(os.getenv("HTTP_KEEPALIVE_EXPIRY_SECONDS", "30"))
DINGTALK_WEBHOOK_URL = os.getenv("DINGTALK_WEBHOOK_URL", "").strip()
DINGTALK_WEBHOOK_TIMEOUT_SECONDS = float(os.getenv("DINGTALK_WEBHOOK_TIMEOUT_SECONDS", "10"))
POSTGRES_CONNECT_TIMEOUT_SECONDS = int(float(os.getenv("POSTGRES_CONNECT_TIMEOUT_SECONDS", "5")))
NEW_API_LOG_DATABASE_URL = (
    os.getenv("NEW_API_LOG_DATABASE_URL")
    or os.getenv("NEW_API_LOG_DB_DSN")
    or os.getenv("NEW_API_LOG_POSTGRES_DSN")
    or os.getenv("NEW_API_LOG_POSTGRES_URL")
)
NEW_API_LOG_QUERY_TIMEOUT_SECONDS = float(os.getenv("NEW_API_LOG_QUERY_TIMEOUT_SECONDS", "3"))
NEW_API_LOG_MAX_ROWS_PER_TABLE = int(os.getenv("NEW_API_LOG_MAX_ROWS_PER_TABLE", "5"))
NEW_API_LOG_TABLES = [item.strip() for item in os.getenv("NEW_API_LOG_TABLES", "logs").split(",") if item.strip()]
NEW_API_LOG_REQUEST_ID_COLUMNS = [
    item.strip()
    for item in os.getenv(
        "NEW_API_LOG_REQUEST_ID_COLUMNS",
        "request_id,x_oneapi_request_id,oneapi_request_id,one_api_request_id,requestid,requestId",
    ).split(",")
    if item.strip()
]
NEW_API_LOG_MAX_JSON_BYTES = int(os.getenv("NEW_API_LOG_MAX_JSON_BYTES", "1000000"))

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
RESERVED_ACCESS_KEYS = {"api", "ws", "health", "docs", "redoc", "openapi.json", "favicon.ico"}
DB_INIT_LOCK = threading.Lock()
DB_INITIALIZED = False
NEW_API_LOG_SCHEMA_LOCK = threading.Lock()
NEW_API_LOG_SCHEMA_CACHE: list[dict[str, Any]] | None = None
HTTP_CLIENT: httpx.AsyncClient | None = None


class LogSocketManager:
    def __init__(self) -> None:
        self.connections: dict[WebSocket, str | None] = {}

    async def connect(self, websocket: WebSocket, access_key: str | None = None) -> None:
        await websocket.accept()
        self.connections[websocket] = access_key

    def disconnect(self, websocket: WebSocket) -> None:
        self.connections.pop(websocket, None)

    async def broadcast(self, payload: dict, access_key: str | None = None) -> None:
        dead_connections = []
        for websocket, connection_access_key in list(self.connections.items()):
            if connection_access_key != access_key:
                continue
            try:
                await websocket.send_json(payload)
            except Exception:
                dead_connections.append(websocket)
        for websocket in dead_connections:
            self.disconnect(websocket)


log_socket_manager = LogSocketManager()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def elapsed_ms(started_at: float, finished_at: float | None = None) -> int:
    return int(((finished_at or time.perf_counter()) - started_at) * 1000)


def perf_log(event: str, **fields: Any) -> None:
    if not PERFORMANCE_LOG_ENABLED:
        return
    payload = {"event": event, "timestamp": utc_now(), **fields}
    print(f"AI_GATEWAY_PERF {json_dumps(payload)}", flush=True)


def create_http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(REQUEST_TIMEOUT_SECONDS, connect=30.0),
        follow_redirects=False,
        trust_env=False,
        limits=httpx.Limits(
            max_connections=HTTP_MAX_CONNECTIONS,
            max_keepalive_connections=HTTP_MAX_KEEPALIVE_CONNECTIONS,
            keepalive_expiry=HTTP_KEEPALIVE_EXPIRY_SECONDS,
        ),
    )


def get_http_client() -> httpx.AsyncClient:
    global HTTP_CLIENT
    if HTTP_CLIENT is None or HTTP_CLIENT.is_closed:
        HTTP_CLIENT = create_http_client()
    return HTTP_CLIENT


def normalize_postgres_dsn(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    if value.startswith("jdbc:postgresql://"):
        value = "postgresql://" + value[len("jdbc:postgresql://") :]
    return value


def build_postgres_dsn(prefix: str, default_database: str) -> str | None:
    url = (
        os.getenv(f"{prefix}DATABASE_URL")
        or os.getenv(f"{prefix}DB_DSN")
        or os.getenv(f"{prefix}POSTGRES_DSN")
        or os.getenv(f"{prefix}POSTGRES_URL")
    )
    if url:
        return normalize_postgres_dsn(url)

    host = os.getenv(f"{prefix}POSTGRES_HOST")
    if not host:
        return None
    port = os.getenv(f"{prefix}POSTGRES_PORT", "5432")
    database = os.getenv(f"{prefix}POSTGRES_DB", default_database)
    user = os.getenv(f"{prefix}POSTGRES_USER")
    password = os.getenv(f"{prefix}POSTGRES_PASSWORD")
    auth = ""
    if user:
        auth = quote(user, safe="")
        if password:
            auth += f":{quote(password, safe='')}"
        auth += "@"
    return f"postgresql://{auth}{host}:{port}/{database}"


DATABASE_URL = build_postgres_dsn("", "ai-gateway") or normalize_postgres_dsn(DATABASE_URL)
NEW_API_LOG_DATABASE_URL = build_postgres_dsn("NEW_API_LOG_", "new-api-log") or normalize_postgres_dsn(NEW_API_LOG_DATABASE_URL)
if DATABASE_TYPE in {"postgresql", "pg"}:
    DATABASE_TYPE = "postgres"
if DATABASE_TYPE not in {"sqlite", "postgres"}:
    DATABASE_TYPE = "sqlite"
if DATABASE_TYPE == "sqlite" and DATABASE_URL:
    DATABASE_TYPE = "postgres"


def using_postgres() -> bool:
    return DATABASE_TYPE == "postgres"


def require_psycopg() -> None:
    if psycopg is None or dict_row is None:
        raise RuntimeError("PostgreSQL support requires psycopg[binary]. Run pip install -r requirements.txt.")


def connect_gateway_postgres(row_factory: Any | None = None):
    require_psycopg()
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_TYPE=postgres requires DATABASE_URL or POSTGRES_* environment variables.")
    kwargs: dict[str, Any] = {"connect_timeout": POSTGRES_CONNECT_TIMEOUT_SECONDS}
    if row_factory is not None:
        kwargs["row_factory"] = row_factory
    return psycopg.connect(DATABASE_URL, **kwargs)


def connect_new_api_log_postgres(row_factory: Any | None = None):
    require_psycopg()
    if not NEW_API_LOG_DATABASE_URL:
        raise RuntimeError("NEW_API_LOG_DATABASE_URL is not configured.")
    kwargs: dict[str, Any] = {
        "connect_timeout": max(1, int(NEW_API_LOG_QUERY_TIMEOUT_SECONDS)),
        "options": f"-c statement_timeout={max(1, int(NEW_API_LOG_QUERY_TIMEOUT_SECONDS * 1000))}",
    }
    if row_factory is not None:
        kwargs["row_factory"] = row_factory
    return psycopg.connect(NEW_API_LOG_DATABASE_URL, **kwargs)


def qmark_to_psycopg(sql: str) -> str:
    return sql.replace("?", "%s")


def json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return {"encoding": "base64", "text": base64.b64encode(value).decode("ascii")}
    return str(value)


def json_dumps(value: Any, *, indent: int | None = None) -> str:
    return json.dumps(value, ensure_ascii=False, indent=indent, default=json_safe)


def limited_json_dumps(value: Any, max_bytes: int = NEW_API_LOG_MAX_JSON_BYTES) -> tuple[str, bool]:
    text = json_dumps(value, indent=2)
    encoded = text.encode("utf-8")
    if max_bytes <= 0 or len(encoded) <= max_bytes:
        return text, False
    clipped = encoded[:max_bytes].decode("utf-8", errors="ignore")
    return clipped + "\n\n... new-api-log 内容过大，已截断 ...", True


class ExecuteResult:
    def __init__(self, lastrowid: int | None = None) -> None:
        self.lastrowid = lastrowid


def ensure_db() -> None:
    global DB_INITIALIZED
    if DB_INITIALIZED:
        return
    with DB_INIT_LOCK:
        if DB_INITIALIZED:
            return
        if using_postgres():
            ensure_postgres_db()
        else:
            ensure_sqlite_db()
        DB_INITIALIZED = True


def ensure_sqlite_db() -> None:
    database_dir = os.path.dirname(DATABASE_PATH)
    if database_dir:
        os.makedirs(database_dir, exist_ok=True)
    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
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
                    output_tokens INTEGER,
                    access_key TEXT,
                    response_failed INTEGER NOT NULL DEFAULT 0,
                    response_failure_code TEXT,
                    response_failure_message TEXT
                )
                """
        )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(request_logs)").fetchall()}
        backfill_response_failed = "response_failed" not in columns
        for column_name, column_type in (
            ("upstream_duration_ms", "INTEGER"),
            ("first_byte_ms", "INTEGER"),
            ("output_tokens", "INTEGER"),
            ("access_key", "TEXT"),
            ("reasoning_tokens", "INTEGER"),
            ("api_type", "TEXT"),
            ("oneapi_request_id", "TEXT"),
            ("new_api_user", "TEXT"),
            ("new_api_log", "TEXT"),
            ("new_api_log_error", "TEXT"),
            ("response_failed", "INTEGER NOT NULL DEFAULT 0"),
            ("response_failure_code", "TEXT"),
            ("response_failure_message", "TEXT"),
        ):
            if column_name not in columns:
                conn.execute(f"ALTER TABLE request_logs ADD COLUMN {column_name} {column_type}")
        if backfill_response_failed:
            candidate_rows = conn.execute(
                """
                SELECT id, response_body
                FROM request_logs
                WHERE instr(CAST(response_body AS TEXT), 'event: response.failed' || char(10) || 'data:') > 0
                   OR instr(CAST(response_body AS TEXT), 'event: response.failed' || char(13) || char(10) || 'data:') > 0
                """
            ).fetchall()
        else:
            candidate_rows = conn.execute(
                """
                SELECT id, response_body
                FROM request_logs
                WHERE response_failed = 1 AND response_failure_code IS NULL
                """
            ).fetchall()
        for log_id, response_body in candidate_rows:
            response_failure = response_failed_from_sse(response_body)
            if response_failure:
                conn.execute(
                    """
                    UPDATE request_logs
                    SET response_failed = 1,
                        response_failure_code = ?,
                        response_failure_message = ?
                    WHERE id = ?
                    """,
                    (
                        str(response_failure.get("code") or response_failure.get("type") or "response_failed"),
                        str(response_failure.get("message") or json_dumps(response_failure)),
                        log_id,
                    ),
                )
            elif not backfill_response_failed:
                conn.execute("UPDATE request_logs SET response_failed = 0 WHERE id = ?", (log_id,))
        conn.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_access_key_id ON request_logs(access_key, id DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_oneapi_request_id ON request_logs(oneapi_request_id)")


def ensure_postgres_db() -> None:
    with connect_gateway_postgres() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS request_logs (
                    id BIGSERIAL PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    method TEXT NOT NULL,
                    target_url TEXT NOT NULL,
                    client_host TEXT,
                    request_headers TEXT NOT NULL,
                    request_body BYTEA NOT NULL,
                    request_body_truncated INTEGER NOT NULL DEFAULT 0,
                    response_status INTEGER,
                    response_headers TEXT,
                    response_body BYTEA NOT NULL DEFAULT ''::bytea,
                    response_body_truncated INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    duration_ms INTEGER,
                    upstream_duration_ms INTEGER,
                    first_byte_ms INTEGER,
                    output_tokens INTEGER,
                    access_key TEXT,
                    reasoning_tokens INTEGER,
                    api_type TEXT,
                    oneapi_request_id TEXT,
                    new_api_user TEXT,
                    new_api_log TEXT,
                    new_api_log_error TEXT,
                    response_failed INTEGER NOT NULL DEFAULT 0,
                    response_failure_code TEXT,
                    response_failure_message TEXT
                )
                """
            )
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'request_logs'
                """
            )
            columns = {row[0] for row in cur.fetchall()}
            backfill_response_failed = "response_failed" not in columns
            for column_name, column_type in (
                ("upstream_duration_ms", "INTEGER"),
                ("first_byte_ms", "INTEGER"),
                ("output_tokens", "INTEGER"),
                ("access_key", "TEXT"),
                ("reasoning_tokens", "INTEGER"),
                ("api_type", "TEXT"),
                ("oneapi_request_id", "TEXT"),
                ("new_api_user", "TEXT"),
                ("new_api_log", "TEXT"),
                ("new_api_log_error", "TEXT"),
                ("response_failed", "INTEGER NOT NULL DEFAULT 0"),
                ("response_failure_code", "TEXT"),
                ("response_failure_message", "TEXT"),
            ):
                if column_name not in columns:
                    cur.execute(f"ALTER TABLE request_logs ADD COLUMN {column_name} {column_type}")
            if backfill_response_failed:
                cur.execute(
                    """
                    SELECT id, response_body
                    FROM request_logs
                    WHERE position(convert_to(E'event: response.failed\ndata:', 'UTF8') in response_body) > 0
                       OR position(convert_to(E'event: response.failed\r\ndata:', 'UTF8') in response_body) > 0
                    """
                )
            else:
                cur.execute(
                    """
                    SELECT id, response_body
                    FROM request_logs
                    WHERE response_failed = 1 AND response_failure_code IS NULL
                    """
                )
            for log_id, response_body in cur.fetchall():
                response_failure = response_failed_from_sse(response_body)
                if response_failure:
                    cur.execute(
                        """
                        UPDATE request_logs
                        SET response_failed = 1,
                            response_failure_code = %s,
                            response_failure_message = %s
                        WHERE id = %s
                        """,
                        (
                            str(response_failure.get("code") or response_failure.get("type") or "response_failed"),
                            str(response_failure.get("message") or json_dumps(response_failure)),
                            log_id,
                        ),
                    )
                elif not backfill_response_failed:
                    cur.execute("UPDATE request_logs SET response_failed = 0 WHERE id = %s", (log_id,))
            cur.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_access_key_id ON request_logs(access_key, id DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_request_logs_oneapi_request_id ON request_logs(oneapi_request_id)")
        conn.commit()


@app.on_event("startup")
async def startup() -> None:
    global HTTP_CLIENT
    ensure_db()
    HTTP_CLIENT = create_http_client()


@app.on_event("shutdown")
async def shutdown() -> None:
    global HTTP_CLIENT
    if HTTP_CLIENT is not None and not HTTP_CLIENT.is_closed:
        await HTTP_CLIENT.aclose()
    HTTP_CLIENT = None


def db_execute(sql: str, params: tuple = (), *, returning_id: bool = False) -> ExecuteResult:
    ensure_db()
    if using_postgres():
        postgres_sql = qmark_to_psycopg(sql)
        if returning_id:
            postgres_sql = postgres_sql.rstrip() + " RETURNING id"
        with connect_gateway_postgres() as conn:
            with conn.cursor() as cur:
                cur.execute(postgres_sql, params)
                lastrowid = None
                if returning_id:
                    row = cur.fetchone()
                    lastrowid = int(row[0]) if row else None
            conn.commit()
        return ExecuteResult(lastrowid)

    conn = sqlite3.connect(DATABASE_PATH)
    try:
        conn.execute("PRAGMA busy_timeout=5000")
        cur = conn.execute(sql, params)
        conn.commit()
        return ExecuteResult(int(cur.lastrowid) if returning_id else None)
    finally:
        conn.close()


def db_fetchone(sql: str, params: tuple = ()) -> dict[str, Any] | None:
    ensure_db()
    if using_postgres():
        with connect_gateway_postgres(dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(qmark_to_psycopg(sql), params)
                row = cur.fetchone()
        return dict(row) if row else None

    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        row = conn.execute(sql, params).fetchone()
    return dict(row) if row else None


def db_fetchall(sql: str, params: tuple = ()) -> list[dict[str, Any]]:
    ensure_db()
    if using_postgres():
        with connect_gateway_postgres(dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(qmark_to_psycopg(sql), params)
                rows = cur.fetchall()
        return [dict(row) for row in rows]

    with sqlite3.connect(DATABASE_PATH) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def bytes_from_db(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    if isinstance(value, str):
        return value.encode("utf-8")
    return bytes(value)


def create_log(
    method: str,
    target_url: str,
    access_key: str | None,
    client_host: str | None,
    request_headers: dict[str, str],
    request_body: bytes,
    request_body_truncated: bool,
) -> int:
    cur = db_execute(
        """
        INSERT INTO request_logs (
            created_at, method, target_url, access_key, client_host, request_headers,
            request_body, request_body_truncated, api_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            utc_now(),
            method,
            target_url,
            access_key,
            client_host,
            json.dumps(request_headers, ensure_ascii=False, indent=2),
            request_body,
            int(request_body_truncated),
            api_type_from_log(target_url, request_body, b""),
        ),
        returning_id=True,
    )
    return int(cur.lastrowid)


def update_request_body(log_id: int, target_url: str, request_body: bytes, request_body_truncated: bool) -> None:
    db_execute(
        """
        UPDATE request_logs
        SET request_body = ?, request_body_truncated = ?, api_type = ?
        WHERE id = ?
        """,
        (
            request_body,
            int(request_body_truncated),
            api_type_from_log(target_url, request_body, b""),
            log_id,
        ),
    )


def header_value(headers: dict[str, str] | None, name: str) -> str | None:
    if not headers:
        return None
    wanted = name.lower()
    for key, value in headers.items():
        if key.lower() == wanted:
            value = str(value).strip()
            return value or None
    return None


def quote_pg_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def qualified_table_name(schema: str, table: str) -> str:
    return f"{quote_pg_identifier(schema)}.{quote_pg_identifier(table)}"


def configured_new_api_tables() -> set[str]:
    return {item.lower() for item in NEW_API_LOG_TABLES}


def discover_new_api_log_schema() -> list[dict[str, Any]]:
    global NEW_API_LOG_SCHEMA_CACHE
    if NEW_API_LOG_SCHEMA_CACHE is not None:
        return NEW_API_LOG_SCHEMA_CACHE
    with NEW_API_LOG_SCHEMA_LOCK:
        if NEW_API_LOG_SCHEMA_CACHE is not None:
            return NEW_API_LOG_SCHEMA_CACHE
        if not NEW_API_LOG_DATABASE_URL:
            NEW_API_LOG_SCHEMA_CACHE = []
            return NEW_API_LOG_SCHEMA_CACHE
        configured_tables = configured_new_api_tables()
        with connect_new_api_log_postgres(dict_row) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_schema, table_name, column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
                    ORDER BY table_schema, table_name, ordinal_position
                    """
                )
                rows = cur.fetchall()
        tables: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            schema = row["table_schema"]
            table = row["table_name"]
            table_key = table.lower()
            qualified_key = f"{schema}.{table}".lower()
            if configured_tables and table_key not in configured_tables and qualified_key not in configured_tables:
                continue
            entry = tables.setdefault((schema, table), {"schema": schema, "table": table, "columns": []})
            entry["columns"].append({"name": row["column_name"], "type": row["data_type"]})
        NEW_API_LOG_SCHEMA_CACHE = list(tables.values())
        return NEW_API_LOG_SCHEMA_CACHE


def new_api_request_columns(columns: list[dict[str, str]]) -> list[str]:
    wanted = {item.lower() for item in NEW_API_LOG_REQUEST_ID_COLUMNS}
    return [column["name"] for column in columns if column["name"].lower() in wanted]


def sortable_new_api_column(columns: list[dict[str, str]]) -> str | None:
    names = {column["name"].lower(): column["name"] for column in columns}
    for candidate in ("created_at", "created_time", "createdat", "timestamp", "time", "id"):
        if candidate in names:
            return names[candidate]
    return None


def sanitize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: json_safe(value) if isinstance(value, (bytes, datetime, date, Decimal)) else value for key, value in row.items()}


def query_new_api_log(request_id: str | None) -> dict[str, Any] | None:
    if not request_id:
        return None
    payload: dict[str, Any] = {
        "enabled": bool(NEW_API_LOG_DATABASE_URL),
        "request_id": request_id,
        "queried_at": utc_now(),
        "matches": [],
        "errors": [],
    }
    if not NEW_API_LOG_DATABASE_URL:
        payload["error"] = "NEW_API_LOG_DATABASE_URL 未配置"
        return payload

    started_at = time.perf_counter()
    try:
        schema = discover_new_api_log_schema()
        payload["tables"] = [
            {"table": f"{item['schema']}.{item['table']}", "columns": [column["name"] for column in item["columns"]]}
            for item in schema
        ]
        with connect_new_api_log_postgres(dict_row) as conn:
            with conn.cursor() as cur:
                for table_info in schema:
                    match_columns = new_api_request_columns(table_info["columns"])
                    if not match_columns:
                        continue
                    sort_column = sortable_new_api_column(table_info["columns"])
                    for match_column in match_columns:
                        sql = (
                            f"SELECT * FROM {qualified_table_name(table_info['schema'], table_info['table'])} "
                            f"WHERE CAST({quote_pg_identifier(match_column)} AS TEXT) = %s"
                        )
                        if sort_column:
                            sql += f" ORDER BY {quote_pg_identifier(sort_column)} DESC"
                        sql += " LIMIT %s"
                        try:
                            cur.execute(sql, (request_id, NEW_API_LOG_MAX_ROWS_PER_TABLE))
                            rows = cur.fetchall()
                        except Exception as exc:
                            payload["errors"].append(
                                {
                                    "table": f"{table_info['schema']}.{table_info['table']}",
                                    "column": match_column,
                                    "error": str(exc),
                                }
                            )
                            continue
                        for row in rows:
                            payload["matches"].append(
                                {
                                    "table": f"{table_info['schema']}.{table_info['table']}",
                                    "match_column": match_column,
                                    "data": sanitize_row(dict(row)),
                                }
                            )
        payload["duration_ms"] = int((time.perf_counter() - started_at) * 1000)
    except Exception as exc:
        payload["error"] = str(exc)
        payload["duration_ms"] = int((time.perf_counter() - started_at) * 1000)
    return payload


def extract_new_api_user(payload: dict[str, Any] | None) -> str | None:
    if not payload:
        return None
    user_keys = (
        "username",
        "user_name",
        "user",
        "email",
        "name",
        "token_name",
        "key_name",
        "api_key_name",
        "channel_name",
    )
    id_keys = ("user_id", "userid", "uid", "token_id", "key_id")
    for item in payload.get("matches", []):
        data = item.get("data") or {}
        name = next((str(data[key]).strip() for key in user_keys if data.get(key) not in (None, "")), "")
        user_id = next((str(data[key]).strip() for key in id_keys if data.get(key) not in (None, "")), "")
        if name and user_id:
            return f"{name} #{user_id}"
        if name:
            return name
        if user_id:
            return f"#{user_id}"
    return None


def new_api_log_cache_fields(request_id: str | None) -> tuple[str | None, str | None, str | None]:
    payload = query_new_api_log(request_id)
    if not payload:
        return None, None, None
    user = extract_new_api_user(payload)
    text, clipped = limited_json_dumps(payload)
    error = payload.get("error") or "; ".join(item.get("error", "") for item in payload.get("errors", [])[:3] if item.get("error"))
    if clipped:
        error = (error + "; " if error else "") + "new-api-log cache clipped"
    return user, text, error or None


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
) -> str | None:
    finished_at = finished_at or time.perf_counter()
    upstream_duration_ms = None
    if upstream_started_at is not None:
        upstream_duration_ms = int((finished_at - upstream_started_at) * 1000)
    first_byte_ms = None
    if first_byte_at is not None:
        first_byte_ms = int((first_byte_at - started_at) * 1000)
    response_bytes = bytes(response_body)
    output_tokens = output_tokens_from_body(response_bytes)
    reasoning_tokens = reasoning_tokens_from_body(response_bytes)
    response_api_type = api_type_from_body(response_bytes)
    response_failure = response_failed_from_sse(response_bytes)
    oneapi_request_id = header_value(response_headers, "x-oneapi-request-id")
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
            output_tokens = ?,
            reasoning_tokens = ?,
            api_type = COALESCE(?, api_type),
            oneapi_request_id = ?,
            response_failed = ?,
            response_failure_code = ?,
            response_failure_message = ?
        WHERE id = ?
        """,
        (
            status_code,
            json_dumps(response_headers or {}, indent=2),
            response_bytes,
            int(response_body_truncated),
            error,
            int((finished_at - started_at) * 1000),
            upstream_duration_ms,
            first_byte_ms,
            output_tokens,
            reasoning_tokens,
            response_api_type,
            oneapi_request_id,
            int(response_failure is not None),
            str(response_failure.get("code") or response_failure.get("type") or "response_failed")
            if response_failure
            else None,
            str(response_failure.get("message") or json_dumps(response_failure)) if response_failure else None,
            log_id,
        ),
    )
    return oneapi_request_id


def enrich_new_api_log(log_id: int, request_id: str) -> None:
    new_api_user, new_api_log, new_api_log_error = new_api_log_cache_fields(request_id)
    db_execute(
        """
        UPDATE request_logs
        SET new_api_user = ?, new_api_log = ?, new_api_log_error = ?
        WHERE id = ?
        """,
        (new_api_user, new_api_log, new_api_log_error, log_id),
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


def last_completed_sse_data(text: str) -> str | None:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    marker_index = text.rfind("event: response.completed")
    if marker_index < 0:
        return None
    block = text[marker_index:].split("\n\n", 1)[0]
    data_lines = []
    for raw_line in block.splitlines():
        line = raw_line.rstrip()
        if line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
    return "\n".join(data_lines) if data_lines else None


def parse_completed_response_from_sse(body: bytes) -> object | None:
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        return None
    completed_data = last_completed_sse_data(text)
    if completed_data is None:
        return None
    try:
        return json.loads(completed_data)
    except json.JSONDecodeError:
        return None
    return None


def response_failed_from_sse(body: bytes | bytearray) -> dict[str, Any] | None:
    try:
        text = bytes(body).decode("utf-8")
    except UnicodeDecodeError:
        return None
    for event in reversed(parse_sse_events(text)):
        try:
            payload = json.loads(event["data"])
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        if event["event"] != "response.failed" and payload.get("type") != "response.failed":
            continue
        response = payload.get("response")
        error = response.get("error") if isinstance(response, dict) else payload.get("error")
        if isinstance(error, dict):
            return error
        return {"code": "response_failed", "message": str(error or "OpenAI Responses 请求失败")}
    return None


async def send_dingtalk_response_failed_alert(error: dict[str, Any], request_id: str | None = None) -> None:
    if not DINGTALK_WEBHOOK_URL:
        return
    reason = error.get("code") or error.get("type") or "response_failed"
    message = error.get("message") or json_dumps(error)
    payload = {
        "msgtype": "text",
        "text": {
            "content": (
                "AI Gateway response.failed 通知\n"
                f"Request ID：{request_id or '-'}\n"
                f"失败原因：{reason}\n"
                f"失败信息：{message}"
            ),
        },
    }
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(DINGTALK_WEBHOOK_TIMEOUT_SECONDS),
        trust_env=False,
    ) as client:
        response = await client.post(DINGTALK_WEBHOOK_URL, json=payload)
        response.raise_for_status()
        result = response.json()
        if isinstance(result, dict) and result.get("errcode") not in (None, 0):
            raise RuntimeError(f"DingTalk webhook failed: {result.get('errmsg') or result.get('errcode')}")


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


def api_type_from_target_url(target_url: str) -> str | None:
    normalized_url = target_url.lower()
    if "/chat/completions" in normalized_url:
        return "chat_completions"
    if "/responses" in normalized_url:
        return "responses"
    if "/messages" in normalized_url:
        return "messages"
    return None


def api_type_from_log(target_url: str, request_body: bytes, response_body: bytes) -> str:
    return api_type_from_target_url(target_url) or api_type_from_body(response_body) or api_type_from_body(request_body) or "other"


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


def normalize_access_key(access_key: str | None) -> str | None:
    if not access_key:
        return None
    access_key = access_key.strip("/")
    if not access_key or access_key in RESERVED_ACCESS_KEYS:
        return None
    return access_key


def tps_from_values(output_tokens: int | None, duration_ms: int | None, first_byte_ms: int | None) -> float | None:
    if not output_tokens or duration_ms is None:
        return None
    generation_ms = duration_ms
    if first_byte_ms is not None and duration_ms > first_byte_ms:
        generation_ms = duration_ms - first_byte_ms
    if generation_ms <= 0:
        return None
    return round(output_tokens / (generation_ms / 1000), 2)


def row_to_summary(row: dict[str, Any]) -> dict:
    output_tokens = row["output_tokens"]
    return {
        "id": row["id"],
        "created_at": row["created_at"],
        "method": row["method"],
        "target_url": row["target_url"],
        "access_key": row["access_key"],
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
        "response_failed": bool(row.get("response_failed")),
        "response_failure_code": row.get("response_failure_code"),
        "response_failure_message": row.get("response_failure_message"),
        "request_body_bytes": row["request_body_bytes"] or 0,
        "response_body_bytes": row["response_body_bytes"] or 0,
        "reasoning_tokens": row["reasoning_tokens"],
        "api_type": row["api_type"] or api_type_from_target_url(row["target_url"] or "") or "other",
        "oneapi_request_id": row.get("oneapi_request_id"),
        "new_api_user": row.get("new_api_user"),
        "new_api_log_error": row.get("new_api_log_error"),
        "request_body_truncated": bool(row["request_body_truncated"]),
        "response_body_truncated": bool(row["response_body_truncated"]),
    }


def log_access_key(log_id: int) -> str | None:
    row = db_fetchone("SELECT access_key FROM request_logs WHERE id = ?", (log_id,))
    return row["access_key"] if row else None


def list_log_summaries(limit: int = 100, access_key: str | None = None) -> list[dict]:
    limit = max(1, min(limit, 500))
    access_key = normalize_access_key(access_key)
    where_clause = "access_key IS NULL" if access_key is None else "access_key = ?"
    params = () if access_key is None else (access_key,)
    byte_length = "octet_length" if using_postgres() else "length"
    rows = db_fetchall(
        f"""
        SELECT id, created_at, method, target_url, client_host, response_status,
               access_key, duration_ms, upstream_duration_ms, first_byte_ms,
               output_tokens, reasoning_tokens, api_type, error,
               response_failed, response_failure_code, response_failure_message,
               oneapi_request_id, new_api_user, new_api_log_error,
               {byte_length}(request_body) AS request_body_bytes,
               {byte_length}(response_body) AS response_body_bytes,
               request_body_truncated, response_body_truncated
        FROM request_logs
        WHERE {where_clause}
        ORDER BY id DESC
        LIMIT ?
        """,
        (*params, limit),
    )
    return [row_to_summary(row) for row in rows]


async def broadcast_logs(changed_id: int | None = None, access_key: str | None = None) -> None:
    access_key = normalize_access_key(access_key)
    rows = await asyncio.to_thread(list_log_summaries, 50, access_key)
    await log_socket_manager.broadcast(
        {
            "type": "logs",
            "changed_id": changed_id,
            "rows": rows,
        },
        access_key,
    )


async def announce_created(log_id_task: asyncio.Task[int]) -> None:
    try:
        log_id = await log_id_task
        access_key = await asyncio.to_thread(log_access_key, log_id)
        await broadcast_logs(log_id, access_key)
    except Exception as exc:
        print(f"Failed to announce log creation: {exc}", flush=True)


async def create_log_async_timed(perf_context: dict[str, Any], *args: Any) -> int:
    started_at = time.perf_counter()
    log_id = await asyncio.to_thread(create_log, *args)
    perf_context["gateway_log_id"] = log_id
    perf_context["db_create_ms"] = elapsed_ms(started_at)
    return log_id


async def persist_request_body_async(
    log_id_task: asyncio.Task[int],
    request_body_finished: asyncio.Event,
    target_url: str,
    request_capture: bytearray,
    request_state: dict[str, Any],
    perf_context: dict[str, Any],
) -> None:
    await request_body_finished.wait()
    snapshot = bytes(request_capture)
    log_id = await log_id_task
    started_at = time.perf_counter()
    await asyncio.to_thread(
        update_request_body,
        log_id,
        target_url,
        snapshot,
        bool(request_state.get("truncated")),
    )
    perf_context["request_body_db_ms"] = elapsed_ms(started_at)


async def finish_log_async(
    log_id_task: asyncio.Task[int],
    request_body_task: asyncio.Task[None] | None,
    *args: Any,
    perf_context: dict[str, Any] | None = None,
) -> None:
    perf_context = perf_context or {}
    background_started_at = time.perf_counter()
    try:
        log_id = await log_id_task
        if request_body_task is not None:
            try:
                await request_body_task
            except Exception as exc:
                perf_context["request_body_db_error"] = str(exc)
                print(f"Failed to persist request body for log {log_id}: {exc}", flush=True)
        db_finish_started_at = time.perf_counter()
        request_id = await asyncio.to_thread(finish_log, log_id, *args)
        perf_context["db_finish_ms"] = elapsed_ms(db_finish_started_at)
        failed_response = response_failed_from_sse(args[2]) if len(args) > 2 else None
        if failed_response:
            dingtalk_started_at = time.perf_counter()
            try:
                await send_dingtalk_response_failed_alert(failed_response, request_id)
                perf_context["dingtalk_notify_ms"] = elapsed_ms(dingtalk_started_at)
            except Exception as exc:
                perf_context["dingtalk_notify_error"] = str(exc)
                print(f"Failed to send DingTalk response.failed alert for log {log_id}: {exc}", flush=True)
        access_key = await asyncio.to_thread(log_access_key, log_id)
        broadcast_started_at = time.perf_counter()
        await broadcast_logs(log_id, access_key)
        perf_context["broadcast_ms"] = elapsed_ms(broadcast_started_at)
        if request_id and NEW_API_LOG_DATABASE_URL:
            enrich_started_at = time.perf_counter()
            await asyncio.to_thread(enrich_new_api_log, log_id, request_id)
            perf_context["new_api_enrich_ms"] = elapsed_ms(enrich_started_at)
            await broadcast_logs(log_id, access_key)
        perf_context["background_total_ms"] = elapsed_ms(background_started_at)
        background_slow = bool(perf_context.get("request_body_db_error")) or any(
            int(perf_context.get(field) or 0) >= PERFORMANCE_LOG_THRESHOLD_MS
            for field in ("db_create_ms", "request_body_db_ms", "db_finish_ms", "broadcast_ms", "new_api_enrich_ms")
        )
        if background_slow:
            perf_log(
                "background",
                request_id=request_id,
                gateway_log_id=log_id,
                db_create_ms=perf_context.get("db_create_ms"),
                request_body_db_ms=perf_context.get("request_body_db_ms"),
                request_body_db_error=perf_context.get("request_body_db_error"),
                db_finish_ms=perf_context.get("db_finish_ms"),
                broadcast_ms=perf_context.get("broadcast_ms"),
                new_api_enrich_ms=perf_context.get("new_api_enrich_ms"),
                dingtalk_notify_ms=perf_context.get("dingtalk_notify_ms"),
                dingtalk_notify_error=perf_context.get("dingtalk_notify_error"),
                background_total_ms=perf_context.get("background_total_ms"),
            )
    except Exception as exc:
        print(f"Failed to finish log: {exc}", flush=True)
        perf_log(
            "background_error",
            request_id=perf_context.get("request_id"),
            gateway_log_id=perf_context.get("gateway_log_id"),
            error=str(exc),
            background_total_ms=elapsed_ms(background_started_at),
        )


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
    .brand-title {
      display: flex;
      align-items: center;
      gap: 8px;
      min-width: 0;
    }
    .commit-id {
      max-width: 180px;
      overflow: hidden;
      padding: 2px 7px;
      border: 1px solid var(--line);
      border-radius: 999px;
      color: var(--muted);
      background: rgba(5, 7, 11, .34);
      font: 10px/1.4 ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      text-overflow: ellipsis;
      white-space: nowrap;
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
    button:disabled {
      cursor: wait;
      opacity: .55;
      color: var(--muted);
      box-shadow: none;
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
    .lookup-form {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 6px;
    }
    .lookup-form input[type="search"] { height: 40px; }
    .lookup-form button { min-height: 40px; padding: 0 12px; }
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
    .item.response-failed {
      border-color: rgba(255, 92, 115, .44);
      background: linear-gradient(90deg, rgba(255, 92, 115, .14), rgba(255, 92, 115, .035) 62%);
      box-shadow: inset 4px 0 0 var(--bad);
    }
    .item.response-failed:hover {
      border-color: rgba(255, 92, 115, .66);
      background: linear-gradient(90deg, rgba(255, 92, 115, .19), rgba(25, 34, 49, .76) 68%);
    }
    .item.response-failed.active {
      border-color: rgba(255, 92, 115, .82);
      background: linear-gradient(90deg, rgba(255, 92, 115, .24), rgba(53, 183, 255, .07) 72%);
      box-shadow: inset 4px 0 0 var(--bad), 0 12px 28px rgba(0, 0, 0, .24);
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
    .badge.response-failed-badge {
      color: #fff1f3;
      border-color: rgba(255, 92, 115, .72);
      background: rgba(190, 24, 55, .72);
      letter-spacing: .035em;
      box-shadow: 0 0 0 1px rgba(255, 92, 115, .08), 0 5px 16px rgba(190, 24, 55, .22);
    }
    .badge.api-type {
      color: var(--accent-2);
      border-color: rgba(53, 183, 255, .34);
      background: rgba(53, 183, 255, .08);
    }
    .badge.requester {
      max-width: 150px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      color: #fde68a;
      border-color: rgba(253, 230, 138, .28);
      background: rgba(253, 230, 138, .07);
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
    .detail-head.response-failed {
      border-color: rgba(255, 92, 115, .62);
      background:
        linear-gradient(135deg, rgba(255, 92, 115, .13), rgba(25, 34, 49, .94) 42%, rgba(12, 17, 26, .94)),
        var(--panel);
      box-shadow: inset 4px 0 0 var(--bad), 0 18px 44px rgba(0, 0, 0, .28);
    }
    .response-failure-banner {
      display: grid;
      grid-template-columns: auto minmax(0, 1fr);
      gap: 10px 14px;
      align-items: start;
      padding: 12px 14px;
      border: 1px solid rgba(255, 92, 115, .52);
      border-radius: 11px;
      background: rgba(130, 15, 38, .28);
      color: #ffe4e8;
    }
    .response-failure-title {
      font-size: 12px;
      font-weight: 900;
      letter-spacing: .035em;
      white-space: nowrap;
    }
    .response-failure-content {
      min-width: 0;
      display: grid;
      gap: 4px;
      font-size: 13px;
      line-height: 1.5;
      overflow-wrap: anywhere;
    }
    .response-failure-code {
      color: #ffb6c1;
      font: 700 12px/1.4 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
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
    .endpoint-context {
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 6px 12px;
      color: var(--muted);
      font: 12px/1.4 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      overflow-wrap: anywhere;
    }
    .endpoint-actions {
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 8px;
      flex-wrap: wrap;
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
    dialog.data-dialog {
      width: min(1040px, calc(100vw - 32px));
      height: min(780px, calc(100dvh - 32px));
      padding: 0;
      border: 1px solid var(--line-strong);
      border-radius: 12px;
      background: var(--panel);
      color: var(--text);
      box-shadow: 0 28px 90px rgba(0, 0, 0, .62);
      overflow: hidden;
    }
    dialog.data-dialog::backdrop { background: rgba(0, 0, 0, .72); }
    .dialog-shell {
      height: 100%;
      display: grid;
      grid-template-rows: auto minmax(0, 1fr);
      overflow: hidden;
    }
    .dialog-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
      background: var(--panel-raised);
    }
    .dialog-head h2 { font-size: 15px; }
    .dialog-head button { min-height: 44px; }
    .dialog-content {
      min-height: 0;
      overflow: auto;
      padding: 14px;
    }
    .dialog-content .json-viewer { max-height: none; }
    .parse-input-button {
      min-height: 44px;
      border-color: rgba(53, 183, 255, .34);
      color: #bde9ff;
      background: rgba(53, 183, 255, .09);
    }
    .parse-input-button:hover {
      border-color: rgba(53, 183, 255, .58);
      color: #e4f7ff;
      background: rgba(53, 183, 255, .15);
      box-shadow: 0 0 0 3px rgba(53, 183, 255, .07);
    }
    .input-dialog-intro {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 16px;
      padding: 14px 16px;
      border: 1px solid rgba(53, 183, 255, .28);
      border-radius: 12px;
      background: linear-gradient(135deg, rgba(53, 183, 255, .10), rgba(39, 209, 127, .055));
    }
    .input-dialog-intro h3 {
      margin: 0 0 5px;
      color: var(--text);
      font-size: 14px;
      letter-spacing: 0;
      text-transform: none;
    }
    .input-dialog-intro p {
      max-width: 680px;
      margin: 0;
      color: var(--muted);
      font-size: 12px;
    }
    .input-summary {
      display: flex;
      justify-content: flex-end;
      gap: 6px;
      flex: 0 0 auto;
      flex-wrap: wrap;
    }
    .input-summary .pill { min-height: 26px; }
    .input-timeline {
      position: relative;
      display: grid;
      gap: 12px;
    }
    .input-timeline::before {
      content: "";
      position: absolute;
      top: 18px;
      bottom: 18px;
      left: 17px;
      width: 1px;
      background: linear-gradient(var(--accent-2), rgba(39, 209, 127, .28));
    }
    .input-item {
      position: relative;
      display: grid;
      grid-template-columns: 36px minmax(0, 1fr);
      gap: 10px;
      align-items: start;
    }
    .input-item-index {
      position: relative;
      z-index: 1;
      display: grid;
      place-items: center;
      width: 36px;
      height: 36px;
      border: 1px solid rgba(53, 183, 255, .48);
      border-radius: 999px;
      color: #dff6ff;
      background: #101824;
      box-shadow: 0 0 0 4px var(--panel), 0 0 18px rgba(53, 183, 255, .12);
      font: 700 11px/1 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-variant-numeric: tabular-nums;
    }
    .input-item.tool .input-item-index {
      border-color: rgba(245, 184, 75, .52);
      color: #fff0c7;
    }
    .input-item.output .input-item-index {
      border-color: rgba(51, 209, 122, .48);
      color: #d9fbe7;
    }
    .input-item-card {
      min-width: 0;
      overflow: hidden;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(5, 7, 11, .34);
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, .025);
    }
    .input-item-head {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
      padding: 11px 12px;
      border-bottom: 1px solid var(--line);
      background: rgba(25, 34, 49, .78);
    }
    .input-item-title {
      min-width: 0;
      color: var(--text);
      font-size: 13px;
      font-weight: 760;
      overflow-wrap: anywhere;
    }
    .input-item-meta {
      display: flex;
      justify-content: flex-end;
      gap: 5px;
      flex-wrap: wrap;
    }
    .input-item-meta .pill {
      min-height: 22px;
      padding: 0 7px;
      font-size: 10px;
    }
    .input-item-body {
      display: grid;
      gap: 10px;
      padding: 12px;
    }
    .input-content-list,
    .input-field-list {
      display: grid;
      gap: 8px;
    }
    .input-content-block,
    .input-field {
      min-width: 0;
      overflow: hidden;
      border: 1px solid rgba(61, 75, 99, .62);
      border-radius: 10px;
      background: rgba(16, 21, 31, .7);
    }
    .input-content-label,
    .input-field-label {
      padding: 7px 10px;
      border-bottom: 1px solid rgba(61, 75, 99, .52);
      color: var(--muted);
      background: rgba(25, 34, 49, .62);
      font: 700 10px/1.4 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      letter-spacing: .025em;
      overflow-wrap: anywhere;
    }
    .input-content-value,
    .input-field-value {
      min-width: 0;
      padding: 10px;
      color: var(--muted-strong);
      overflow-wrap: anywhere;
    }
    .input-content-value.text,
    .input-field-value.text {
      color: var(--text);
      font: 13px/1.65 Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .input-content-value > .json-viewer,
    .input-field-value > .json-viewer {
      margin: -10px;
      border: 0;
      border-radius: 0;
    }
    details.input-raw {
      border-top: 1px solid var(--line);
      background: rgba(5, 7, 11, .24);
    }
    details.input-raw summary {
      min-height: 44px;
      padding: 0 12px;
      display: flex;
      align-items: center;
      gap: 7px;
      color: var(--muted);
      cursor: pointer;
      font-size: 11px;
      font-weight: 700;
      list-style: none;
    }
    details.input-raw summary::-webkit-details-marker { display: none; }
    details.input-raw summary::before {
      content: "▶";
      color: var(--accent-2);
      font-size: 9px;
    }
    details.input-raw[open] summary::before { content: "▼"; }
    .input-raw-content { padding: 0 12px 12px; }
    .input-raw-content .json-viewer { max-height: 460px; }
    .record-list {
      display: grid;
      gap: 12px;
    }
    .record-block {
      border: 1px solid var(--line);
      border-radius: 10px;
      background: rgba(5, 7, 11, .34);
      overflow: hidden;
    }
    .record-title {
      margin: 0;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      background: var(--panel-raised);
      color: var(--muted-strong);
      font-size: 12px;
      font-weight: 750;
    }
    .record-block .kv { border: 0; border-radius: 0; }
    .record-block .kv-row { grid-template-columns: minmax(150px, 24%) minmax(0, 1fr); }
    .record-block .kv-value .json-viewer {
      margin: -4px;
      max-height: 320px;
    }
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
      .endpoint-actions { justify-content: flex-start; }
      .metric-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .kv-row { grid-template-columns: 1fr; }
      .record-block .kv-row { grid-template-columns: 1fr; }
      .kv-key { border-right: 0; border-bottom: 1px solid var(--line); }
      .input-dialog-intro { display: grid; }
      .input-summary { justify-content: flex-start; }
      .input-item-head { display: grid; }
      .input-item-meta { justify-content: flex-start; }
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
          <div class="brand-title">
            <h1 translate="no">AI Gateway</h1>
            <span class="commit-id" title="部署 Commit：__APP_COMMIT_FULL__">commit __APP_COMMIT_SHORT__</span>
          </div>
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
          <div class="type-filter">
            <div class="type-filter-title">Request ID 反查</div>
            <form class="lookup-form" id="lookupForm">
              <input id="lookupInput" name="request-id" type="search" autocomplete="off" placeholder="x-oneapi-request-id" aria-label="Request ID" />
              <button type="submit">查询</button>
            </form>
          </div>
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
  <dialog class="data-dialog" id="dataDialog" aria-labelledby="dialogTitle">
    <div class="dialog-shell">
      <div class="dialog-head">
        <h2 id="dialogTitle">详情</h2>
        <button id="dialogClose" type="button">关闭</button>
      </div>
      <div class="dialog-content" id="dialogContent"></div>
    </div>
  </dialog>
  <script>
    const listEl = document.getElementById('list');
    const detailEl = document.getElementById('detail');
    const searchEl = document.getElementById('search');
    const typeFilterButtons = Array.from(document.querySelectorAll('[data-api-type-filter]'));
    const liveTextEl = document.getElementById('liveText');
    const totalCountEl = document.getElementById('totalCount');
    const successCountEl = document.getElementById('successCount');
    const errorCountEl = document.getElementById('errorCount');
    const lookupForm = document.getElementById('lookupForm');
    const lookupInput = document.getElementById('lookupInput');
    const lookupSubmit = lookupForm.querySelector('button[type="submit"]');
    const dataDialog = document.getElementById('dataDialog');
    const dialogTitle = document.getElementById('dialogTitle');
    const dialogContent = document.getElementById('dialogContent');
    const dateFormatter = new Intl.DateTimeFormat(navigator.languages, {
      month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit'
    });
    const numberFormatter = new Intl.NumberFormat(navigator.languages);
    const byteFormatter = new Intl.NumberFormat(navigator.languages, { maximumFractionDigits: 1 });
    const pathAccessKey = decodeURIComponent(window.location.pathname.split('/')[1] || '');
    const apiBase = pathAccessKey ? `/${encodeURIComponent(pathAccessKey)}` : '';
    let activeId = null;
    let activeTab = 'request';
    let rowsCache = [];
    let activeRequestBodyView = 'json';
    let activeResponseBodyView = 'json';
    let activeDetailPending = false;
    let activeApiTypeFilter = '';
    let logSocket = null;
    let reconnectTimer = null;
    const JSON_PARSE_MAX_CHARS = 2_000_000;
    const JSON_TREE_MAX_CHARS = 450_000;
    const JSON_NODE_LIMIT = 1_500;
    const JSON_AUTO_OPEN_DEPTH = 1;
    const TEXT_RENDER_LIMIT = 320_000;

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
      return row.error || row.response_failed ? 'err' : statusClass(row.response_status);
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
      if (text.length > JSON_PARSE_MAX_CHARS) return text;
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

    function displayText(text, limit = TEXT_RENDER_LIMIT) {
      const value = String(text ?? '');
      if (value.length <= limit) return value;
      const headLength = Math.floor(limit * 0.72);
      const tailLength = Math.max(0, limit - headLength);
      return `${value.slice(0, headLength)}\n\n... 已省略 ${numberFormatter.format(value.length - limit)} 个字符，复制 Body 可获取完整内容 ...\n\n${value.slice(value.length - tailLength)}`;
    }

    function renderPreText(text, attrs = '') {
      const attrText = attrs ? ` ${attrs}` : '';
      return `<pre${attrText} translate="no">${esc(displayText(text || '(empty)'))}</pre>`;
    }

    function renderJsonValue(value, key = '', depth = 0, state = { count: 0, clipped: false }) {
      state.count += 1;
      if (state.count > JSON_NODE_LIMIT) {
        if (state.clipped) return '';
        state.clipped = true;
        return '<div class="json-leaf json-preview">已省略后续节点，复制 Body 可获取完整内容</div>';
      }
      const keyHtml = key === '' ? '' : `<span class="json-key">${esc(key)}:</span>`;
      if (value && typeof value === 'object') {
        const isArray = Array.isArray(value);
        const openAttr = depth <= JSON_AUTO_OPEN_DEPTH ? ' open' : '';
        let childMarkup = '';
        let hasEntries = false;
        if (isArray) {
          for (let index = 0; index < value.length; index += 1) {
            hasEntries = true;
            childMarkup += renderJsonValue(value[index], String(index), depth + 1, state);
            if (state.clipped) break;
          }
        } else {
          for (const childKey in value) {
            if (!Object.prototype.hasOwnProperty.call(value, childKey)) continue;
            hasEntries = true;
            childMarkup += renderJsonValue(value[childKey], childKey, depth + 1, state);
            if (state.clipped) break;
          }
        }
        return `
          <details class="json-node"${openAttr}>
            <summary>${keyHtml}<span class="json-type">${isArray ? 'Array' : 'Object'}</span><span class="json-preview">${esc(jsonSummary(value))}</span></summary>
            <div class="json-children">
              ${hasEntries ? childMarkup : '<div class="json-leaf json-preview">(empty)</div>'}
            </div>
          </details>
        `;
      }
      return `<div class="json-leaf">${keyHtml}<span class="${primitiveClass(value)}">${esc(primitivePreview(value))}</span></div>`;
    }

    function renderBodyContent(text) {
      const body = text || '';
      if (!body.trim()) return '<pre translate="no">(empty)</pre>';
      if (body.length > JSON_PARSE_MAX_CHARS) {
        return renderPreText(`内容较大，已切换为文本预览。\n\n${body}`);
      }
      try {
        const parsed = JSON.parse(body);
        const intro = body.length > JSON_TREE_MAX_CHARS
          ? '<div class="json-leaf json-preview">JSON 较大，仅渲染部分节点，复制 Body 可获取完整内容</div>'
          : '';
        return `<div class="json-viewer" translate="no">${intro}${renderJsonValue(parsed)}</div>`;
      } catch {
        return renderPreText(body);
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

    function lastCompletedSseData(text) {
      const source = String(text || '');
      const marker = 'event: response.completed';
      const markerIndex = source.lastIndexOf(marker);
      if (markerIndex < 0) return '';
      const block = source.slice(markerIndex).split(/\\r?\\n\\r?\\n/, 1)[0];
      const dataLines = [];
      for (const rawLine of block.split(/\\r?\\n/)) {
        const line = rawLine.trimEnd();
        if (line.startsWith('data:')) dataLines.push(line.slice(5).trimStart());
      }
      return dataLines.join('\\n');
    }

    function tryParseJson(text) {
      try {
        return JSON.parse(text);
      } catch {
        return null;
      }
    }

    function joinTextParts(parts) {
      return parts
        .map(item => String(item ?? '').trim())
        .filter(Boolean)
        .join('\\n');
    }

    function textFromContent(content) {
      if (content === null || content === undefined) return '';
      if (typeof content === 'string') return content;
      if (typeof content === 'number' || typeof content === 'boolean') return String(content);
      if (Array.isArray(content)) return joinTextParts(content.map(item => textFromContent(item)));
      if (typeof content !== 'object') return '';

      const directKeys = ['text', 'output_text', 'input_text', 'value', 'refusal', 'completion'];
      for (const key of directKeys) {
        if (typeof content[key] === 'string') return content[key];
      }
      if (typeof content.delta === 'string') return content.delta;
      if (content.delta && typeof content.delta.text === 'string') return content.delta.text;
      if (content.delta && typeof content.delta.content === 'string') return content.delta.content;
      if (content.content !== undefined) return textFromContent(content.content);
      if (content.message !== undefined) return textFromMessage(content.message);
      return '';
    }

    function textFromMessage(message) {
      if (message === null || message === undefined) return '';
      if (typeof message !== 'object') return textFromContent(message);
      const candidates = [
        message.content,
        message.text,
        message.output_text,
        message.input,
        message.message,
        message.completion,
        message.refusal,
      ];
      for (const candidate of candidates) {
        if (candidate === undefined || candidate === null) continue;
        const text = textFromContent(candidate);
        if (text.trim()) return text;
      }
      return '';
    }

    function latestTextFromRole(items, role) {
      if (!Array.isArray(items)) return '';
      for (let index = items.length - 1; index >= 0; index -= 1) {
        const item = items[index];
        const itemRole = item && typeof item === 'object' ? (item.role || item.author?.role) : '';
        if (itemRole === role || (role === 'user' && itemRole === 'human')) {
          const text = textFromMessage(item);
          if (text.trim()) return text;
        }
      }
      return '';
    }

    function latestUserTextFromRequestBody(text) {
      const body = tryParseJson(text);
      if (!body) return '';

      const messageLists = [body.messages, body.input, body.contents].filter(Array.isArray);
      for (const list of messageLists) {
        const textFromUser = latestTextFromRole(list, 'user');
        if (textFromUser.trim()) return textFromUser;
      }

      if (typeof body.input === 'string') return body.input;
      const directCandidates = [body.prompt, body.query, body.message, body.content];
      for (const candidate of directCandidates) {
        const candidateText = textFromContent(candidate);
        if (candidateText.trim()) return candidateText;
      }

      for (const list of messageLists) {
        for (let index = list.length - 1; index >= 0; index -= 1) {
          const fallbackText = textFromMessage(list[index]);
          if (fallbackText.trim()) return fallbackText;
        }
      }
      return '';
    }

    function latestAssistantTextFromPayload(payload) {
      if (payload === null || payload === undefined) return '';
      if (typeof payload === 'string') return payload;
      if (Array.isArray(payload)) {
        for (let index = payload.length - 1; index >= 0; index -= 1) {
          const text = latestAssistantTextFromPayload(payload[index]);
          if (text.trim()) return text;
        }
        return '';
      }
      if (typeof payload !== 'object') return '';

      if (payload.response) {
        const responseText = latestAssistantTextFromPayload(payload.response);
        if (responseText.trim()) return responseText;
      }
      if (typeof payload.output_text === 'string' && payload.output_text.trim()) return payload.output_text;

      if (Array.isArray(payload.choices)) {
        for (let index = payload.choices.length - 1; index >= 0; index -= 1) {
          const choice = payload.choices[index];
          const text = textFromMessage(choice?.message || choice?.delta || choice);
          if (text.trim()) return text;
        }
      }

      if (Array.isArray(payload.output)) {
        for (let index = payload.output.length - 1; index >= 0; index -= 1) {
          const item = payload.output[index];
          const text = textFromMessage(item);
          if (text.trim()) return text;
        }
      }

      if (payload.role === 'assistant' || payload.role === 'model' || payload.type === 'message') {
        const text = textFromMessage(payload);
        if (text.trim()) return text;
      }

      const fallback = textFromMessage(payload);
      return fallback.trim() ? fallback : '';
    }

    function latestAssistantTextFromSse(text) {
      const completedData = lastCompletedSseData(text);
      if (completedData && completedData.length <= JSON_PARSE_MAX_CHARS) {
        const completedText = latestAssistantTextFromPayload(tryParseJson(completedData));
        if (completedText.trim()) return completedText;
      }
      if (completedData && completedData.length > JSON_PARSE_MAX_CHARS) return '';

      const events = parseSseEvents(text);
      if (!events.length) return '';

      for (let index = events.length - 1; index >= 0; index -= 1) {
        const item = events[index];
        if (item.event === 'response.completed' || item.event === 'message_stop') {
          const parsed = tryParseJson(item.data);
          const text = latestAssistantTextFromPayload(parsed);
          if (text.trim()) return text;
        }
      }

      const chunks = [];
      for (const item of events) {
        if (item.data === '[DONE]') continue;
        const parsed = tryParseJson(item.data);
        if (!parsed) continue;

        const choiceDelta = parsed.choices?.[0]?.delta?.content || parsed.choices?.[0]?.message?.content || '';
        if (choiceDelta) {
          chunks.push(textFromContent(choiceDelta));
          continue;
        }

        if (parsed.type === 'response.output_text.delta' || item.event === 'response.output_text.delta') {
          if (typeof parsed.delta === 'string') chunks.push(parsed.delta);
          else if (typeof parsed.delta?.text === 'string') chunks.push(parsed.delta.text);
          continue;
        }

        if (parsed.type === 'content_block_delta' || item.event === 'content_block_delta') {
          if (typeof parsed.delta?.text === 'string') chunks.push(parsed.delta.text);
          continue;
        }

        const deltaText = textFromContent(parsed.delta);
        if (deltaText.trim()) chunks.push(deltaText);
      }

      const streamedText = chunks.join('');
      if (streamedText.trim()) return streamedText;

      for (let index = events.length - 1; index >= 0; index -= 1) {
        const parsed = tryParseJson(events[index].data);
        const text = latestAssistantTextFromPayload(parsed);
        if (text.trim()) return text;
      }
      return '';
    }

    function latestAssistantTextFromResponseBody(text) {
      const sseText = latestAssistantTextFromSse(text);
      if (sseText.trim()) return sseText;
      return latestAssistantTextFromPayload(tryParseJson(text));
    }

    function completedResponseJsonFromSse(text) {
      const completedData = lastCompletedSseData(text);
      if (completedData) {
        if (completedData.length > JSON_PARSE_MAX_CHARS) return completedData;
        const parsed = tryParseJson(completedData);
        return parsed ? JSON.stringify(parsed, null, 2) : completedData;
      }

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
        return `${row.method} ${row.target_url} ${statusLabel(row)} ${row.error ?? ''} ${apiTypeLabel(row.api_type)} ${row.oneapi_request_id ?? ''} ${row.new_api_user ?? ''} ${row.response_failure_code ?? ''} ${row.response_failure_message ?? ''} ${row.response_failed ? 'response failed 失败' : ''}`.toLowerCase().includes(q);
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
      const success = rows.filter(row => !row.response_failed && row.response_status >= 200 && row.response_status < 400).length;
      const errors = rows.filter(row => row.error || row.response_failed || row.response_status >= 400).length;
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
        <button class="item ${row.response_failed ? 'response-failed' : ''} ${row.id === activeId ? 'active' : ''}" type="button" data-id="${row.id}" aria-current="${row.id === activeId ? 'true' : 'false'}">
          <div class="meta">
            <span class="method" translate="no">${esc(row.method)}</span>
            <span class="badge status ${statusClassForRow(row)}">${esc(statusLabel(row))}</span>
            <span class="badge api-type">${esc(apiTypeLabel(row.api_type))}</span>
            ${row.response_failed ? '<span class="badge response-failed-badge">RESPONSE FAILED</span>' : ''}
            ${row.reasoning_tokens === 516 ? '<span class="badge anomaly" title="reasoning_tokens 异常">516</span>' : ''}
            ${row.new_api_user ? `<span class="badge requester" title="${esc(row.new_api_user)}">${esc(row.new_api_user)}</span>` : ''}
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
      try {
        const res = await fetch(`${apiBase}/api/logs?limit=50`);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const rows = await res.json();
        applyRows(rows);
        const params = new URLSearchParams(window.location.search);
        const selectedFromUrl = Number(params.get('id')) || null;
        const selected = selectedFromUrl || activeId;
        const nextTab = params.get('tab');
        if (nextTab) activeTab = nextTab;
        if (refreshDetail && selected && rowsCache.some(row => row.id === selected)) {
          await loadDetail(selected, false);
        } else if (!rowsCache.length) {
          detailEl.innerHTML = '<div class="empty">暂无记录</div>';
        } else if (!activeId) {
          detailEl.innerHTML = '<div class="empty">选择左侧请求查看详情</div>';
        }
        liveTextEl.textContent = `最近更新 ${dateFormatter.format(new Date())}`;
      } catch (error) {
        liveTextEl.textContent = `加载失败：${String(error.message || error)}`;
        if (!rowsCache.length) {
          detailEl.innerHTML = '<div class="empty">请求列表加载失败，请稍后重试</div>';
        }
      }
    }

    function connectLogSocket() {
      if (logSocket && (logSocket.readyState === WebSocket.OPEN || logSocket.readyState === WebSocket.CONNECTING)) return;
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      logSocket = new WebSocket(`${protocol}//${window.location.host}${apiBase}/ws/logs`);
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

    function setRequestBodyView(view) {
      activeRequestBodyView = view;
      detailEl.querySelectorAll('[data-request-body-view]').forEach(panel => {
        panel.hidden = panel.dataset.requestBodyView !== view;
      });
      detailEl.querySelectorAll('[data-request-view-button]').forEach(button => {
        const isActive = button.dataset.requestViewButton === view;
        button.classList.toggle('active', isActive);
        button.setAttribute('aria-pressed', String(isActive));
      });
    }

    function legacyCopyText(text) {
      const textarea = document.createElement('textarea');
      textarea.value = text || '';
      textarea.setAttribute('readonly', '');
      textarea.style.position = 'fixed';
      textarea.style.left = '-9999px';
      textarea.style.top = '0';
      textarea.style.opacity = '0';
      document.body.appendChild(textarea);

      const selection = document.getSelection();
      const selectedRange = selection && selection.rangeCount ? selection.getRangeAt(0) : null;
      textarea.focus();
      textarea.select();
      textarea.setSelectionRange(0, textarea.value.length);

      let copied = false;
      try {
        copied = document.execCommand('copy');
      } finally {
        document.body.removeChild(textarea);
        if (selection) {
          selection.removeAllRanges();
          if (selectedRange) selection.addRange(selectedRange);
        }
      }
      if (!copied) throw new Error('legacy copy failed');
    }

    async function copyText(text) {
      const value = text || '';
      try {
        if (navigator.clipboard?.writeText && window.isSecureContext) {
          await navigator.clipboard.writeText(value);
        } else {
          legacyCopyText(value);
        }
        liveTextEl.textContent = '已复制到剪贴板';
      } catch {
        try {
          legacyCopyText(value);
          liveTextEl.textContent = '已复制到剪贴板';
        } catch {
          liveTextEl.textContent = '复制失败，当前浏览器不允许访问剪贴板';
        }
      }
    }

    function openDataDialog(title, loadingText = '正在加载…') {
      dialogTitle.textContent = title;
      dialogContent.innerHTML = `<div class="empty">${esc(loadingText)}</div>`;
      if (typeof dataDialog.showModal === 'function') {
        if (!dataDialog.open) dataDialog.showModal();
      } else {
        dataDialog.setAttribute('open', '');
      }
    }

    const responsesInputTypeLabels = {
      message: '消息',
      reasoning: '推理',
      function_call: '函数调用',
      function_call_output: '函数结果',
      custom_tool_call: '自定义工具调用',
      custom_tool_call_output: '自定义工具结果',
      web_search_call: '网页搜索',
      file_search_call: '文件搜索',
      computer_call: '计算机调用',
      computer_call_output: '计算机调用结果',
      code_interpreter_call: '代码解释器调用',
      image_generation_call: '图片生成调用',
      local_shell_call: '本地 Shell 调用',
      local_shell_call_output: '本地 Shell 结果',
      shell_call: 'Shell 调用',
      shell_call_output: 'Shell 结果',
      mcp_call: 'MCP 调用',
      mcp_approval_request: 'MCP 授权请求',
      mcp_approval_response: 'MCP 授权结果',
      item_reference: '条目引用',
    };

    const responsesRoleLabels = {
      system: 'System',
      developer: 'Developer',
      user: 'User',
      assistant: 'Assistant',
      tool: 'Tool',
    };

    function responsesInputTypeLabel(type) {
      const value = String(type || 'item');
      return responsesInputTypeLabels[value] || value;
    }

    function responsesInputCategory(item) {
      if (!item || typeof item !== 'object' || Array.isArray(item)) return 'other';
      const type = String(item.type || '').toLowerCase();
      if (type === 'message' || item.role) return 'message';
      if (type.endsWith('_output') || type.endsWith('_response') || type.includes('result')) return 'output';
      if (/(call|tool|search|computer|shell|interpreter|generation|mcp)/.test(type)) return 'tool';
      return 'other';
    }

    function responsesInputItemTitle(item) {
      if (!item || typeof item !== 'object' || Array.isArray(item)) return 'Input 值';
      const category = responsesInputCategory(item);
      const type = String(item.type || (item.role ? 'message' : 'item'));
      if (category === 'message') {
        const role = responsesRoleLabels[item.role] || item.role || 'Unknown';
        return `${role} 消息`;
      }
      const name = item.name || item.action?.type || item.action?.name;
      return `${responsesInputTypeLabel(type)}${name ? ` · ${name}` : ''}`;
    }

    function responsesInputMeta(item) {
      if (!item || typeof item !== 'object' || Array.isArray(item)) return '';
      const values = [
        item.type ? ['type', item.type] : null,
        item.role ? ['role', item.role] : null,
        item.status ? ['status', item.status] : null,
        item.call_id ? ['call', item.call_id] : null,
        item.id ? ['id', item.id] : null,
      ].filter(Boolean);
      return values.map(([label, value]) => `<span class="pill" title="${esc(value)}">${esc(label)}: ${esc(value)}</span>`).join('');
    }

    function renderResponsesInputValue(value, key = '') {
      if (value === null || value === undefined) return '<span class="secondary">null</span>';
      if (typeof value === 'string') {
        const trimmed = value.trim();
        if ((trimmed.startsWith('{') || trimmed.startsWith('[')) && value.length <= JSON_PARSE_MAX_CHARS) {
          const parsed = tryParseJson(value);
          if (parsed !== null) return renderBodyContent(JSON.stringify(parsed, null, 2));
        }
        const textClass = ['text', 'output', 'input_text', 'refusal', 'content'].includes(key) ? ' text' : '';
        return `<div class="input-field-value${textClass}" translate="no">${esc(displayText(value, 80_000))}</div>`;
      }
      if (typeof value === 'number' || typeof value === 'boolean') {
        return `<div class="input-field-value" translate="no">${esc(value)}</div>`;
      }
      return `<div class="input-field-value">${renderBodyContent(JSON.stringify(value, null, 2))}</div>`;
    }

    function renderResponsesContentBlock(block, index) {
      if (typeof block === 'string') {
        return `
          <div class="input-content-block">
            <div class="input-content-label">内容 ${numberFormatter.format(index + 1)} · input_text</div>
            <div class="input-content-value text" translate="no">${esc(displayText(block, 80_000))}</div>
          </div>
        `;
      }
      if (block === null || block === undefined || typeof block !== 'object') {
        return `
          <div class="input-content-block">
            <div class="input-content-label">内容 ${numberFormatter.format(index + 1)}</div>
            <div class="input-content-value" translate="no">${esc(String(block))}</div>
          </div>
        `;
      }
      const type = block.type || 'content';
      const fields = Object.entries(block).filter(([key]) => key !== 'type');
      return `
        <div class="input-content-block">
          <div class="input-content-label">内容 ${numberFormatter.format(index + 1)} · ${esc(type)}</div>
          <div class="input-content-value">
            ${fields.length ? `<div class="input-field-list">${fields.map(([key, value]) => `
              <div class="input-field">
                <div class="input-field-label">${esc(key)}</div>
                ${renderResponsesInputValue(value, key)}
              </div>
            `).join('')}</div>` : '<span class="secondary">没有附加内容</span>'}
          </div>
        </div>
      `;
    }

    function renderResponsesMessageContent(content) {
      const blocks = Array.isArray(content) ? content : [content];
      if (!blocks.length) return '<div class="empty">消息内容为空</div>';
      return `<div class="input-content-list">${blocks.map(renderResponsesContentBlock).join('')}</div>`;
    }

    function renderResponsesInputFields(item, excludedKeys = new Set()) {
      if (!item || typeof item !== 'object' || Array.isArray(item)) {
        return `<div class="input-field-list"><div class="input-field">${renderResponsesInputValue(item, 'content')}</div></div>`;
      }
      const fields = Object.entries(item).filter(([key]) => !excludedKeys.has(key));
      if (!fields.length) return '';
      return `<div class="input-field-list">${fields.map(([key, value]) => `
        <div class="input-field">
          <div class="input-field-label">${esc(key)}</div>
          ${renderResponsesInputValue(value, key)}
        </div>
      `).join('')}</div>`;
    }

    function renderResponsesInputItem(item, index) {
      const category = responsesInputCategory(item);
      const isMessage = category === 'message' && item && typeof item === 'object' && !Array.isArray(item);
      const excluded = new Set([
        'type',
        'role',
        'status',
        'call_id',
        'id',
        'internal_chat_message_metadata_passthrough',
      ]);
      if (isMessage) excluded.add('content');
      const rawJson = JSON.stringify(item, null, 2) ?? String(item);
      return `
        <article class="input-item ${esc(category)}">
          <div class="input-item-index" aria-label="第 ${numberFormatter.format(index + 1)} 项">${numberFormatter.format(index + 1)}</div>
          <div class="input-item-card">
            <div class="input-item-head">
              <div class="input-item-title">${esc(responsesInputItemTitle(item))}</div>
              <div class="input-item-meta">${responsesInputMeta(item)}</div>
            </div>
            <div class="input-item-body">
              ${isMessage ? renderResponsesMessageContent(item.content) : ''}
              ${renderResponsesInputFields(item, excluded)}
              ${isMessage && item.content === undefined ? '<div class="empty">消息中没有 content 字段</div>' : ''}
            </div>
            <details class="input-raw">
              <summary>查看原始 JSON</summary>
              <div class="input-raw-content">${renderBodyContent(rawJson)}</div>
            </details>
          </div>
        </article>
      `;
    }

    function openResponsesInputDialog(requestBodyText, requestBodyTruncated = false) {
      openDataDialog('OpenAI Responses · Input 解析', '正在解析 Input…');
      const payload = tryParseJson(requestBodyText);
      if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
        dialogContent.innerHTML = '<div class="empty">Request Body 不是有效的 JSON 对象，无法解析 Responses Input。</div>';
        return;
      }
      if (!Object.prototype.hasOwnProperty.call(payload, 'input')) {
        dialogContent.innerHTML = '<div class="empty">Request Body 中没有找到 input 字段。</div>';
        return;
      }
      const inputIsArray = Array.isArray(payload.input);
      const items = inputIsArray ? payload.input : [payload.input];
      const counts = items.reduce((result, item) => {
        const category = responsesInputCategory(item);
        result[category] = (result[category] || 0) + 1;
        return result;
      }, {});
      const summary = [
        ['消息', counts.message],
        ['工具调用', counts.tool],
        ['工具结果', counts.output],
        ['其他', counts.other],
      ].filter(([, count]) => count).map(([label, count]) => `<span class="pill">${label} ${numberFormatter.format(count)}</span>`).join('');
      dialogContent.innerHTML = `
        <div class="input-dialog-intro">
          <div>
            <h3>Input 时间线 · ${numberFormatter.format(items.length)} 项</h3>
            <p>保持 input ${inputIsArray ? '数组' : '字段'}的原始顺序；每项末尾可展开完整 JSON，未知类型也会原样保留。${requestBodyTruncated ? '当前请求 Body 已被截断，末尾内容可能不完整。' : ''}</p>
          </div>
          <div class="input-summary" aria-label="Input 类型统计">
            ${summary || '<span class="pill">空数组</span>'}
          </div>
        </div>
        ${items.length ? `<div class="input-timeline">${items.map(renderResponsesInputItem).join('')}</div>` : '<div class="empty">input 数组为空</div>'}
      `;
      dialogContent.scrollTop = 0;
    }

    const newApiColumnLabels = {
      id: '记录编号',
      user_id: '用户 ID',
      created_at: '创建时间',
      type: '日志类型',
      content: '日志内容',
      username: '用户名',
      token_name: '令牌名称',
      model_name: '模型名称',
      quota: '消耗额度',
      prompt_tokens: '输入 Tokens',
      completion_tokens: '输出 Tokens',
      use_time: '请求用时',
      is_stream: '流式请求',
      channel: '渠道 ID',
      channel_id: '渠道 ID',
      channel_name: '渠道名称',
      token_id: '令牌 ID',
      group: '用户分组',
      ip: '请求 IP',
      request_id: '请求 ID',
      upstream_request_id: '上游请求 ID',
      other: '其他信息',
    };
    const newApiColumnOrder = Object.keys(newApiColumnLabels);
    const newApiLogTypes = {
      0: '未知', 1: '充值', 2: '消费', 3: '管理', 4: '系统', 5: '错误', 6: '退款', 7: '登录'
    };

    function newApiColumnLabel(key) {
      return newApiColumnLabels[key] || `其他字段（${key}）`;
    }

    function sortedNewApiEntries(data) {
      return Object.entries(data || {}).sort(([left], [right]) => {
        const leftIndex = newApiColumnOrder.indexOf(left);
        const rightIndex = newApiColumnOrder.indexOf(right);
        if (leftIndex < 0 && rightIndex < 0) return left.localeCompare(right);
        if (leftIndex < 0) return 1;
        if (rightIndex < 0) return -1;
        return leftIndex - rightIndex;
      });
    }

    function newApiCellValue(key, value) {
      if (value === null || value === undefined || value === '') return '<span class="secondary">-</span>';
      if (key === 'type' && newApiLogTypes[value] !== undefined) {
        return `${esc(newApiLogTypes[value])}（${esc(value)}）`;
      }
      if (key === 'created_at' && /^\\d+$/.test(String(value))) {
        const timestamp = Number(value);
        const milliseconds = timestamp < 10_000_000_000 ? timestamp * 1000 : timestamp;
        return `${esc(formatDate(new Date(milliseconds).toISOString()))}<span class="secondary"> · ${esc(value)}</span>`;
      }
      if (typeof value === 'boolean') return value ? '是' : '否';
      if (key === 'use_time' && typeof value === 'number') return `${esc(value)} 秒`;
      if (typeof value === 'object') return renderBodyContent(JSON.stringify(value, null, 2));
      if (typeof value === 'string' && (value.trim().startsWith('{') || value.trim().startsWith('['))) {
        const parsed = tryParseJson(value);
        if (parsed !== null) return renderBodyContent(JSON.stringify(parsed, null, 2));
      }
      return esc(value);
    }

    function newApiRecordsMarkup(payload) {
      const records = [];
      const seen = new Set();
      for (const match of payload?.matches || []) {
        const data = match?.data;
        if (!data || typeof data !== 'object') continue;
        const fingerprint = JSON.stringify(data);
        if (seen.has(fingerprint)) continue;
        seen.add(fingerprint);
        records.push(data);
      }
      if (!records.length) {
        return '<div class="empty">没有查询到对应的 new-api 日志记录</div>';
      }
      return `<div class="record-list">${records.map((record, index) => `
        <article class="record-block">
          <h3 class="record-title">日志记录 ${numberFormatter.format(index + 1)}</h3>
          <div class="kv">${sortedNewApiEntries(record).map(([key, value]) => `
            <div class="kv-row">
              <div class="kv-key">${esc(newApiColumnLabel(key))}</div>
              <div class="kv-value" translate="no">${newApiCellValue(key, value)}</div>
            </div>
          `).join('')}</div>
        </article>
      `).join('')}</div>`;
    }

    function renderNewApiRecords(payload) {
      dialogContent.innerHTML = newApiRecordsMarkup(payload);
    }

    async function loadNewApiDetail(logId, requestId) {
      openDataDialog(`new-api 请求详情 · ${requestId}`);
      try {
        const res = await fetch(`${apiBase}/api/logs/${logId}/new-api`);
        const payload = await res.json();
        if (!res.ok) throw new Error(payload.error || `HTTP ${res.status}`);
        renderNewApiRecords(payload);
      } catch (error) {
        dialogContent.innerHTML = `<div class="empty">加载失败：${esc(String(error.message || error))}</div>`;
      }
    }

    async function lookupRequestId(requestId) {
      const value = String(requestId || '').trim();
      if (!value) return;
      if (dataDialog.open) dataDialog.close();
      activeId = null;
      activeDetailPending = false;
      renderList();
      detailEl.innerHTML = `<div class="empty">正在反查 Request ID：${esc(value)}</div>`;
      lookupSubmit.disabled = true;
      lookupSubmit.textContent = '查询中';
      try {
        const lookupUrl = `${apiBase}/api/request-lookup/${encodeURIComponent(value)}`;
        const gatewayRes = await fetch(`${lookupUrl}?include_new_api=false`);
        let payload = await gatewayRes.json();
        if (!gatewayRes.ok) throw new Error(payload.error || `HTTP ${gatewayRes.status}`);
        const gatewayLog = Array.isArray(payload.gateway_logs) ? payload.gateway_logs[0] : null;
        if (gatewayLog?.id) {
          await loadDetail(Number(gatewayLog.id), true, gatewayLog);
          liveTextEl.textContent = `已定位 Request ID：${value}`;
          return;
        }
        detailEl.innerHTML = `<div class="empty">AI Gateway 中没有匹配，正在查询 new-api-log：${esc(value)}</div>`;
        const newApiRes = await fetch(lookupUrl);
        payload = await newApiRes.json();
        if (!newApiRes.ok) throw new Error(payload.error || `HTTP ${newApiRes.status}`);
        const newApiPayload = payload.new_api_log || {};
        const hasNewApiRecords = Array.isArray(newApiPayload.matches)
          && newApiPayload.matches.some(match => match?.data && typeof match.data === 'object');
        detailEl.innerHTML = `
          <div class="detail-head">
            <div class="detail-topline">
              <div class="endpoint-block">
                <div class="endpoint-badges">
                  <span class="pill type-pill">Request ID 反查</span>
                  <span class="pill status-pill ${hasNewApiRecords ? 'warn' : 'err'}">${hasNewApiRecords ? '仅 new-api 记录' : '未找到匹配'}</span>
                </div>
                <div class="endpoint-context"><span>Request ID: ${esc(value)}</span></div>
              </div>
            </div>
          </div>
          ${hasNewApiRecords ? `
            <section>
              <div class="copy-row"><h3>new-api 日志</h3></div>
              ${newApiRecordsMarkup(newApiPayload)}
            </section>
          ` : '<div class="empty">AI Gateway 和 new-api-log 中都没有找到该 Request ID，请检查 ID 是否完整。</div>'}
        `;
        detailEl.focus({ preventScroll: true });
        liveTextEl.textContent = `已查询 Request ID：${value}`;
      } catch (error) {
        detailEl.innerHTML = `<div class="empty">反查失败：${esc(String(error.message || error))}</div>`;
        liveTextEl.textContent = 'Request ID 反查失败';
      } finally {
        lookupSubmit.disabled = false;
        lookupSubmit.textContent = '查询';
      }
    }

    async function loadDetail(id, shouldFocus = true, providedRow = null) {
      activeId = id;
      renderList();
      detailEl.innerHTML = '<div class="empty">正在加载详情…</div>';
      let row = providedRow;
      if (!row) {
        try {
          const res = await fetch(`${apiBase}/api/logs/${id}`);
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          row = await res.json();
        } catch (error) {
          activeDetailPending = false;
          detailEl.innerHTML = `<div class="empty">详情加载失败：${esc(String(error.message || error))}</div>`;
          liveTextEl.textContent = '详情加载失败';
          return;
        }
      }
      activeDetailPending = row.response_status === null && !row.error;
      const responseIsSse = isSseResponse(row);
      const completedJson = responseIsSse ? completedResponseJsonFromSse(row.response_body.text) : '';
      const responseJsonText = responseIsSse ? (completedJson || '(没有找到可解析的 SSE JSON)') : prettyBody(row.response_body.text);
      const responseSseText = row.response_body.text || '(empty)';
      const requestBodyText = prettyBody(row.request_body.text);
      const requestMessageText = latestUserTextFromRequestBody(row.request_body.text) || '(没有解析到用户消息)';
      const responseMessageText = latestAssistantTextFromResponseBody(row.response_body.text) || '(没有解析到返回消息)';
      const requestHeaderText = formatHeaderText(row.request_headers);
      const responseHeaderText = formatHeaderText(row.response_headers);
      const overheadMs = gatewayOverhead(row);
      const reasoningTokens = row.reasoning_tokens;
      const apiType = row.api_type || 'other';
      const isReasoningAnomaly = reasoningTokens === 516;
      activeRequestBodyView = ['json', 'text'].includes(activeRequestBodyView) ? activeRequestBodyView : 'json';
      const responseViews = responseIsSse ? ['json', 'text', 'sse'] : ['json', 'text'];
      activeResponseBodyView = responseViews.includes(activeResponseBodyView) ? activeResponseBodyView : 'json';
      renderList();
      detailEl.innerHTML = `
        <div class="detail-head ${row.response_failed ? 'response-failed' : ''}">
          <div class="detail-topline">
            <div class="endpoint-block">
              <div class="endpoint-badges">
                <span class="pill method-pill" translate="no">${esc(row.method)}</span>
                <span class="pill type-pill">${esc(apiTypeLabel(apiType))}</span>
                <span class="pill status-pill ${statusClassForRow(row)}">${esc(statusLabel(row))}</span>
                ${row.response_failed ? '<span class="pill status-pill err">Responses SSE 失败</span>' : ''}
                ${isReasoningAnomaly ? '<span class="pill status-pill err">reasoning 516</span>' : ''}
              </div>
              <div class="endpoint-url" translate="no">${esc(row.target_url)}</div>
              ${(row.oneapi_request_id || row.new_api_user) ? `
                <div class="endpoint-context">
                  ${row.oneapi_request_id ? `<span>Request ID: ${esc(row.oneapi_request_id)}</span>` : ''}
                  ${row.new_api_user ? `<span>请求人: ${esc(row.new_api_user)}</span>` : ''}
                </div>
              ` : ''}
            </div>
            <div class="endpoint-actions">
              ${row.oneapi_request_id ? '<button type="button" data-new-api-detail>new-api 详情</button>' : ''}
              <button type="button" data-copy="url">复制 URL</button>
            </div>
          </div>
          ${row.response_failed ? `
            <div class="response-failure-banner" role="alert">
              <div class="response-failure-title">RESPONSE FAILED</div>
              <div class="response-failure-content">
                <div class="response-failure-code">${esc(row.response_failure_code || 'response_failed')}</div>
                <div>${esc(row.response_failure_message || 'OpenAI Responses SSE 返回失败事件')}</div>
              </div>
            </div>
          ` : ''}
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
            <div class="copy-row">
              <h3>Request Body${row.request_body_truncated ? ' (truncated)' : ''}</h3>
              <div class="row-actions">
                ${apiType === 'responses' ? '<button class="parse-input-button" type="button" data-responses-input>解析 Input</button>' : ''}
                <div class="view-switch" aria-label="Request Body 视图">
                  <button type="button" data-request-view-button="json">JSON</button>
                  <button type="button" data-request-view-button="text">Text</button>
                </div>
                <button type="button" data-copy="requestBody">复制 Body</button>
              </div>
            </div>
            <div data-request-body-view="json">${renderBodyContent(row.request_body.text)}</div>
            ${renderPreText(requestMessageText, 'data-request-body-view="text" hidden')}
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
                    <button type="button" data-response-view-button="text">Text</button>
                    <button type="button" data-response-view-button="sse">SSE</button>
                  </div>
                ` : `
                  <div class="view-switch" aria-label="Response Body 视图">
                    <button type="button" data-response-view-button="json">JSON</button>
                    <button type="button" data-response-view-button="text">Text</button>
                  </div>
                `}
                <button type="button" data-copy="responseBody">复制 Body</button>
              </div>
            </div>
            ${responseIsSse ? `
              <div data-response-body-view="json">${renderBodyContent(responseJsonText)}</div>
              ${renderPreText(responseMessageText, 'data-response-body-view="text" hidden')}
              ${renderPreText(responseSseText, 'data-response-body-view="sse" hidden')}
            ` : `
              <div data-response-body-view="json">${renderBodyContent(row.response_body.text)}</div>
              ${renderPreText(responseMessageText, 'data-response-body-view="text" hidden')}
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
        responseHeaders: responseHeaderText,
      };
      detailEl.querySelectorAll('[data-copy]').forEach(button => {
        button.addEventListener('click', event => {
          event.preventDefault();
          event.stopPropagation();
          if (button.dataset.copy === 'requestBody') {
            copyText(activeRequestBodyView === 'text' ? requestMessageText : requestBodyText);
            return;
          }
          if (button.dataset.copy === 'responseBody') {
            const responseCopyText = activeResponseBodyView === 'text'
              ? responseMessageText
              : (activeResponseBodyView === 'sse' ? responseSseText : responseJsonText);
            copyText(responseCopyText);
            return;
          }
          copyText(copyMap[button.dataset.copy]);
        });
      });
      detailEl.querySelectorAll('[data-request-view-button]').forEach(button => {
        button.addEventListener('click', () => setRequestBodyView(button.dataset.requestViewButton));
      });
      detailEl.querySelectorAll('[data-response-view-button]').forEach(button => {
        button.addEventListener('click', () => setResponseBodyView(button.dataset.responseViewButton));
      });
      detailEl.querySelector('[data-new-api-detail]')?.addEventListener('click', () => {
        loadNewApiDetail(row.id, row.oneapi_request_id);
      });
      detailEl.querySelector('[data-responses-input]')?.addEventListener('click', () => {
        openResponsesInputDialog(row.request_body.text, row.request_body_truncated);
      });
      setRequestBodyView(activeRequestBodyView);
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
    lookupForm.addEventListener('submit', event => {
      event.preventDefault();
      lookupRequestId(lookupInput.value);
    });
    document.getElementById('dialogClose').addEventListener('click', () => dataDialog.close());
    dataDialog.addEventListener('click', event => {
      if (event.target === dataDialog) dataDialog.close();
    });
    document.getElementById('refresh').addEventListener('click', () => loadList({ refreshDetail: true }));
    loadList({ refreshDetail: true });
    connectLogSocket();
  </script>
</body>
</html>
""".replace("__APP_COMMIT_FULL__", escape(APP_COMMIT, quote=True)).replace(
        "__APP_COMMIT_SHORT__", escape(APP_COMMIT[:12])
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "commit": APP_COMMIT}


@app.get("/favicon.ico")
async def favicon() -> PlainTextResponse:
    return PlainTextResponse("", status_code=204)


@app.get("/api/logs")
async def api_logs(limit: int = 100) -> list[dict]:
    return await asyncio.to_thread(list_log_summaries, limit, None)


@app.get("/{access_key}/api/logs")
async def scoped_api_logs(access_key: str, limit: int = 100) -> list[dict]:
    return await asyncio.to_thread(list_log_summaries, limit, access_key)


@app.websocket("/ws/logs")
async def websocket_logs(websocket: WebSocket) -> None:
    await log_socket_manager.connect(websocket, None)
    try:
        rows = await asyncio.to_thread(list_log_summaries, 50, None)
        await websocket.send_json({"type": "logs", "changed_id": None, "rows": rows})
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        log_socket_manager.disconnect(websocket)


@app.websocket("/{access_key}/ws/logs")
async def scoped_websocket_logs(access_key: str, websocket: WebSocket) -> None:
    access_key = normalize_access_key(access_key)
    await log_socket_manager.connect(websocket, access_key)
    try:
        rows = await asyncio.to_thread(list_log_summaries, 50, access_key)
        await websocket.send_json({"type": "logs", "changed_id": None, "rows": rows})
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        log_socket_manager.disconnect(websocket)


@app.get("/api/logs/{log_id}")
async def api_log_detail(log_id: int) -> JSONResponse:
    return await asyncio.to_thread(log_detail_response, log_id, None)


@app.get("/{access_key}/api/logs/{log_id}")
async def scoped_api_log_detail(access_key: str, log_id: int) -> JSONResponse:
    return await asyncio.to_thread(log_detail_response, log_id, access_key)


def json_object_from_text(value: Any, fallback: Any) -> Any:
    if value in (None, ""):
        return fallback
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return fallback


def log_row_by_id(log_id: int, access_key: str | None = None) -> dict[str, Any] | None:
    access_key = normalize_access_key(access_key)
    if access_key is None:
        return db_fetchone("SELECT * FROM request_logs WHERE id = ? AND access_key IS NULL", (log_id,))
    return db_fetchone("SELECT * FROM request_logs WHERE id = ? AND access_key = ?", (log_id, access_key))


def log_row_payload(row: dict[str, Any]) -> dict[str, Any]:
    response_body = bytes_from_db(row.get("response_body"))
    request_body = bytes_from_db(row.get("request_body"))
    output_tokens = row.get("output_tokens")
    if output_tokens is None:
        output_tokens = output_tokens_from_body(response_body)
    reasoning_tokens = row.get("reasoning_tokens")
    if reasoning_tokens is None:
        reasoning_tokens = reasoning_tokens_from_body(response_body)
    api_type = row.get("api_type") or api_type_from_log(row.get("target_url") or "", request_body, response_body)
    response_failure = response_failed_from_sse(response_body)
    new_api_log = json_object_from_text(row.get("new_api_log"), None)
    if new_api_log is None and row.get("new_api_log"):
        new_api_log = {"raw": str(row["new_api_log"]), "clipped": True}
    duration_ms = row.get("duration_ms")
    upstream_duration_ms = row.get("upstream_duration_ms")
    return {
        "id": row["id"],
        "created_at": row["created_at"],
        "method": row["method"],
        "target_url": row["target_url"],
        "access_key": row.get("access_key"),
        "client_host": row.get("client_host"),
        "request_headers": json_object_from_text(row.get("request_headers"), {}),
        "request_body": body_payload(request_body),
        "request_body_truncated": bool(row.get("request_body_truncated")),
        "response_status": row.get("response_status"),
        "response_headers": json_object_from_text(row.get("response_headers"), {}),
        "response_body": body_payload(response_body),
        "response_body_truncated": bool(row.get("response_body_truncated")),
        "reasoning_tokens": reasoning_tokens,
        "api_type": api_type,
        "error": row.get("error"),
        "response_failed": bool(row.get("response_failed")) or response_failure is not None,
        "response_failure_code": row.get("response_failure_code")
        or (response_failure.get("code") if response_failure else None),
        "response_failure_message": row.get("response_failure_message")
        or (response_failure.get("message") if response_failure else None),
        "duration_ms": duration_ms,
        "upstream_duration_ms": upstream_duration_ms,
        "first_byte_ms": row.get("first_byte_ms"),
        "output_tokens": output_tokens,
        "tps": tps_from_values(output_tokens, duration_ms, row.get("first_byte_ms")),
        "gateway_overhead_ms": (
            duration_ms - upstream_duration_ms
            if duration_ms is not None and upstream_duration_ms is not None
            else None
        ),
        "oneapi_request_id": row.get("oneapi_request_id"),
        "new_api_user": row.get("new_api_user"),
        "new_api_log": new_api_log,
        "new_api_log_error": row.get("new_api_log_error"),
    }


def log_detail_response(log_id: int, access_key: str | None = None) -> JSONResponse:
    row = log_row_by_id(log_id, access_key)
    if row is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    return JSONResponse(log_row_payload(row))


def gateway_logs_by_request_id(request_id: str, access_key: str | None = None) -> list[dict[str, Any]]:
    access_key = normalize_access_key(access_key)
    request_id = request_id.strip()
    if access_key is None:
        rows = db_fetchall(
            """
            SELECT * FROM request_logs
            WHERE access_key IS NULL AND oneapi_request_id = ?
            ORDER BY id DESC
            LIMIT 20
            """,
            (request_id,),
        )
    else:
        rows = db_fetchall(
            """
            SELECT * FROM request_logs
            WHERE access_key = ? AND oneapi_request_id = ?
            ORDER BY id DESC
            LIMIT 20
            """,
            (access_key, request_id),
        )
    if not rows:
        header_pattern = f"%{request_id}%"
        if access_key is None:
            id_rows = db_fetchall(
                """
                SELECT id FROM request_logs
                WHERE access_key IS NULL AND response_headers LIKE ?
                ORDER BY id DESC
                LIMIT 20
                """,
                (header_pattern,),
            )
        else:
            id_rows = db_fetchall(
                """
                SELECT id FROM request_logs
                WHERE access_key = ? AND response_headers LIKE ?
                ORDER BY id DESC
                LIMIT 20
                """,
                (access_key, header_pattern),
            )
        ids = [int(row["id"]) for row in id_rows]
        if ids:
            placeholders = ", ".join("?" for _ in ids)
            rows = db_fetchall(
                f"SELECT * FROM request_logs WHERE id IN ({placeholders}) ORDER BY id DESC",
                tuple(ids),
            )
    return [log_row_payload(row) for row in rows]


def cached_new_api_log(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row or not row.get("new_api_log"):
        return None
    parsed = json_object_from_text(row["new_api_log"], None)
    return parsed if isinstance(parsed, dict) else {"raw": str(row["new_api_log"])}


def new_api_log_detail_response(log_id: int, access_key: str | None = None) -> JSONResponse:
    row = log_row_by_id(log_id, access_key)
    if row is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    request_id = row.get("oneapi_request_id")
    if not request_id:
        return JSONResponse({"error": "response header 中没有 x-oneapi-request-id"}, status_code=404)
    payload = cached_new_api_log(row)
    if payload is None:
        payload = query_new_api_log(request_id)
    return JSONResponse(payload or {"request_id": request_id, "matches": []})


def request_lookup_response(
    request_id: str,
    access_key: str | None = None,
    include_new_api: bool = True,
) -> JSONResponse:
    request_id = request_id.strip()
    if not request_id or len(request_id) > 256:
        return JSONResponse({"error": "invalid request id"}, status_code=400)
    gateway_logs = gateway_logs_by_request_id(request_id, access_key)
    new_api_log = None
    if gateway_logs:
        new_api_log = gateway_logs[0].get("new_api_log")
    if include_new_api and (not new_api_log or not new_api_log.get("matches")):
        new_api_log = query_new_api_log(request_id)
    return JSONResponse(
        {
            "request_id": request_id,
            "gateway_count": len(gateway_logs),
            "gateway_logs": gateway_logs,
            "new_api_log": new_api_log,
        }
    )


@app.get("/api/logs/{log_id}/new-api")
async def api_log_new_api_detail(log_id: int) -> JSONResponse:
    return await asyncio.to_thread(new_api_log_detail_response, log_id, None)


@app.get("/{access_key}/api/logs/{log_id}/new-api")
async def scoped_api_log_new_api_detail(access_key: str, log_id: int) -> JSONResponse:
    return await asyncio.to_thread(new_api_log_detail_response, log_id, access_key)


@app.get("/api/request-lookup/{request_id}")
async def api_request_lookup(request_id: str, include_new_api: bool = True) -> JSONResponse:
    return await asyncio.to_thread(request_lookup_response, request_id, None, include_new_api)


@app.get("/{access_key}/api/request-lookup/{request_id}")
async def scoped_api_request_lookup(access_key: str, request_id: str, include_new_api: bool = True) -> JSONResponse:
    return await asyncio.to_thread(request_lookup_response, request_id, access_key, include_new_api)


@app.get("/{access_key}", response_class=HTMLResponse)
async def scoped_dashboard(access_key: str) -> str:
    if normalize_access_key(access_key) is None:
        return PlainTextResponse("Not found", status_code=404)
    return await dashboard()


@app.api_route("/{access_key}/{target_url:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
async def scoped_proxy(access_key: str, target_url: str, request: Request):
    if access_key in {"http:", "https:"}:
        separator = "/" if target_url.startswith("/") else "//"
        return await proxy_to_target(f"{access_key}{separator}{target_url}", request, None)
    return await proxy_to_target(target_url, request, normalize_access_key(access_key))


@app.api_route("/{target_url:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
async def proxy(target_url: str, request: Request):
    return await proxy_to_target(target_url, request, None)


async def proxy_to_target(target_url: str, request: Request, access_key: str | None):
    if request.url.query:
        target_url = f"{target_url}?{request.url.query}"

    validation_error = validate_target_url(target_url)
    if validation_error:
        return PlainTextResponse(validation_error, status_code=400)

    started_at = time.perf_counter()
    perf_context: dict[str, Any] = {
        "method": request.method,
        "target_url": target_url,
    }
    request_capture = bytearray()
    request_state: dict[str, Any] = {
        "bytes_received": 0,
        "truncated": False,
        "first_chunk_at": None,
        "finished_at": None,
    }
    request_body_finished = asyncio.Event()
    log_id_task = asyncio.create_task(
        create_log_async_timed(
            perf_context,
            request.method,
            target_url,
            access_key,
            request.client.host if request.client else None,
            dict(request.headers),
            b"",
            False,
        )
    )
    asyncio.create_task(announce_created(log_id_task))
    request_body_task = asyncio.create_task(
        persist_request_body_async(
            log_id_task,
            request_body_finished,
            target_url,
            request_capture,
            request_state,
            perf_context,
        )
    )
    response_capture = bytearray()
    response_truncated = False

    content_length_header = request.headers.get("content-length")
    transfer_encoding = request.headers.get("transfer-encoding")
    has_request_body = (
        (content_length_header is not None and content_length_header != "0")
        or bool(transfer_encoding)
        or request.method.upper() in {"POST", "PUT", "PATCH"}
    )
    upstream_headers = filtered_request_headers(request)
    if content_length_header and content_length_header.isdigit():
        upstream_headers["content-length"] = content_length_header

    async def stream_request_body() -> AsyncIterator[bytes]:
        try:
            async for chunk in request.stream():
                if not chunk:
                    continue
                now = time.perf_counter()
                if request_state["first_chunk_at"] is None:
                    request_state["first_chunk_at"] = now
                request_state["bytes_received"] += len(chunk)
                request_state["truncated"] = append_capture(request_capture, chunk) or request_state["truncated"]
                yield chunk
        finally:
            request_state["finished_at"] = time.perf_counter()
            request_body_finished.set()

    if has_request_body:
        upstream_content: bytes | AsyncIterator[bytes] = stream_request_body()
    else:
        upstream_content = b""
        request_state["finished_at"] = time.perf_counter()
        request_body_finished.set()

    client = get_http_client()
    upstream_request = client.build_request(
        request.method,
        target_url,
        headers=upstream_headers,
        content=upstream_content,
    )

    upstream_started_at = time.perf_counter()
    perf_context["pre_upstream_ms"] = elapsed_ms(started_at, upstream_started_at)
    try:
        upstream_response = await client.send(upstream_request, stream=True)
    except Exception as exc:
        if not request_body_finished.is_set():
            request_state["finished_at"] = time.perf_counter()
            request_body_finished.set()
        finished_at = time.perf_counter()
        body = f"Upstream request failed: {exc}".encode("utf-8")
        asyncio.create_task(
            finish_log_async(
                log_id_task,
                request_body_task,
                502,
                {"content-type": "text/plain; charset=utf-8"},
                body,
                False,
                started_at,
                upstream_started_at,
                finished_at,
                None,
                str(exc),
                perf_context=perf_context,
            )
        )
        perf_log(
            "proxy_error",
            gateway_log_id=perf_context.get("gateway_log_id"),
            method=request.method,
            target_url=target_url,
            error=str(exc),
            gateway_total_ms=elapsed_ms(started_at, finished_at),
            upstream_total_ms=elapsed_ms(upstream_started_at, finished_at),
            pre_upstream_ms=perf_context.get("pre_upstream_ms"),
            request_body_bytes=request_state.get("bytes_received"),
        )
        return PlainTextResponse(body.decode("utf-8"), status_code=502)

    upstream_headers_at = time.perf_counter()
    oneapi_request_id = header_value(dict(upstream_response.headers), "x-oneapi-request-id")
    perf_context["request_id"] = oneapi_request_id
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
            asyncio.create_task(
                finish_log_async(
                    log_id_task,
                    request_body_task,
                    upstream_response.status_code,
                    dict(upstream_response.headers),
                    response_capture,
                    response_truncated,
                    started_at,
                    upstream_started_at,
                    finished_at,
                    first_byte_at,
                    error,
                    perf_context=perf_context,
                )
            )
            gateway_total_ms = elapsed_ms(started_at, finished_at)
            upstream_total_ms = elapsed_ms(upstream_started_at, finished_at)
            gateway_overhead_ms = gateway_total_ms - upstream_total_ms
            if gateway_overhead_ms >= PERFORMANCE_LOG_THRESHOLD_MS or error:
                request_finished_at = request_state.get("finished_at")
                request_first_chunk_at = request_state.get("first_chunk_at")
                perf_log(
                    "proxy",
                    request_id=oneapi_request_id,
                    gateway_log_id=perf_context.get("gateway_log_id"),
                    method=request.method,
                    target_url=target_url,
                    status_code=upstream_response.status_code,
                    gateway_total_ms=gateway_total_ms,
                    upstream_total_ms=upstream_total_ms,
                    gateway_overhead_ms=gateway_overhead_ms,
                    pre_upstream_ms=perf_context.get("pre_upstream_ms"),
                    request_first_chunk_ms=(
                        elapsed_ms(started_at, request_first_chunk_at) if request_first_chunk_at is not None else None
                    ),
                    request_upload_ms=(
                        elapsed_ms(upstream_started_at, request_finished_at) if request_finished_at is not None else None
                    ),
                    upstream_headers_ms=elapsed_ms(upstream_started_at, upstream_headers_at),
                    first_response_byte_ms=(elapsed_ms(started_at, first_byte_at) if first_byte_at is not None else None),
                    response_stream_ms=(elapsed_ms(first_byte_at, finished_at) if first_byte_at is not None else None),
                    request_body_bytes=request_state.get("bytes_received"),
                    request_capture_bytes=len(request_capture),
                    response_body_bytes=len(response_capture),
                    request_body_truncated=bool(request_state.get("truncated")),
                    response_body_truncated=response_truncated,
                    error=error,
                )

    return StreamingResponse(
        stream_response(),
        status_code=upstream_response.status_code,
        headers=response_headers,
        media_type=upstream_response.headers.get("content-type"),
    )
