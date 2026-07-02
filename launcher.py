import argparse
import os
import socket
import sys
import threading
import time
import webbrowser
from pathlib import Path

import uvicorn


DEFAULT_PORT = 20000


def app_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def runtime_data_dir() -> Path:
    if getattr(sys, "frozen", False) and sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "AI Gateway"
    return app_base_dir() / "data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run AI Gateway locally.")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host. Default: 127.0.0.1")
    parser.add_argument("--port", type=int, help=f"Bind port. Default: {DEFAULT_PORT}")
    parser.add_argument("--no-browser", action="store_true", help="Do not open the dashboard in a browser.")
    return parser.parse_args()


def port_is_available(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.2)
        return sock.connect_ex((host, port)) != 0


def prompt_port(host: str, requested_port: int | None) -> int:
    if requested_port is not None:
        return requested_port

    if not sys.stdin.isatty():
        return DEFAULT_PORT

    while True:
        raw = input(f"Port [{DEFAULT_PORT}]: ").strip()
        if not raw:
            port = DEFAULT_PORT
        else:
            try:
                port = int(raw)
            except ValueError:
                print("Please enter a valid numeric port.")
                continue

        if not 1 <= port <= 65535:
            print("Port must be between 1 and 65535.")
            continue
        if not port_is_available(host, port):
            print(f"Port {port} is already in use.")
            continue
        return port


def open_browser_later(url: str) -> None:
    time.sleep(1.0)
    webbrowser.open(url)


def main() -> None:
    args = parse_args()
    data_dir = runtime_data_dir()
    data_dir.mkdir(parents=True, exist_ok=True)

    os.environ.setdefault("DATABASE_PATH", str(data_dir / "ai_gateway.sqlite3"))
    os.environ.setdefault("REQUEST_TIMEOUT_SECONDS", "600")
    os.environ.setdefault("MAX_CAPTURE_BYTES", "0")

    port = prompt_port(args.host, args.port)
    url = f"http://{args.host}:{port}/"
    print(f"AI Gateway is starting at {url}")
    print("Press Ctrl+C to stop.")

    if not args.no_browser:
        threading.Thread(target=open_browser_later, args=(url,), daemon=True).start()

    from app.main import app

    uvicorn.run(app, host=args.host, port=port, proxy_headers=True, log_level="info")


if __name__ == "__main__":
    main()
