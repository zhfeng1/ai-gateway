import os
import socket
import subprocess
import sys
import threading
import time
import webbrowser
from pathlib import Path

import uvicorn


DEFAULT_PORT = 20000


def app_data_dir() -> Path:
    return Path.home() / "Library" / "Application Support" / "AI Gateway"


def port_is_available(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.2)
        return sock.connect_ex((host, port)) != 0


def osascript(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["osascript", "-e", script], text=True, capture_output=True, check=False)


def apple_string(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def show_error(message: str) -> None:
    osascript(
        f'display dialog {apple_string(message)} '
        'with title "AI Gateway" buttons {"好"} default button "好" with icon caution'
    )


def ask_port(host: str) -> int | None:
    while True:
        result = osascript(
            'display dialog "请选择启动端口" '
            f'default answer "{DEFAULT_PORT}" '
            'with title "AI Gateway" '
            'buttons {"取消", "启动"} default button "启动" cancel button "取消"'
        )
        if result.returncode != 0:
            return None

        marker = "text returned:"
        output = result.stdout.strip()
        raw = output.split(marker, 1)[1].strip() if marker in output else str(DEFAULT_PORT)
        try:
            port = int(raw)
        except ValueError:
            show_error("请输入有效的数字端口。")
            continue

        if not 1 <= port <= 65535:
            show_error("端口必须在 1 到 65535 之间。")
            continue
        if not port_is_available(host, port):
            show_error(f"端口 {port} 已被占用，请换一个端口。")
            continue
        return port


def open_browser_later(url: str) -> None:
    time.sleep(0.8)
    webbrowser.open(url)


def run_server(host: str, port: int) -> uvicorn.Server:
    from app.main import app

    config = uvicorn.Config(app, host=host, port=port, proxy_headers=True, log_level="info")
    server = uvicorn.Server(config)
    threading.Thread(target=server.run, daemon=True).start()
    return server


def control_loop(server: uvicorn.Server, url: str) -> None:
    while not server.should_exit:
        result = osascript(
            f'display dialog {apple_string("AI Gateway 正在运行：\\n" + url)} '
            'with title "AI Gateway" '
            'buttons {"退出", "打开控制台"} default button "打开控制台"'
        )
        if result.returncode != 0 or "button returned:退出" in result.stdout:
            server.should_exit = True
            break
        webbrowser.open(url)


def main() -> None:
    host = "127.0.0.1"
    data_dir = app_data_dir()
    data_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("DATABASE_PATH", str(data_dir / "ai_gateway.sqlite3"))
    os.environ.setdefault("REQUEST_TIMEOUT_SECONDS", "600")
    os.environ.setdefault("MAX_CAPTURE_BYTES", "0")

    port = ask_port(host)
    if port is None:
        return

    url = f"http://127.0.0.1:{port}/"
    server = run_server(host, port)
    threading.Thread(target=open_browser_later, args=(url,), daemon=True).start()
    control_loop(server, url)


if __name__ == "__main__":
    main()
