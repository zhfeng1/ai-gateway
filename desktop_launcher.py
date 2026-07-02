import os
import socket
import sys
import threading
import time
import traceback
from pathlib import Path

import uvicorn
import webview


DEFAULT_PORT = 20000


def runtime_data_dir() -> Path:
    if getattr(sys, "frozen", False):
        if sys.platform == "darwin":
            return Path.home() / "Library" / "Application Support" / "AI Gateway"
        return Path(sys.executable).resolve().parent / "data"
    return Path(__file__).resolve().parent / "data"


def port_is_available(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.2)
        return sock.connect_ex((host, port)) != 0


def wait_for_server(host: str, port: int, timeout: float = 8.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not port_is_available(host, port):
            return True
        time.sleep(0.1)
    return False


def setup_environment() -> None:
    data_dir = runtime_data_dir()
    data_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("DATABASE_PATH", str(data_dir / "ai_gateway.sqlite3"))
    os.environ.setdefault("REQUEST_TIMEOUT_SECONDS", "600")
    os.environ.setdefault("MAX_CAPTURE_BYTES", "0")


def log_launcher_error(error: BaseException) -> None:
    try:
        log_path = runtime_data_dir() / "launcher-error.log"
        with log_path.open("a", encoding="utf-8") as file:
            file.write(traceback.format_exc())
            file.write("\n")
    except Exception:
        pass


class GatewayApi:
    def __init__(self, host: str = "127.0.0.1") -> None:
        self.host = host
        self.server: uvicorn.Server | None = None
        self.port: int | None = None
        self.window = None

    def start_gateway(self, raw_port: str) -> dict:
        try:
            port = int(str(raw_port).strip() or DEFAULT_PORT)
        except ValueError:
            return {"ok": False, "error": "请输入有效的数字端口。"}

        if not 1 <= port <= 65535:
            return {"ok": False, "error": "端口必须在 1 到 65535 之间。"}
        if not port_is_available(self.host, port):
            return {"ok": False, "error": f"端口 {port} 已被占用，请换一个端口。"}

        setup_environment()

        from app.main import app

        config = uvicorn.Config(app, host=self.host, port=port, proxy_headers=True, log_level="warning")
        self.server = uvicorn.Server(config)
        self.port = port
        threading.Thread(target=self.server.run, daemon=True).start()

        if not wait_for_server(self.host, port):
            return {"ok": False, "error": "服务启动超时，请重试。"}

        url = f"http://{self.host}:{port}/"
        if self.window is not None:
            def load_dashboard() -> None:
                try:
                    self.window.load_url(url)
                except Exception as exc:
                    log_launcher_error(exc)

            threading.Timer(0.2, load_dashboard).start()
        return {"ok": True, "url": url}

    def stop_gateway(self) -> None:
        if self.server is not None:
            self.server.should_exit = True


def start_page() -> str:
    return f"""
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>AI Gateway</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #080b10;
      --panel: #10151f;
      --line: #253044;
      --text: #f4f7fb;
      --muted: #97a4b8;
      --accent: #27d17f;
      --danger: #ff5c73;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      background:
        radial-gradient(circle at 24% 0%, rgba(53, 183, 255, .14), transparent 34%),
        radial-gradient(circle at 76% 0%, rgba(39, 209, 127, .12), transparent 32%),
        var(--bg);
      color: var(--text);
      font: 15px/1.5 Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    main {{
      width: min(460px, calc(100vw - 40px));
      border: 1px solid var(--line);
      border-radius: 18px;
      background: rgba(16, 21, 31, .92);
      box-shadow: 0 26px 70px rgba(0, 0, 0, .34);
      padding: 28px;
    }}
    .mark {{
      width: 44px;
      height: 44px;
      border-radius: 12px;
      display: grid;
      place-items: center;
      background: #0b2017;
      border: 1px solid rgba(39, 209, 127, .42);
      color: var(--accent);
      font-weight: 800;
      margin-bottom: 18px;
      box-shadow: 0 0 28px rgba(39, 209, 127, .18);
    }}
    h1 {{ margin: 0 0 6px; font-size: 24px; }}
    p {{ margin: 0 0 22px; color: var(--muted); }}
    label {{ display: block; color: var(--muted); font-weight: 650; margin-bottom: 8px; }}
    input {{
      width: 100%;
      height: 46px;
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #05070b;
      color: var(--text);
      padding: 0 12px;
      font: inherit;
      margin-bottom: 14px;
    }}
    input:focus-visible, button:focus-visible {{
      outline: 3px solid rgba(39, 209, 127, .25);
      outline-offset: 2px;
      border-color: var(--accent);
    }}
    button {{
      width: 100%;
      height: 46px;
      border: 1px solid rgba(39, 209, 127, .46);
      border-radius: 10px;
      background: rgba(39, 209, 127, .13);
      color: var(--accent);
      font: inherit;
      font-weight: 760;
      cursor: pointer;
    }}
    button:disabled {{ opacity: .62; cursor: wait; }}
    .error {{ min-height: 22px; color: var(--danger); margin-top: 12px; }}
  </style>
</head>
<body>
  <main>
    <div class="mark" aria-hidden="true">AI</div>
    <h1>AI Gateway</h1>
    <p>选择本地端口后，将在此窗口内打开网关控制台。</p>
    <label for="port">启动端口</label>
    <input id="port" value="{DEFAULT_PORT}" inputmode="numeric" autocomplete="off" />
    <button id="start" type="button">启动</button>
    <div class="error" id="error" role="alert"></div>
  </main>
  <script>
    const port = document.getElementById('port');
    const start = document.getElementById('start');
    const error = document.getElementById('error');

    async function launch() {{
      error.textContent = '';
      start.disabled = true;
      start.textContent = '启动中...';
      try {{
        const result = await window.pywebview.api.start_gateway(port.value);
        if (result.ok) {{
          error.textContent = '正在打开控制台...';
          return;
        }}
        error.textContent = result.error || '启动失败，请重试。';
      }} catch (err) {{
        error.textContent = String(err);
      }} finally {{
        start.disabled = false;
        start.textContent = '启动';
      }}
    }}

    start.addEventListener('click', launch);
    port.addEventListener('keydown', event => {{
      if (event.key === 'Enter') launch();
    }});
    window.addEventListener('pywebviewready', () => port.focus());
  </script>
</body>
</html>
"""


def main() -> None:
    api = GatewayApi()
    window = webview.create_window(
        "AI Gateway",
        html=start_page(),
        js_api=api,
        width=1280,
        height=860,
        min_size=(920, 640),
    )
    api.window = window

    def on_closed() -> None:
        api.stop_gateway()

    window.events.closed += on_closed
    webview.start(debug=False)


if __name__ == "__main__":
    main()
