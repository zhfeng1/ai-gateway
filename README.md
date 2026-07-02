# AI Gateway

Python HTTP gateway for forwarding requests and recording request/response headers and bodies, including streamed SSE responses such as OpenAI-compatible APIs.

## Run

### Docker Compose

```bash
cd /opt/docker/ai-gateway
docker compose up -d --build
```

Open the dashboard:

```text
http://127.0.0.1:20000/
```

Proxy an upstream request:

```text
http://127.0.0.1:20000/http://123.123.123.123:18088/v1
```

Paths, query strings, request headers, request body, response headers, response body, status code, gateway elapsed time, upstream elapsed time, and gateway overhead are recorded in SQLite at `./data/ai_gateway.sqlite3`.

### Local Release Package

GitHub Releases include macOS and Windows packages built by GitHub Actions.

On macOS, double-click `AI Gateway.app`, choose a port in the dialog, and the dashboard opens in the browser.

```powershell
.\ai-gateway.exe
```

You can also pass a port directly:

```bash
./ai-gateway --port 20000
```

The Windows package stores logs in `data/ai_gateway.sqlite3` next to the executable. The macOS app stores logs in `~/Library/Application Support/AI Gateway/ai_gateway.sqlite3`.

## CI/CD

- `.github/workflows/docker-image.yml` builds multi-arch Docker images and pushes them to GitHub Container Registry on pushes to `main`, tags, and manual runs.
- `.github/workflows/release.yml` builds local macOS x64, macOS arm64, and Windows x64 release packages. Tag pushes such as `v1.0.0` publish the zip files to a GitHub Release.

## Notes

- `MAX_CAPTURE_BYTES=0` means capture full bodies. Set a positive byte count to cap stored request and response body size.
- Hop-by-hop headers such as `connection`, `transfer-encoding`, and `content-length` are not forwarded directly.
- The gateway binds to local host only by default: `127.0.0.1:20000`.
