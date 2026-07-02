# AI Gateway

AI Gateway 是一个通用 Python HTTP 网关，用于转发上游接口并记录完整请求与响应信息。它适合调试 OpenAI 兼容接口、Responses API、Chat Completions、Messages，以及其他普通 HTTP API。

## 功能

- 通用转发：访问 `http://127.0.0.1:20000/http://upstream/v1` 会自动转发到上游地址。
- 完整记录：Request Header、Response Header、Request Body、Response Body。
- 支持流式 SSE：可透传并记录 OpenAI Responses、Chat Completions、Messages 等流式接口。
- Web 控制台：查看请求列表、耗时、上游耗时、差值、Header、Body 和 JSON 树形预览。
- WebSocket 推送：请求列表实时更新，已完成请求不会反复刷新右侧详情。
- Docker 部署：支持 Docker Compose。
- 本地运行包：GitHub Release 提供 macOS `.app` 和 Windows `.exe` 包。

## Docker Compose 部署

```bash
cd /opt/docker/ai-gateway
docker compose up -d --build
```

打开控制台：

```text
http://127.0.0.1:20000/
```

转发示例：

```text
http://127.0.0.1:20000/http://123.123.123.123:18088/v1
```

数据默认保存在：

```text
./data/ai_gateway.sqlite3
```

## 本地 Release 包

### macOS

下载并解压：

```text
ai-gateway-macos.zip
```

双击 `AI Gateway.app`，应用会打开一个桌面窗口。先在启动页面选择端口，然后控制台会在同一个窗口中加载。

macOS 本地数据保存位置：

```text
~/Library/Application Support/AI Gateway/ai_gateway.sqlite3
```

### Windows

下载并解压：

```text
ai-gateway-windows-x64.zip
```

双击 `ai-gateway.exe`，应用会打开一个桌面窗口。先在启动页面选择端口，然后控制台会在同一个窗口中加载。

也可以在 PowerShell 中运行：

```powershell
.\ai-gateway.exe
```

Windows 本地数据保存在可执行文件旁边：

```text
data\ai_gateway.sqlite3
```

## GitHub Actions

- `Docker Image`：推送 `main` 或 `v*` tag 时构建多架构 Docker 镜像，并推送到 GitHub Container Registry。
- `Release Packages`：推送 `v*` tag 时构建 macOS `.app` zip 和 Windows `.exe` zip，并上传到 GitHub Release。

## 配置项

可通过环境变量调整：

- `DATABASE_PATH`：SQLite 数据库路径。
- `REQUEST_TIMEOUT_SECONDS`：上游请求超时时间，默认 `600`。
- `MAX_CAPTURE_BYTES`：Body 最大记录字节数，`0` 表示完整记录。

## 注意

- 如果完整记录超大流式响应，内存和磁盘占用会增加。生产环境可设置 `MAX_CAPTURE_BYTES` 限制记录大小。
- 网关默认绑定本地地址，Docker Compose 中为 `127.0.0.1:20000`。
