# AI Gateway

AI Gateway 是一个用于调试 AI 接口的 Python HTTP 网关。它可以把本地请求转发到任意上游接口，同时记录完整的请求与响应信息，适合排查 OpenAI 兼容接口、Responses API、Chat Completions、Messages API，以及普通 HTTP 接口。

## 功能特性

- 通用转发：访问 `http://127.0.0.1:20000/http://upstream/v1` 会自动转发到上游地址。
- 路径隔离：支持 `http://127.0.0.1:20000/password/https://upstream/v1`，打开 `/password` 只查看该路径空间下的请求记录。
- 完整记录：Request Header、Response Header、Request Body、Response Body 可写入 SQLite 或 PostgreSQL。
- 流式支持：支持 SSE 流式响应，适配 OpenAI Responses、Chat Completions、Messages 等接口。
- 多视图查看：Body 支持 JSON 树形预览、Text 提取视图，SSE 响应支持 JSON / Text / SSE 切换。
- 性能指标：展示本项目耗时、上游接口耗时、差值、首字用时、TPS、Reasoning Tokens。
- 实时列表：通过 WebSocket 推送请求列表，已完成请求不会反复刷新右侧详情。
- new-api 联动：根据 `x-oneapi-request-id` 查询 `new-api-log`，展示请求人和完整消费日志，并支持 Request ID 反查。
- 流式请求转发：Request Body 边接收、边转发、边记录，避免大请求体完整缓存后才请求上游。
- 性能诊断：慢请求会输出带 Request ID 的分阶段性能日志，上游连接使用共享连接池。
- 桌面包：Release 提供 macOS `.app` 和 Windows `.exe`，双击即可打开内嵌控制台窗口。

## Docker Compose 部署

```bash
cd /opt/docker/ai-gateway
APP_COMMIT="$(git rev-parse --short HEAD)" docker compose up -d --build
```

默认控制台地址：

```text
http://127.0.0.1:20000/
```

默认数据目录：

```text
./data/ai_gateway.sqlite3
```

默认使用 SQLite。需要切换 PostgreSQL 时，先复制环境变量示例并填写实际账号密码：

```bash
cp .env.example .env
```

```env
DATABASE_TYPE=postgres
DATABASE_URL=jdbc:postgresql://user:password@192.168.1.26:8097/ai-gateway
NEW_API_LOG_DATABASE_URL=jdbc:postgresql://user:password@192.168.1.26:8097/new-api-log
```

`DATABASE_URL` 和 `NEW_API_LOG_DATABASE_URL` 同时兼容 `jdbc:postgresql://` 与 `postgresql://` 格式。未设置 PostgreSQL 配置时，原有 SQLite 部署方式不受影响。

Docker Compose 默认只监听本机地址：

```text
127.0.0.1:20000
```

## 使用方式

### 普通转发

把目标上游地址直接放到网关路径后面：

```text
http://127.0.0.1:20000/http://123.123.123.123:18088/v1
```

如果上游是 HTTPS：

```text
http://127.0.0.1:20000/https://api.example.com/v1
```

### 隔离空间

如果希望不同用户或不同用途只看到自己的请求，可以在路径前加一个空间名：

```text
http://127.0.0.1:20000/my-secret/https://api.example.com/v1
```

然后打开：

```text
http://127.0.0.1:20000/my-secret
```

这里只会显示 `my-secret` 空间下产生的日志。原始入口 `/` 和 `/https://...` 仍然保留，不受影响。

## 页面能力

控制台主要用于快速排查接口行为：

- 左侧按接口类型过滤：Chat Completions、Responses、Messages。
- 右侧展示请求摘要、状态码、耗时、上游耗时、差值、首字用时和 TPS。
- Header 默认折叠，点击后以键值对形式查看。
- JSON Body 使用可展开的树形预览。
- Responses API 的 Request Body 支持按原始顺序解析 `input` 数组，以时间线展示消息、工具调用、工具结果及未知类型。
- SSE Response 可以切换 JSON、Text、SSE 三种视图。
- `reasoning_tokens = 516` 时会在列表和详情中标记异常。
- Response Header 包含 `x-oneapi-request-id` 时，详情顶部可打开 new-api 日志弹窗，左侧会显示请求人。
- 左侧支持输入 Request ID 反查，同时返回 AI Gateway 与 `new-api-log` 两侧的数据。

## 本地桌面版

GitHub Release 提供本地桌面包。

### macOS

下载并解压：

```text
ai-gateway-macos.zip
```

双击 `AI Gateway.app`，选择端口后会在应用窗口中打开控制台。

macOS 数据位置：

```text
~/Library/Application Support/AI Gateway/ai_gateway.sqlite3
```

### Windows

下载并解压：

```text
ai-gateway-windows-x64.zip
```

双击 `ai-gateway.exe`，或在 PowerShell 中运行：

```powershell
.\ai-gateway.exe
```

Windows 数据位置：

```text
data\ai_gateway.sqlite3
```

## 配置项

可通过环境变量调整：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_TYPE` | `sqlite` | 日志存储类型：`sqlite` 或 `postgres` |
| `APP_COMMIT` | `unknown` | 页面和 `/health` 展示的部署 Commit，构建镜像时注入 |
| `DATABASE_PATH` | `/data/ai_gateway.sqlite3` | SQLite 数据库路径 |
| `DATABASE_URL` | - | AI Gateway PostgreSQL 连接串，设置后自动启用 PostgreSQL |
| `NEW_API_LOG_DATABASE_URL` | - | new-api 日志数据库 PostgreSQL 连接串 |
| `NEW_API_LOG_TABLES` | `logs` | new-api 日志查询表，多个表用逗号分隔 |
| `NEW_API_LOG_QUERY_TIMEOUT_SECONDS` | `3` | new-api 日志查询超时时间 |
| `PERFORMANCE_LOG_ENABLED` | `1` | 是否输出慢请求性能分析日志 |
| `PERFORMANCE_LOG_THRESHOLD_MS` | `100` | 差值或后台阶段超过该值时输出性能日志 |
| `HTTP_MAX_CONNECTIONS` | `500` | 上游 HTTP 连接池最大连接数 |
| `HTTP_MAX_KEEPALIVE_CONNECTIONS` | `200` | 上游 HTTP 连接池最大空闲连接数 |
| `HTTP_KEEPALIVE_EXPIRY_SECONDS` | `30` | 上游空闲连接保留时间 |
| `REQUEST_TIMEOUT_SECONDS` | `600` | 上游请求超时时间 |
| `MAX_CAPTURE_BYTES` | `0` | Body 最大记录字节数，`0` 表示完整记录 |

## GitHub Actions

- `Docker Image`：推送 `main` 或 `v*` tag 时构建多架构 Docker 镜像，并推送到 GitHub Container Registry。
- `Release Packages`：推送 `v*` tag 时构建 macOS `.app` zip 和 Windows `.exe` zip，并上传到 GitHub Release。

## 注意事项

- 网关会记录请求和响应正文，请避免在不可信环境中暴露控制台。
- 完整记录大型流式响应会增加内存和磁盘占用，可通过 `MAX_CAPTURE_BYTES` 限制记录大小。
- Hop-by-hop headers，例如 `connection`、`transfer-encoding`、`content-length`，不会直接转发。
- Docker Compose 默认只绑定 `127.0.0.1:20000`，如需公网访问建议配合反向代理或 Cloudflare Tunnel。

## 性能日志

慢请求会输出单行 JSON 日志，前缀为 `AI_GATEWAY_PERF`。拿到 `x-oneapi-request-id` 后可以这样检索：

```bash
docker logs ai-gateway 2>&1 | grep 'AI_GATEWAY_PERF' | grep '202607300540391603842458268d9d6pZ0mn1Ik'
```

`proxy` 事件包含请求上传、上游响应头、首字、响应流以及网关差值等时间；`background` 事件包含数据库写入、WebSocket 广播和 new-api-log 查询时间。

## 致谢与社区

感谢所有在使用、反馈和测试中提供帮助的朋友。

- [LINUX.DO](https://linux.do/)：一个活跃的中文技术社区，提供了很多关于 AI 工具、代理网关和开发调试的讨论与灵感。
