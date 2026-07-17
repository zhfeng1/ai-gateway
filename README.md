# AI Gateway

AI Gateway 是一个用于调试 AI 接口的 Python HTTP 网关。它可以把本地请求转发到任意上游接口，同时记录完整的请求与响应信息，适合排查 OpenAI 兼容接口、Responses API、Chat Completions、Messages API，以及普通 HTTP 接口。

## 功能特性

- 通用转发：访问 `http://127.0.0.1:20000/http://upstream/v1` 会自动转发到上游地址。
- 路径隔离：支持 `http://127.0.0.1:20000/password/https://upstream/v1`，打开 `/password` 只查看该路径空间下的请求记录。
- 完整记录：Request Header、Response Header、Request Body、Response Body 都会写入 SQLite。
- 流式支持：支持 SSE 流式响应，适配 OpenAI Responses、Chat Completions、Messages 等接口。
- 多视图查看：Body 支持 JSON 树形预览、Text 提取视图，SSE 响应支持 JSON / Text / SSE 切换。
- 性能指标：展示本项目耗时、上游接口耗时、差值、首字用时、TPS、Reasoning Tokens。
- 实时列表：通过 WebSocket 推送请求列表，已完成请求不会反复刷新右侧详情。
- 桌面包：Release 提供 macOS `.app` 和 Windows `.exe`，双击即可打开内嵌控制台窗口。

## Docker Compose 部署

```bash
cd /opt/docker/ai-gateway
docker compose up -d --build
```

默认控制台地址：

```text
http://127.0.0.1:20000/
```

默认数据目录：

```text
./data/ai_gateway.sqlite3
```

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
- SSE Response 可以切换 JSON、Text、SSE 三种视图。
- `reasoning_tokens = 516` 时会在列表和详情中标记异常。

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
| `DATABASE_PATH` | `/data/ai_gateway.sqlite3` | SQLite 数据库路径 |
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

## 致谢与社区

感谢所有在使用、反馈和测试中提供帮助的朋友。

- [LINUX.DO](https://linux.do/)：一个活跃的中文技术社区，提供了很多关于 AI 工具、代理网关和开发调试的讨论与灵感。
