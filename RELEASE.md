# AI Gateway Local Release

This package runs AI Gateway locally without Docker.

## Start

### macOS

Double-click `AI Gateway.app`. Choose a port in the startup screen, then the dashboard loads inside the same desktop window.

Captured logs are stored at:

```text
~/Library/Application Support/AI Gateway/ai_gateway.sqlite3
```

### Windows

Double-click `ai-gateway.exe`, or run:

```powershell
.\ai-gateway.exe
```

Choose a port in the startup screen, then the dashboard loads inside the same desktop window.

The dashboard opens at:

```text
http://127.0.0.1:<port>/
```

Windows captured logs are stored next to the executable:

```text
data/ai_gateway.sqlite3
```
