# AI Gateway Local Release

This package runs AI Gateway locally without Docker.

## Start

### macOS

Double-click `AI Gateway.app`, choose a port in the dialog, and the dashboard will open in your browser.

Captured logs are stored at:

```text
~/Library/Application Support/AI Gateway/ai_gateway.sqlite3
```

### Windows

Double-click `ai-gateway.exe`, or run:

```powershell
.\ai-gateway.exe
```

At startup, enter the port to bind. Press Enter to use `20000`.

The dashboard opens at:

```text
http://127.0.0.1:<port>/
```

Windows captured logs are stored next to the executable:

```text
data/ai_gateway.sqlite3
```

You can also pass the Windows port directly:

```powershell
.\ai-gateway.exe --port 20000
```
