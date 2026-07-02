# AI Gateway Local Release

This package runs AI Gateway locally without Docker.

## Start

### macOS

```bash
./ai-gateway
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

Captured logs are stored next to the executable:

```text
data/ai_gateway.sqlite3
```

You can also pass the port directly:

```bash
./ai-gateway --port 20000
```

```powershell
.\ai-gateway.exe --port 20000
```
