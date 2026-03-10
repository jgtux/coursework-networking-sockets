# coursework-networking-sockets
Uber's driver actions simulation using sockets. 

## Branches

| Branch | Description |
|---|---|
| `main` | Final version of the project |
| `development` | Active development branch |
| `test` | Testing branch |

## How to run

**Server:**
```bash
go run ./cmd/main.go -mode uber-server -addr 127.0.0.1:9000
```

**Client:**
```bash
go run ./cmd/main.go -mode uber-client -addr 127.0.0.1:9000
```

## Available commands

| Command | Description |
|---|---|
| `:accept` | Accept the latest ride request |
| `:cancel` | Cancel the current accepted ride |
| `:status` | Display current driver status |
| `:quit` | Disconnect from the system |
